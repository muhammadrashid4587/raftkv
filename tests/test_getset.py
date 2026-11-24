"""Tests for raftkv getset (atomically set and return the old value).

A getset is a write replicated through the Raft log like any other:
{"op": "getset", "key": k, "value": v} sets the key to ``v`` and
answers with the value the key held before — ``old_value`` None when
the key did not exist, which the getset creates. The old value is
decided when the entry applies — at its exact position in the log —
so a retry with the same session id returns the first attempt's old
value instead of setting twice, and a second getset sees the first
one's value as the old value, which is the atomic swap Redis
promises. A getset always writes, even on a missing key, and it
keeps the key's TTL deadline, like a CAS.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def getset(addr, key="k", value="new", client_id=None, request_id=None):
    msg = {"type": "client", "op": "getset", "key": key, "value": value}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="k", value="old"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestGetsetStore(unittest.TestCase):
    def test_getset_on_missing_key_returns_none_and_sets(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "getset", "key": "k", "value": "v"}),
            {"type": "client_response", "status": "ok",
             "old_value": None})
        self.assertEqual(store.get("k"), "v")

    def test_getset_returns_old_value_and_replaces(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "getset", "key": "k", "value": "second"}),
            {"type": "client_response", "status": "ok",
             "old_value": "first"})
        self.assertEqual(store.get("k"), "second")

    def test_getset_after_delete_returns_none(self):
        # A key that was deleted holds nothing, so a later getset
        # answers None — the old value is live state, not history.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        store.apply({"op": "del", "key": "k"})
        self.assertEqual(
            store.apply({"op": "getset", "key": "k", "value": "second"})
            ["old_value"], None)
        self.assertEqual(store.get("k"), "second")

    def test_getset_keeps_ttl_deadline(self):
        # A getset is an update, not a fresh set: the deadline
        # survives it, like a CAS or an incr.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "old",
                     "ttl_entries": 10}, index=5)
        self.assertEqual(store._expiry["k"], 15)
        self.assertEqual(
            store.apply({"op": "getset", "key": "k", "value": "new"},
                        index=6),
            {"type": "client_response", "status": "ok",
             "old_value": "old"})
        self.assertEqual(store.get("k"), "new")
        self.assertEqual(store._expiry["k"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["k"])
        self.assertIsNone(store.get("k"))

    def test_getset_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "getset", "key": "a", "value": "y"})
        store.apply({"op": "getset", "key": "b", "value": "z"})
        store.apply({"op": "getset", "key": "c", "value": "w"})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))
        self.assertEqual(store.get("b"), "z")


class TestGetsetCluster(unittest.TestCase):
    def test_getset_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = getset(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("old_value"), resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "new"))
            self.assertEqual(read(addr).get("value"), "new")
        finally:
            cluster.stop_all()

    def test_getset_returns_old_value_everywhere(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = getset(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("old_value"), "old", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "new"))
        finally:
            cluster.stop_all()

    def test_getset_requires_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(
                addr, {"type": "client", "op": "getset", "key": "k"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("value", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_getset_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = getset(cluster.addrs[follower])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("old_value"), resp)
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "new")
        finally:
            cluster.stop_all()

    def test_getset_retry_appends_once_and_returns_first_old_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = getset(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("old_value"), "old", resp)
            # A second getset with a different id sees the first
            # one's value as the old value — the atomic swap.
            resp = getset(addr, value="third")
            self.assertEqual(resp.get("old_value"), "new", resp)
            # But the retry of the first attempt still answers the
            # first attempt's old value, not the current one, and
            # appends no second entry.
            resp = getset(addr, value="someone-else",
                          client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("old_value"), "old", resp)
            self.assertEqual(read(addr).get("value"), "third")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_getset_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertIsNone(getset(addr).get("old_value"))
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The key holds the first getset's value on the new
            # leader too, so this one answers it as the old value.
            self.assertEqual(getset(addr, value="second")
                             .get("old_value"), "new")
            self.assertEqual(read(addr).get("value"), "second")
        finally:
            cluster.stop_all()

    def test_getset_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("k", events.append)
            follower.watch.watch("k", events.append)
            self.assertIsNone(getset(addr).get("old_value"))
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "k", event)
                # The event carries the new value, not the old one.
                self.assertEqual(event["value"], "new", event)
                self.assertEqual(event["op"], "set", event)
            # A getset on an existing key fires again, with the
            # new value.
            self.assertEqual(getset(addr, value="second")
                             .get("old_value"), "new")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            for event in events[2:]:
                self.assertEqual(event["value"], "second", event)
        finally:
            cluster.stop_all()

    def test_getset_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertIsNone(getset(addr).get("old_value"))
            # Enough writes to compact the log past the getset.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(getset(addr, value="second")
                             .get("old_value"), "new")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr).get("value"), "second")
            self.assertEqual(getset(addr, value="third")
                             .get("old_value"), "second")
        finally:
            cluster.stop_all()

    def test_session_getset_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.getset("k", "new")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertIsNone(resp.get("old_value"), resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("k").get("value"), "new")
                # A second getset through the same session answers
                # the first one's value and still advances the
                # watermark.
                resp = session.getset("k", "second")
                self.assertEqual(resp.get("old_value"), "new", resp)
                self.assertEqual(session.get("k").get("value"),
                                 "second")
        finally:
            cluster.stop_all()

    def test_pipeline_getset_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "getset", "key": "a", "value": "1"},
                    {"op": "getset", "key": "a", "value": "2"},
                    {"op": "set", "key": "b", "value": "x"},
                    {"op": "getset", "key": "b"},
                    {"op": "getset", "key": "c", "value": "3"},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertIsNone(replies[0].get("old_value"), replies)
            self.assertEqual(replies[1].get("old_value"), "1", replies)
            self.assertEqual(replies[2].get("status"), "ok", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertIsNone(replies[4].get("old_value"), replies)
            self.assertEqual(read(addr, key="a").get("value"), "2")
            self.assertEqual(read(addr, key="c").get("value"), "3")
        finally:
            cluster.stop_all()

    def test_getset_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # A missing key prints (nil) and is created.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getset", "k", "new"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getset", "k",
                                  "second"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "new")
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "second")
            # A missing value is a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "getset", "k"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
