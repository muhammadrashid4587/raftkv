"""Tests for raftkv SET with GET.

A ``set`` may carry Redis's SET GET option: one round trip both
swaps the value and reports what it replaced — the reply carries
``old_value``, the value the key held at the entry's exact position
in the log, or None when the key was absent. A losing conditional
set answers ``old_value`` None, like Redis answering nil when the
condition fails, and writes nothing at all. GET changes nothing
about the write itself: a winning GET set clears the deadline like
a plain set, or keeps it with KEEPTTL, and a retry with the same
session id returns the first attempt's old value instead of setting
twice.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def write(addr, key="k", value="v", **flags):
    msg = {"type": "client", "op": "set", "key": key, "value": value}
    msg.update(flags)
    return client_request(addr, msg)


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestSetGetStore(unittest.TestCase):
    def test_store_get_on_missing_key_answers_none(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "get": True}),
            {"type": "client_response", "status": "ok", "set": 1,
             "old_value": None})
        self.assertEqual(store.get("k"), "v")

    def test_store_get_returns_old_value(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "get": True}),
            {"type": "client_response", "status": "ok", "set": 1,
             "old_value": "first"})
        self.assertEqual(store.get("k"), "second")

    def test_store_plain_set_carries_no_old_value(self):
        # Without GET the reply shape is exactly what it was
        # before — no old_value key at all.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second"}),
            {"type": "client_response", "status": "ok", "set": 1})

    def test_store_losing_nx_with_get_answers_none(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "nx": True, "get": True}),
            {"type": "client_response", "status": "ok", "set": 0,
             "old_value": None})
        # The losing set wrote nothing: value and deadline both
        # untouched.
        self.assertEqual(store.get("k"), "first")
        self.assertEqual(store.expiries(), {"k": 15})

    def test_store_losing_xx_with_get_answers_none(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "xx": True, "get": True}),
            {"type": "client_response", "status": "ok", "set": 0,
             "old_value": None})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.keys(), ([], False))

    def test_store_get_clears_log_deadline_like_plain_set(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(store.expiries(), {"k": 15})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "get": True}, index=11)["old_value"], "v")
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.expiries(), {})

    def test_store_get_with_keepttl_keeps_log_deadline(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "keepttl": True, "get": True},
                        index=11),
            {"type": "client_response", "status": "ok", "set": 1,
             "old_value": "v"})
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.expiries(), {"k": 15})

    def test_store_get_clears_wall_deadline_like_plain_set(self):
        store = KVStore()
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": 1_800_000_000.0})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "get": True})["old_value"], "v")
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.wall_expiries(), {})

    def test_store_get_with_keepttl_keeps_wall_deadline(self):
        store = KVStore()
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": 1_800_000_000.0})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "keepttl": True, "get": True})["old_value"],
            "v")
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.wall_expiries(),
                         {"k": 1_800_000_000.0})

    def test_store_get_with_fresh_ttl_sets_deadline(self):
        # GET composes with a fresh TTL: the old value is
        # reported and the new deadline is recorded.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "ttl_entries": 5, "get": True},
                        index=10)["old_value"], "v")
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.expiries(), {"k": 15})


class TestSetGetCluster(unittest.TestCase):
    def test_set_get_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = write(addr, get=True)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertIsNone(resp.get("old_value"), resp)
            resp = write(addr, value="v2", get=True)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(resp.get("old_value"), "v", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v2"))
        finally:
            cluster.stop_all()

    def test_set_get_rejected_flag_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            # A GET that is not a bare boolean is malformed.
            resp = write(addr, get="yes")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("GET", resp.get("error", ""), resp)
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(read(addr).get("value"))
        finally:
            cluster.stop_all()

    def test_set_get_retry_returns_first_old_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            msg = {"type": "client", "op": "set", "key": "k",
                   "value": "v2", "get": True,
                   "client_id": "c1", "request_id": "r1"}
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(resp.get("old_value"), "v", resp)
            # The retry returns the first attempt's old value —
            # "v", not "v2" — and appends no second entry.
            resp = client_request(addr, dict(msg, value="v3"))
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(resp.get("old_value"), "v", resp)
            self.assertEqual(read(addr).get("value"), "v2")
            self.assertEqual(count_entries(leader, "r1"), 1)
            # And a losing set's retry returns the same None,
            # not a fresh attempt that might win after a delete.
            msg = {"type": "client", "op": "set", "key": "k",
                   "value": "other", "nx": True, "get": True,
                   "client_id": "c1", "request_id": "r2"}
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("set"), 0, resp)
            self.assertIsNone(resp.get("old_value"), resp)
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("set"), 0, resp)
            self.assertIsNone(resp.get("old_value"), resp)
            self.assertEqual(count_entries(leader, "r2"), 1)
            self.assertEqual(read(addr).get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_set_get_fires_watch_event_only_on_win(self):
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
            self.assertEqual(write(addr, get=True).get("set"), 1)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "k", event)
                self.assertEqual(event["value"], "v", event)
                self.assertEqual(event["op"], "set", event)
            # A losing NX with GET fires nothing.
            self.assertEqual(write(addr, value="other", nx=True,
                                   get=True).get("set"), 0)
            time.sleep(0.3)
            self.assertEqual(len(events), 2, events)
        finally:
            cluster.stop_all()

    def test_session_set_get(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("k", "v", get=True)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("set"), 1, resp)
                self.assertIsNone(resp.get("old_value"), resp)
                resp = session.set("k", "v2", get=True)
                self.assertEqual(resp.get("old_value"), "v", resp)
                self.assertEqual(session.get("k").get("value"), "v2")
                # A losing NX through the same session still
                # advances the watermark: it committed an entry.
                before = session.last_write_index
                resp = session.set("k", "other", nx=True, get=True)
                self.assertEqual(resp.get("set"), 0, resp)
                self.assertIsNone(resp.get("old_value"), resp)
                self.assertGreater(session.last_write_index, before)
                self.assertEqual(session.get("k").get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_pipeline_set_get(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "a", "value": "1",
                     "get": True},
                    {"op": "set", "key": "a", "value": "2",
                     "get": True},
                    {"op": "set", "key": "a", "value": "3",
                     "nx": True, "get": True},
                    {"op": "set", "key": "b", "value": "x",
                     "get": "yes"},
                ])
            self.assertEqual(len(replies), 4, replies)
            self.assertEqual(replies[0].get("set"), 1, replies)
            self.assertIsNone(replies[0].get("old_value"), replies)
            self.assertEqual(replies[1].get("set"), 1, replies)
            self.assertEqual(replies[1].get("old_value"), "1", replies)
            self.assertEqual(replies[2].get("set"), 0, replies)
            self.assertIsNone(replies[2].get("old_value"), replies)
            self.assertEqual(replies[3].get("status"), "error",
                             replies)
            self.assertEqual(read(addr, key="a").get("value"), "2")
        finally:
            cluster.stop_all()

    def test_set_get_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # --get on a missing key prints (nil).
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v",
                                  "--get"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            # --get on an existing key prints the old value.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v2",
                                  "--get"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "v")
            # A losing NX with --get prints (nil) and writes
            # nothing.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "other",
                                  "--nx", "--get"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v2")
            # A winning XX with --get prints the old value.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v3",
                                  "--xx", "--get"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "v2")
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v3")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
