"""Tests for raftkv set-if-not-exists (setnx).

A setnx is a write replicated through the Raft log like any other:
{"op": "setnx", "key": k, "value": v} sets the key only when it is
absent and answers 1 when it set it, 0 when the key already existed
(and the existing value is left untouched). The outcome is decided
when the entry applies — at its exact position in the log — so two
concurrent setnx on the same key race in log order and exactly one
wins, and a retry with the same session id returns the first
attempt's result instead of setting twice.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def setnx(addr, key="lock", value="holder", client_id=None,
          request_id=None):
    msg = {"type": "client", "op": "setnx", "key": key, "value": value}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="lock", value="taken"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def read(addr, key="lock"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestSetnxStore(unittest.TestCase):
    def test_setnx_sets_absent_key(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "setnx", "key": "k", "value": "v"}),
            {"type": "client_response", "status": "ok", "setnx": 1})
        self.assertEqual(store.get("k"), "v")

    def test_setnx_leaves_existing_key_untouched(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "setnx", "key": "k", "value": "second"}),
            {"type": "client_response", "status": "ok", "setnx": 0})
        self.assertEqual(store.get("k"), "first")

    def test_setnx_after_delete_sets_again(self):
        # A key that was deleted is absent again, so a later setnx
        # wins — the "exists" check is against live state, not
        # history.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        store.apply({"op": "del", "key": "k"})
        self.assertEqual(
            store.apply({"op": "setnx", "key": "k", "value": "second"})
            ["setnx"], 1)
        self.assertEqual(store.get("k"), "second")

    def test_setnx_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "setnx", "key": "a", "value": "y"})
        # A setnx on an existing key writes nothing, so the index
        # is untouched there too.
        store.apply({"op": "setnx", "key": "b", "value": "z"})
        store.apply({"op": "setnx", "key": "c", "value": "w"})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))
        self.assertEqual(store.get("b"), "x")


class TestSetnxCluster(unittest.TestCase):
    def test_setnx_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = setnx(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("setnx"), 1, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "lock",
                                 "holder"))
            self.assertEqual(read(addr).get("value"), "holder")
        finally:
            cluster.stop_all()

    def test_setnx_on_existing_key_is_noop_everywhere(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = setnx(addr, value="other")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("setnx"), 0, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "lock",
                                 "taken"))
        finally:
            cluster.stop_all()

    def test_setnx_requires_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(
                addr, {"type": "client", "op": "setnx", "key": "lock"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("value", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_setnx_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = setnx(cluster.addrs[follower])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("setnx"), 1, resp)
        finally:
            cluster.stop_all()

    def test_setnx_retry_appends_once_and_returns_same_result(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = setnx(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("setnx"), 1, resp)
            resp = setnx(addr, value="someone-else",
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("setnx"), 1, resp)
            self.assertEqual(read(addr).get("value"), "holder")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_setnx_noop_retry_returns_same_zero(self):
        # A retry of a setnx that lost (key existed) returns the
        # same 0, not a fresh attempt that might win after a
        # delete in between.
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = setnx(addr, value="other",
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("setnx"), 0, resp)
            resp = setnx(addr, value="other",
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("setnx"), 0, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(read(addr).get("value"), "taken")
        finally:
            cluster.stop_all()

    def test_setnx_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(setnx(addr).get("setnx"), 1)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The key exists on the new leader too, so this loses.
            self.assertEqual(setnx(addr, value="other").get("setnx"),
                             0)
            self.assertEqual(read(addr).get("value"), "holder")
        finally:
            cluster.stop_all()

    def test_setnx_fires_watch_event_only_on_success(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("lock", events.append)
            follower.watch.watch("lock", events.append)
            self.assertEqual(setnx(addr).get("setnx"), 1)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "lock", event)
                self.assertEqual(event["value"], "holder", event)
                self.assertEqual(event["op"], "set", event)
            # A losing setnx fires nothing.
            self.assertEqual(setnx(addr, value="other").get("setnx"),
                             0)
            time.sleep(0.3)
            self.assertEqual(len(events), 2, events)
        finally:
            cluster.stop_all()

    def test_setnx_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(setnx(addr).get("setnx"), 1)
            # Enough writes to compact the log past the setnx.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(setnx(addr, value="other").get("setnx"),
                             0)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr).get("value"), "holder")
            self.assertEqual(setnx(addr, value="other").get("setnx"),
                             0)
        finally:
            cluster.stop_all()

    def test_session_setnx_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.setnx("lock", "holder")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("setnx"), 1, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("lock").get("value"),
                                 "holder")
                # A second setnx through the same session loses
                # and still advances the watermark.
                resp = session.setnx("lock", "other")
                self.assertEqual(resp.get("setnx"), 0, resp)
                self.assertEqual(session.get("lock").get("value"),
                                 "holder")
        finally:
            cluster.stop_all()

    def test_pipeline_setnx_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "setnx", "key": "a", "value": "1"},
                    {"op": "setnx", "key": "a", "value": "2"},
                    {"op": "set", "key": "b", "value": "x"},
                    {"op": "setnx", "key": "b"},
                    {"op": "setnx", "key": "c", "value": "3"},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("setnx"), 1, replies)
            self.assertEqual(replies[1].get("setnx"), 0, replies)
            self.assertEqual(replies[2].get("status"), "ok", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("setnx"), 1, replies)
            self.assertEqual(read(addr, key="a").get("value"), "1")
            self.assertEqual(read(addr, key="c").get("value"), "3")
        finally:
            cluster.stop_all()

    def test_setnx_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setnx", "lock",
                                  "holder"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setnx", "lock",
                                  "other"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "holder")
            # A missing value is a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "setnx", "lock"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
