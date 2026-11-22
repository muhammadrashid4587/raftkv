"""Tests for raftkv linearizable compare-and-swap (CAS).

A CAS is a conditional write replicated through the Raft log like any
other write: {"op": "cas", "key": k, "expected": e, "value": v} sets k
to v only if k's current value equals e, and the outcome is decided
when the entry applies — at its exact position in the log — so every
node computes the same result and a read after a committed CAS sees
the new value (or the old one, if it failed). A missing key fails
distinctly from a value mismatch. With a (client_id, request_id)
session id a retry returns the first attempt's exact result instead
of appending a second entry, exactly like set/del.
"""

import contextlib
import io
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import main as client_main


def cas(addr, key="k", expected="v1", value="v2", client_id=None,
        request_id=None):
    msg = {"type": "client", "op": "cas", "key": key,
           "expected": expected, "value": value}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="k", value="v1"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestCompareAndSwap(unittest.TestCase):
    def test_cas_succeeds_on_match(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = cas(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("cas"), "success", resp)
            # The new value replicated to every node.
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v2"))
        finally:
            cluster.stop_all()

    def test_cas_fails_on_mismatch_value_unchanged(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = cas(addr, expected="not-v1", value="v2")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            self.assertEqual(resp.get("reason"), "mismatch", resp)
            self.assertEqual(resp.get("current"), "v1", resp)
            # The failed CAS wrote nothing, on any node.
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v1"))
            self.assertEqual(read(addr, "k").get("value"), "v1")
        finally:
            cluster.stop_all()

    def test_cas_fails_on_missing_key(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = cas(addr, key="absent")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            self.assertEqual(resp.get("reason"), "missing_key", resp)
            self.assertIsNone(resp.get("current"), resp)
            # Distinct from a mismatch, and nothing was created.
            self.assertNotEqual(resp.get("reason"), "mismatch")
            for node_id in cluster.live:
                self.assertIsNone(cluster.nodes[node_id].sm.get("absent"))
        finally:
            cluster.stop_all()

    def test_cas_requires_expected_and_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "cas",
                                         "key": "k", "value": "v2"})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(addr, {"type": "client", "op": "cas",
                                         "key": "k", "expected": "v1"})
            self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_cas_survives_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            old_leader_id = leader.id
            cluster.stop_node(old_leader_id)
            new_leader = cluster.wait_for_stable_leader()
            self.assertNotEqual(new_leader.id, old_leader_id)
            # The CAS runs against the new leader's replicated log,
            # and the old leader rejoins with the same value.
            addr = cluster.addrs[new_leader.id]
            resp = cas(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            cluster.restart_node(old_leader_id)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v2"))
        finally:
            cluster.stop_all()

    def test_cas_retry_with_same_session_id_returns_same_result(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            # A successful CAS retried: same ok, no second entry — and
            # a second entry would have failed anyway (the value
            # already changed), so the log count is the real check.
            resp = cas(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = cas(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            # A failed CAS retried: the same failure comes back, not a
            # fresh attempt — and no second entry is appended either.
            resp = cas(addr, expected="wrong", value="v3",
                       client_id="c1", request_id="r2")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            self.assertEqual(resp.get("reason"), "mismatch", resp)
            resp = cas(addr, expected="wrong", value="v3",
                       client_id="c1", request_id="r2")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            self.assertEqual(resp.get("reason"), "mismatch", resp)
            self.assertEqual(resp.get("current"), "v2", resp)
            self.assertEqual(count_entries(leader, "r2"), 1)
            self.assertTrue(wait_for_key(leader, "k", "v2"))
        finally:
            cluster.stop_all()

    def test_cas_failed_result_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = cas(addr, expected="wrong", value="v3",
                       client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            # Enough further writes to compact the log past the
            # threshold, so the r1 entry leaves the log and its result
            # lives only in the snapshot's dedup table.
            for i in range(10):
                resp = write(addr, key=f"k{i}", value="x")
                self.assertEqual(resp.get("status"), "ok", resp)
            deadline = time.monotonic() + 5.0
            while (time.monotonic() < deadline
                   and leader.snapshot_index == 0):
                time.sleep(0.05)
            self.assertGreater(leader.snapshot_index, 0)
            self.assertIn(("c1", "r1"), leader._dedup)
            # Restart the leader: the snapshot reloads the dedup table
            # with the CAS result, so the retry returns the same
            # failure instead of appending a fresh attempt.
            leader_id = leader.id
            cluster.stop_node(leader_id)
            cluster.restart_node(leader_id)
            new_leader = cluster.wait_for_stable_leader()
            addr = cluster.addrs[new_leader.id]
            resp = cas(addr, expected="wrong", value="v3",
                       client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            self.assertEqual(resp.get("reason"), "mismatch", resp)
            self.assertEqual(resp.get("current"), "v1", resp)
            self.assertIn(("c1", "r1"), new_leader._dedup)
            self.assertTrue(wait_for_key(new_leader, "k", "v1"))
        finally:
            cluster.stop_all()

    def test_cas_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                self.assertEqual(
                    client_main(["--node", node, "set", "k", "v1"]), 0)
                self.assertEqual(
                    client_main(["--node", node, "cas", "k", "v1", "v2"]), 0)
            self.assertEqual(out.getvalue().strip().splitlines()[-1], "OK")
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v2")
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "cas", "k", "v1", "v3"])
            self.assertEqual(rc, 1)
            self.assertIn("cas failed", err.getvalue())
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v2")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
