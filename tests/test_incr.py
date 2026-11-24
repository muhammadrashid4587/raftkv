"""Tests for raftkv atomic counters (incr / incrby).

An incr is a write replicated through the Raft log like any other:
{"op": "incr", "key": k, "by": n} adds n (default 1) to the key's
integer value and returns the new value as a string. A missing key
counts as 0, so the first incr creates it; the outcome is decided
when the entry applies — at its exact position in the log — so every
node computes the same result, a non-integer value fails distinctly
from a 64-bit overflow, and a retry with the same session id returns
the first attempt's result instead of incrementing twice.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def incr(addr, key="counter", by=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "incr", "key": key}
    if by is not None:
        msg["by"] = by
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="counter", value="41"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def read(addr, key="counter"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestIncrStore(unittest.TestCase):
    def test_incr_missing_key_creates_at_by(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "incr", "key": "c"}),
            {"type": "client_response", "status": "ok", "value": "1"})
        self.assertEqual(store.get("c"), "1")
        self.assertEqual(
            store.apply({"op": "incr", "key": "d", "by": 5}),
            {"type": "client_response", "status": "ok", "value": "5"})

    def test_incr_adds_and_subtracts(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "41"})
        self.assertEqual(
            store.apply({"op": "incr", "key": "c", "by": 1})["value"],
            "42")
        self.assertEqual(
            store.apply({"op": "incr", "key": "c", "by": -2})["value"],
            "40")
        self.assertEqual(
            store.apply({"op": "incr", "key": "c", "by": 0})["value"],
            "40")
        self.assertEqual(store.get("c"), "40")

    def test_incr_rejects_non_integer_value(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "not-a-number"})
        resp = store.apply({"op": "incr", "key": "c"})
        self.assertEqual(resp["status"], "incr_failed", resp)
        self.assertEqual(resp["reason"], "not_an_integer", resp)
        self.assertEqual(resp["current"], "not-a-number", resp)
        self.assertEqual(store.get("c"), "not-a-number")

    def test_incr_rejects_loose_integer_spellings(self):
        # int() would accept these; Redis does not — an optional sign
        # and digits only, no whitespace, no underscores, no decimals.
        store = KVStore()
        for bad in ("1_0", " 1", "1 ", "1.0", "+ 1", "", "+", "-"):
            store.apply({"op": "set", "key": "c", "value": bad})
            resp = store.apply({"op": "incr", "key": "c"})
            self.assertEqual(resp["status"], "incr_failed", resp)
            self.assertEqual(resp["reason"], "not_an_integer", resp)
            self.assertEqual(store.get("c"), bad)

    def test_incr_rejects_out_of_range_value(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c",
                     "value": "99999999999999999999999"})
        resp = store.apply({"op": "incr", "key": "c"})
        self.assertEqual(resp["status"], "incr_failed", resp)
        self.assertEqual(resp["reason"], "not_an_integer", resp)

    def test_incr_rejects_overflow(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c",
                     "value": str(2 ** 63 - 1)})
        resp = store.apply({"op": "incr", "key": "c", "by": 1})
        self.assertEqual(resp["status"], "incr_failed", resp)
        self.assertEqual(resp["reason"], "overflow", resp)
        self.assertEqual(resp["current"], str(2 ** 63 - 1), resp)
        self.assertEqual(store.get("c"), str(2 ** 63 - 1))
        store.apply({"op": "set", "key": "c", "value": str(-(2 ** 63))})
        resp = store.apply({"op": "incr", "key": "c", "by": -1})
        self.assertEqual(resp["reason"], "overflow", resp)

    def test_incr_keeps_ttl_deadline(self):
        # A successful incr is an update, not a fresh set: the
        # deadline survives it, like a CAS.
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "41",
                     "ttl_entries": 10}, index=5)
        self.assertEqual(store._expiry["c"], 15)
        store.apply({"op": "incr", "key": "c", "by": 1}, index=6)
        self.assertEqual(store.get("c"), "42")
        self.assertEqual(store._expiry["c"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["c"])
        self.assertIsNone(store.get("c"))

    def test_incr_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "incr", "key": "a"})
        store.apply({"op": "incr", "key": "c", "by": 3})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))


class TestIncrCluster(unittest.TestCase):
    def test_incr_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = incr(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "1", resp)
            resp = incr(addr, by=41)
            self.assertEqual(resp.get("value"), "42", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "counter",
                                 "42"))
            self.assertEqual(read(addr).get("value"), "42")
        finally:
            cluster.stop_all()

    def test_incr_on_non_integer_fails_and_writes_nothing(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="abc").get("status"),
                             "ok")
            resp = incr(addr)
            self.assertEqual(resp.get("status"), "incr_failed", resp)
            self.assertEqual(resp.get("reason"), "not_an_integer", resp)
            self.assertEqual(resp.get("current"), "abc", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "counter",
                                 "abc"))
        finally:
            cluster.stop_all()

    def test_incr_rejects_non_integer_by(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(
                addr, {"type": "client", "op": "incr",
                       "key": "counter", "by": "two"})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "incr",
                       "key": "counter", "by": True})
            self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_incr_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = incr(cluster.addrs[follower])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "1", resp)
        finally:
            cluster.stop_all()

    def test_incr_retry_appends_once_and_returns_same_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = incr(addr, by=5, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("value"), "5", resp)
            resp = incr(addr, by=5, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "5", resp)
            self.assertEqual(read(addr).get("value"), "5")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_failed_incr_retry_returns_same_failure(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="abc").get("status"),
                             "ok")
            resp = incr(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "incr_failed", resp)
            resp = incr(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "incr_failed", resp)
            self.assertEqual(resp.get("reason"), "not_an_integer", resp)
            # The failed attempt committed one entry; the retry none.
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(read(addr).get("value"), "abc")
        finally:
            cluster.stop_all()

    def test_incr_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(incr(addr, by=10).get("value"), "10")
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(incr(addr, by=5).get("value"), "15")
            self.assertEqual(read(addr).get("value"), "15")
        finally:
            cluster.stop_all()

    def test_incr_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("counter", events.append)
            follower.watch.watch("counter", events.append)
            self.assertEqual(incr(addr, by=7).get("value"), "7")
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "counter", event)
                self.assertEqual(event["value"], "7", event)
                self.assertEqual(event["op"], "set", event)
            # The set to "abc" fires one event per node too.
            self.assertEqual(write(addr, value="abc").get("status"),
                             "ok")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            # A failed incr fires nothing.
            self.assertEqual(incr(addr).get("status"), "incr_failed")
            time.sleep(0.3)
            self.assertEqual(len(events), 4, events)
        finally:
            cluster.stop_all()

    def test_incr_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(incr(addr, by=3).get("value"), "3")
            # Enough writes to compact the log past the counter.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(incr(addr, by=4).get("value"), "7")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr).get("value"), "7")
            self.assertEqual(incr(addr, by=1).get("value"), "8")
        finally:
            cluster.stop_all()

    def test_session_incr_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.incr("counter", by=9)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), "9", resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("counter").get("value"),
                                 "9")
        finally:
            cluster.stop_all()

    def test_pipeline_incr_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "incr", "key": "counter"},
                    {"op": "incr", "key": "counter", "by": 4},
                    {"op": "set", "key": "other", "value": "x"},
                    {"op": "incr", "key": "counter", "by": "bad"},
                    {"op": "incr", "key": "counter", "by": -1},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("value"), "1", replies)
            self.assertEqual(replies[1].get("value"), "5", replies)
            self.assertEqual(replies[2].get("status"), "ok", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("value"), "4", replies)
            self.assertEqual(read(addr).get("value"), "4")
        finally:
            cluster.stop_all()

    def test_incr_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "incr", "counter"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "incr", "counter",
                                  "41"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "42")
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "42")
            # A failed incr exits 1 and names the failure.
            self.assertEqual(write(cluster.addrs[leader.id],
                                   value="abc").get("status"), "ok")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "incr", "counter"])
            self.assertEqual(rc, 1)
            self.assertIn("not an integer", err.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
