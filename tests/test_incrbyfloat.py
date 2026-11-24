"""Tests for raftkv float counters (incrbyfloat).

An incrbyfloat is a write replicated through the Raft log like any
other: {"op": "incrbyfloat", "key": k, "by": x} adds x (default 1.0)
to the key's float value and returns the new value as a string. A
missing key counts as 0, so the first incrbyfloat creates it; the
outcome is decided when the entry applies — at its exact position
in the log — so every node computes the same result, a non-float
value fails distinctly from a result that would be NaN or
Infinity, and a retry with the same session id returns the first
attempt's result instead of adding twice.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def incrbyfloat(addr, key="counter", by=None, client_id=None,
                request_id=None):
    msg = {"type": "client", "op": "incrbyfloat", "key": key}
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


class TestIncrByFloatStore(unittest.TestCase):
    def test_incrbyfloat_missing_key_creates_at_by(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c"}),
            {"type": "client_response", "status": "ok", "value": "1"})
        self.assertEqual(store.get("c"), "1")
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "d", "by": 2.5}),
            {"type": "client_response", "status": "ok", "value": "2.5"})

    def test_incrbyfloat_adds_and_subtracts(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "10.5"})
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c",
                         "by": 0.25})["value"],
            "10.75")
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c",
                         "by": -1.5})["value"],
            "9.25")
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c",
                         "by": 0})["value"],
            "9.25")
        self.assertEqual(store.get("c"), "9.25")

    def test_incrbyfloat_on_integer_value(self):
        # An integer string is a valid float too, like Redis: "41"
        # plus 0.5 is "41.5" — and the result is no longer an
        # integer string, so a later incr on it fails.
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "41"})
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c",
                         "by": 0.5})["value"],
            "41.5")
        resp = store.apply({"op": "incr", "key": "c"})
        self.assertEqual(resp["status"], "incr_failed", resp)
        self.assertEqual(resp["reason"], "not_an_integer", resp)

    def test_incrbyfloat_formats_integral_results_without_dot_zero(self):
        # Redis prints an integral INCRBYFLOAT result as "2", not
        # "2.0" — and a float sum keeps its full precision.
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c",
                         "by": 2.0})["value"],
            "2")
        store.apply({"op": "set", "key": "d", "value": "0.1"})
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "d",
                         "by": 0.2})["value"],
            "0.30000000000000004")

    def test_incrbyfloat_rejects_non_float_value(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "not-a-number"})
        resp = store.apply({"op": "incrbyfloat", "key": "c"})
        self.assertEqual(resp["status"], "incrbyfloat_failed", resp)
        self.assertEqual(resp["reason"], "not_a_float", resp)
        self.assertEqual(resp["current"], "not-a-number", resp)
        self.assertEqual(store.get("c"), "not-a-number")

    def test_incrbyfloat_rejects_loose_float_spellings(self):
        # float() would accept these; Redis does not — an optional
        # sign, digits with an optional fraction, an optional
        # exponent, no whitespace, no underscores, no inf or nan.
        store = KVStore()
        for bad in ("1_0", " 1", "1 ", "inf", "-inf", "nan", "Infinity",
                    "1e999", "", "+", "-", ".", "1e", "0x10"):
            store.apply({"op": "set", "key": "c", "value": bad})
            resp = store.apply({"op": "incrbyfloat", "key": "c"})
            self.assertEqual(resp["status"], "incrbyfloat_failed",
                             (bad, resp))
            self.assertEqual(resp["reason"], "not_a_float", (bad, resp))
            self.assertEqual(store.get("c"), bad)

    def test_incrbyfloat_accepts_exponent_spellings(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "1e3"})
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "c",
                         "by": 0.5})["value"],
            "1000.5")
        store.apply({"op": "set", "key": "d", "value": ".5"})
        self.assertEqual(
            store.apply({"op": "incrbyfloat", "key": "d",
                         "by": 0.5})["value"],
            "1")

    def test_incrbyfloat_rejects_non_finite_result(self):
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "1e308"})
        resp = store.apply({"op": "incrbyfloat", "key": "c",
                            "by": 1e308})
        self.assertEqual(resp["status"], "incrbyfloat_failed", resp)
        self.assertEqual(resp["reason"], "overflow", resp)
        self.assertEqual(resp["current"], "1e308", resp)
        self.assertEqual(store.get("c"), "1e308")

    def test_incrbyfloat_keeps_ttl_deadline(self):
        # A successful incrbyfloat is an update, not a fresh set:
        # the deadline survives it, like an incr or a CAS.
        store = KVStore()
        store.apply({"op": "set", "key": "c", "value": "41",
                     "ttl_entries": 10}, index=5)
        self.assertEqual(store._expiry["c"], 15)
        store.apply({"op": "incrbyfloat", "key": "c", "by": 0.5},
                    index=6)
        self.assertEqual(store.get("c"), "41.5")
        self.assertEqual(store._expiry["c"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["c"])
        self.assertIsNone(store.get("c"))

    def test_incrbyfloat_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "incrbyfloat", "key": "a"})
        store.apply({"op": "incrbyfloat", "key": "c", "by": 3.5})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))


class TestIncrByFloatCluster(unittest.TestCase):
    def test_incrbyfloat_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = incrbyfloat(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "1", resp)
            resp = incrbyfloat(addr, by=9.5)
            self.assertEqual(resp.get("value"), "10.5", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "counter",
                                 "10.5"))
            self.assertEqual(read(addr).get("value"), "10.5")
        finally:
            cluster.stop_all()

    def test_incrbyfloat_on_non_float_fails_and_writes_nothing(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="abc").get("status"),
                             "ok")
            resp = incrbyfloat(addr)
            self.assertEqual(resp.get("status"), "incrbyfloat_failed",
                             resp)
            self.assertEqual(resp.get("reason"), "not_a_float", resp)
            self.assertEqual(resp.get("current"), "abc", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "counter",
                                 "abc"))
        finally:
            cluster.stop_all()

    def test_incrbyfloat_rejects_bad_by(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for bad in ("two", True, float("inf")):
                resp = client_request(
                    addr, {"type": "client", "op": "incrbyfloat",
                           "key": "counter", "by": bad})
                self.assertEqual(resp.get("status"), "error",
                                 (bad, resp))
        finally:
            cluster.stop_all()

    def test_incrbyfloat_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = incrbyfloat(cluster.addrs[follower], by=2.5)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "2.5", resp)
        finally:
            cluster.stop_all()

    def test_incrbyfloat_retry_appends_once_and_returns_same_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = incrbyfloat(addr, by=2.5, client_id="c1",
                               request_id="r1")
            self.assertEqual(resp.get("value"), "2.5", resp)
            resp = incrbyfloat(addr, by=2.5, client_id="c1",
                               request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "2.5", resp)
            self.assertEqual(read(addr).get("value"), "2.5")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_failed_incrbyfloat_retry_returns_same_failure(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="abc").get("status"),
                             "ok")
            resp = incrbyfloat(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "incrbyfloat_failed",
                             resp)
            resp = incrbyfloat(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "incrbyfloat_failed",
                             resp)
            self.assertEqual(resp.get("reason"), "not_a_float", resp)
            # The failed attempt committed one entry; the retry none.
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(read(addr).get("value"), "abc")
        finally:
            cluster.stop_all()

    def test_incrbyfloat_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(incrbyfloat(addr, by=10.5).get("value"),
                             "10.5")
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(incrbyfloat(addr, by=0.5).get("value"),
                             "11")
            self.assertEqual(read(addr).get("value"), "11")
        finally:
            cluster.stop_all()

    def test_incrbyfloat_fires_watch_event_with_new_value(self):
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
            self.assertEqual(incrbyfloat(addr, by=2.5).get("value"),
                             "2.5")
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "counter", event)
                self.assertEqual(event["value"], "2.5", event)
                self.assertEqual(event["op"], "set", event)
            # The set to "abc" fires one event per node too.
            self.assertEqual(write(addr, value="abc").get("status"),
                             "ok")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            # A failed incrbyfloat fires nothing.
            self.assertEqual(incrbyfloat(addr).get("status"),
                             "incrbyfloat_failed")
            time.sleep(0.3)
            self.assertEqual(len(events), 4, events)
        finally:
            cluster.stop_all()

    def test_incrbyfloat_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(incrbyfloat(addr, by=3.5).get("value"),
                             "3.5")
            # Enough writes to compact the log past the counter.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(incrbyfloat(addr, by=4).get("value"),
                             "7.5")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr).get("value"), "7.5")
            self.assertEqual(incrbyfloat(addr, by=0.5).get("value"),
                             "8")
        finally:
            cluster.stop_all()

    def test_session_incrbyfloat_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.incrbyfloat("counter", by=9.5)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), "9.5", resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("counter").get("value"),
                                 "9.5")
        finally:
            cluster.stop_all()

    def test_pipeline_incrbyfloat_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "incrbyfloat", "key": "counter"},
                    {"op": "incrbyfloat", "key": "counter", "by": 4.5},
                    {"op": "set", "key": "other", "value": "x"},
                    {"op": "incrbyfloat", "key": "counter",
                     "by": "bad"},
                    {"op": "incrbyfloat", "key": "counter", "by": -1},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("value"), "1", replies)
            self.assertEqual(replies[1].get("value"), "5.5", replies)
            self.assertEqual(replies[2].get("status"), "ok", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("value"), "4.5", replies)
            self.assertEqual(read(addr).get("value"), "4.5")
        finally:
            cluster.stop_all()

    def test_incrbyfloat_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "incrbyfloat",
                                  "counter"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "incrbyfloat",
                                  "counter", "41.5"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "42.5")
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "42.5")
            # A failed incrbyfloat exits 1 and names the failure.
            self.assertEqual(write(cluster.addrs[leader.id],
                                   value="abc").get("status"), "ok")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "incrbyfloat",
                                  "counter"])
            self.assertEqual(rc, 1)
            self.assertIn("not a valid float", err.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
