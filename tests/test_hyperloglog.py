"""Tests for raftkv HyperLogLog (PFADD / PFCOUNT, v0.47.0).

``pfadd key element [element ...]`` adds elements to the HyperLogLog
at the key and answers ``pfadd`` 1 when at least one register rose —
the HLL was modified — and 0 when every element was already counted,
plus ``estimate``, the cardinality estimate after the add;
``pfcount key`` answers the estimate, 0 when the key does not exist.
A missing key is created as an empty HLL. The registers are 16384
one-byte registers (precision 14, Redis's default), hashed with
SHA-256 so every node applying the same log computes the same bytes,
and the estimate is the standard HyperLogLog one with the small- and
large-range corrections — good to under 1% once the cardinality is
past a few thousand, at the cost of ~16 KiB per key. A pfadd is a
write replicated through the Raft log like any other, its outcome
decided when the entry applies — at its exact position in the log —
so a retry with the same session id returns the first attempt's
pfadd flag and estimate instead of adding twice. A pfcount is a read
through the same ReadIndex path as a ``get``. A successful pfadd
keeps the key's TTL deadline, like a CAS, and fires a ``set`` watch
event with the new value — but only when a register rose: a pfadd
that changed nothing fires nothing. A plain string value fails a
pfadd with ``pfadd_failed`` / ``wrongtype`` instead of writing, and
a pfcount on one is a client error, not an estimate. These spin up
real nodes (real TCP, real threads, real disk) on localhost, like
the rest of the suite.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import pfadd as client_pfadd
from raftkv.client import pfcount as client_pfcount
from raftkv.store import KVStore, is_hll


def pfadd(addr, key="h", elements=("a",), client_id=None,
          request_id=None):
    msg = {"type": "client", "op": "pfadd", "key": key,
           "elements": list(elements)}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def pfcount(addr, key="h"):
    return client_request(addr, {"type": "client", "op": "pfcount",
                                 "key": key})


def write(addr, key="k", value="old"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestHyperLogLogStore(unittest.TestCase):
    def test_pfadd_on_missing_key_creates_hll(self):
        """A pfadd on a missing key creates an HLL holding one
        element: it answers pfadd 1 with estimate 1, the stored
        value is an HLL blob, and pfcount reads back 1."""
        store = KVStore()
        result = store.apply({"op": "pfadd", "key": "h",
                              "elements": ["a"]})
        self.assertEqual(result, {"type": "client_response",
                                  "status": "ok", "pfadd": 1,
                                  "estimate": 1})
        self.assertTrue(is_hll(store.get("h")))
        self.assertEqual(store.pfcount("h"), 1)

    def test_pfadd_same_element_again_is_zero(self):
        """Adding an element that is already counted answers pfadd
        0 with the estimate unchanged — and the stored blob is
        byte-identical, so nothing was rewritten."""
        store = KVStore()
        store.apply({"op": "pfadd", "key": "h", "elements": ["a"]})
        before = store.get("h")
        result = store.apply({"op": "pfadd", "key": "h",
                              "elements": ["a"]})
        self.assertEqual(result["pfadd"], 0)
        self.assertEqual(result["estimate"], 1)
        self.assertEqual(store.get("h"), before)
        # A batch that mixes a counted element with a new one
        # still rises a register, so it answers 1.
        result = store.apply({"op": "pfadd", "key": "h",
                              "elements": ["a", "b"]})
        self.assertEqual(result["pfadd"], 1)
        self.assertEqual(store.pfcount("h"), 2)

    def test_pfcount_on_missing_key_is_zero(self):
        """A pfcount on a missing key is 0 — never an error."""
        store = KVStore()
        self.assertEqual(store.pfcount("missing"), 0)

    def test_pfadd_accuracy_within_bounds(self):
        """The estimate tracks the true cardinality: 1000 distinct
        elements estimate within 5%, 10000 within 3% — the
        standard error at precision 14 is ~0.81%, so both bounds
        have several sigma of slack, and the hash is SHA-256, so
        these numbers are deterministic, not luck."""
        store = KVStore()
        store.apply({"op": "pfadd", "key": "h",
                     "elements": [f"elem-{i}" for i in range(1000)]})
        estimate = store.pfcount("h")
        self.assertGreaterEqual(estimate, 950, estimate)
        self.assertLessEqual(estimate, 1050, estimate)
        store.apply({"op": "pfadd", "key": "h",
                     "elements": [f"elem-{i}"
                                  for i in range(1000, 10000)]})
        estimate = store.pfcount("h")
        self.assertGreaterEqual(estimate, 9700, estimate)
        self.assertLessEqual(estimate, 10300, estimate)

    def test_pfadd_is_order_independent(self):
        """The stored blob is a pure function of the set of elements
        added, not their order: two stores adding the same elements
        in different orders hold byte-identical blobs, which is
        what makes the op replay-safe across nodes."""
        first = KVStore()
        second = KVStore()
        first.apply({"op": "pfadd", "key": "h",
                     "elements": ["x", "y", "z"]})
        second.apply({"op": "pfadd", "key": "h",
                      "elements": ["z", "x", "y"]})
        self.assertEqual(first.get("h"), second.get("h"))
        self.assertEqual(first.pfcount("h"), second.pfcount("h"))

    def test_pfadd_keeps_ttl_deadline(self):
        # A pfadd is an update, not a fresh set: the deadline
        # survives it, like a CAS or a setbit.
        store = KVStore()
        store.apply({"op": "pfadd", "key": "g", "elements": ["a"]},
                    index=10)
        store.apply({"op": "expire", "key": "g", "ttl_entries": 10},
                    index=10)
        self.assertEqual(store.expiries(), {"g": 20})
        result = store.apply({"op": "pfadd", "key": "g",
                             "elements": ["b"]}, index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["pfadd"], 1)
        self.assertEqual(store.expiries(), {"g": 20})
        self.assertEqual(store.expire_up_to(19), [])
        self.assertEqual(store.expire_up_to(20), ["g"])

    def test_pfadd_new_key_joins_sorted_index(self):
        """A pfadd that creates a key inserts it in the sorted
        index, so a scan finds it in order."""
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "pfadd", "key": "a", "elements": ["e"]})
        store.apply({"op": "pfadd", "key": "c", "elements": ["e"]})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))

    def test_pfadd_wrongtype_leaves_value_untouched(self):
        """A plain string is not an HLL: pfadd fails with
        pfadd_failed / wrongtype and writes nothing, and pfcount
        on it raises TypeError."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "plain"})
        result = store.apply({"op": "pfadd", "key": "k",
                             "elements": ["a"]})
        self.assertEqual(result["status"], "pfadd_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("k"), "plain")
        with self.assertRaises(TypeError):
            store.pfcount("k")

    def test_pfadd_bad_elements_raise(self):
        """An empty elements list, a non-list, or a non-string
        element is a programming error at apply time — the leader
        validates all three before logging, so a hand-built entry
        with any of them raises instead of counting nonsense."""
        store = KVStore()
        with self.assertRaises(ValueError):
            store.apply({"op": "pfadd", "key": "h", "elements": []})
        with self.assertRaises(ValueError):
            store.apply({"op": "pfadd", "key": "h",
                         "elements": "a"})
        with self.assertRaises(ValueError):
            store.apply({"op": "pfadd", "key": "h",
                         "elements": ["a", 1]})
        with self.assertRaises(ValueError):
            store.apply({"op": "pfadd", "key": "h"})

    def test_hll_survives_snapshot_load(self):
        """The registers ride in _data as a plain string value, so
        a snapshot load restores them byte-identically and the
        estimate is unchanged."""
        store = KVStore()
        store.apply({"op": "pfadd", "key": "h",
                     "elements": [f"elem-{i}" for i in range(500)]})
        estimate = store.pfcount("h")
        blob = store.get("h")
        restored = KVStore()
        restored.load(store.items(), store.expiries())
        self.assertEqual(restored.get("h"), blob)
        self.assertEqual(restored.pfcount("h"), estimate)


class TestHyperLogLogCluster(unittest.TestCase):
    def test_pfadd_replicates_to_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = pfadd(addr, elements=["a", "b", "c"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("pfadd"), 1, resp)
            self.assertEqual(resp.get("estimate"), 3, resp)
            # Every node holds the same blob: the leader's stored
            # value is the expected one, since the blob is a pure
            # function of the elements added.
            blob = leader.sm.get("h")
            self.assertTrue(is_hll(blob))
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(wait_for_key(node, "h", blob))
                self.assertEqual(node.sm.pfcount("h"), 3)
            # A second pfadd with the same elements answers 0 on
            # every node, and the estimate stays 3.
            resp = pfadd(addr, elements=["a", "b", "c"])
            self.assertEqual(resp.get("pfadd"), 0, resp)
            self.assertEqual(resp.get("estimate"), 3, resp)
            self.assertEqual(pfcount(addr).get("estimate"), 3)
            self.assertEqual(pfcount(addr, key="missing")
                             .get("estimate"), 0)
        finally:
            cluster.stop_all()

    def test_pfadd_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = client_pfadd(cluster.addrs[follower], "h",
                                "a", "b")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("pfadd"), 1, resp)
            self.assertEqual(resp.get("estimate"), 2, resp)
            self.assertEqual(
                client_pfcount(cluster.addrs[follower], "h")
                .get("estimate"), 2)
        finally:
            cluster.stop_all()

    def test_invalid_pfadd_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = client_request(
                addr, {"type": "client", "op": "pfadd", "key": "h",
                       "elements": []})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("elements", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "pfadd", "key": "h",
                       "elements": "a"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("elements", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "pfadd", "key": "h",
                       "elements": ["a", 1]})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("elements", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "pfadd", "key": "h",
                       "elements": [f"e{i}" for i in range(1001)]})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("elements", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "pfadd", "key": "h"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("elements", resp.get("error", ""), resp)
            # None of the rejected writes left a key behind, and
            # none appended a log entry.
            self.assertIsNone(read(addr, key="h").get("value"))
            self.assertEqual(len(leader.log), before)
        finally:
            cluster.stop_all()

    def test_pfadd_wrongtype_replicates_and_pfcount_errors(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="k", value="plain")
                             .get("status"), "ok")
            resp = pfadd(addr, key="k", elements=["a"])
            self.assertEqual(resp.get("status"), "pfadd_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            self.assertEqual(read(addr, key="k").get("value"),
                             "plain")
            resp = pfcount(addr, key="k")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("hyperloglog", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_pfadd_retry_returns_first_result_and_writes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = pfadd(addr, elements=["a"],
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("pfadd"), 1, resp)
            self.assertEqual(resp.get("estimate"), 1, resp)
            # A later pfadd moves the estimate on — but the retry
            # of the first pfadd still answers the first attempt's
            # flag and estimate, and appends no second entry.
            resp = pfadd(addr, elements=["b", "c", "d"])
            self.assertEqual(resp.get("estimate"), 4, resp)
            resp = pfadd(addr, elements=["x"],
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("pfadd"), 1, resp)
            self.assertEqual(resp.get("estimate"), 1, resp)
            self.assertEqual(pfcount(addr).get("estimate"), 4)
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_pfadd_fires_watch_event_only_when_changed(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("h", events.append)
            follower.watch.watch("h", events.append)
            resp = pfadd(addr, elements=["a"])
            self.assertEqual(resp.get("pfadd"), 1, resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "h", event)
                # The event carries the new HLL blob value.
                self.assertTrue(is_hll(event["value"]), event)
                self.assertEqual(event["op"], "set", event)
            # A pfadd that changes nothing writes nothing new, so
            # it fires nothing — and a failed pfadd fires nothing
            # either.
            resp = pfadd(addr, elements=["a"])
            self.assertEqual(resp.get("pfadd"), 0, resp)
            self.assertEqual(write(addr, key="w", value="plain")
                             .get("status"), "ok")
            resp = pfadd(addr, key="w", elements=["a"])
            self.assertEqual(resp.get("status"), "pfadd_failed", resp)
            time.sleep(0.3)
            self.assertEqual(len(events), 2, events)
        finally:
            cluster.stop_all()

    def test_pfadd_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(pfadd(addr, elements=["a", "b"])
                             .get("estimate"), 2)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The HLL survived the failover, so the estimate still
            # answers 2 and a new pfadd still counts.
            self.assertEqual(pfcount(addr).get("estimate"), 2)
            self.assertEqual(pfadd(addr, elements=["c"])
                             .get("estimate"), 3)
            self.assertEqual(pfcount(addr).get("estimate"), 3)
        finally:
            cluster.stop_all()

    def test_pfadd_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            # 100 distinct elements estimate within 5% — the
            # estimate is approximate, not exact (this hash lands
            # on 98), so the test bounds it instead of asserting
            # the true count.
            estimate = pfadd(
                addr, elements=[f"elem-{i}" for i in range(100)]
                ).get("estimate")
            self.assertGreaterEqual(estimate, 95, estimate)
            self.assertLessEqual(estimate, 105, estimate)
            # Enough writes to compact the log past the pfadd.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            estimate = pfcount(addr).get("estimate")
            self.assertGreaterEqual(estimate, 95, estimate)
            self.assertLessEqual(estimate, 105, estimate)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            estimate = pfcount(addr).get("estimate")
            self.assertGreaterEqual(estimate, 95, estimate)
            self.assertLessEqual(estimate, 105, estimate)
            estimate = pfadd(addr, elements=["elem-100"]
                             ).get("estimate")
            self.assertGreaterEqual(estimate, 96, estimate)
            self.assertLessEqual(estimate, 106, estimate)
        finally:
            cluster.stop_all()

    def test_session_pfadd_pfcount_are_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.pfadd("h", "a", "b")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("pfadd"), 1, resp)
                self.assertEqual(resp.get("estimate"), 2, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.pfcount("h")
                                 .get("estimate"), 2)
                # A pfadd through the same session still advances
                # the watermark, and the estimate follows it.
                resp = session.pfadd("h", "c")
                self.assertEqual(resp.get("estimate"), 3, resp)
                self.assertEqual(session.pfcount("h")
                                 .get("estimate"), 3)
                # A pfcount on a missing key is 0 through the
                # session too, and still advances the read
                # watermark.
                self.assertEqual(session.pfcount("missing")
                                 .get("estimate"), 0)
                # A failed pfadd still committed its entry, so
                # its index is a valid watermark too.
                resp = session.set("w", "plain")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.pfadd("w", "a")
                self.assertEqual(resp.get("status"), "pfadd_failed",
                                 resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("w").get("value"),
                                 "plain")
        finally:
            cluster.stop_all()

    def test_pipeline_pfadd_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "pfadd", "key": "a", "elements": ["x"]},
                    {"op": "pfadd", "key": "a", "elements": ["y"]},
                    {"op": "pfadd", "key": "a", "elements": ["x"]},
                    {"op": "pfadd", "key": "b", "elements": []},
                    {"op": "pfadd", "key": "b",
                     "elements": ["x", 1]},
                    {"op": "pfadd", "key": "c",
                     "elements": ["x", "y", "z"]},
                ])
            self.assertEqual(len(replies), 6, replies)
            self.assertEqual(replies[0].get("pfadd"), 1, replies)
            self.assertEqual(replies[0].get("estimate"), 1, replies)
            self.assertEqual(replies[1].get("pfadd"), 1, replies)
            self.assertEqual(replies[1].get("estimate"), 2, replies)
            self.assertEqual(replies[2].get("pfadd"), 0, replies)
            self.assertEqual(replies[2].get("estimate"), 2, replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("status"), "error", replies)
            self.assertEqual(replies[5].get("pfadd"), 1, replies)
            self.assertEqual(replies[5].get("estimate"), 3, replies)
            self.assertEqual(pfcount(addr, key="a").get("estimate"),
                             2)
            self.assertIsNone(read(addr, key="b").get("value"))
            self.assertEqual(pfcount(addr, key="c").get("estimate"),
                             3)
        finally:
            cluster.stop_all()

    def test_pfadd_pfcount_module_functions(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(client_pfadd(addr, "h", "a", "b")
                             .get("pfadd"), 1)
            self.assertEqual(client_pfcount(addr, "h")
                             .get("estimate"), 2)
            self.assertEqual(client_pfadd(addr, "h", "a")
                             .get("pfadd"), 0)
            self.assertEqual(client_pfcount(addr, "h")
                             .get("estimate"), 2)
            self.assertEqual(client_pfcount(addr, "missing")
                             .get("estimate"), 0)
        finally:
            cluster.stop_all()

    def test_hyperloglog_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # Pfcount on a missing key prints 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "pfcount", "h"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # Pfadd prints 1 and creates the key.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "pfadd", "h",
                                  "a", "b"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "pfcount", "h"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            # Adding the same elements again prints 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "pfadd", "h",
                                  "a", "b"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # Pfadd with no elements is a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "pfadd", "h"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
