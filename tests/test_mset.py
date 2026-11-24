"""Multi-set writes: one ``mset`` RPC writing many keys.

One request appends a single log entry carrying the whole batch and
applies atomically at that entry's position — a read after it commits
sees every pair, never a prefix — so the per-key cost is one state
machine apply instead of one dispatch, one log entry, and one fsync
each. These spin up real nodes (real TCP, real threads, real disk)
on localhost, like the rest of the suite.
"""

import json
import subprocess
import sys
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, mget, mset, ttl as ttl_rpc
from raftkv.store import KVStore


def _fillers(addr, n, prefix="pad"):
    """Commit ``n`` throwaway entries so the log advances by ``n``."""
    for i in range(n):
        resp = client_request(addr, {"type": "client", "op": "set",
                                     "key": f"{prefix}{i}", "value": "x"})
        assert resp.get("status") == "ok", resp


class TestMsetTTLStore(unittest.TestCase):
    def test_store_mset_with_ttl_records_one_deadline_per_pair(self):
        """An mset with ttl_entries at index 10 records a deadline
        of 15 for every pair: all alive at 14, all gone at 15 —
        the batch expires together at one log position."""
        store = KVStore()
        store.apply({"op": "mset",
                     "pairs": [["a", "1"], ["b", "2"], ["c", "3"]],
                     "ttl_entries": 5}, index=10)
        self.assertEqual(store.expiries(),
                         {"a": 15, "b": 15, "c": 15})
        self.assertEqual(store.expire_up_to(14), [])
        self.assertEqual([store.get(k) for k in ("a", "b", "c")],
                         ["1", "2", "3"])
        self.assertEqual(store.expire_up_to(15), ["a", "b", "c"])
        self.assertEqual([store.get(k) for k in ("a", "b", "c")],
                         [None, None, None])
        self.assertEqual(store.expiries(), {})

    def test_store_mset_with_ttl_without_index_raises(self):
        """An mset with a TTL and no log position to anchor it
        raises instead of recording a deadline nobody could agree
        on — exactly like a single set with a TTL."""
        store = KVStore()
        with self.assertRaises(ValueError):
            store.apply({"op": "mset", "pairs": [["a", "1"]],
                         "ttl_entries": 5})

    def test_store_plain_mset_clears_deadline(self):
        """An mset without a TTL drops every pair's deadline, so
        the keys outlive the log position they would have expired
        at — a fresh batch is a fresh set, like a plain set."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "1",
                     "ttl_entries": 2}, index=10)
        store.apply({"op": "set", "key": "b", "value": "1",
                     "ttl_entries": 2}, index=10)
        store.apply({"op": "mset", "pairs": [["a", "2"], ["b", "2"]]},
                    index=11)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.expire_up_to(12), [])
        self.assertEqual([store.get(k) for k in ("a", "b")],
                         ["2", "2"])

    def test_store_mset_with_ttl_replaces_deadline(self):
        """An mset with a TTL on keys that already had deadlines
        replaces them — the batch's deadline wins, for every pair
        it touches."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "1",
                     "ttl_entries": 2}, index=10)
        store.apply({"op": "mset", "pairs": [["a", "2"]],
                     "ttl_entries": 5}, index=11)
        self.assertEqual(store.expiries(), {"a": 16})
        self.assertEqual(store.expire_up_to(12), [])
        self.assertEqual(store.get("a"), "2")


class TestMsetTTL(unittest.TestCase):
    def test_mset_with_ttl_expires_whole_batch_at_deadline(self):
        """An mset with ttl 3 lives for two more entries and is
        gone once the third lands — on the leader and every
        follower — and a ttl query counts the batch down first."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = mset(addr, [["t0", "a"], ["t1", "b"], ["t2", "c"]],
                        ttl_entries=3)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(ttl_rpc(addr, "t1").get("ttl"), 3)
            _fillers(addr, 2)
            got = mget(addr, ["t0", "t1", "t2"])
            self.assertEqual(got.get("values"), ["a", "b", "c"])
            self.assertEqual(ttl_rpc(addr, "t0").get("ttl"), 1)
            _fillers(addr, 1)
            got = mget(addr, ["t0", "t1", "t2"])
            self.assertEqual(got.get("values"), [None, None, None])
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "t0", None), node.id)
        finally:
            cluster.stop_all()

    def test_mset_with_ttl_fires_del_watch_events(self):
        """A watch on the batch's keys fires a set event per pair
        when the mset applies, then a del event per pair at the
        deadline index — a watcher sees the whole batch vanish
        exactly when it vanishes."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            events = EventLog()
            watch_id = leader.watch.watch("w", events, prefix=True)
            try:
                resp = mset(addr, [["w0", "a"], ["w1", "b"]],
                            ttl_entries=1)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertTrue(events.wait_for(2), events.events)
                _fillers(addr, 1)
                self.assertTrue(events.wait_for(4), events.events)
            finally:
                leader.watch.cancel(watch_id)
            self.assertEqual(
                [(e["key"], e["value"], e["op"]) for e in events.events],
                [("w0", "a", "set"), ("w1", "b", "set"),
                 ("w0", None, "del"), ("w1", None, "del")])
            self.assertEqual({e["index"] for e in events.events[2:]},
                             {resp.get("index") + 1})
        finally:
            cluster.stop_all()

    def test_mset_with_ttl_survives_snapshot_and_restart(self):
        """The batch's deadlines survive log compaction and a
        restart: the snapshot carries the expiry map, the
        restarted node restores it, and the keys still expire at
        their original deadline instead of living forever."""
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = mset(addr, [["s0", "a"], ["s1", "b"]],
                        ttl_entries=50)
            self.assertEqual(resp.get("status"), "ok", resp)
            deadline = resp["index"] + 50
            _fillers(addr, 15, prefix="snap")
            self.assertGreater(leader.snapshot_index, 0)
            self.assertEqual(
                mget(addr, ["s0", "s1"]).get("values"), ["a", "b"])
            cluster.stop_node(leader.id)
            restarted = cluster.restart_node(leader.id)
            cluster.wait_for_stable_leader()
            new_addr = cluster.addrs[restarted.id]
            self.assertEqual(
                mget(new_addr, ["s0", "s1"]).get("values"), ["a", "b"])
            self.assertEqual(restarted.sm.expiries(),
                             {"s0": deadline, "s1": deadline})
            while restarted.last_applied < deadline:
                client_request(new_addr, {"type": "client", "op": "set",
                                          "key": "pad", "value": "x"})
            self.assertEqual(
                mget(new_addr, ["s0", "s1"]).get("values"), [None, None])
        finally:
            cluster.stop_all()

    def test_mset_rejects_bad_ttl(self):
        """A TTL of 0, a negative, a non-integer, or one past the
        cap is a client error — and it appends no log entry, so
        no key in the batch is ever written either."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            for bad in (0, -1, "5", True, 1_000_001, 1.5):
                resp = client_request(
                    addr, {"type": "client", "op": "mset",
                           "pairs": [["k", "v"]], "ttl_entries": bad})
                self.assertEqual(resp.get("status"), "error", (bad, resp))
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "k"}).get("value"))
        finally:
            cluster.stop_all()

    def test_session_mset_with_ttl(self):
        """Session.mset takes ttl_entries, and the session's
        read-your-writes watermark still works: a read after the
        TTL write sees the batch, and after expiry it sees
        nothing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.mset([["temp0", "a"], ["temp1", "b"]],
                                    ttl_entries=2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(
                    session.mget(["temp0", "temp1"]).get("values"),
                    ["a", "b"])
                session.set("pad0", "x")
                session.set("pad1", "x")
                self.assertEqual(
                    session.mget(["temp0", "temp1"]).get("values"),
                    [None, None])
        finally:
            cluster.stop_all()

    def test_mset_cli_with_ttl(self):
        """The ``mset --ttl-entries N`` CLI writes the batch with
        one deadline, and a bad TTL is rejected by the server
        instead of writing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "mset",
                 "c0", "a", "c1", "b", "--ttl-entries", "2"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "OK")
            self.assertEqual(mget(addr, ["c0", "c1"]).get("values"),
                             ["a", "b"])
            _fillers(addr, 2)
            self.assertEqual(mget(addr, ["c0", "c1"]).get("values"),
                             [None, None])
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "mset",
                 "bad", "v", "--ttl-entries", "0"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("ttl_entries", out.stderr)
            self.assertIsNone(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "bad"}).get("value"))
        finally:
            cluster.stop_all()


def _mset_entries(node) -> list[dict]:
    return [e for e in node.log
            if e["command"].get("op") == "mset"]


class EventLog:
    """Thread-safe sink for watch events, with a wait helper."""

    def __init__(self):
        self.events: list[dict] = []
        self._cond = threading.Condition()

    def __call__(self, event):
        with self._cond:
            self.events.append(event)
            self._cond.notify_all()

    def wait_for(self, count: int, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self.events) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return True


class TestMset(unittest.TestCase):
    def test_mset_writes_many_keys_in_one_log_entry(self):
        """50 pairs in one mset come back with the right values —
        and the leader's log grew by exactly one entry, so the
        batch is one atomic unit, not 50 writes."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            pairs = [[f"mk{i}", f"v{i}"] for i in range(50)]
            resp = mset(addr, pairs)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNotNone(resp.get("index"))
            self.assertIsNotNone(resp.get("term"))
            self.assertEqual(len(leader.log), before + 1)
            self.assertEqual(_mset_entries(leader)[-1]["command"]["pairs"],
                             pairs)
            got = mget(addr, [f"mk{i}" for i in range(50)])
            self.assertEqual(got.get("values"),
                             [f"v{i}" for i in range(50)])
        finally:
            cluster.stop_all()

    def test_mset_overwrites_and_last_duplicate_wins(self):
        """An mset overwrites keys a previous set wrote, and a
        duplicate key inside one batch keeps its last value — the
        pairs apply in order, like successive sets."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "a", "value": "old"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = mset(addr, [["a", "new"], ["b", "1"], ["b", "2"]])
            self.assertEqual(resp.get("status"), "ok", resp)
            got = mget(addr, ["a", "b"])
            self.assertEqual(got.get("values"), ["new", "2"])
        finally:
            cluster.stop_all()

    def test_mset_replicates_to_followers(self):
        """An mset committed on the leader lands on every follower
        — the batch replicates as one entry, so no follower can
        hold half of it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = mset(addr, [["r0", "x"], ["r1", "y"], ["r2", "z"]])
            self.assertEqual(resp.get("status"), "ok", resp)
            for node in cluster.nodes.values():
                self.assertTrue(
                    wait_for_key(node, "r2", "z"),
                    node.id)
                got = mget(cluster.addrs[node.id], ["r0", "r1", "r2"])
                self.assertEqual(got.get("values"), ["x", "y", "z"],
                                 node.id)
        finally:
            cluster.stop_all()

    def test_mset_from_follower_redirects(self):
        """An mset sent to a follower is redirected to the leader
        and still writes — the client follows it like any other
        write."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            resp = mset(cluster.addrs[follower.id],
                        [["f0", "a"], ["f1", "b"]])
            self.assertEqual(resp.get("status"), "ok", resp)
            got = mget(cluster.addrs[leader.id], ["f0", "f1"])
            self.assertEqual(got.get("values"), ["a", "b"])
        finally:
            cluster.stop_all()

    def test_mset_rejects_bad_pair_lists(self):
        """An empty list, a non-list, a pair that is not a
        [key, value] pair of strings, or more than MAX_MSET_PAIRS
        pairs is an error, not a write — the batch is bounded so
        one frame and one log entry can never hold an unbounded
        list."""
        from raftkv.raft import MAX_MSET_PAIRS
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for bad in ([], "mk0", [1], [["mk0"]], [["mk0", "v", "x"]],
                        [["mk0", 1]], [[1, "v"]], [[None, "v"]],
                        [[f"k{i}", "v"]
                         for i in range(MAX_MSET_PAIRS + 1)]):
                resp = client_request(
                    addr, {"type": "client", "op": "mset", "pairs": bad})
                self.assertEqual(resp.get("status"), "error", (bad, resp))
            self.assertEqual(mset(addr, []), {
                "type": "client_response", "status": "error",
                "error": "mset takes a list of 1-1000 [key, value] "
                         "pairs"})
            # Nothing was written by any of the rejected batches.
            got = mget(addr, ["mk0", "k0"])
            self.assertEqual(got.get("values"), [None, None])
        finally:
            cluster.stop_all()

    def test_session_mset_is_read_your_writes(self):
        """A session that just mset keys reads them back — the
        batch's index advances the write watermark, so a later
        read (even from a follower) waits until the batch is
        applied — and the response carries the entry's index."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.mset([["s0", "a"], ["s1", "b"]])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertGreater(session.last_write_index, 0)
                index = session.last_write_index
                resp = session.mget(["s0", "s1"])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("values"), ["a", "b"])
                # The same index comes back from a follower: the
                # session read waits for its own write there.
                follower = next(n for n in cluster.nodes.values()
                                if n is not leader)
                session.node_addr = cluster.addrs[follower.id]
                resp = session.get("s1")
                self.assertEqual(resp.get("value"), "b", resp)
                self.assertGreaterEqual(session.last_read_index, index)
        finally:
            cluster.stop_all()

    def test_mset_retry_with_same_request_id_appends_once(self):
        """A retried mset with the same session id returns the
        first attempt's result and appends no second log entry —
        the batch dedups as one unit, exactly like a single set."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            msg = {"type": "client", "op": "mset",
                   "pairs": [["d0", "a"], ["d1", "b"]],
                   "client_id": "sess-1", "request_id": "1"}
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("status"), "ok", resp)
            index = resp.get("index")
            resp2 = client_request(addr, msg)
            self.assertEqual(resp2.get("status"), "ok", resp2)
            self.assertEqual(resp2.get("index"), index)
            self.assertEqual(len(_mset_entries(leader)), 1)
            got = mget(addr, ["d0", "d1"])
            self.assertEqual(got.get("values"), ["a", "b"])
        finally:
            cluster.stop_all()

    def test_mset_fires_one_watch_event_per_pair(self):
        """A prefix watch on the leader fires once per pair in the
        batch, in batch order, all at the batch entry's index — a
        watcher sees the batch as the sequence of sets it is."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            events = EventLog()
            watch_id = leader.watch.watch("w", events, prefix=True)
            try:
                resp = mset(addr, [["w0", "a"], ["w1", "b"], ["w2", "c"]])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertTrue(events.wait_for(3), events.events)
            finally:
                leader.watch.cancel(watch_id)
            self.assertEqual(
                [(e["key"], e["value"], e["op"]) for e in events.events],
                [("w0", "a", "set"), ("w1", "b", "set"),
                 ("w2", "c", "set")])
            self.assertEqual({e["index"] for e in events.events},
                             {resp.get("index")})
        finally:
            cluster.stop_all()

    def test_mset_keys_appear_in_sorted_scan(self):
        """Keys written by mset show up in a paged scan in sorted
        order — the batch maintains the sorted index exactly like
        single sets do."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = mset(addr, [["z", "1"], ["a", "2"], ["m", "3"]])
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_request(addr, {"type": "client", "op": "keys",
                                         "limit": 10})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), ["a", "m", "z"])
        finally:
            cluster.stop_all()

    def test_mset_cli(self):
        """The ``mset`` CLI subcommand writes the batch and prints
        OK; an odd number of arguments is a usage error, not a
        half-written batch."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "mset",
                 "c0", "a", "c1", "b", "c2", "c"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "OK")
            got = mget(addr, ["c0", "c1", "c2"])
            self.assertEqual(got.get("values"), ["a", "b", "c"])
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "mset", "c3", "x", "c4"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            got = mget(addr, ["c3", "c4"])
            self.assertEqual(got.get("values"), [None, None])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
