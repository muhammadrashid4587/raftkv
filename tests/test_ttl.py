"""TTL / key expiry, in log-index units (v0.36.0).

``set k v --ttl-entries N`` gives the key a deadline of this entry's
log index + N: when any node applies an entry at or past that index
it purges the key first, so the key is gone from every replica at
exactly the same log position. No wall clock anywhere — the unit is
log entries, not seconds — so replicas can never disagree about
whether a key is alive. A plain ``set`` clears the deadline, a
successful CAS keeps it, and expiry fires a ``del`` watch event at
the index of the entry whose apply purged the key. These spin up
real nodes (real TCP, real threads, real disk) on localhost, like
the rest of the suite.
"""

import subprocess
import sys
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, keys as keys_rpc
from raftkv.store import KVStore


def _set(addr, key, value, ttl=None):
    msg = {"type": "client", "op": "set", "key": key, "value": value}
    if ttl is not None:
        msg["ttl_entries"] = ttl
    return client_request(addr, msg)


def _get(addr, key):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def _fillers(addr, n, prefix="pad"):
    """Commit ``n`` throwaway entries so the log advances by ``n``."""
    for i in range(n):
        resp = _set(addr, f"{prefix}{i}", "x")
        assert resp.get("status") == "ok", resp


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


class TestTTLStore(unittest.TestCase):
    def test_store_records_deadline_at_apply_index(self):
        """A set with ttl_entries at index 10 is alive at index 14
        and gone at index 15 — the deadline is index + ttl, and the
        purge runs before the entry at the deadline applies."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expiries(), {"k": 15})
        self.assertEqual(store.expire_up_to(14), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_up_to(15), ["k"])
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.expiries(), {})

    def test_store_plain_set_clears_deadline(self):
        """A set without a TTL drops the key's deadline, so the key
        outlives the log position it would have expired at."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 2}, index=10)
        store.apply({"op": "set", "key": "k", "value": "v2"}, index=11)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.expire_up_to(12), [])
        self.assertEqual(store.get("k"), "v2")

    def test_store_cas_keeps_deadline(self):
        """A successful CAS updates the value but keeps the
        deadline: it is a conditional update, not a fresh set, so
        it must not resurrect a key about to expire."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 3}, index=10)
        result = store.apply({"op": "cas", "key": "k", "expected": "v",
                              "value": "v2"}, index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.expiries(), {"k": 13})
        self.assertEqual(store.expire_up_to(13), ["k"])
        self.assertIsNone(store.get("k"))

    def test_store_mset_and_del_clear_deadline(self):
        """An mset pair and a delete both drop the key's deadline,
        exactly like a plain set."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "1",
                     "ttl_entries": 5}, index=10)
        store.apply({"op": "set", "key": "b", "value": "1",
                     "ttl_entries": 5}, index=10)
        store.apply({"op": "mset", "pairs": [["a", "2"]]}, index=11)
        store.apply({"op": "del", "key": "b"}, index=11)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.expire_up_to(15), [])
        self.assertEqual(store.get("a"), "2")

    def test_store_ttl_without_index_raises(self):
        """A TTL with no log position to anchor it is meaningless,
        so applying one outside a log raises instead of recording
        a deadline nobody could agree on."""
        store = KVStore()
        with self.assertRaises(ValueError):
            store.apply({"op": "set", "key": "k", "value": "v",
                         "ttl_entries": 5})

    def test_store_load_restores_expiry(self):
        """A snapshot load restores the deadlines alongside the
        data, and drops deadlines for keys the snapshot does not
        hold; a load with no expiry (an old snapshot) expires
        nothing."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        store.apply({"op": "set", "key": "keep", "value": "1"}, index=11)
        restored = KVStore()
        restored.load(store.items(), store.expiries())
        self.assertEqual(restored.get("k"), "v")
        self.assertEqual(restored.expiries(), {"k": 15})
        self.assertEqual(restored.expire_up_to(15), ["k"])
        old = KVStore()
        old.load(store.items())
        self.assertEqual(old.expiries(), {})
        self.assertEqual(old.expire_up_to(100), [])


class TestTTLCluster(unittest.TestCase):
    def test_set_with_ttl_expires_at_deadline(self):
        """A key set with ttl 3 is readable for the next two
        entries and gone once the third lands — on the leader and
        on every follower, at the same log position."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v", ttl=3)
            self.assertEqual(resp.get("status"), "ok", resp)
            deadline = resp["index"] + 3
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            _fillers(addr, 2)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            _fillers(addr, 1)
            self.assertEqual(leader.last_applied, deadline)
            self.assertIsNone(_get(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_plain_set_clears_deadline_on_cluster(self):
        """Re-setting a key without a TTL clears its deadline: the
        key survives past the log position it would have expired
        at, while a sibling key with the same TTL still expires."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "keep", "v", ttl=2)
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _set(addr, "gone", "v", ttl=2)
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _set(addr, "keep", "v2")
            self.assertEqual(resp.get("status"), "ok", resp)
            _fillers(addr, 3)
            self.assertEqual(_get(addr, "keep").get("value"), "v2")
            self.assertIsNone(_get(addr, "gone").get("value"))
        finally:
            cluster.stop_all()

    def test_cas_on_expired_key_is_missing(self):
        """Once the log passes a key's deadline, a CAS against it
        fails as missing_key — the purge runs before the CAS
        entry applies, so the key is already gone when the CAS
        is decided."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "k", "v1", ttl=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            _fillers(addr, 1)
            self.assertIsNone(_get(addr, "k").get("value"))
            resp = client_request(addr, {"type": "client", "op": "cas",
                                         "key": "k", "expected": "v1",
                                         "value": "v2"})
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            self.assertEqual(resp.get("reason"), "missing_key")
        finally:
            cluster.stop_all()

    def test_expired_key_leaves_sorted_scan(self):
        """A key that expires disappears from the keys scan too —
        the purge removes it from the sorted index, not just the
        dict."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _set(addr, "a", "1")
            _set(addr, "temp", "v", ttl=2)
            _set(addr, "z", "1")
            self.assertEqual(keys_rpc(addr).get("keys"), ["a", "temp", "z"])
            _fillers(addr, 1)
            got = keys_rpc(addr).get("keys")
            self.assertNotIn("temp", got)
            self.assertIn("a", got)
            self.assertIn("z", got)
        finally:
            cluster.stop_all()

    def test_expiry_fires_del_watch_event(self):
        """A watch on a key with a TTL fires a del event (value
        None) at the index of the entry whose apply purged it —
        a watcher sees the key vanish exactly when it vanishes."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("temp", log)
            resp = _set(addr, "temp", "v", ttl=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(log.events[0]["op"], "set")
            _fillers(addr, 1)
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1],
                             {"key": "temp", "value": None, "op": "del",
                              "index": resp["index"] + 1})
        finally:
            cluster.stop_all()

    def test_ttl_survives_leader_change(self):
        """A key's deadline survives killing the leader: the new
        leader replays the same log, records the same deadline,
        and the key still expires at the same log position."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v", ttl=5)
            self.assertEqual(resp.get("status"), "ok", resp)
            deadline = resp["index"] + 5
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            new_addr = cluster.addrs[new_leader.id]
            self.assertEqual(_get(new_addr, "temp").get("value"), "v")
            while new_leader.last_applied < deadline:
                _set(new_addr, "pad", "x")
            self.assertIsNone(_get(new_addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_ttl_survives_snapshot_and_restart(self):
        """A deadline survives log compaction and a restart: the
        snapshot carries the expiry map, the restarted node
        restores it, and the key still expires at its original
        deadline instead of living forever."""
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v", ttl=50)
            self.assertEqual(resp.get("status"), "ok", resp)
            deadline = resp["index"] + 50
            _fillers(addr, 15, prefix="snap")
            self.assertGreater(leader.snapshot_index, 0)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            cluster.stop_node(leader.id)
            restarted = cluster.restart_node(leader.id)
            cluster.wait_for_stable_leader()
            new_addr = cluster.addrs[restarted.id]
            self.assertEqual(_get(new_addr, "temp").get("value"), "v")
            self.assertEqual(restarted.sm.expiries(), {"temp": deadline})
            while restarted.last_applied < deadline:
                _set(new_addr, "pad", "x")
            self.assertIsNone(_get(new_addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_ttl_rejects_bad_values(self):
        """A TTL of 0, a negative, a non-integer, or one past the
        cap is a client error — and it appends no log entry, so
        the key is never written either."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            for bad in (0, -1, "5", True, 1_000_001, 1.5):
                resp = _set(addr, "k", "v", ttl=bad)
                self.assertEqual(resp.get("status"), "error", (bad, resp))
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(_get(addr, "k").get("value"))
        finally:
            cluster.stop_all()

    def test_session_set_with_ttl(self):
        """Session.set takes ttl_entries, and the session's
        read-your-writes watermark still works: a read after the
        TTL write sees the key, and after expiry it sees None."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("temp", "v", ttl_entries=2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(session.get("temp").get("value"), "v")
                session.set("pad0", "x")
                session.set("pad1", "x")
                self.assertIsNone(session.get("temp").get("value"))
        finally:
            cluster.stop_all()

    def test_pipeline_set_with_ttl(self):
        """A pipelined set may carry ttl_entries, and a malformed
        TTL in a pipelined op is an error in its position while
        the ops around it still run."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "temp", "value": "v",
                     "ttl_entries": 1},
                    {"op": "set", "key": "bad", "value": "v",
                     "ttl_entries": 0},
                    {"op": "set", "key": "keep", "value": "v"},
                ])
            self.assertEqual(replies[0].get("status"), "ok", replies[0])
            self.assertEqual(replies[1].get("status"), "error", replies[1])
            self.assertEqual(replies[2].get("status"), "ok", replies[2])
            _fillers(addr, 1)
            self.assertIsNone(_get(addr, "temp").get("value"))
            self.assertEqual(_get(addr, "keep").get("value"), "v")
            self.assertIsNone(_get(addr, "bad").get("value"))
        finally:
            cluster.stop_all()

    def test_ttl_cli(self):
        """The ``set --ttl-entries N`` CLI writes the key with a
        deadline, and a bad TTL is rejected by the server instead
        of writing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "set",
                 "temp", "v", "--ttl-entries", "2"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "OK")
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            _fillers(addr, 2)
            self.assertIsNone(_get(addr, "temp").get("value"))
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "set",
                 "bad", "v", "--ttl-entries", "0"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("ttl_entries", out.stderr)
            self.assertIsNone(_get(addr, "bad").get("value"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
