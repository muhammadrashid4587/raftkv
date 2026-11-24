"""Wall-clock TTL (setex, v0.48.0; expireat / pexpireat, v0.49.0).

``setex k v SECONDS`` writes the key with a wall-clock TTL: the
leader fixes an absolute deadline (its wall clock at log time +
seconds) in the log entry itself, so every replica applies the same
deadline, and the key dies when the wall clock reaches it — enforced
by a background sweep, by a purge before each log entry applies,
and by a purge before serving a read, so a read never returns a key
whose deadline passed. The opt-in alternative to the log-entry TTL:
use it when the expiry must be a duration in seconds, not a distance
in the log — and accept that two nodes' clocks may disagree briefly
about whether a key is alive, which a log-entry TTL can never do.
A plain ``set`` clears a wall deadline, a ``setex`` clears a log
deadline, and a successful CAS keeps the wall deadline like it keeps
the log one.

``expireat k TIMESTAMP`` / ``pexpireat k TIMESTAMP_MS`` set the
same absolute wall-clock deadline on a key that already exists —
the wall-clock analogue of ``expire``: the value is untouched, only
the deadline changes, and a log-entry deadline the key had is
dropped. They answer 1 when the key exists and 0 when it does not,
and a deadline already past deletes the key outright (still
answering 1). These spin up real nodes (real TCP, real threads, real
disk) on localhost, like the rest of the suite.
"""

import subprocess
import sys
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, keys as keys_rpc
from raftkv.store import KVStore


def _setex(addr, key, value, seconds):
    return client_request(addr, {"type": "client", "op": "setex",
                                 "key": key, "value": value,
                                 "seconds": seconds})


def _expireat(addr, key, timestamp):
    return client_request(addr, {"type": "client", "op": "expireat",
                                 "key": key, "timestamp": timestamp})


def _pexpireat(addr, key, timestamp_ms):
    return client_request(addr, {"type": "client", "op": "pexpireat",
                                 "key": key,
                                 "timestamp_ms": timestamp_ms})


def _set(addr, key, value, ttl=None):
    msg = {"type": "client", "op": "set", "key": key, "value": value}
    if ttl is not None:
        msg["ttl_entries"] = ttl
    return client_request(addr, msg)


def _get(addr, key):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def _ttl(addr, key):
    return client_request(addr, {"type": "client", "op": "ttl", "key": key})


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


class TestWallTTLStore(unittest.TestCase):
    def test_store_setex_records_wall_deadline(self):
        """A setex records its absolute wall deadline, alive
        before it and gone at it — the purge runs at or past the
        deadline, in sorted order like the log-entry purge."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": now + 10}, index=10)
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.wall_expiries(), {"k": now + 10})
        self.assertEqual(store.expire_wall_up_to(now + 9), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_wall_up_to(now + 10), ["k"])
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.wall_expiries(), {})

    def test_store_setex_without_deadline_raises(self):
        """A setex with no wall deadline to record is meaningless,
        so applying one raises instead of recording a deadline
        nobody could agree on."""
        store = KVStore()
        with self.assertRaises(ValueError):
            store.apply({"op": "setex", "key": "k", "value": "v"},
                        index=10)

    def test_store_plain_set_mset_del_clear_wall_deadline(self):
        """A plain set, an mset pair, and a delete all drop the
        key's wall deadline, exactly like they drop a log
        deadline: a fresh set is a new life for the key."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "setex", "key": "a", "value": "1",
                     "wall_deadline": now + 10}, index=10)
        store.apply({"op": "setex", "key": "b", "value": "1",
                     "wall_deadline": now + 10}, index=10)
        store.apply({"op": "setex", "key": "c", "value": "1",
                     "wall_deadline": now + 10}, index=10)
        store.apply({"op": "set", "key": "a", "value": "2"}, index=11)
        store.apply({"op": "mset", "pairs": [["b", "2"]]}, index=11)
        store.apply({"op": "del", "key": "c"}, index=11)
        self.assertEqual(store.wall_expiries(), {})
        self.assertEqual(store.expire_wall_up_to(now + 20), [])
        self.assertEqual(store.get("a"), "2")
        self.assertEqual(store.get("b"), "2")
        self.assertIsNone(store.get("c"))

    def test_store_cas_and_incr_keep_wall_deadline(self):
        """A successful CAS and a successful incr update the value
        but keep the wall deadline: they are updates, not fresh
        sets, so neither must resurrect a key about to expire."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "setex", "key": "k", "value": "41",
                     "wall_deadline": now + 10}, index=10)
        result = store.apply({"op": "cas", "key": "k", "expected": "41",
                              "value": "42"}, index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.wall_expiries(), {"k": now + 10})
        result = store.apply({"op": "incr", "key": "k", "by": 1},
                             index=12)
        self.assertEqual(result["value"], "43")
        self.assertEqual(store.wall_expiries(), {"k": now + 10})
        self.assertEqual(store.expire_wall_up_to(now + 10), ["k"])
        self.assertIsNone(store.get("k"))

    def test_store_setex_clears_log_deadline(self):
        """A setex drops the key's log-entry deadline: a key holds
        at most one kind of TTL, and the wall-clock one replaces
        the log one."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 50}, index=10)
        self.assertEqual(store.expiries(), {"k": 60})
        store.apply({"op": "setex", "key": "k", "value": "v2",
                     "wall_deadline": now + 10}, index=11)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.wall_expiries(), {"k": now + 10})
        self.assertEqual(store.expire_up_to(60), [])
        self.assertEqual(store.get("k"), "v2")

    def test_store_wall_ttl_remaining(self):
        """``KVStore.wall_ttl`` answers in Redis's convention, in
        seconds: -2 for a missing key, -1 for a key with no wall
        deadline, otherwise the whole seconds left — plus the
        absolute deadline itself, or None without one."""
        store = KVStore()
        now = time.time()
        self.assertEqual(store.wall_ttl("missing", now), (-2, None))
        store.apply({"op": "set", "key": "plain", "value": "v"},
                    index=10)
        self.assertEqual(store.wall_ttl("plain", now), (-1, None))
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": now + 60}, index=10)
        remaining, deadline = store.wall_ttl("k", now)
        self.assertEqual(deadline, now + 60)
        self.assertTrue(59 <= remaining <= 60, remaining)
        store.apply({"op": "set", "key": "k", "value": "v2"}, index=11)
        self.assertEqual(store.wall_ttl("k", now), (-1, None))

    def test_store_load_restores_wall_deadline(self):
        """A snapshot load restores the wall deadlines alongside
        the data, and drops deadlines for keys the snapshot does
        not hold; a load with no wall expiry (an old snapshot)
        expires nothing by the wall clock."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": now + 10}, index=10)
        store.apply({"op": "set", "key": "keep", "value": "1"}, index=11)
        restored = KVStore()
        restored.load(store.items(), store.expiries(),
                      store.wall_expiries())
        self.assertEqual(restored.get("k"), "v")
        self.assertEqual(restored.wall_expiries(), {"k": now + 10})
        self.assertEqual(restored.expire_wall_up_to(now + 10), ["k"])
        old = KVStore()
        old.load(store.items(), store.expiries())
        self.assertEqual(old.wall_expiries(), {})
        self.assertEqual(old.expire_wall_up_to(now + 20), [])


class TestWallTTLCluster(unittest.TestCase):
    def test_setex_readable_then_expires(self):
        """A key set with a 1-second wall TTL is readable at once
        and gone a little past the deadline — on the leader and on
        every follower, at the same wall-clock moment."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _setex(addr, "temp", "v", 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            time.sleep(1.3)
            self.assertIsNone(_get(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_ttl_query_reports_wall_deadline(self):
        """A TTL query reports the wall side too: ``wall_ttl`` the
        seconds left and ``wall_deadline`` the absolute Unix time
        — while the log-entry TTL stays -1 with no deadline."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = time.time()
            resp = _setex(addr, "temp", "v", 60)
            self.assertEqual(resp.get("status"), "ok", resp)
            got = _ttl(addr, "temp")
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("ttl"), -1)
            self.assertIsNone(got.get("deadline"))
            wall_deadline = got.get("wall_deadline")
            self.assertIsNotNone(wall_deadline)
            self.assertTrue(before + 55 <= wall_deadline <= time.time() + 60,
                            wall_deadline)
            self.assertTrue(55 <= got.get("wall_ttl") <= 60,
                            got.get("wall_ttl"))
            # A plain key reports no wall deadline at all.
            resp = _set(addr, "plain", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            got = _ttl(addr, "plain")
            self.assertEqual(got.get("wall_ttl"), -1)
            self.assertIsNone(got.get("wall_deadline"))
        finally:
            cluster.stop_all()

    def test_setex_rejects_bad_seconds(self):
        """Seconds of 0, a negative, a non-integer, or one past the
        cap is a client error — and it appends no log entry, so
        the key is never written either."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            for bad in (0, -1, "5", True, 1_000_001, 1.5):
                resp = _setex(addr, "k", "v", bad)
                self.assertEqual(resp.get("status"), "error", (bad, resp))
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(_get(addr, "k").get("value"))
        finally:
            cluster.stop_all()

    def test_plain_set_clears_wall_deadline_on_cluster(self):
        """Re-setting a key without a TTL clears its wall deadline:
        the key survives past the wall-clock moment it would have
        expired at, while a sibling key with the same TTL still
        expires."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _setex(addr, "keep", "v", 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _setex(addr, "gone", "v", 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _set(addr, "keep", "v2")
            self.assertEqual(resp.get("status"), "ok", resp)
            time.sleep(1.3)
            self.assertEqual(_get(addr, "keep").get("value"), "v2")
            self.assertIsNone(_get(addr, "gone").get("value"))
        finally:
            cluster.stop_all()

    def test_wall_expiry_fires_del_watch_event(self):
        """A watch on a key with a wall TTL fires a del event
        (value None) once the deadline passes — the sweep retires
        the key and fires the event even though no log entry
        landed to purge it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("temp", log)
            resp = _setex(addr, "temp", "v", 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(log.events[0]["op"], "set")
            self.assertTrue(log.wait_for(2, timeout=5.0), log.events)
            self.assertEqual(log.events[1]["key"], "temp")
            self.assertEqual(log.events[1]["op"], "del")
            self.assertIsNone(log.events[1]["value"])
        finally:
            cluster.stop_all()

    def test_wall_deadline_survives_snapshot_and_restart(self):
        """A wall deadline survives log compaction and a restart:
        the snapshot carries the wall-expiry map, the restarted
        node restores it, the key is still readable — and it still
        expires at its original wall-clock deadline instead of
        living forever."""
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            started = time.time()
            resp = _setex(addr, "temp", "v", 4)
            self.assertEqual(resp.get("status"), "ok", resp)
            _fillers(addr, 15, prefix="snap")
            self.assertGreater(leader.snapshot_index, 0)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            cluster.stop_node(leader.id)
            restarted = cluster.restart_node(leader.id)
            cluster.wait_for_stable_leader()
            new_addr = cluster.addrs[restarted.id]
            self.assertEqual(_get(new_addr, "temp").get("value"), "v")
            wall = restarted.sm.wall_expiries()
            self.assertIn("temp", wall)
            self.assertTrue(started + 3 <= wall["temp"] <= started + 5,
                            wall["temp"])
            remaining = wall["temp"] - time.time() + 0.5
            if remaining > 0:
                time.sleep(remaining)
            self.assertIsNone(_get(new_addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_expired_key_leaves_sorted_scan(self):
        """A key that expires by the wall clock disappears from
        the keys scan too — the purge removes it from the sorted
        index, not just the dict."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _set(addr, "a", "1")
            _setex(addr, "temp", "v", 1)
            _set(addr, "z", "1")
            self.assertEqual(keys_rpc(addr).get("keys"), ["a", "temp", "z"])
            time.sleep(1.3)
            got = keys_rpc(addr).get("keys")
            self.assertNotIn("temp", got)
            self.assertIn("a", got)
            self.assertIn("z", got)
        finally:
            cluster.stop_all()

    def test_session_setex(self):
        """Session.setex writes with a wall TTL, and the session's
        read-your-writes watermark still works: a read after the
        write sees the key, and after expiry it sees None."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.setex("temp", "v", 1)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(session.get("temp").get("value"), "v")
                time.sleep(1.3)
                self.assertIsNone(session.get("temp").get("value"))
        finally:
            cluster.stop_all()

    def test_pipeline_setex(self):
        """A pipelined setex may carry seconds, and a malformed
        seconds value in a pipelined op is an error in its position
        while the ops around it still run."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "setex", "key": "temp", "value": "v",
                     "seconds": 1},
                    {"op": "setex", "key": "bad", "value": "v",
                     "seconds": 0},
                    {"op": "set", "key": "keep", "value": "v"},
                ])
            self.assertEqual(replies[0].get("status"), "ok", replies[0])
            self.assertEqual(replies[1].get("status"), "error", replies[1])
            self.assertEqual(replies[2].get("status"), "ok", replies[2])
            time.sleep(1.3)
            self.assertIsNone(_get(addr, "temp").get("value"))
            self.assertEqual(_get(addr, "keep").get("value"), "v")
            self.assertIsNone(_get(addr, "bad").get("value"))
        finally:
            cluster.stop_all()

    def test_setex_cli(self):
        """The ``setex`` CLI writes the key with a wall deadline,
        and a bad seconds value is rejected by the server instead
        of writing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "setex",
                 "temp", "v", "2"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "OK")
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            time.sleep(2.5)
            self.assertIsNone(_get(addr, "temp").get("value"))
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "setex",
                 "bad", "v", "0"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("seconds", out.stderr)
            self.assertIsNone(_get(addr, "bad").get("value"))
        finally:
            cluster.stop_all()


class TestExpireAtStore(unittest.TestCase):
    def test_store_expireat_sets_wall_deadline_and_clears_log(self):
        """An expireat records its absolute wall deadline and drops
        the key's log-entry deadline: a key holds at most one kind
        of TTL, and the wall-clock one replaces the log one."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 50}, index=10)
        self.assertEqual(store.expiries(), {"k": 60})
        result = store.apply({"op": "expireat", "key": "k",
                              "wall_deadline": now + 10}, index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["expireat"], 1)
        self.assertEqual(result["wall_deadline"], now + 10)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.wall_expiries(), {"k": now + 10})
        self.assertEqual(store.expire_up_to(60), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_wall_up_to(now + 10), ["k"])
        self.assertIsNone(store.get("k"))

    def test_store_expireat_missing_key_returns_zero(self):
        """Expiring a missing key answers 0, records no deadline,
        and touches no other key — a no-op, not an error."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "other", "value": "v"},
                    index=10)
        result = store.apply({"op": "expireat", "key": "nope",
                              "wall_deadline": now + 10}, index=11)
        self.assertEqual(result["expireat"], 0)
        self.assertEqual(store.wall_expiries(), {})
        self.assertEqual(store.get("other"), "v")

    def test_store_pexpireat_sets_wall_deadline(self):
        """A pexpireat records the same absolute wall deadline in
        seconds — the millisecond timestamp was divided by 1000
        before the entry was logged — and answers 1."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "pexpireat", "key": "k",
                              "wall_deadline": now + 1.5}, index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["pexpireat"], 1)
        self.assertEqual(result["wall_deadline"], now + 1.5)
        self.assertEqual(store.wall_expiries(), {"k": now + 1.5})
        self.assertEqual(store.expire_wall_up_to(now + 1), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_wall_up_to(now + 1.5), ["k"])
        self.assertIsNone(store.get("k"))

    def test_store_expireat_replaces_wall_deadline(self):
        """Re-expiring a key replaces its wall deadline, whether it
        came from a setex or an earlier expireat — the value is
        untouched either way."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": now + 10}, index=10)
        result = store.apply({"op": "expireat", "key": "k",
                              "wall_deadline": now + 60}, index=11)
        self.assertEqual(result["expireat"], 1)
        self.assertEqual(store.wall_expiries(), {"k": now + 60})
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_wall_up_to(now + 10), [])
        self.assertEqual(store.get("k"), "v")

    def test_store_expireat_past_deadline_deletes_key(self):
        """An expireat whose deadline was already past at log time
        deletes the key outright — value, index entry, and both
        deadlines gone — and answers 1 with ``deleted`` set, so
        the apply path fires the del watch event for it."""
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 50}, index=10)
        result = store.apply({"op": "expireat", "key": "k",
                              "wall_deadline": now - 5,
                              "delete_now": True}, index=11)
        self.assertEqual(result["expireat"], 1)
        self.assertTrue(result["deleted"])
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.wall_expiries(), {})
        self.assertEqual(store.keys(), ([], False))

    def test_store_expireat_past_deadline_missing_key_returns_zero(self):
        """A past-deadline expireat on a missing key still answers
        0: there was nothing to delete, so the delete is a no-op
        like any other expireat on a missing key."""
        store = KVStore()
        now = time.time()
        result = store.apply({"op": "expireat", "key": "nope",
                              "wall_deadline": now - 5,
                              "delete_now": True}, index=11)
        self.assertEqual(result["expireat"], 0)
        self.assertNotIn("deleted", result)

    def test_store_expireat_without_deadline_raises(self):
        """An expireat with no wall deadline to record is
        meaningless, so applying one raises instead of recording
        a deadline nobody could agree on — like a setex."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        with self.assertRaises(ValueError):
            store.apply({"op": "expireat", "key": "k"}, index=11)
        with self.assertRaises(ValueError):
            store.apply({"op": "pexpireat", "key": "k"}, index=11)


class TestExpireAtCluster(unittest.TestCase):
    def test_expireat_readable_then_expires(self):
        """A key given a wall deadline 2 seconds out is readable
        at once and gone past the deadline — on the leader and on
        every follower, at the same wall-clock moment."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _expireat(addr, "temp", int(time.time()) + 2)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("expireat"), 1, resp)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            time.sleep(2.5)
            self.assertIsNone(_get(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_expireat_missing_key_returns_zero(self):
        """An expireat on a missing key answers 0 and appends an
        entry that changes nothing — the key stays missing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _expireat(addr, "nope", int(time.time()) + 60)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("expireat"), 0, resp)
            self.assertIsNone(_get(addr, "nope").get("value"))
        finally:
            cluster.stop_all()

    def test_expireat_past_deadline_deletes_immediately(self):
        """An expireat with a deadline already past deletes the key
        at once — the reply answers 1 with ``deleted`` set, the
        key reads back as missing right away, and a watch on it
        fires a del event at the entry's index."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("temp", log)
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            resp = _expireat(addr, "temp", int(time.time()) - 10)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("expireat"), 1, resp)
            self.assertTrue(resp.get("deleted"), resp)
            self.assertIsNone(_get(addr, "temp").get("value"))
            self.assertTrue(log.wait_for(2, timeout=5.0), log.events)
            self.assertEqual(log.events[1]["key"], "temp")
            self.assertEqual(log.events[1]["op"], "del")
            self.assertIsNone(log.events[1]["value"])
        finally:
            cluster.stop_all()

    def test_pexpireat_milliseconds(self):
        """A pexpireat with a millisecond timestamp 1.5 seconds out
        is readable at once and gone past the deadline, and the
        TTL query reports the deadline in seconds."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            before = time.time()
            resp = _pexpireat(addr, "temp",
                              int((before + 1.5) * 1000))
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("pexpireat"), 1, resp)
            self.assertTrue(before + 1 <= resp.get("wall_deadline")
                            <= before + 2, resp)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            got = _ttl(addr, "temp")
            self.assertEqual(got.get("status"), "ok", got)
            self.assertTrue(0 <= got.get("wall_ttl") <= 2,
                            got.get("wall_ttl"))
            time.sleep(2.0)
            self.assertIsNone(_get(addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_expireat_rejects_bad_timestamps(self):
        """A timestamp of 0, a negative, a non-integer, or a boolean
        is a client error — and it appends no log entry, so the
        key is never given a deadline either."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "k", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            before = len(leader.log)
            for bad in (0, -1, "x", True, 1.5):
                resp = _expireat(addr, "k", bad)
                self.assertEqual(resp.get("status"), "error",
                                 (bad, resp))
                resp = _pexpireat(addr, "k", bad)
                self.assertEqual(resp.get("status"), "error",
                                 (bad, resp))
            self.assertEqual(len(leader.log), before)
            got = _ttl(addr, "k")
            self.assertEqual(got.get("wall_ttl"), -1)
            self.assertIsNone(got.get("wall_deadline"))
        finally:
            cluster.stop_all()

    def test_expireat_clears_log_deadline_on_cluster(self):
        """An expireat on a key with a log-entry deadline drops the
        log deadline: the key survives the log advancing past
        where the log deadline would have purged it, and still
        dies at its wall-clock deadline."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v", ttl=5)
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _expireat(addr, "temp", int(time.time()) + 60)
            self.assertEqual(resp.get("expireat"), 1, resp)
            _fillers(addr, 10, prefix="past")
            self.assertEqual(_get(addr, "temp").get("value"), "v")
        finally:
            cluster.stop_all()

    def test_session_expireat(self):
        """Session.expireat and Session.pexpireat set wall
        deadlines, and the session's read-your-writes watermark
        still works: a read after the write sees the key, and
        after expiry it sees None."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("temp", "v")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.expireat("temp", int(time.time()) + 1)
                self.assertEqual(resp.get("expireat"), 1, resp)
                self.assertEqual(session.get("temp").get("value"), "v")
                time.sleep(1.3)
                self.assertIsNone(session.get("temp").get("value"))
                resp = session.set("temp2", "v")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.pexpireat(
                    "temp2", int((time.time() + 1) * 1000))
                self.assertEqual(resp.get("pexpireat"), 1, resp)
                self.assertEqual(session.get("temp2").get("value"), "v")
                time.sleep(1.3)
                self.assertIsNone(session.get("temp2").get("value"))
        finally:
            cluster.stop_all()

    def test_pipeline_expireat(self):
        """Pipelined expireat / pexpireat ops set wall deadlines,
        and a malformed timestamp in a pipelined op is an error in
        its position while the ops around it still run."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                session.set("temp", "v")
                session.set("temp2", "v")
                session.set("keep", "v")
                replies = session.pipeline([
                    {"op": "expireat", "key": "temp",
                     "timestamp": int(time.time()) + 1},
                    {"op": "pexpireat", "key": "temp2",
                     "timestamp_ms": int((time.time() + 1) * 1000)},
                    {"op": "expireat", "key": "keep",
                     "timestamp": 0},
                    {"op": "expireat", "key": "missing",
                     "timestamp": int(time.time()) + 60},
                ])
            self.assertEqual(replies[0].get("expireat"), 1, replies[0])
            self.assertEqual(replies[1].get("pexpireat"), 1, replies[1])
            self.assertEqual(replies[2].get("status"), "error",
                             replies[2])
            self.assertEqual(replies[3].get("expireat"), 0, replies[3])
            time.sleep(1.3)
            self.assertIsNone(_get(addr, "temp").get("value"))
            self.assertIsNone(_get(addr, "temp2").get("value"))
            self.assertEqual(_get(addr, "keep").get("value"), "v")
        finally:
            cluster.stop_all()

    def test_expireat_cli(self):
        """The ``expireat`` and ``pexpireat`` CLIs set wall
        deadlines — printing 1 for an existing key and 0 for a
        missing one — and a bad timestamp is rejected by the
        server instead of writing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "set",
                 "temp", "v"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "expireat",
                 "temp", str(int(time.time()) + 2)],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "1")
            time.sleep(2.5)
            self.assertIsNone(_get(addr, "temp").get("value"))
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "expireat",
                 "missing", str(int(time.time()) + 60)],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "0")
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "pexpireat",
                 "temp", "0"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("timestamp_ms", out.stderr)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
