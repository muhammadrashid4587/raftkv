"""``expire`` — set a deadline on an existing key (v0.38.0).

``expire key N`` gives the key a deadline of this entry's log index +
N, in the same log-index units as a ``set`` with ``ttl_entries``: when
any node applies an entry at or past that index it purges the key
first, so the key is gone from every replica at exactly the same log
position. Unlike a set it does not touch the value — it only sets (or
replaces) the deadline — and unlike a set it is a no-op on a missing
key: Redis's EXPIRE answers 0 there, so this returns ``expire: 0``
and records nothing instead of an error. The outcome is decided at
apply time, like a CAS, so the entry still commits (and a retry with
the same session id returns the first attempt's result instead of
appending a second entry). Expiry fires the same ``del`` watch event
as a TTL expiry, and the deadline rides snapshots with the rest of
the expiry map. These spin up real nodes (real TCP, real threads,
real disk) on localhost, like the rest of the suite.
"""

import subprocess
import sys
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, expire as expire_rpc
from raftkv.store import KVStore


def _set(addr, key, value, ttl=None):
    msg = {"type": "client", "op": "set", "key": key, "value": value}
    if ttl is not None:
        msg["ttl_entries"] = ttl
    return client_request(addr, msg)


def _get(addr, key):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def _ttl(addr, key):
    return client_request(addr, {"type": "client", "op": "ttl", "key": key})


def _expire(addr, key, entries):
    return client_request(addr, {"type": "client", "op": "expire",
                                 "key": key, "ttl_entries": entries})


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


class TestExpireStore(unittest.TestCase):
    def test_store_expire_sets_deadline_at_apply_index(self):
        """An expire at index 11 with 5 entries sets the deadline
        to 16 — the entry's own index plus N — and the key is alive
        at 15 and gone at 16, exactly like a set with the same TTL."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "expire", "key": "k",
                              "ttl_entries": 5}, index=11)
        self.assertEqual(result["expire"], 1)
        self.assertEqual(result["deadline"], 16)
        self.assertEqual(store.expiries(), {"k": 16})
        self.assertEqual(store.expire_up_to(15), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_up_to(16), ["k"])
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.expiries(), {})

    def test_store_expire_missing_key_returns_zero(self):
        """Expiring a missing key is a no-op that answers 0, like
        Redis — it records no deadline and touches no other key."""
        store = KVStore()
        store.apply({"op": "set", "key": "keep", "value": "v"},
                    index=10)
        result = store.apply({"op": "expire", "key": "nope",
                              "ttl_entries": 5}, index=11)
        self.assertEqual(result["expire"], 0)
        self.assertNotIn("deadline", result)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.get("keep"), "v")

    def test_store_expire_replaces_deadline(self):
        """Re-expiring replaces the deadline — whether the key got
        it from a set with a TTL or from an earlier expire — and
        expiring a key with no deadline sets one."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "v",
                     "ttl_entries": 10}, index=10)
        store.apply({"op": "set", "key": "b", "value": "v"}, index=10)
        result = store.apply({"op": "expire", "key": "a",
                              "ttl_entries": 3}, index=12)
        self.assertEqual(result["expire"], 1)
        self.assertEqual(result["deadline"], 15)
        result = store.apply({"op": "expire", "key": "b",
                              "ttl_entries": 2}, index=12)
        self.assertEqual(result["expire"], 1)
        self.assertEqual(store.expiries(), {"a": 15, "b": 14})
        result = store.apply({"op": "expire", "key": "a",
                              "ttl_entries": 7}, index=13)
        self.assertEqual(result["deadline"], 20)
        self.assertEqual(store.expiries(), {"a": 20, "b": 14})

    def test_store_expire_without_index_raises(self):
        """An expire with no log position to anchor it is
        meaningless, so applying one outside a log raises instead
        of recording a deadline nobody could agree on — same rule
        as a set with a TTL."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        with self.assertRaises(ValueError):
            store.apply({"op": "expire", "key": "k",
                         "ttl_entries": 5})

    def test_store_expire_after_deadline_is_missing(self):
        """A key whose deadline already passed is already gone by
        the time the expire entry applies — the purge runs before
        the entry — so expiring it answers 0, not 1."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 1}, index=10)
        self.assertEqual(store.expire_up_to(11), ["k"])
        result = store.apply({"op": "expire", "key": "k",
                              "ttl_entries": 5}, index=12)
        self.assertEqual(result["expire"], 0)


class TestExpireCluster(unittest.TestCase):
    def test_expire_sets_deadline_and_key_expires(self):
        """An expire of 3 on a live key answers 1 with the deadline,
        the TTL query counts 3, 2, 1, and once the deadline entry
        lands the key is gone — on the leader and on every
        follower, at the same log position."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _expire(addr, "temp", 3)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("expire"), 1)
            deadline = resp["index"] + 3
            self.assertEqual(resp.get("deadline"), deadline)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 3)
            _fillers(addr, 1)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 2)
            _fillers(addr, 1)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 1)
            _fillers(addr, 1)
            self.assertEqual(leader.last_applied, deadline)
            self.assertIsNone(_get(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_expire_missing_key_returns_zero(self):
        """Expiring a missing key answers 0 — and the entry still
        commits, like a failed CAS: the outcome is decided at
        apply time, so the log grows by exactly one entry and a
        later set of the same key works normally."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = _expire(addr, "nope", 5)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("expire"), 0)
            self.assertEqual(len(leader.log), before + 1)
            self.assertIsNone(_get(addr, "nope").get("value"))
            resp = _set(addr, "nope", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(_get(addr, "nope").get("value"), "v")
        finally:
            cluster.stop_all()

    def test_expire_replaces_deadline(self):
        """A key set with a long TTL and then expired with a short
        one dies at the new deadline, not the old one — and a key
        with no deadline at all gains one from expire."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _set(addr, "short", "v", ttl=100)
            _set(addr, "plain", "v")
            resp = _expire(addr, "short", 3)
            self.assertEqual(resp.get("expire"), 1)
            self.assertEqual(_ttl(addr, "short").get("ttl"), 3)
            resp = _expire(addr, "plain", 3)
            self.assertEqual(resp.get("expire"), 1)
            self.assertEqual(_ttl(addr, "plain").get("ttl"), 3)
            _fillers(addr, 1)
            self.assertEqual(_get(addr, "short").get("value"), "v")
            self.assertEqual(_get(addr, "plain").get("value"), "v")
            _fillers(addr, 1)
            self.assertIsNone(_get(addr, "short").get("value"))
            self.assertEqual(_get(addr, "plain").get("value"), "v")
            _fillers(addr, 1)
            self.assertIsNone(_get(addr, "plain").get("value"))
        finally:
            cluster.stop_all()

    def test_expire_from_follower_redirects(self):
        """An expire sent to a follower is redirected to the leader
        and still answers 1 with the right deadline — the
        module-level client follows the redirect like any other
        write."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            follower = next(n for n in cluster.nodes.values()
                            if n.id != leader.id)
            got = expire_rpc(cluster.addrs[follower.id], "temp", 4)
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("expire"), 1)
            self.assertEqual(got.get("deadline"), got["index"] + 4)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 4)
        finally:
            cluster.stop_all()

    def test_expire_fires_del_watch_event(self):
        """A watch on a key fires nothing for the expire itself —
        the value did not change — and then a del event (value
        None) at the index of the entry whose apply purged it, so
        a watcher sees the key vanish exactly when it vanishes."""
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
            self.assertEqual(log.events[0]["op"], "set")
            resp = _expire(addr, "temp", 1)
            self.assertEqual(resp.get("expire"), 1)
            time.sleep(0.2)
            self.assertEqual(len(log.events), 1, log.events)
            _fillers(addr, 1)
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1],
                             {"key": "temp", "value": None, "op": "del",
                              "index": resp["index"] + 1})
        finally:
            cluster.stop_all()

    def test_expire_survives_snapshot_and_restart(self):
        """A deadline set by expire survives log compaction and a
        restart: the snapshot carries the expiry map, the restarted
        node restores it, and the key still expires at its
        original deadline instead of living forever."""
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _expire(addr, "temp", 50)
            self.assertEqual(resp.get("expire"), 1)
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

    def test_expire_rejects_bad_values(self):
        """An expire of 0, a negative, a non-integer, or one past
        the cap is a client error — and it appends no log entry —
        and neither does an expire with no ttl_entries at all. The
        key keeps its value and (no) deadline throughout."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "k", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            before = len(leader.log)
            for bad in (0, -1, "5", True, 1_000_001, 1.5):
                resp = _expire(addr, "k", bad)
                self.assertEqual(resp.get("status"), "error", (bad, resp))
            resp = client_request(addr, {"type": "client", "op": "expire",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("ttl_entries", resp.get("error", ""))
            self.assertEqual(len(leader.log), before)
            self.assertEqual(_get(addr, "k").get("value"), "v")
            self.assertEqual(_ttl(addr, "k").get("ttl"), -1)
        finally:
            cluster.stop_all()

    def test_expire_retry_with_same_request_id_appends_once(self):
        """A retried expire with the same session id returns the
        first attempt's result — same expire, same deadline, same
        index — and appends no second log entry, so the deadline
        is not pushed out by the retry."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            before = len(leader.log)
            msg = {"type": "client", "op": "expire", "key": "temp",
                   "ttl_entries": 5, "client_id": "c1",
                   "request_id": "r1"}
            first = client_request(addr, msg)
            self.assertEqual(first.get("expire"), 1, first)
            second = client_request(addr, msg)
            self.assertEqual(second.get("expire"), 1, second)
            self.assertEqual(second.get("deadline"),
                             first.get("deadline"))
            self.assertEqual(second.get("index"), first.get("index"))
            self.assertEqual(len(leader.log), before + 1)
            self.assertEqual(_ttl(addr, "temp").get("deadline"),
                             first["index"] + 5)
        finally:
            cluster.stop_all()

    def test_session_expire_is_read_your_writes(self):
        """Session.expire sets the deadline, advances the write
        watermark like any write, and a read after it sees the key
        — until the deadline entry lands, when the session read
        agrees it is gone."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("temp", "v")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.expire("temp", 2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("expire"), 1)
                self.assertEqual(session.get("temp").get("value"), "v")
                self.assertEqual(session.ttl("temp").get("ttl"), 2)
                self.assertEqual(session.expire("nope", 2).get("expire"),
                                 0)
                session.set("pad0", "x")
                session.set("pad1", "x")
                self.assertIsNone(session.get("temp").get("value"))
        finally:
            cluster.stop_all()

    def test_expire_cli(self):
        """The ``expire`` CLI prints 1 for an existing key and 0
        for a missing one, and a bad entry count is rejected by
        the server instead of writing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "expire", "temp", "2"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "1")
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 2)
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "expire", "nope", "2"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "0")
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "expire", "temp", "0"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("ttl_entries", out.stderr)
            # The missing-key expire above committed one entry, so
            # the remaining TTL is 1 now, not 2 — and the rejected
            # expire appended nothing, so it stays 1.
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 1)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
