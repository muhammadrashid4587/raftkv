"""``getex`` — get a value and optionally set, refresh, or clear its
expiry in the same atomic operation (v0.45.0).

``getex key [EX n | PX n | EXAT t | PXAT t | PERSIST]`` answers with
the value the key held — None when it did not exist — and, with an
option, sets the key's deadline in the same entry: EX / PX a relative
deadline (this entry's index + N, in the store's log-entry units, like
``ttl_entries`` — the Redis seconds / milliseconds are read as log
entries, since this store has no wall clock), EXAT / PXAT the absolute
log index of the deadline, PERSIST clearing it. With no option the
value and the deadline are both left alone. The outcome is decided at
apply time, like a CAS, so a retry with the same session id returns
the first attempt's value and deadline instead of re-expiring, and a
missing key is a no-op answering None with ``getex`` 0. A GETEX never
changes the value, so a pure expiry change fires no watch event —
only the purge fires a ``del`` event, when the deadline entry lands,
like an expire. The one exception is an absolute deadline already
past at apply time: Redis deletes the key there, so this does too.
The deadline rides snapshots with the rest of the expiry map. These
spin up real nodes (real TCP, real threads, real disk) on localhost,
like the rest of the suite.
"""

import subprocess
import sys
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, getex as getex_rpc
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


def _getex(addr, key, **options):
    msg = {"type": "client", "op": "getex", "key": key}
    msg.update(options)
    return client_request(addr, msg)


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


class TestGetexStore(unittest.TestCase):
    def test_store_getex_ex_sets_relative_deadline(self):
        """A getex with EX 5 at index 11 answers the value with
        ``getex`` 1 and the deadline 16 — the entry's own index
        plus N — and the key is alive at 15 and gone at 16,
        exactly like a set with the same TTL."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "getex", "key": "k", "ex": 5},
                             index=11)
        self.assertEqual(result["value"], "v")
        self.assertEqual(result["getex"], 1)
        self.assertEqual(result["deadline"], 16)
        self.assertEqual(store.expiries(), {"k": 16})
        self.assertEqual(store.expire_up_to(15), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_up_to(16), ["k"])
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.expiries(), {})

    def test_store_getex_px_sets_relative_deadline(self):
        """PX takes the same relative form as EX — the deadline is
        this entry's index + N, in log entries, not milliseconds:
        this store has no wall clock, so the two spellings differ
        only in name."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "getex", "key": "k", "px": 3},
                             index=11)
        self.assertEqual(result["value"], "v")
        self.assertEqual(result["getex"], 1)
        self.assertEqual(result["deadline"], 14)
        self.assertEqual(store.expiries(), {"k": 14})

    def test_store_getex_exat_sets_absolute_deadline(self):
        """EXAT names the deadline outright — the absolute log
        index — instead of a distance from this entry: a getex
        with EXAT 20 at index 11 records 20, whatever the entry's
        own index is."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "getex", "key": "k", "exat": 20},
                             index=11)
        self.assertEqual(result["value"], "v")
        self.assertEqual(result["getex"], 1)
        self.assertEqual(result["deadline"], 20)
        self.assertEqual(store.expiries(), {"k": 20})
        self.assertEqual(store.expire_up_to(19), [])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_up_to(20), ["k"])
        self.assertIsNone(store.get("k"))

    def test_store_getex_pxat_sets_absolute_deadline(self):
        """PXAT takes the same absolute form as EXAT — the deadline
        is the log index named, not a distance from this entry."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "getex", "key": "k", "pxat": 25},
                             index=11)
        self.assertEqual(result["value"], "v")
        self.assertEqual(result["getex"], 1)
        self.assertEqual(result["deadline"], 25)
        self.assertEqual(store.expiries(), {"k": 25})

    def test_store_getex_persist_clears_deadline(self):
        """PERSIST drops the deadline and answers the value with
        ``deadline`` None — the key keeps its value and stops
        expiring, like a SET without a TTL."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 10}, index=10)
        self.assertEqual(store.expiries(), {"k": 20})
        result = store.apply({"op": "getex", "key": "k",
                              "persist": True}, index=12)
        self.assertEqual(result["value"], "v")
        self.assertEqual(result["getex"], 1)
        self.assertIsNone(result["deadline"])
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expire_up_to(20), [])

    def test_store_getex_no_option_leaves_deadline(self):
        """A getex with no option is a plain read: the value comes
        back, the deadline is reported as it stands, and nothing
        is recorded or cleared."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 10}, index=10)
        result = store.apply({"op": "getex", "key": "k"}, index=12)
        self.assertEqual(result["value"], "v")
        self.assertEqual(result["getex"], 1)
        self.assertEqual(result["deadline"], 20)
        self.assertEqual(store.expiries(), {"k": 20})

    def test_store_getex_missing_key_returns_none(self):
        """A getex on a missing key answers None with ``getex`` 0 —
        a no-op, not an error — and records no deadline, whatever
        option it carried."""
        store = KVStore()
        store.apply({"op": "set", "key": "keep", "value": "v"},
                    index=10)
        for options in ({"ex": 5}, {"px": 5}, {"exat": 20},
                        {"pxat": 20}, {"persist": True}, {}):
            result = store.apply({"op": "getex", "key": "nope",
                                  **options}, index=11)
            self.assertIsNone(result["value"])
            self.assertEqual(result["getex"], 0)
            self.assertNotIn("deadline", result)
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.get("keep"), "v")

    def test_store_getex_replaces_deadline(self):
        """Re-expiring replaces the deadline — whether the key got
        it from a set with a TTL or from an earlier getex — and
        the value is untouched throughout."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "v",
                     "ttl_entries": 100}, index=10)
        store.apply({"op": "set", "key": "b", "value": "v"}, index=10)
        result = store.apply({"op": "getex", "key": "a", "ex": 3},
                             index=12)
        self.assertEqual(result["deadline"], 15)
        result = store.apply({"op": "getex", "key": "b", "exat": 30},
                             index=12)
        self.assertEqual(result["deadline"], 30)
        self.assertEqual(store.expiries(), {"a": 15, "b": 30})
        self.assertEqual(store.get("a"), "v")
        self.assertEqual(store.get("b"), "v")

    def test_store_getex_past_absolute_deadline_deletes(self):
        """An absolute deadline already past at apply time deletes
        the key outright — Redis's behaviour for an expiry in the
        past — answering None with ``getex`` 0 and ``deleted``
        set, instead of recording a deadline nobody could ever
        honour."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        result = store.apply({"op": "getex", "key": "k", "exat": 5},
                             index=11)
        self.assertIsNone(result["value"])
        self.assertEqual(result["getex"], 0)
        self.assertTrue(result["deleted"])
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.keys(), ([], False))

    def test_store_getex_relative_without_index_raises(self):
        """A getex with a relative or absolute deadline and no log
        position to anchor it is meaningless, so applying one
        outside a log raises instead of recording a deadline
        nobody could agree on — same rule as a set with a TTL.
        A getex with no option, or PERSIST, needs no index."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"}, index=10)
        with self.assertRaises(ValueError):
            store.apply({"op": "getex", "key": "k", "ex": 5})
        with self.assertRaises(ValueError):
            store.apply({"op": "getex", "key": "k", "exat": 20})
        result = store.apply({"op": "getex", "key": "k",
                              "persist": True})
        self.assertEqual(result["value"], "v")
        result = store.apply({"op": "getex", "key": "k"})
        self.assertEqual(result["value"], "v")


class TestGetexCluster(unittest.TestCase):
    def test_getex_ex_sets_deadline_and_key_expires(self):
        """A getex with EX 3 on a live key answers the value with
        ``getex`` 1 and the deadline, the TTL query counts 3, 2, 1,
        and once the deadline entry lands the key is gone — on
        the leader and on every follower, at the same log
        position — while the value stays readable until then."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _getex(addr, "temp", ex=3)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v")
            self.assertEqual(resp.get("getex"), 1)
            deadline = resp["index"] + 3
            self.assertEqual(resp.get("deadline"), deadline)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 3)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
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

    def test_getex_all_option_forms(self):
        """Every option form sets the deadline it names: EX and PX
        relative to the entry's index, EXAT and PXAT absolute, and
        PERSIST clearing a deadline a set had left — each verified
        through the TTL query, which counts down in log entries."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _set(addr, "a", "v")
            resp = _getex(addr, "a", ex=4)
            self.assertEqual(resp.get("getex"), 1)
            self.assertEqual(resp.get("deadline"), resp["index"] + 4)
            self.assertEqual(_ttl(addr, "a").get("ttl"), 4)
            _set(addr, "b", "v")
            resp = _getex(addr, "b", px=4)
            self.assertEqual(resp.get("getex"), 1)
            self.assertEqual(resp.get("deadline"), resp["index"] + 4)
            self.assertEqual(_ttl(addr, "b").get("ttl"), 4)
            _set(addr, "c", "v")
            target = _get(addr, "c")["read_index"] + 5
            resp = _getex(addr, "c", exat=target)
            self.assertEqual(resp.get("getex"), 1)
            self.assertEqual(resp.get("deadline"), target)
            self.assertEqual(_ttl(addr, "c").get("ttl"),
                             target - resp["index"])
            _set(addr, "d", "v")
            target = _get(addr, "d")["read_index"] + 5
            resp = _getex(addr, "d", pxat=target)
            self.assertEqual(resp.get("getex"), 1)
            self.assertEqual(resp.get("deadline"), target)
            self.assertEqual(_ttl(addr, "d").get("ttl"),
                             target - resp["index"])
            _set(addr, "e", "v", ttl=100)
            self.assertEqual(_ttl(addr, "e").get("ttl"), 100)
            resp = _getex(addr, "e", persist=True)
            self.assertEqual(resp.get("value"), "v")
            self.assertEqual(resp.get("getex"), 1)
            self.assertIsNone(resp.get("deadline"))
            self.assertEqual(_ttl(addr, "e").get("ttl"), -1)
            resp = _getex(addr, "e")
            self.assertEqual(resp.get("value"), "v")
            self.assertEqual(resp.get("getex"), 1)
            self.assertIsNone(resp.get("deadline"))
            self.assertEqual(_ttl(addr, "e").get("ttl"), -1)
        finally:
            cluster.stop_all()

    def test_getex_missing_key_returns_none(self):
        """A getex on a missing key answers None with ``getex`` 0 —
        and the entry still commits, like a failed CAS: the outcome
        is decided at apply time, so the log grows by exactly one
        entry and a later set of the same key works normally."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = _getex(addr, "nope", ex=5)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("value"))
            self.assertEqual(resp.get("getex"), 0)
            self.assertEqual(len(leader.log), before + 1)
            self.assertIsNone(_get(addr, "nope").get("value"))
            resp = _set(addr, "nope", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(_get(addr, "nope").get("value"), "v")
        finally:
            cluster.stop_all()

    def test_getex_persist_clears_deadline(self):
        """A key set with a long TTL and then got with PERSIST stops
        expiring: its TTL reads -1, and fillers past the old
        deadline leave the value in place on every node."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v", ttl=3)
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _getex(addr, "temp", persist=True)
            self.assertEqual(resp.get("value"), "v")
            self.assertEqual(resp.get("getex"), 1)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), -1)
            _fillers(addr, 4)
            self.assertEqual(_get(addr, "temp").get("value"), "v")
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", "v"),
                                node.id)
        finally:
            cluster.stop_all()

    def test_getex_past_exat_deletes_key(self):
        """A getex with an absolute deadline already past deletes
        the key in its own entry: the reply is None with
        ``getex`` 0, a read after it agrees the key is gone on
        every node, and the TTL query answers -2."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = _getex(addr, "temp", exat=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("value"))
            self.assertEqual(resp.get("getex"), 0)
            self.assertTrue(resp.get("deleted"))
            self.assertIsNone(_get(addr, "temp").get("value"))
            self.assertEqual(_ttl(addr, "temp").get("ttl"), -2)
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_getex_fires_no_watch_event(self):
        """A watch on a key fires nothing for the getex itself —
        the value did not change — and then a del event (value
        None) at the index of the entry whose apply purged it, so
        a watcher sees the key vanish exactly when it vanishes,
        and nothing before."""
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
            resp = _getex(addr, "temp", ex=1)
            self.assertEqual(resp.get("getex"), 1)
            time.sleep(0.2)
            self.assertEqual(len(log.events), 1, log.events)
            _fillers(addr, 1)
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1],
                             {"key": "temp", "value": None, "op": "del",
                              "index": resp["index"] + 1})
        finally:
            cluster.stop_all()

    def test_getex_past_exat_fires_del_watch_event(self):
        """The one getex that deletes — an absolute deadline already
        past — fires a del event in its own entry, like a getdel
        that found its key: a watcher sees the key vanish at the
        getex's own index."""
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
            resp = _getex(addr, "temp", exat=1)
            self.assertEqual(resp.get("getex"), 0)
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1],
                             {"key": "temp", "value": None, "op": "del",
                              "index": resp["index"]})
        finally:
            cluster.stop_all()

    def test_getex_rejects_bad_option_combinations(self):
        """Two options at once, an unknown option, a non-integer
        or out-of-range value, or a PERSIST with an argument are
        all client errors — and they append no log entry — and
        neither does an EXAT of 0. The key keeps its value and
        (no) deadline throughout."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "k", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            before = len(leader.log)
            bad = [
                {"ex": 5, "px": 5},
                {"ex": 5, "exat": 100},
                {"ex": 5, "persist": True},
                {"exat": 100, "pxat": 200},
                {"expx": 5},
                {"ex": 0},
                {"ex": -1},
                {"ex": "5"},
                {"ex": True},
                {"ex": 1.5},
                {"ex": 1_000_001},
                {"px": 0},
                {"exat": 0},
                {"exat": -5},
                {"exat": "100"},
                {"pxat": True},
                {"persist": "yes"},
                {"persist": 1},
            ]
            for options in bad:
                resp = _getex(addr, "k", **options)
                self.assertEqual(resp.get("status"), "error",
                                 (options, resp))
            self.assertEqual(len(leader.log), before)
            self.assertEqual(_get(addr, "k").get("value"), "v")
            self.assertEqual(_ttl(addr, "k").get("ttl"), -1)
        finally:
            cluster.stop_all()

    def test_getex_retry_with_same_request_id_appends_once(self):
        """A retried getex with the same session id returns the
        first attempt's result — same value, same deadline, same
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
            msg = {"type": "client", "op": "getex", "key": "temp",
                   "ex": 5, "client_id": "c1", "request_id": "r1"}
            first = client_request(addr, msg)
            self.assertEqual(first.get("value"), "v", first)
            self.assertEqual(first.get("getex"), 1)
            second = client_request(addr, msg)
            self.assertEqual(second.get("value"), "v", second)
            self.assertEqual(second.get("getex"), 1)
            self.assertEqual(second.get("deadline"),
                             first.get("deadline"))
            self.assertEqual(second.get("index"), first.get("index"))
            self.assertEqual(len(leader.log), before + 1)
            self.assertEqual(_ttl(addr, "temp").get("deadline"),
                             first["index"] + 5)
        finally:
            cluster.stop_all()

    def test_getex_from_follower_redirects(self):
        """A getex sent to a follower is redirected to the leader
        and still answers the value with the right deadline — the
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
            got = getex_rpc(cluster.addrs[follower.id], "temp", ex=4)
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("value"), "v")
            self.assertEqual(got.get("getex"), 1)
            self.assertEqual(got.get("deadline"), got["index"] + 4)
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 4)
        finally:
            cluster.stop_all()

    def test_getex_survives_snapshot_and_restart(self):
        """A deadline set by getex survives log compaction and a
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
            resp = _getex(addr, "temp", ex=50)
            self.assertEqual(resp.get("getex"), 1)
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

    def test_getex_continues_after_leader_change(self):
        """A getex still works after the leader dies and a new one
        is elected — and a retry of the same session id on the new
        leader returns the first attempt's result instead of
        re-expiring, since the dedup table is rebuilt by replay."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = _set(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_stable_leader()
            new_addr = cluster.addrs[new_leader.id]
            msg = {"type": "client", "op": "getex", "key": "temp",
                   "ex": 5, "client_id": "c1", "request_id": "r1"}
            first = client_request(new_addr, msg)
            self.assertEqual(first.get("value"), "v", first)
            self.assertEqual(first.get("getex"), 1)
            self.assertEqual(_ttl(new_addr, "temp").get("ttl"), 5)
            second = client_request(new_addr, msg)
            self.assertEqual(second.get("value"), "v", second)
            self.assertEqual(second.get("deadline"),
                             first.get("deadline"))
            self.assertEqual(second.get("index"), first.get("index"))
        finally:
            cluster.stop_all()

    def test_session_getex_is_read_your_writes(self):
        """Session.getex returns the value, sets the deadline, and
        advances the write watermark like any write — a read after
        it sees the key, until the deadline entry lands, when the
        session read agrees it is gone; a getex on a missing key
        answers None with ``getex`` 0."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("temp", "v")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.getex("temp", ex=2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), "v")
                self.assertEqual(resp.get("getex"), 1)
                self.assertEqual(session.get("temp").get("value"), "v")
                self.assertEqual(session.ttl("temp").get("ttl"), 2)
                self.assertIsNone(session.getex("nope",
                                                ex=2).get("value"))
                self.assertEqual(session.getex("nope",
                                               ex=2).get("getex"), 0)
                session.set("pad0", "x")
                session.set("pad1", "x")
                self.assertIsNone(session.get("temp").get("value"))
        finally:
            cluster.stop_all()

    def test_getex_pipelined(self):
        """Getex ops ride a pipeline like any other write: a
        pipelined getex answers the value and sets the deadline,
        a malformed getex (two options) comes back an error in
        its position without going on the wire, and the ops
        around it still run."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("temp", "v")
                self.assertEqual(resp.get("status"), "ok", resp)
                replies = session.pipeline([
                    {"op": "getex", "key": "temp", "ex": 5},
                    {"op": "getex", "key": "temp", "ex": 2, "px": 2},
                    {"op": "getex", "key": "temp", "persist": True},
                    {"op": "getex", "key": "missing"},
                ])
                self.assertEqual(len(replies), 4)
                self.assertEqual(replies[0].get("value"), "v",
                                 replies[0])
                self.assertEqual(replies[0].get("getex"), 1)
                self.assertEqual(replies[0].get("deadline"),
                                 replies[0]["index"] + 5)
                self.assertEqual(replies[1].get("status"), "error",
                                 replies[1])
                self.assertEqual(replies[2].get("value"), "v",
                                 replies[2])
                self.assertIsNone(replies[2].get("deadline"))
                self.assertIsNone(replies[3].get("value"))
                self.assertEqual(replies[3].get("getex"), 0)
                # The persist in position 2 cleared the deadline
                # the first op set, so the key no longer expires.
                self.assertEqual(session.ttl("temp").get("ttl"), -1)
                self.assertEqual(session.get("temp").get("value"), "v")
        finally:
            cluster.stop_all()

    def test_getex_cli(self):
        """The ``getex`` CLI prints the value for an existing key
        and (nil) for a missing one, --ex sets the deadline, and
        --persist clears it; a bad count is rejected by the server
        instead of writing, and two options are rejected by
        argparse before anything is sent."""
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
                 "--node", f"{host}:{port}",
                 "getex", "temp", "--ex", "10"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "v")
            self.assertEqual(_ttl(addr, "temp").get("ttl"), 10)
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "getex", "nope"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "(nil)")
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}",
                 "getex", "temp", "--persist"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(out.stdout.strip(), "v")
            self.assertEqual(_ttl(addr, "temp").get("ttl"), -1)
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}",
                 "getex", "temp", "--ex", "0"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("ex", out.stderr.lower())
            self.assertEqual(_ttl(addr, "temp").get("ttl"), -1)
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}",
                 "getex", "temp", "--ex", "2", "--px", "2"],
                capture_output=True, text=True, timeout=15)
            self.assertNotEqual(out.returncode, 0)
            self.assertIn("not allowed with", out.stderr)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
