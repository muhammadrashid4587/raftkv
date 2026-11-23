"""Tests for the raftkv watch/subscribe primitive (v0.11.0).

A watch registers a callback on a key (or key prefix) on one node and
fires — exactly once per committed change, in log order — when a
committed log entry changes that key. Notification happens in the
apply path, so it fires on every node that applies the entry, leader
and followers alike, and only for changes that actually landed: a
failed CAS fires nothing. Watches are local to a node and in-memory
only: they are not replicated, not persisted, and do not survive a
restart.
"""

import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.watch import WatchManager


def write(addr, key="k", value="v1"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def delete(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "del", "key": key})


def cas(addr, key="k", expected="v1", value="v2"):
    return client_request(addr, {"type": "client", "op": "cas", "key": key,
                                 "expected": expected, "value": value})


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


class TestWatchManager(unittest.TestCase):
    def test_fires_on_exact_key(self):
        wm = WatchManager()
        log = EventLog()
        wm.watch("k", log)
        wm.notify("k", "v1", "set")
        self.assertEqual(log.events, [{"key": "k", "value": "v1",
                                       "op": "set"}])

    def test_no_fire_on_unrelated_key(self):
        wm = WatchManager()
        log = EventLog()
        wm.watch("k", log)
        wm.notify("other", "v1", "set")
        self.assertEqual(log.events, [])

    def test_prefix_watch(self):
        wm = WatchManager()
        log = EventLog()
        wm.watch("user:", log, prefix=True)
        wm.notify("user:1", "a", "set")
        wm.notify("user:2", "b", "set")
        wm.notify("users", "c", "set")   # starts with "user" but not "user:"
        wm.notify("order:1", "d", "set")
        self.assertEqual([e["key"] for e in log.events],
                         ["user:1", "user:2"])

    def test_delete_notifies_with_none(self):
        wm = WatchManager()
        log = EventLog()
        wm.watch("k", log)
        wm.notify("k", None, "del")
        self.assertEqual(log.events, [{"key": "k", "value": None,
                                       "op": "del"}])

    def test_cancel_stops_notifications(self):
        wm = WatchManager()
        log = EventLog()
        wid = wm.watch("k", log)
        self.assertTrue(wm.cancel(wid))
        self.assertFalse(wm.cancel(wid))  # already gone
        wm.notify("k", "v1", "set")
        self.assertEqual(log.events, [])
        self.assertEqual(len(wm), 0)

    def test_raising_callback_does_not_break_others(self):
        wm = WatchManager()
        log = EventLog()

        def boom(event):
            raise RuntimeError("watcher blew up")

        wm.watch("k", boom)
        wm.watch("k", log)
        wm.notify("k", "v1", "set")  # must not raise
        self.assertEqual(len(log.events), 1)


class TestWatchIntegration(unittest.TestCase):
    def test_watch_fires_on_committed_set_on_leader_and_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            leader_log, follower_log = EventLog(), EventLog()
            leader.watch.watch("k", leader_log)
            follower.watch.watch("k", follower_log)
            self.assertEqual(write(cluster.addrs[leader.id]).get("status"),
                             "ok")
            self.assertTrue(leader_log.wait_for(1), leader_log.events)
            self.assertTrue(follower_log.wait_for(1), follower_log.events)
            # Events now also carry the log index of the entry that
            # caused them (v0.17.0, for RPC watchers resuming after a
            # disconnect); the local-watch tests compare the fields
            # they own and check the index separately.
            self.assertEqual(
                [{k: v for k, v in e.items() if k != "index"}
                 for e in leader_log.events],
                [{"key": "k", "value": "v1", "op": "set"}])
            self.assertEqual(leader_log.events, follower_log.events)
            self.assertIsInstance(leader_log.events[0].get("index"), int)
        finally:
            cluster.stop_all()

    def test_watch_ignores_unrelated_keys(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            log = EventLog()
            leader.watch.watch("k", log)
            self.assertEqual(
                write(cluster.addrs[leader.id], key="other").get("status"),
                "ok")
            self.assertTrue(wait_for_key(leader, "other", "v1"))
            time.sleep(0.3)  # give a spurious fire time to arrive
            self.assertEqual(log.events, [])
        finally:
            cluster.stop_all()

    def test_cancel_stops_firing(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            log = EventLog()
            wid = leader.watch.watch("k", log)
            self.assertEqual(write(cluster.addrs[leader.id]).get("status"),
                             "ok")
            self.assertTrue(log.wait_for(1))
            self.assertTrue(leader.watch.cancel(wid))
            self.assertEqual(
                write(cluster.addrs[leader.id], value="v2").get("status"),
                "ok")
            self.assertTrue(wait_for_key(leader, "k", "v2"))
            time.sleep(0.3)
            self.assertEqual(len(log.events), 1)
        finally:
            cluster.stop_all()

    def test_delete_fires_with_none_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            log = EventLog()
            leader.watch.watch("k", log)
            self.assertEqual(delete(addr).get("status"), "ok")
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(
                [{k: v for k, v in e.items() if k != "index"}
                 for e in log.events],
                [{"key": "k", "value": None, "op": "del"}])
            self.assertIsInstance(log.events[0].get("index"), int)
        finally:
            cluster.stop_all()

    def test_successful_cas_fires_failed_cas_does_not(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            log = EventLog()
            leader.watch.watch("k", log)
            resp = cas(addr, expected="wrong", value="v2")
            self.assertEqual(resp.get("status"), "cas_failed", resp)
            time.sleep(0.3)
            self.assertEqual(log.events, [])
            resp = cas(addr, expected="v1", value="v2")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(
                [{k: v for k, v in e.items() if k != "index"}
                 for e in log.events],
                [{"key": "k", "value": "v2", "op": "set"}])
            self.assertIsInstance(log.events[0].get("index"), int)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
