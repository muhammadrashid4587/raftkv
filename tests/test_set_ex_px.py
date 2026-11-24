"""Tests for raftkv SET with EX / PX.

A ``set`` may carry Redis's SET EX / PX options: an inline
wall-clock TTL — EX names seconds, PX milliseconds — instead of
the separate ``setex`` op. The leader fixes the absolute
wall-clock deadline at log time, so every replica applies the
same deadline, and the key dies when the wall clock reaches it —
enforced by the background sweep, by a purge before each log
entry applies, and by a purge before serving a read, exactly
like a ``setex`` deadline. EX and PX are mutually exclusive,
and neither rides with a log-entry TTL (``ttl_entries``) or
with KEEPTTL — Redis rejects all three combinations. A winning
EX/PX set is a fresh life for the key: any log-entry deadline
it had is dropped, so a key still holds at most one kind of
TTL; a losing conditional set writes nothing at all, deadline
included. It composes with NX / XX and with GET exactly like
the other flags.
"""

import contextlib
import io
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def write(addr, key="k", value="v", **flags):
    msg = {"type": "client", "op": "set", "key": key, "value": value}
    msg.update(flags)
    return client_request(addr, msg)


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def ttl_query(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "ttl", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


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


class TestSetExPxStore(unittest.TestCase):
    def test_store_ex_records_wall_deadline_and_drops_log_deadline(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(store.expiries(), {"k": 15})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "wall_deadline": now + 60}, index=11),
            {"type": "client_response", "status": "ok", "set": 1})
        self.assertEqual(store.get("k"), "v2")
        # The wall deadline replaced the log one: a key holds at
        # most one kind of TTL.
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.wall_expiries(), {"k": now + 60})
        self.assertEqual(store.expire_wall_up_to(now + 59), [])
        self.assertEqual(store.expire_wall_up_to(now + 60), ["k"])
        self.assertEqual(store.wall_expiries(), {})

    def test_store_ex_with_get_reports_old_value(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "wall_deadline": now + 60, "get": True}),
            {"type": "client_response", "status": "ok", "set": 1,
             "old_value": "first"})
        self.assertEqual(store.get("k"), "second")
        self.assertEqual(store.wall_expiries(), {"k": now + 60})

    def test_store_plain_set_clears_ex_deadline(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "wall_deadline": now + 60})
        self.assertEqual(store.wall_expiries(), {"k": now + 60})
        store.apply({"op": "set", "key": "k", "value": "v2"})
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.wall_expiries(), {})

    def test_store_losing_nx_with_ex_writes_nothing(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "first",
                     "wall_deadline": now + 60})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "nx": True, "wall_deadline": now + 120}),
            {"type": "client_response", "status": "ok", "set": 0})
        # The losing set wrote nothing: value and deadline both
        # untouched.
        self.assertEqual(store.get("k"), "first")
        self.assertEqual(store.wall_expiries(), {"k": now + 60})

    def test_store_losing_xx_with_ex_writes_nothing(self):
        store = KVStore()
        now = time.time()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "xx": True, "wall_deadline": now + 60}),
            {"type": "client_response", "status": "ok", "set": 0})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.wall_expiries(), {})

    def test_store_ex_keeps_sorted_index(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "wall_deadline": now + 60})
        self.assertEqual(store.keys(), (["k"], False))
        store.expire_wall_up_to(now + 60)
        self.assertEqual(store.keys(), ([], False))


class TestSetExPxCluster(unittest.TestCase):
    def test_set_ex_readable_then_expires(self):
        """A key set with EX 1 is readable at once and gone a
        little past the deadline — on the leader and on every
        follower, at the same wall-clock moment."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = write(addr, "temp", "v", ex=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(read(addr, "temp").get("value"), "v")
            time.sleep(1.3)
            self.assertIsNone(read(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_set_px_readable_then_expires(self):
        """PX names the same deadline in milliseconds: a key set
        with PX 500 is readable at once and gone well inside a
        second."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = time.time()
            resp = write(addr, "temp", "v", px=500)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(read(addr, "temp").get("value"), "v")
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("status"), "ok", got)
            wall_deadline = got.get("wall_deadline")
            self.assertIsNotNone(wall_deadline)
            self.assertTrue(before + 0.4 <= wall_deadline
                            <= time.time() + 0.5, wall_deadline)
            time.sleep(0.9)
            self.assertIsNone(read(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_ttl_query_reports_ex_deadline(self):
        """A TTL query reports the wall side of an EX set too:
        ``wall_ttl`` the seconds left and ``wall_deadline`` the
        absolute Unix time — while the log-entry TTL stays -1
        with no deadline."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = time.time()
            resp = write(addr, "temp", "v", ex=60)
            self.assertEqual(resp.get("status"), "ok", resp)
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("ttl"), -1)
            self.assertIsNone(got.get("deadline"))
            wall_deadline = got.get("wall_deadline")
            self.assertIsNotNone(wall_deadline)
            self.assertTrue(before + 55 <= wall_deadline
                            <= time.time() + 60, wall_deadline)
            self.assertTrue(55 <= got.get("wall_ttl") <= 60,
                            got.get("wall_ttl"))
        finally:
            cluster.stop_all()

    def test_set_ex_clears_log_deadline_on_cluster(self):
        """An EX set drops the key's log-entry deadline: the TTL
        query shows no log deadline and a wall one, and the key
        then dies by the wall clock, not by the log."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = write(addr, "temp", "v", ttl_entries=100)
            self.assertEqual(resp.get("status"), "ok", resp)
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("ttl"), 100, got)
            self.assertIsNone(got.get("wall_deadline"), got)
            resp = write(addr, "temp", "v2", ex=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("ttl"), -1, got)
            self.assertIsNotNone(got.get("wall_deadline"), got)
            time.sleep(1.3)
            self.assertIsNone(read(addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_set_ex_rejects_bad_values(self):
        """Bad EX / PX values, EX and PX together, EX/PX with a
        log-entry TTL, and EX/PX with KEEPTTL are all client
        errors — and append no log entry, so the key is never
        written either."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            bad = [
                {"ex": 0}, {"ex": -1}, {"ex": "5"}, {"ex": True},
                {"ex": 1_000_001}, {"ex": 1.5},
                {"px": 0}, {"px": -1}, {"px": "500"}, {"px": True},
                {"px": 1_000_000_001}, {"px": 1.5},
                {"ex": 60, "px": 500},
                {"ex": 60, "ttl_entries": 10},
                {"px": 500, "ttl_entries": 10},
                {"ex": 60, "keepttl": True},
                {"px": 500, "keepttl": True},
            ]
            for flags in bad:
                resp = write(addr, "k", "v", **flags)
                self.assertEqual(resp.get("status"), "error",
                                 (flags, resp))
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(read(addr, "k").get("value"))
        finally:
            cluster.stop_all()

    def test_set_ex_retry_returns_first_result(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            msg = {"type": "client", "op": "set", "key": "k",
                   "value": "v", "ex": 60,
                   "client_id": "c1", "request_id": "r1"}
            before = time.time()
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            first_deadline = ttl_query(addr, "k").get("wall_deadline")
            self.assertIsNotNone(first_deadline)
            self.assertTrue(before + 55 <= first_deadline
                            <= time.time() + 60, first_deadline)
            # The retry — with a different EX — returns the first
            # attempt's result and appends no second entry, and
            # the deadline is still the first attempt's.
            resp = client_request(addr, dict(msg, ex=600))
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(ttl_query(addr, "k").get("wall_deadline"),
                             first_deadline)
        finally:
            cluster.stop_all()

    def test_set_ex_fires_del_watch_event_on_sweep(self):
        """A watch on a key set with EX fires a del event (value
        None) once the deadline passes — the sweep retires the
        key and fires the event even though no log entry landed
        to purge it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("temp", log)
            resp = write(addr, "temp", "v", ex=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(log.events[0]["op"], "set")
            self.assertEqual(log.events[0]["value"], "v")
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1]["op"], "del")
            self.assertIsNone(log.events[1]["value"])
        finally:
            cluster.stop_all()

    def test_set_ex_survives_snapshot_and_restart(self):
        """An EX deadline survives log compaction and a restart:
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
            resp = write(addr, "temp", "v", ex=4)
            self.assertEqual(resp.get("status"), "ok", resp)
            for i in range(15):
                self.assertEqual(write(addr, f"snap{i}", "x")
                                 .get("status"), "ok")
            self.assertGreater(leader.snapshot_index, 0)
            self.assertEqual(read(addr, "temp").get("value"), "v")
            cluster.stop_node(leader.id)
            restarted = cluster.restart_node(leader.id)
            cluster.wait_for_stable_leader()
            new_addr = cluster.addrs[restarted.id]
            self.assertEqual(read(new_addr, "temp").get("value"), "v")
            wall = restarted.sm.wall_expiries()
            self.assertIn("temp", wall)
            self.assertTrue(started + 3 <= wall["temp"] <= started + 5,
                            wall["temp"])
            remaining = wall["temp"] - time.time() + 0.5
            if remaining > 0:
                time.sleep(remaining)
            self.assertIsNone(read(new_addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_session_set_ex_px(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("k", "v", ex=60)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("set"), 1, resp)
                got = session.ttl("k")
                self.assertEqual(got.get("status"), "ok", got)
                self.assertEqual(got.get("ttl"), -1, got)
                self.assertIsNotNone(got.get("wall_deadline"), got)
                self.assertEqual(session.get("k").get("value"), "v")
                # PX through the same session, and a losing NX
                # with EX through it too.
                resp = session.set("k2", "v", px=60000)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertIsNotNone(
                    session.ttl("k2").get("wall_deadline"))
                resp = session.set("k", "other", nx=True, ex=60)
                self.assertEqual(resp.get("set"), 0, resp)
                self.assertEqual(session.get("k").get("value"), "v")
                self.assertIsNotNone(
                    session.ttl("k").get("wall_deadline"))
        finally:
            cluster.stop_all()

    def test_pipeline_set_ex_px(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "a", "value": "1", "ex": 60},
                    {"op": "set", "key": "b", "value": "2", "px": 60000},
                    {"op": "set", "key": "a", "value": "3", "ex": 0},
                    {"op": "set", "key": "a", "value": "3",
                     "ex": 60, "px": 500},
                    {"op": "set", "key": "a", "value": "3",
                     "ex": 60, "keepttl": True},
                    {"op": "set", "key": "a", "value": "3",
                     "px": "500"},
                ])
            self.assertEqual(len(replies), 6, replies)
            self.assertEqual(replies[0].get("set"), 1, replies)
            self.assertEqual(replies[1].get("set"), 1, replies)
            for bad in replies[2:]:
                self.assertEqual(bad.get("status"), "error", replies)
            self.assertIsNotNone(ttl_query(addr, "a")
                                 .get("wall_deadline"))
            self.assertIsNotNone(ttl_query(addr, "b")
                                 .get("wall_deadline"))
            self.assertEqual(read(addr, "a").get("value"), "1")
        finally:
            cluster.stop_all()

    def test_set_ex_px_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # --ex writes the key with a wall deadline.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v",
                                  "--ex", "60"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "OK")
            self.assertIsNotNone(ttl_query(cluster.addrs[leader.id],
                                           "k").get("wall_deadline"))
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v")
            # --px writes the key with a wall deadline too.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k2", "v",
                                  "--px", "60000"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "OK")
            self.assertIsNotNone(ttl_query(cluster.addrs[leader.id],
                                           "k2").get("wall_deadline"))
            # --ex and --px together are rejected by argparse.
            with self.assertRaises(SystemExit):
                client_main(["--node", node, "set", "k3", "v",
                             "--ex", "60", "--px", "500"])
            # A bad --ex value is a server error, exit code 1.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "set", "k3", "v",
                                  "--ex", "0"])
            self.assertEqual(rc, 1)
            self.assertIsNone(read(cluster.addrs[leader.id], "k3")
                              .get("value"))
            # --ex composes with --get: the old value prints.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v2",
                                  "--ex", "60", "--get"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "v")
            self.assertIsNotNone(ttl_query(cluster.addrs[leader.id],
                                           "k").get("wall_deadline"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
