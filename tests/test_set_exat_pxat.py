"""Tests for raftkv SET with EXAT / PXAT.

A ``set`` may carry Redis's SET EXAT / PXAT options: an
absolute wall-clock deadline inline on SET — EXAT names it
as Unix seconds, PXAT as Unix milliseconds — instead of the
separate ``expireat`` / ``pexpireat`` ops. The timestamp
itself is the deadline, recorded as-is by the leader, so
every replica applies the same one, and the key dies when
the wall clock reaches it — enforced by the background
sweep, by a purge before each log entry applies, and by a
purge before serving a read, exactly like a ``setex``
deadline. EXAT and PXAT are mutually exclusive, and neither
rides with EX or PX, with a log-entry TTL (``ttl_entries``),
or with KEEPTTL — Redis rejects all of those combinations.
A winning EXAT/PXAT set is a fresh life for the key: any
log-entry deadline it had is dropped, so a key still holds
at most one kind of TTL; a losing conditional set writes
nothing at all, deadline included. A deadline already past
at log time deletes the key outright at apply time, like an
``expireat`` whose deadline was already past: the set still
answers ``set`` 1, with ``deleted`` set when a key actually
went, and the apply path fires a ``del`` watch event for it.
It composes with NX / XX and with GET exactly like the
other flags.
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


class TestSetExatPxatStore(unittest.TestCase):
    def test_store_exat_records_wall_deadline_and_drops_log_deadline(self):
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

    def test_store_exat_with_get_reports_old_value(self):
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

    def test_store_plain_set_clears_exat_deadline(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "wall_deadline": now + 60})
        self.assertEqual(store.wall_expiries(), {"k": now + 60})
        store.apply({"op": "set", "key": "k", "value": "v2"})
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.wall_expiries(), {})

    def test_store_losing_nx_with_exat_writes_nothing(self):
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

    def test_store_losing_xx_with_exat_writes_nothing(self):
        store = KVStore()
        now = time.time()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "xx": True, "wall_deadline": now + 60}),
            {"type": "client_response", "status": "ok", "set": 0})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.wall_expiries(), {})

    def test_store_past_exat_deletes_existing_key(self):
        # A deadline already past at log time deletes the key
        # outright at apply time: the set still answers 1,
        # with ``deleted`` set, and the old value rides along
        # with GET — the value itself is never recorded.
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "first",
                     "wall_deadline": now + 60})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "wall_deadline": now - 10,
                         "delete_now": True, "get": True}),
            {"type": "client_response", "status": "ok", "set": 1,
             "deleted": True, "old_value": "first"})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.wall_expiries(), {})
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.keys(), ([], False))

    def test_store_past_pxat_deletes_existing_key(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "wall_deadline": now - 0.5,
                         "delete_now": True}),
            {"type": "client_response", "status": "ok", "set": 1,
             "deleted": True})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.keys(), ([], False))

    def test_store_past_exat_on_missing_key_writes_nothing(self):
        # A past deadline on a key that does not exist writes
        # nothing at all: no value, no deadline, no sorted-index
        # entry — and no ``deleted``, since there was nothing
        # to delete.
        store = KVStore()
        now = time.time()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "wall_deadline": now - 10,
                         "delete_now": True, "get": True}),
            {"type": "client_response", "status": "ok", "set": 1,
             "old_value": None})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.keys(), ([], False))
        self.assertEqual(store.wall_expiries(), {})

    def test_store_losing_nx_with_past_exat_deletes_nothing(self):
        # A losing conditional set never reaches the delete:
        # NX loses, so the existing key and its deadline stay
        # exactly as they were.
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "first",
                     "wall_deadline": now + 60})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "nx": True, "wall_deadline": now - 10,
                         "delete_now": True}),
            {"type": "client_response", "status": "ok", "set": 0})
        self.assertEqual(store.get("k"), "first")
        self.assertEqual(store.wall_expiries(), {"k": now + 60})

    def test_store_exat_keeps_sorted_index(self):
        store = KVStore()
        now = time.time()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "wall_deadline": now + 60})
        self.assertEqual(store.keys(), (["k"], False))
        store.expire_wall_up_to(now + 60)
        self.assertEqual(store.keys(), ([], False))


class TestSetExatPxatCluster(unittest.TestCase):
    def test_set_exat_readable_then_expires(self):
        """A key set with EXAT 2 seconds ahead is readable at
        once and gone a little past the deadline — on the
        leader and on every follower, at the same wall-clock
        moment."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            deadline = int(time.time()) + 2
            resp = write(addr, "temp", "v", exat=deadline)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(read(addr, "temp").get("value"), "v")
            time.sleep(2.4)
            self.assertIsNone(read(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_set_pxat_readable_then_expires(self):
        """PXAT names the same deadline in milliseconds: a key
        set with PXAT 500ms ahead is readable at once and gone
        well inside a second — and the TTL query reports the
        exact timestamp the client named."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            deadline_ms = int(time.time() * 1000) + 500
            resp = write(addr, "temp", "v", pxat=deadline_ms)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(read(addr, "temp").get("value"), "v")
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("wall_deadline"),
                             deadline_ms / 1000.0, got)
            time.sleep(0.9)
            self.assertIsNone(read(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
        finally:
            cluster.stop_all()

    def test_ttl_query_reports_exat_deadline(self):
        """A TTL query reports the wall side of an EXAT set too:
        ``wall_ttl`` the seconds left and ``wall_deadline`` the
        exact Unix time the client named — while the log-entry
        TTL stays -1 with no deadline."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            deadline = int(time.time()) + 60
            resp = write(addr, "temp", "v", exat=deadline)
            self.assertEqual(resp.get("status"), "ok", resp)
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("ttl"), -1)
            self.assertIsNone(got.get("deadline"))
            self.assertEqual(got.get("wall_deadline"),
                             float(deadline), got)
            self.assertTrue(55 <= got.get("wall_ttl") <= 60,
                            got.get("wall_ttl"))
        finally:
            cluster.stop_all()

    def test_set_exat_clears_log_deadline_on_cluster(self):
        """An EXAT set drops the key's log-entry deadline: the
        TTL query shows no log deadline and a wall one, and
        the key then dies by the wall clock, not by the log."""
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
            resp = write(addr, "temp", "v2",
                         exat=int(time.time()) + 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            got = ttl_query(addr, "temp")
            self.assertEqual(got.get("ttl"), -1, got)
            self.assertIsNotNone(got.get("wall_deadline"), got)
            time.sleep(1.3)
            self.assertIsNone(read(addr, "temp").get("value"))
        finally:
            cluster.stop_all()

    def test_set_exat_rejects_bad_values(self):
        """Bad EXAT / PXAT values, EXAT and PXAT together,
        EXAT/PXAT with EX/PX, with a log-entry TTL, and with
        KEEPTTL are all client errors — and append no log
        entry, so the key is never written either."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            future = int(time.time()) + 60
            future_ms = int(time.time() * 1000) + 60000
            bad = [
                {"exat": 0}, {"exat": -1}, {"exat": "5"},
                {"exat": True}, {"exat": 1.5},
                {"pxat": 0}, {"pxat": -1}, {"pxat": "500"},
                {"pxat": True}, {"pxat": 1.5},
                {"exat": future, "pxat": future_ms},
                {"exat": future, "ex": 60},
                {"pxat": future_ms, "px": 500},
                {"exat": future, "ttl_entries": 10},
                {"pxat": future_ms, "ttl_entries": 10},
                {"exat": future, "keepttl": True},
                {"pxat": future_ms, "keepttl": True},
            ]
            for flags in bad:
                resp = write(addr, "k", "v", **flags)
                self.assertEqual(resp.get("status"), "error",
                                 (flags, resp))
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(read(addr, "k").get("value"))
        finally:
            cluster.stop_all()

    def test_set_exat_retry_returns_first_result(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            first = int(time.time()) + 60
            msg = {"type": "client", "op": "set", "key": "k",
                   "value": "v", "exat": first,
                   "client_id": "c1", "request_id": "r1"}
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(ttl_query(addr, "k").get("wall_deadline"),
                             float(first))
            # The retry — with a different EXAT — returns the
            # first attempt's result and appends no second
            # entry, and the deadline is still the first
            # attempt's.
            resp = client_request(addr, dict(msg,
                                             exat=first + 600))
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(ttl_query(addr, "k").get("wall_deadline"),
                             float(first))
        finally:
            cluster.stop_all()

    def test_set_past_exat_deletes_at_apply_time(self):
        """A set whose EXAT is already past deletes the key
        outright when the entry applies — on every node, with
        ``deleted`` in the reply — instead of recording a
        deadline nobody could honour."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = write(addr, "temp", "old")
            self.assertEqual(resp.get("status"), "ok", resp)
            past = int(time.time()) - 10
            resp = write(addr, "temp", "new", exat=past, get=True)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertTrue(resp.get("deleted"), resp)
            self.assertEqual(resp.get("old_value"), "old", resp)
            self.assertIsNone(read(addr, "temp").get("value"))
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "temp", None),
                                node.id)
            # A past deadline on a missing key writes nothing:
            # the set still answers 1, but with no ``deleted``
            # and the key still absent.
            resp = write(addr, "gone", "v", exat=past)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertNotIn("deleted", resp)
            self.assertIsNone(read(addr, "gone").get("value"))
        finally:
            cluster.stop_all()

    def test_set_past_exat_fires_del_watch_event(self):
        """A watch on a key set with a past EXAT fires a del
        event (value None) at apply time — not a set event —
        and a past EXAT on a missing key fires nothing at
        all."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("temp", log)
            resp = write(addr, "temp", "v")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(log.events[0]["op"], "set")
            past = int(time.time()) - 10
            resp = write(addr, "temp", "v2", exat=past)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(resp.get("deleted"), resp)
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1]["op"], "del")
            self.assertIsNone(log.events[1]["value"])
            # A past deadline on a missing key fires nothing.
            log2 = EventLog()
            leader.watch.watch("gone", log2)
            resp = write(addr, "gone", "v", exat=past)
            self.assertEqual(resp.get("status"), "ok", resp)
            time.sleep(0.3)
            self.assertEqual(log2.events, [])
        finally:
            cluster.stop_all()

    def test_set_exat_fires_del_watch_event_on_sweep(self):
        """A watch on a key set with a future EXAT fires a del
        event (value None) once the deadline passes — the
        sweep retires the key and fires the event even though
        no log entry landed to purge it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("temp", log)
            resp = write(addr, "temp", "v",
                         exat=int(time.time()) + 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(log.wait_for(1), log.events)
            self.assertEqual(log.events[0]["op"], "set")
            self.assertEqual(log.events[0]["value"], "v")
            self.assertTrue(log.wait_for(2), log.events)
            self.assertEqual(log.events[1]["op"], "del")
            self.assertIsNone(log.events[1]["value"])
        finally:
            cluster.stop_all()

    def test_session_set_exat_pxat(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("k", "v",
                                   exat=int(time.time()) + 60)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("set"), 1, resp)
                got = session.ttl("k")
                self.assertEqual(got.get("status"), "ok", got)
                self.assertEqual(got.get("ttl"), -1, got)
                self.assertIsNotNone(got.get("wall_deadline"), got)
                self.assertEqual(session.get("k").get("value"), "v")
                # PXAT through the same session, and a losing NX
                # with EXAT through it too.
                resp = session.set(
                    "k2", "v",
                    pxat=int(time.time() * 1000) + 60000)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertIsNotNone(
                    session.ttl("k2").get("wall_deadline"))
                resp = session.set("k", "other", nx=True,
                                   exat=int(time.time()) + 60)
                self.assertEqual(resp.get("set"), 0, resp)
                self.assertEqual(session.get("k").get("value"), "v")
                self.assertIsNotNone(
                    session.ttl("k").get("wall_deadline"))
        finally:
            cluster.stop_all()

    def test_pipeline_set_exat_pxat(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            future = int(time.time()) + 60
            future_ms = int(time.time() * 1000) + 60000
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "a", "value": "1",
                     "exat": future},
                    {"op": "set", "key": "b", "value": "2",
                     "pxat": future_ms},
                    {"op": "set", "key": "a", "value": "3",
                     "exat": 0},
                    {"op": "set", "key": "a", "value": "3",
                     "exat": future, "pxat": future_ms},
                    {"op": "set", "key": "a", "value": "3",
                     "exat": future, "ex": 60},
                    {"op": "set", "key": "a", "value": "3",
                     "exat": future, "keepttl": True},
                    {"op": "set", "key": "a", "value": "3",
                     "pxat": "500"},
                ])
            self.assertEqual(len(replies), 7, replies)
            self.assertEqual(replies[0].get("set"), 1, replies)
            self.assertEqual(replies[1].get("set"), 1, replies)
            for bad in replies[2:]:
                self.assertEqual(bad.get("status"), "error", replies)
            self.assertEqual(ttl_query(addr, "a")
                             .get("wall_deadline"), float(future))
            self.assertEqual(ttl_query(addr, "b")
                             .get("wall_deadline"),
                             future_ms / 1000.0)
            self.assertEqual(read(addr, "a").get("value"), "1")
        finally:
            cluster.stop_all()

    def test_set_exat_pxat_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # --exat writes the key with a wall deadline.
            deadline = int(time.time()) + 60
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v",
                                  "--exat", str(deadline)])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "OK")
            self.assertEqual(ttl_query(cluster.addrs[leader.id],
                                       "k").get("wall_deadline"),
                             float(deadline))
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v")
            # --pxat writes the key with a wall deadline too.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k2", "v",
                                  "--pxat",
                                  str(int(time.time() * 1000)
                                      + 60000)])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "OK")
            self.assertIsNotNone(ttl_query(cluster.addrs[leader.id],
                                           "k2").get("wall_deadline"))
            # --exat and --pxat together are rejected by argparse.
            with self.assertRaises(SystemExit):
                client_main(["--node", node, "set", "k3", "v",
                             "--exat", str(int(time.time()) + 60),
                             "--pxat",
                             str(int(time.time() * 1000) + 60000)])
            # A bad --exat value is a server error, exit code 1.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "set", "k3", "v",
                                  "--exat", "0"])
            self.assertEqual(rc, 1)
            self.assertIsNone(read(cluster.addrs[leader.id], "k3")
                              .get("value"))
            # --exat composes with --get: the old value prints.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v2",
                                  "--exat",
                                  str(int(time.time()) + 60),
                                  "--get"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "v")
            self.assertIsNotNone(ttl_query(cluster.addrs[leader.id],
                                           "k").get("wall_deadline"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
