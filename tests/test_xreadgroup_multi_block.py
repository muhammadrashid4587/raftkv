"""Tests for raftkv BLOCK on multi-stream XREADGROUP (v0.78.0).

``xreadgroup_multi [[key, id], ...] group consumer [--block ms]``
waits on the leader for a new entry on any of its ">" streams when
no stream has anything to hand out yet — Redis XREADGROUP BLOCK
with STREAMS — watching every ">" stream key at once, so an entry
on the second stream wakes a wait that started on the first. The
reply carries ``timed_out`` when the wait expired with nothing
delivered; BLOCK 0 waits up to the server's 25s cap, like a watch.
A call with entries already waiting on any stream answers at once,
as does a call whose key or group is missing on some stream, and
with explicit IDs only the block is ignored, like a single-stream
explicit-ID read. The store half runs against a bare KVStore with
fixed stamps, so the peek behind the wait is exact; the cluster
half spins up real nodes (real TCP, real threads, real disk) on
localhost, like the rest of the suite.
"""

import contextlib
import io
import json
import threading
import time
import unittest

from harness import Cluster, client_request
from raftkv.client import Session, main as client_main
from raftkv.client import xreadgroup_multi as client_xreadgroup_multi
from test_xreadgroup import groups_of
from test_xreadgroup_multi import _seed_store

T0 = 1700000000000


def _multi(addr, streams=(("s1", ">"), ("s2", ">")),
           group="g", consumer="c1", count=None, noack=None,
           block=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xreadgroup_multi",
           "streams": [[k, i] for k, i in streams],
           "group": group, "consumer": consumer}
    if count is not None:
        msg["count"] = count
    if noack is not None:
        msg["noack"] = noack
    if block is not None:
        msg["block"] = block
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def _xadd(addr, key, entry_id, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xadd", "key": key,
           "pairs": [["a", entry_id]], "id": entry_id}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def _xgroup_create(addr, key, group="g", start_id="$",
                   mkstream=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xgroup_create", "key": key,
           "group": group, "start_id": start_id}
    if mkstream is not None:
        msg["mkstream"] = mkstream
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def _count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestXReadGroupMultiBlockStore(unittest.TestCase):
    def test_peek_hands_out_new_entries_across_streams(self):
        """The peek behind a blocking multi-stream read hands out
        every new entry on each stream, in stream order — the same
        streams a ">" read would hand out, without handing
        anything out."""
        store = _seed_store()
        peeked = store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c1")
        self.assertEqual(peeked,
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        # A peek never hands anything out: the pending lists stay
        # empty and the last deliveries stay where they were, so a
        # second peek answers exactly the same.
        for key in ("s1", "s2"):
            self.assertEqual(
                groups_of(store.get(key))["g"]["pending"], {})
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c1"), peeked)

    def test_peek_shares_count_budget(self):
        """The peek shares one count budget across the streams, in
        the order they were given: a budget the first stream fills
        leaves nothing for the second."""
        store = _seed_store()
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c1", count=4),
            [["s1", [["100-5", {"a": "100-5"}],
                     ["100-6", {"a": "100-6"}],
                     ["101-0", {"a": "101-0"}]]],
             ["s2", [["200-1", {"b": "200-1"}]]]])
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c1", count=2),
            [["s1", [["100-5", {"a": "100-5"}],
                     ["100-6", {"a": "100-6"}]]]])

    def test_peek_explicit_id_rereads_own_pending(self):
        """The peek re-reads the consumer's own pending entries on
        an explicit-ID stream, and hands out nothing new on a ">"
        stream whose entries were already delivered."""
        store = _seed_store()
        store.apply({"op": "xreadgroup_multi",
                     "streams": [["s1", ">"], ["s2", ">"]],
                     "group": "g", "consumer": "c1",
                     "seen_ms": T0})
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", "0"], ["s2", "0"]], "g", "c1"),
            [["s1", [["100-5", {"a": "100-5"}],
                     ["100-6", {"a": "100-6"}],
                     ["101-0", {"a": "101-0"}]]],
             ["s2", [["200-1", {"b": "200-1"}],
                     ["200-2", {"b": "200-2"}]]]])
        # Nothing new anywhere: the ">" peek is empty, so a
        # blocking ">" read would wait.
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c1"), [])
        # A re-read only returns the reading consumer's entries.
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", "0"], ["s2", "0"]], "g", "c2"), [])

    def test_peek_none_for_missing_key_group_wrongtype(self):
        """A missing key, a missing group, or a plain string value
        on any stream peeks None: there is nothing to wait for,
        since the write itself answers the error at apply time."""
        store = _seed_store()
        self.assertIsNone(store.xreadgroup_multi_peek(
            [["s1", ">"], ["gone", ">"]], "g", "c1"))
        self.assertIsNone(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "nope", "c1"))
        store.apply({"op": "set", "key": "plain", "value": "v"})
        self.assertIsNone(store.xreadgroup_multi_peek(
            [["plain", ">"], ["s2", ">"]], "g", "c1"))
        # But a stream with nothing new peeks an empty list, not
        # None: the group exists, it just has nothing to hand out.
        store.apply({"op": "xreadgroup_multi",
                     "streams": [["s1", ">"], ["s2", ">"]],
                     "group": "g", "consumer": "c1",
                     "seen_ms": T0})
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c1"), [])

    def test_peek_bad_arguments_raise(self):
        """A bad group, consumer, stream list, or count raises
        instead of peeking nonsense."""
        store = _seed_store()
        for kwargs in ({"group": ""},
                       {"group": 7},
                       {"consumer": ""},
                       {"streams": []},
                       {"streams": [["s1", ">"], ["s2"]]},
                       {"streams": [["", ">"]]},
                       {"streams": [["s1", 5]]},
                       {"count": -1},
                       {"count": True}):
            args: dict = {"streams": [["s1", ">"], ["s2", ">"]],
                          "group": "g", "consumer": "c1"}
            args.update(kwargs)
            with self.assertRaises(ValueError, msg=str(kwargs)):
                store.xreadgroup_multi_peek(**args)

    def test_peek_repeated_key_reads_first_pass(self):
        """A key named twice reads the first pass's would-be
        delivery on the second, like the apply path: a second ">"
        pass hands out nothing, since the first already moved the
        last delivery past every entry."""
        store = _seed_store()
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", ">"], ["s1", ">"]], "g", "c1"),
            [["s1", [["100-5", {"a": "100-5"}],
                     ["100-6", {"a": "100-6"}],
                     ["101-0", {"a": "101-0"}]]]])
        store.apply({"op": "xreadgroup_multi",
                     "streams": [["s1", ">"]],
                     "group": "g", "consumer": "c1",
                     "seen_ms": T0})
        # The "0" pass re-reads the pending entries the ">"
        # delivery left, and the ">" pass after it hands out
        # nothing new — the peek carries the stream once.
        self.assertEqual(store.xreadgroup_multi_peek(
            [["s1", "0"], ["s1", ">"]], "g", "c1"),
            [["s1", [["100-5", {"a": "100-5"}],
                     ["100-6", {"a": "100-6"}],
                     ["101-0", {"a": "101-0"}]]]])


class TestXReadGroupMultiBlockCluster(unittest.TestCase):
    def test_block_waits_for_new_entry_on_second_stream(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   mkstream=True).get("status"), "ok")
            # Nothing new to hand out on either stream: a blocking
            # multi-stream read waits, and an entry on the second
            # stream wakes it.
            results: list[dict] = []
            thread = threading.Thread(
                target=lambda: results.append(
                    client_xreadgroup_multi(
                        addr, [["s1", ">"], ["s2", ">"]], "g", "c1",
                        block=5000)))
            started = time.monotonic()
            thread.start()
            time.sleep(0.5)
            resp = _xadd(addr, "s2", "200-1")
            self.assertEqual(resp.get("status"), "ok", resp)
            thread.join(timeout=10.0)
            self.assertFalse(thread.is_alive())
            elapsed = time.monotonic() - started
            self.assertEqual(len(results), 1, results)
            resp = results[0]
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["s2", [["200-1", {"a": "200-1"}]]]],
                             resp)
            self.assertIs(resp.get("timed_out"), False, resp)
            # The wait really waited for the entry, and the
            # delivery went through the log: the entry is pending
            # for c1 on every node.
            self.assertGreaterEqual(elapsed, 0.4)
            self.assertLess(elapsed, 8.0)
            self.assertEqual(
                groups_of(leader.sm.get("s2"))["g"]["pending"]
                ["200-1"]["consumer"], "c1")
        finally:
            cluster.stop_all()

    def test_block_answers_at_once_when_entries_waiting(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            _xadd(addr, "s2", "200-1")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   start_id="0").get("status"), "ok")
            # Entries are already waiting on both streams: the
            # blocking read answers at once, without waiting.
            started = time.monotonic()
            resp = client_xreadgroup_multi(
                addr, [["s1", ">"], ["s2", ">"]], "g", "c1",
                block=5000)
            elapsed = time.monotonic() - started
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["s1", [["100-5", {"a": "100-5"}]]],
                              ["s2", [["200-1", {"a": "200-1"}]]]],
                             resp)
            self.assertIs(resp.get("timed_out"), False, resp)
            self.assertLess(elapsed, 3.0)
        finally:
            cluster.stop_all()

    def test_block_times_out_with_empty_streams(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   mkstream=True).get("status"), "ok")
            started = time.monotonic()
            resp = client_xreadgroup_multi(
                addr, [["s1", ">"], ["s2", ">"]], "g", "c1",
                block=1000)
            elapsed = time.monotonic() - started
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"), [], resp)
            self.assertIs(resp.get("timed_out"), True, resp)
            self.assertGreaterEqual(elapsed, 0.9)
            self.assertLess(elapsed, 8.0)
        finally:
            cluster.stop_all()

    def test_block_zero_waits_until_entry_arrives(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key, start_id="$",
                                   mkstream=True).get("status"), "ok")
            results: list[dict] = []
            thread = threading.Thread(
                target=lambda: results.append(
                    client_xreadgroup_multi(
                        addr, [["s1", ">"], ["s2", ">"]], "g", "c1",
                        block=0)))
            started = time.monotonic()
            thread.start()
            time.sleep(0.5)
            resp = _xadd(addr, "s1", "100-5")
            self.assertEqual(resp.get("status"), "ok", resp)
            thread.join(timeout=10.0)
            self.assertFalse(thread.is_alive())
            elapsed = time.monotonic() - started
            self.assertEqual(len(results), 1, results)
            self.assertEqual(results[0].get("streams"),
                             [["s1", [["100-5", {"a": "100-5"}]]]],
                             results[0])
            self.assertIs(results[0].get("timed_out"), False,
                          results[0])
            self.assertGreaterEqual(elapsed, 0.4)
            self.assertLess(elapsed, 8.0)
        finally:
            cluster.stop_all()

    def test_block_ignored_for_explicit_ids_only(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            _xadd(addr, "s2", "200-1")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   start_id="0").get("status"), "ok")
            first = _multi(addr)
            self.assertEqual(first.get("status"), "ok", first)
            # A block on an explicit-ID-only re-read answers at
            # once with the consumer's pending entries: there is
            # nothing new to wait for, and the reply carries no
            # timed_out.
            started = time.monotonic()
            resp = client_xreadgroup_multi(
                addr, [["s1", "0"], ["s2", "0"]], "g", "c1",
                block=5000)
            elapsed = time.monotonic() - started
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["s1", [["100-5", {"a": "100-5"}]]],
                              ["s2", [["200-1", {"a": "200-1"}]]]],
                             resp)
            self.assertNotIn("timed_out", resp)
            self.assertLess(elapsed, 3.0)
        finally:
            cluster.stop_all()

    def test_block_missing_group_fails_immediately(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            # No stream at all: the wait is skipped and the
            # no_group failure comes back at once, not after the
            # block expires.
            started = time.monotonic()
            resp = client_xreadgroup_multi(
                addr, [["s1", ">"], ["s2", ">"]], "g", "c1",
                block=5000)
            elapsed = time.monotonic() - started
            self.assertEqual(resp.get("status"), "xreadgroup_failed",
                             resp)
            self.assertEqual(resp.get("reason"), "no_group", resp)
            self.assertLess(elapsed, 3.0)
        finally:
            cluster.stop_all()

    def test_blocked_multi_retry_returns_first_result(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            _xadd(addr, "s2", "200-1")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   start_id="0").get("status"), "ok")
            first = _multi(addr, block=5000,
                           client_id="c1", request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            self.assertEqual(first.get("streams"),
                             [["s1", [["100-5", {"a": "100-5"}]]],
                              ["s2", [["200-1", {"a": "200-1"}]]]],
                             first)
            self.assertIs(first.get("timed_out"), False, first)
            # The retry skips the wait: the first attempt already
            # applied, so its result comes back at once and the
            # entries are delivered exactly once.
            started = time.monotonic()
            second = _multi(addr, block=5000,
                            client_id="c1", request_id="r1")
            elapsed = time.monotonic() - started
            self.assertEqual(second.get("streams"),
                             first.get("streams"), second)
            self.assertEqual(_count_entries(leader, "r1"), 1)
            self.assertEqual(
                groups_of(leader.sm.get("s1"))["g"]["pending"]
                ["100-5"]["consumer"], "c1")
            self.assertLess(elapsed, 2.0)
        finally:
            cluster.stop_all()

    def test_invalid_block_args_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            self.assertEqual(
                _xgroup_create(addr, "s1",
                               start_id="0").get("status"), "ok")
            before = len(leader.log)
            for msg in ({"block": -1}, {"block": "x"},
                        {"block": True}):
                resp = _multi(addr, streams=[("s1", ">")], **msg)
                self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(len(leader.log), before)
            # The Session surface rejects a bad block the same way,
            # before anything goes on the wire.
            with Session(addr) as session:
                resp = session.xreadgroup_multi(
                    [["s1", ">"]], "g", "c1", block=-1)
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xreadgroup_multi(
                    [["s1", ">"]], "g", "c1", block="x")
                self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(len(leader.log), before)
        finally:
            cluster.stop_all()

    def test_module_function_from_follower_with_block(self):
        """The module-level xreadgroup_multi follows the redirect
        from a follower and blocks there too, so a caller holding
        any node's address gets the same wait."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower_id = next(n for n in cluster.live
                               if n != leader.id)
            follower_addr = cluster.addrs[follower_id]
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   mkstream=True).get("status"), "ok")
            results: list[dict] = []
            thread = threading.Thread(
                target=lambda: results.append(
                    client_xreadgroup_multi(
                        follower_addr, [["s1", ">"], ["s2", ">"]],
                        "g", "c1", block=5000)))
            started = time.monotonic()
            thread.start()
            time.sleep(0.5)
            resp = _xadd(addr, "s2", "200-1")
            self.assertEqual(resp.get("status"), "ok", resp)
            thread.join(timeout=10.0)
            self.assertFalse(thread.is_alive())
            elapsed = time.monotonic() - started
            self.assertEqual(len(results), 1, results)
            self.assertEqual(results[0].get("streams"),
                             [["s2", [["200-1", {"a": "200-1"}]]]],
                             results[0])
            self.assertIs(results[0].get("timed_out"), False,
                          results[0])
            self.assertGreaterEqual(elapsed, 0.4)
            self.assertLess(elapsed, 8.0)
        finally:
            cluster.stop_all()

    def test_cli_end_to_end_with_block(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            node = f"127.0.0.1:{cluster.addrs[leader.id][1]}"
            addr = cluster.addrs[leader.id]
            _xadd(addr, "s1", "100-5")
            for key in ("s1", "s2"):
                self.assertEqual(
                    _xgroup_create(addr, key,
                                   mkstream=True).get("status"), "ok")
            # A blocking CLI read that times out prints an empty
            # list, like a timed-out single-stream read.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xreadgroup-multi",
                                  "g", "c1", "s1", ">", "s2", ">",
                                  "--block", "1000"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), [])
            # And one that wakes on a new entry prints it.
            outputs: list[str] = []

            def _run_cli():
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    client_main(["--node", node, "xreadgroup-multi",
                                 "g", "c1", "s1", ">", "s2", ">",
                                 "--block", "5000"])
                outputs.append(buf.getvalue())

            thread = threading.Thread(target=_run_cli)
            thread.start()
            time.sleep(0.5)
            resp = _xadd(addr, "s2", "200-1")
            self.assertEqual(resp.get("status"), "ok", resp)
            thread.join(timeout=10.0)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(outputs), 1, outputs)
            self.assertEqual(json.loads(outputs[0]),
                             [["s2", [["200-1", {"a": "200-1"}]]]])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
