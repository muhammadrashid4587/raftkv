"""Tests for raftkv streams: XADD (Redis stream append, v0.63.0).

``xadd key [id] field value [field value ...]`` appends one entry to
the stream at the key and answers with the ID the entry got —
Redis XADD. The ID is "*" (auto-generate, from the leader's wall
clock in milliseconds, fixed in the log entry so every replica
resolves the same ID), an explicit "ms-seq", or "ms-*" (the
sequence auto-generated). A missing key is created as an empty
stream; an explicit ID must be greater than the stream's top item,
or the xadd fails with ``xadd_failed`` / ``id_not_greater`` and
writes nothing — but still committed its entry, so a retry with
the same session id returns the first attempt's failure instead of
appending again. A plain string value fails with ``xadd_failed`` /
``wrongtype`` instead of writing. Entries ride in the store as a
JSON blob with a marker prefix, like a hash — the blob is a pure
function of the entries, so every node applying the same log
computes the same bytes, and it survives snapshots and replay
exactly like any other value. XTRIM (v0.66.0) trims entries from
the front — MAXLEN keeps at most N, MINID drops every entry below
an ID — and the top ID never moves when entries are trimmed, so
an xadd after a trim still refuses an ID at or below the old top.
XDEL (v0.80.0) deletes named entries — the count it returns, the
empty stream it leaves behind with its top ID remembered, and the
pending entries it drops from every group. XREVRANGE (v0.81.0)
reads a stream's entries back newest-first — the same range as
XRANGE, in reverse ID order, with COUNT taken from the newest end.
There is no XREADGROUP yet, so the only way to read entries back
is XRANGE / XLEN / XREAD. These spin up
real nodes (real TCP, real threads, real disk) on localhost, like
the rest of the suite.
"""

import contextlib
import io
import json
import re
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import xadd as client_xadd
from raftkv.client import xdel as client_xdel
from raftkv.client import xlen as client_xlen
from raftkv.client import xrange as client_xrange
from raftkv.client import xread as client_xread
from raftkv.client import xrevrange as client_xrevrange
from raftkv.client import xtrim as client_xtrim
from raftkv.store import KVStore, STREAM_PREFIX, is_stream

ID_RE = re.compile(r"^\d+-\d+$")


def xadd(addr, key="events", pairs=(("type", "login"),), id="*",
         client_id=None, request_id=None):
    msg = {"type": "client", "op": "xadd", "key": key,
           "pairs": [[f, v] for f, v in pairs], "id": id}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xlen(addr, key="events"):
    return client_request(addr, {"type": "client", "op": "xlen",
                                 "key": key})


def xrange(addr, key="events", start="-", end="+", count=None):
    msg = {"type": "client", "op": "xrange", "key": key,
           "start": start, "end": end}
    if count is not None:
        msg["count"] = count
    return client_request(addr, msg)


def xrevrange(addr, key="events", end="+", start="-", count=None):
    msg = {"type": "client", "op": "xrevrange", "key": key,
           "end": end, "start": start}
    if count is not None:
        msg["count"] = count
    return client_request(addr, msg)


def xread(addr, keys=("events",), ids=("0-0",), count=None,
          block=None):
    msg = {"type": "client", "op": "xread", "keys": list(keys),
           "ids": list(ids)}
    if count is not None:
        msg["count"] = count
    if block is not None:
        msg["block"] = block
    return client_request(addr, msg)


def xtrim(addr, key="events", strategy="MAXLEN", threshold=0,
          approximate=None, mode=None, client_id=None,
          request_id=None):
    msg = {"type": "client", "op": "xtrim", "key": key,
           "strategy": strategy, "threshold": threshold}
    if approximate is not None:
        msg["approximate"] = approximate
    if mode is not None:
        msg["mode"] = mode
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xdel(addr, key="events", ids=("100-1",), client_id=None,
         request_id=None):
    msg = {"type": "client", "op": "xdel", "key": key,
           "ids": list(ids)}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="k", value="old"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def entries_of(value):
    """The [(id, fields), ...] entries behind a stream blob, or
    None when the key holds nothing."""
    if value is None:
        return None
    assert is_stream(value), value
    data = json.loads(value[len(STREAM_PREFIX):])
    return [(entry_id, dict(fields))
            for entry_id, fields in data["entries"]]


def id_tuple(id_str):
    """A stream ID as an (ms, seq) pair, so IDs compare in
    stream order."""
    ms, seq = id_str.split("-")
    return int(ms), int(seq)


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestStreamsStore(unittest.TestCase):
    def test_xadd_auto_id_appends_in_order(self):
        """Two auto xadds get increasing IDs, and the entries land
        in ID order with their fields — a field repeated in one
        call counts once, at its last value, like an hset."""
        store = KVStore()
        result = store.apply({"op": "xadd", "key": "events",
                              "pairs": [["type", "login"],
                                        ["user", "mo"],
                                        ["user", "al"]],
                              "id": "*", "auto_ms": 1700000000000})
        self.assertEqual(result["xadd"], "1700000000000-0")
        result = store.apply({"op": "xadd", "key": "events",
                              "pairs": [["type", "logout"]],
                              "id": "*", "auto_ms": 1700000000000})
        self.assertEqual(result["xadd"], "1700000000000-1")
        self.assertEqual(entries_of(store.get("events")),
                         [("1700000000000-0",
                           {"type": "login", "user": "al"}),
                          ("1700000000000-1", {"type": "logout"})])

    def test_xadd_explicit_id_and_partial_id(self):
        """An explicit ID is used as-is; "ms-*" takes sequence 0
        past the last millisecond and the next sequence within
        it; "0-*" on an empty stream is 0-1, since 0-0 is not a
        valid ID."""
        store = KVStore()
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "1"]], "id": "100-5"})
        self.assertEqual(result["xadd"], "100-5")
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "2"]], "id": "100-*"})
        self.assertEqual(result["xadd"], "100-6")
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "3"]], "id": "101-*"})
        self.assertEqual(result["xadd"], "101-0")
        result = store.apply({"op": "xadd", "key": "t",
                              "pairs": [["a", "1"]], "id": "0-*"})
        self.assertEqual(result["xadd"], "0-1")
        self.assertEqual([e[0] for e in entries_of(store.get("s"))],
                         ["100-5", "100-6", "101-0"])

    def test_xadd_id_not_greater_fails_without_writing(self):
        """An explicit ID equal to or smaller than the top item
        fails with id_not_greater and writes nothing — and so
        does a "ms-*" whose millisecond is behind the top one."""
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "100-5"})
        for bad_id in ("100-5", "100-4", "99-999", "99-*"):
            result = store.apply({"op": "xadd", "key": "s",
                                  "pairs": [["a", "2"]], "id": bad_id})
            self.assertEqual(result["status"], "xadd_failed",
                             bad_id)
            self.assertEqual(result["reason"], "id_not_greater",
                             bad_id)
            self.assertEqual(result["last_id"], "100-5", bad_id)
        self.assertEqual(entries_of(store.get("s")),
                         [("100-5", {"a": "1"})])
        # But "ms-*" at the top millisecond is fine: it takes the
        # next sequence.
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "2"]], "id": "100-*"})
        self.assertEqual(result["xadd"], "100-6")

    def test_xadd_auto_id_after_explicit_future_id(self):
        """An auto ID whose millisecond is behind the stream's
        top takes the top millisecond with the next sequence —
        the leader's clock can never produce an ID smaller than
        the top, however far into the future an explicit ID ran."""
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "9999999999999-0"})
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "2"]], "id": "*",
                              "auto_ms": 1700000000000})
        self.assertEqual(result["xadd"], "9999999999999-1")
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "3"]], "id": "*",
                              "auto_ms": 1700000000000})
        self.assertEqual(result["xadd"], "9999999999999-2")

    def test_xadd_keeps_ttl_deadline(self):
        # An xadd is an append, not a fresh set: the deadline
        # survives it, like an hset. The deadline is put on the
        # stream key with expire, since a set would leave a plain
        # string an xadd rightly refuses.
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "1-0"}, index=10)
        store.apply({"op": "expire", "key": "s",
                     "ttl_entries": 10}, index=11)
        self.assertEqual(store.expiries(), {"s": 21})
        result = store.apply({"op": "xadd", "key": "s",
                              "pairs": [["a", "2"]], "id": "2-0"},
                             index=12)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.expiries(), {"s": 21})
        self.assertEqual(store.expire_up_to(20), [])
        self.assertEqual(store.expire_up_to(21), ["s"])

    def test_xadd_new_key_joins_sorted_index(self):
        """An xadd that creates a key inserts it in the sorted
        index, so a scan finds it in order."""
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "xadd", "key": "a",
                     "pairs": [["f", "1"]], "id": "1-0"})
        store.apply({"op": "xadd", "key": "c",
                     "pairs": [["f", "1"]], "id": "1-0"})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))

    def test_xadd_wrongtype_leaves_value_untouched(self):
        """A plain string value fails an xadd with xadd_failed /
        wrongtype and writes nothing."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "x"})
        result = store.apply({"op": "xadd", "key": "k",
                              "pairs": [["a", "1"]], "id": "*",
                              "auto_ms": 1700000000000})
        self.assertEqual(result["status"], "xadd_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("k"), "x")

    def test_xadd_bad_pairs_and_ids_raise(self):
        """Bad pairs and bad IDs are a programming error at apply
        time — the leader validates them before logging, so a
        hand-built entry with any of them raises instead of
        writing nonsense."""
        store = KVStore()
        bad_pairs = [
            [],
            [["a", "1"]] * 1001,
            [["a"]],
            [["a", "1", "extra"]],
            [[1, "1"]],
            [["a", 1]],
        ]
        for pairs in bad_pairs:
            with self.assertRaises(ValueError, msg=str(pairs[:1])):
                store.apply({"op": "xadd", "key": "k",
                             "pairs": pairs, "id": "*",
                             "auto_ms": 1700000000000})
        bad_ids = [
            "0-0", "abc", "1-2-3", "", "5-", "5",
            "18446744073709551616-0", "5-18446744073709551616",
            5, None,
        ]
        for bad_id in bad_ids:
            with self.assertRaises(ValueError, msg=str(bad_id)):
                store.apply({"op": "xadd", "key": "k",
                             "pairs": [["a", "1"]], "id": bad_id,
                             "auto_ms": 1700000000000})
        # An auto ID with no millisecond fixed by the leader is
        # a hand-built entry, not a real one: it raises too.
        with self.assertRaises(ValueError):
            store.apply({"op": "xadd", "key": "k",
                         "pairs": [["a", "1"]], "id": "*"})
        self.assertIsNone(store.get("k"))

    def test_xadd_survives_snapshot_load(self):
        """The stream blob is a plain string in the store, so a
        snapshot load rebuilds the same entries and the same top
        ID — and an explicit ID below the top still fails after
        the load."""
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "100-5"})
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "2"]], "id": "100-6"})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(entries_of(loaded.get("s")),
                         [("100-5", {"a": "1"}),
                          ("100-6", {"a": "2"})])
        result = loaded.apply({"op": "xadd", "key": "s",
                               "pairs": [["a", "3"]], "id": "100-5"})
        self.assertEqual(result["status"], "xadd_failed")
        self.assertEqual(result["reason"], "id_not_greater")

    def test_xlen_counts_entries_and_missing_key_is_zero(self):
        """XLEN counts the entries in a stream — 0 on a missing
        key — and tracks appends as they land."""
        store = KVStore()
        self.assertEqual(store.xlen("s"), 0)
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "100-5"})
        self.assertEqual(store.xlen("s"), 1)
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "2"]], "id": "100-6"})
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "3"]], "id": "101-0"})
        self.assertEqual(store.xlen("s"), 3)

    def test_xrange_full_and_partial_ranges(self):
        """XRANGE with "-" / "+" returns every entry in ID order;
        explicit bounds cut the range, a bare millisecond names
        that millisecond with sequence 0, and a start past the
        end answers an empty list rather than an error."""
        store = KVStore()
        self.assertEqual(store.xrange("s", "-", "+"), [])
        for entry_id in ("100-5", "100-6", "101-0", "102-3"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        self.assertEqual(store.xrange("s", "-", "+"),
                         [["100-5", {"a": "100-5"}],
                          ["100-6", {"a": "100-6"}],
                          ["101-0", {"a": "101-0"}],
                          ["102-3", {"a": "102-3"}]])
        self.assertEqual(store.xrange("s", "100-6", "101-0"),
                         [["100-6", {"a": "100-6"}],
                          ["101-0", {"a": "101-0"}]])
        # A bare millisecond is that millisecond, sequence 0.
        self.assertEqual(store.xrange("s", "101", "101"),
                         [["101-0", {"a": "101-0"}]])
        self.assertEqual(store.xrange("s", "100", "100-6"),
                         [["100-5", {"a": "100-5"}],
                          ["100-6", {"a": "100-6"}]])
        # 0-0 names the very start: it covers every entry.
        self.assertEqual(
            [e[0] for e in store.xrange("s", "0-0", "+")],
            ["100-5", "100-6", "101-0", "102-3"])
        # A start past the end is empty, not an error.
        self.assertEqual(store.xrange("s", "102-3", "100-5"), [])
        self.assertEqual(store.xrange("s", "200", "+"), [])

    def test_xrange_count_caps_and_zero_is_empty(self):
        """COUNT caps how many entries one call returns, in ID
        order — and COUNT 0 answers an empty list."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        self.assertEqual(store.xrange("s", "-", "+", count=2),
                         [["100-5", {"a": "100-5"}],
                          ["100-6", {"a": "100-6"}]])
        self.assertEqual(store.xrange("s", "-", "+", count=0), [])
        self.assertEqual(store.xrange("s", "-", "+", count=99),
                         [["100-5", {"a": "100-5"}],
                          ["100-6", {"a": "100-6"}],
                          ["101-0", {"a": "101-0"}]])
        # A count past the range end stops at the range end.
        self.assertEqual(store.xrange("s", "100-6", "+", count=99),
                         [["100-6", {"a": "100-6"}],
                          ["101-0", {"a": "101-0"}]])

    def test_xlen_xrange_wrongtype_leaves_value_untouched(self):
        """A plain string value fails an xlen and an xrange with
        TypeError and leaves the value untouched."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "x"})
        with self.assertRaises(TypeError):
            store.xlen("k")
        with self.assertRaises(TypeError):
            store.xrange("k", "-", "+")
        self.assertEqual(store.get("k"), "x")

    def test_xrange_bad_bounds_and_count_raise(self):
        """A bad bound or count raises ValueError — even on a
        missing key, since the bounds are parsed before the key
        is looked up."""
        store = KVStore()
        bad_bounds = ["bogus", "1-2-3", "", "5-",
                      "18446744073709551616-0",
                      "5-18446744073709551616",
                      "18446744073709551616",
                      5, None]
        for bad in bad_bounds:
            with self.assertRaises(ValueError, msg=str(bad)):
                store.xrange("s", bad, "+")
            with self.assertRaises(ValueError, msg=str(bad)):
                store.xrange("s", "-", bad)
        for bad_count in (-1, True, "2", 1.5):
            with self.assertRaises(ValueError, msg=str(bad_count)):
                store.xrange("s", "-", "+", count=bad_count)

    def test_xread_multi_stream_after_ids(self):
        """XREAD reads the entries after one ID per stream, in ID
        order — only the streams that hold one ride along, in the
        order the keys were given, and a missing key or a stream
        with nothing new is simply absent."""
        store = KVStore()
        self.assertEqual(store.xread(["s"], ["0-0"]), [])
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        for entry_id in ("200-0", "200-1"):
            store.apply({"op": "xadd", "key": "t",
                         "pairs": [["b", entry_id]], "id": entry_id})
        self.assertEqual(store.xread(["s", "t"], ["0-0", "0-0"]),
                         [["s", [["100-5", {"a": "100-5"}],
                                 ["100-6", {"a": "100-6"}],
                                 ["101-0", {"a": "101-0"}]]],
                          ["t", [["200-0", {"b": "200-0"}],
                                 ["200-1", {"b": "200-1"}]]]])
        # An ID past a stream's top leaves that stream out.
        self.assertEqual(store.xread(["s", "t"], ["101-0", "0-0"]),
                         [["t", [["200-0", {"b": "200-0"}],
                                 ["200-1", {"b": "200-1"}]]]])
        # A missing key never rides along.
        self.assertEqual(store.xread(["nope", "s"], ["0-0", "100-6"]),
                         [["s", [["101-0", {"a": "101-0"}]]]])
        # The order follows the keys given, not the keyspace.
        self.assertEqual(
            [pair[0] for pair in
             store.xread(["t", "s"], ["0-0", "0-0"])],
            ["t", "s"])

    def test_xread_dollar_means_only_new_entries(self):
        """An ID of "$" resolves to the stream's top at read time:
        only entries appended after the call count — and on a
        missing key it resolves to 0-0, so every future entry
        counts."""
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "100-5"})
        self.assertEqual(store.xread(["s"], ["$"]), [])
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "2"]], "id": "100-6"})
        self.assertEqual(store.xread(["s"], ["$"]), [])
        # Resolving once, then appending, is what a blocking
        # "$" read measures from: the entry that lands after
        # the resolution is the one it sees.
        resolved = store.xread_resolve(["s"], ["$"])
        self.assertEqual(resolved, ["100-6"])
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "3"]], "id": "101-0"})
        self.assertEqual(store.xread(["s"], resolved),
                         [["s", [["101-0", {"a": "3"}]]]])
        # A missing key resolves "$" to 0-0.
        self.assertEqual(store.xread_resolve(["nope"], ["$"]),
                         ["0-0"])

    def test_xread_count_caps_per_stream_and_zero_is_empty(self):
        """COUNT caps how many entries one stream returns — per
        stream, not across the call — and COUNT 0 answers nothing
        for that stream."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
            store.apply({"op": "xadd", "key": "t",
                         "pairs": [["b", entry_id]], "id": entry_id})
        self.assertEqual(store.xread(["s", "t"], ["0-0", "0-0"],
                                     count=2),
                         [["s", [["100-5", {"a": "100-5"}],
                                 ["100-6", {"a": "100-6"}]]],
                          ["t", [["100-5", {"b": "100-5"}],
                                 ["100-6", {"b": "100-6"}]]]])
        self.assertEqual(store.xread(["s"], ["0-0"], count=0), [])

    def test_xread_wrongtype_and_bad_args_raise(self):
        """A plain string value fails an xread with TypeError and
        leaves the value untouched; bad keys, ids, and counts
        raise ValueError — even on a missing key, since the ids
        are parsed before any key is looked up."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "x"})
        with self.assertRaises(TypeError):
            store.xread(["k"], ["0-0"])
        with self.assertRaises(TypeError):
            store.xread(["k"], ["$"])
        self.assertEqual(store.get("k"), "x")
        for bad_keys, bad_ids in (([], ["0-0"]),
                                  (["s"], []),
                                  (["s"], ["0-0", "0-0"]),
                                  (["s", "t"], ["0-0"]),
                                  ([5], ["0-0"]),
                                  ([""], ["0-0"]),
                                  ("s", ["0-0"]),
                                  (["s"], "0-0"),
                                  (["s"], [5]),
                                  (["s"], [None])):
            with self.assertRaises(ValueError,
                                   msg=str((bad_keys, bad_ids))):
                store.xread(bad_keys, bad_ids)
        for bad_id in ("bogus", "1-2-3", "", "-", "+", "*", "5-",
                       "18446744073709551616-0"):
            with self.assertRaises(ValueError, msg=bad_id):
                store.xread(["s"], [bad_id])
        for bad_count in (-1, True, "2", 1.5):
            with self.assertRaises(ValueError, msg=str(bad_count)):
                store.xread(["s"], ["0-0"], count=bad_count)


class TestStreamsCluster(unittest.TestCase):
    def test_xadd_replicates_to_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = xadd(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            first_id = resp.get("xadd")
            self.assertIsNotNone(first_id, resp)
            self.assertRegex(first_id, ID_RE, resp)
            resp = xadd(addr, pairs=(("type", "logout"),))
            self.assertEqual(resp.get("status"), "ok", resp)
            second_id = resp.get("xadd")
            self.assertGreater(id_tuple(second_id),
                               id_tuple(first_id))
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                self.assertEqual(
                    entries_of(cluster.nodes[node_id].sm.get("events")),
                    [(first_id, {"type": "login"}),
                     (second_id, {"type": "logout"})])
        finally:
            cluster.stop_all()

    def test_xadd_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = client_xadd(cluster.addrs[follower], "events",
                               {"type": "login"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertRegex(resp.get("xadd", ""), ID_RE, resp)
            self.assertEqual(
                entries_of(cluster.nodes[leader.id].sm.get("events")),
                [(resp["xadd"], {"type": "login"})])
        finally:
            cluster.stop_all()

    def test_invalid_xadd_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = xadd(addr, id="0-0")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("0-0", resp.get("error", ""), resp)
            resp = xadd(addr, id="bogus")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("id", resp.get("error", ""), resp)
            resp = xadd(addr, id="5")
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xadd", "key": "k",
                       "pairs": [], "id": "*"})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xadd", "key": "k",
                       "pairs": [["a", "1"]] * 1001, "id": "*"})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xadd", "key": "k",
                       "pairs": [["a", 1]], "id": "*"})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xadd", "key": "k",
                       "pairs": [["a", "1"]], "id": 5})
            self.assertEqual(resp.get("status"), "error", resp)
            # None of the rejected writes left a key behind, and
            # none appended a log entry.
            self.assertIsNone(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "events"}).get("value"))
            self.assertEqual(len(leader.log), before)
        finally:
            cluster.stop_all()

    def test_xadd_retry_returns_first_id_and_writes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = xadd(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            first_id = resp.get("xadd")
            resp = xadd(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xadd"), first_id, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            # A retry with different pairs still returns the first
            # attempt's ID, not a second entry.
            resp = xadd(addr, pairs=(("type", "other"),),
                        client_id="c1", request_id="r1")
            self.assertEqual(resp.get("xadd"), first_id, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(
                entries_of(leader.sm.get("events")),
                [(first_id, {"type": "login"})])
        finally:
            cluster.stop_all()

    def test_xadd_failed_retry_returns_first_failure(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = xadd(addr, id="100-5", client_id="c1",
                        request_id="r1")
            self.assertEqual(resp.get("xadd"), "100-5", resp)
            resp = xadd(addr, id="100-4", client_id="c1",
                        request_id="r2")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            self.assertEqual(resp.get("reason"), "id_not_greater",
                             resp)
            resp = xadd(addr, id="100-3", client_id="c1",
                        request_id="r2")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            self.assertEqual(resp.get("reason"), "id_not_greater",
                             resp)
            self.assertEqual(count_entries(leader, "r2"), 1)
            # The failed entry wrote nothing: only the first
            # entry is in the stream.
            self.assertEqual(
                entries_of(leader.sm.get("events")),
                [("100-5", {"type": "login"})])
        finally:
            cluster.stop_all()

    def test_xadd_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("events", events.append)
            follower.watch.watch("events", events.append)
            resp = xadd(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "events", event)
                self.assertEqual(event["value"],
                                 leader.sm.get("events"), event)
                self.assertEqual(event["op"], "set", event)
            # A failed xadd — an ID behind the top — writes
            # nothing, so it fires nothing.
            resp = xadd(addr, id="1-0")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            time.sleep(0.3)
            self.assertEqual(len(events), 2, events)
        finally:
            cluster.stop_all()

    def test_xadd_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = xadd(addr, id="100-5")
            self.assertEqual(resp.get("xadd"), "100-5", resp)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            resp = xadd(addr, id="100-6")
            self.assertEqual(resp.get("xadd"), "100-6", resp)
            resp = xadd(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertGreater(id_tuple(resp["xadd"]),
                               (100, 6))
            self.assertEqual(
                entries_of(new_leader.sm.get("events")),
                [("100-5", {"type": "login"}),
                 ("100-6", {"type": "login"}),
                 (resp["xadd"], {"type": "login"})])
        finally:
            cluster.stop_all()

    def test_xadd_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(xadd(addr, id="100-5").get("xadd"),
                             "100-5")
            # Enough writes to compact the log past the xadd.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(
                entries_of(leader.sm.get("events")),
                [("100-5", {"type": "login"})])
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The stream survived the snapshot + restart: an ID
            # below the top still fails, and a new entry appends
            # after it.
            resp = xadd(addr, id="100-5")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            self.assertEqual(resp.get("reason"), "id_not_greater",
                             resp)
            resp = xadd(addr, id="100-6")
            self.assertEqual(resp.get("xadd"), "100-6", resp)
        finally:
            cluster.stop_all()

    def test_session_xadd_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"type": "login"})
                self.assertEqual(resp.get("status"), "ok", resp)
                first_id = resp.get("xadd")
                self.assertRegex(first_id, ID_RE, resp)
                # A session read sees its own xadd's entry.
                resp = session.get("events")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(entries_of(resp.get("value")),
                                 [(first_id, {"type": "login"})])
                # A session read from a follower still sees the
                # session's own xadd.
                follower = next(n for n in cluster.live
                                if n != leader.id)
                with Session(cluster.addrs[follower],
                             client_id=session.client_id) as other:
                    other.last_write_index = session.last_write_index
                    other.last_write_term = session.last_write_term
                    resp = other.get("events")
                    self.assertEqual(resp.get("status"), "ok", resp)
                    self.assertEqual(entries_of(resp.get("value")),
                                     [(first_id, {"type": "login"})])
        finally:
            cluster.stop_all()

    def test_pipeline_xadd_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    # No id: "*" is the default.
                    {"op": "xadd", "key": "events",
                     "pairs": [["type", "login"]]},
                    {"op": "xadd", "key": "events",
                     "pairs": [["type", "logout"]], "id": "*"},
                    {"op": "xadd", "key": "events",
                     "pairs": [["type", "x"]], "id": "100-0"},
                    {"op": "xadd", "key": "events",
                     "pairs": [["type", "x"]], "id": "bogus"},
                    {"op": "xadd", "key": "events",
                     "pairs": []},
                    {"op": "xadd", "key": "events",
                     "pairs": [["a", 1]]},
                    {"op": "set", "key": "plain", "value": "v"},
                    {"op": "xadd", "key": "plain",
                     "pairs": [["a", "b"]], "id": "*"},
                    {"op": "xadd", "key": "other",
                     "pairs": [["a", "b"]], "id": "5-5"},
                ])
            self.assertEqual(len(replies), 9, replies)
            self.assertEqual(replies[0].get("status"), "ok", replies)
            self.assertRegex(replies[0].get("xadd", ""), ID_RE,
                             replies)
            self.assertEqual(replies[1].get("status"), "ok", replies)
            self.assertGreater(id_tuple(replies[1]["xadd"]),
                               id_tuple(replies[0]["xadd"]))
            # An explicit ID behind the auto-generated top fails
            # in position, and the entry is not appended.
            self.assertEqual(replies[2].get("status"), "xadd_failed",
                             replies)
            self.assertEqual(replies[2].get("reason"),
                             "id_not_greater", replies)
            self.assertEqual(replies[3].get("status"), "error",
                             replies)
            self.assertEqual(replies[4].get("status"), "error",
                             replies)
            self.assertEqual(replies[5].get("status"), "error",
                             replies)
            self.assertEqual(replies[6].get("status"), "ok", replies)
            self.assertEqual(replies[7].get("status"), "xadd_failed",
                             replies)
            self.assertEqual(replies[7].get("reason"), "wrongtype",
                             replies)
            self.assertEqual(replies[8].get("xadd"), "5-5", replies)
            self.assertEqual(
                [e[0] for e in entries_of(leader.sm.get("events"))],
                [replies[0]["xadd"], replies[1]["xadd"]])
        finally:
            cluster.stop_all()

    def test_xadd_module_function(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = client_xadd(cluster.addrs[follower], "events",
                               {"type": "login"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertRegex(resp.get("xadd", ""), ID_RE, resp)
            resp = client_xadd(cluster.addrs[follower], "events",
                               {"type": "logout"}, id="100-0")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            self.assertEqual(resp.get("reason"), "id_not_greater",
                             resp)
        finally:
            cluster.stop_all()

    def test_xadd_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # Xadd prints the ID the entry got.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xadd", "events",
                                  "*", "type", "login", "user", "mo"])
            self.assertEqual(rc, 0)
            first_id = out.getvalue().strip()
            self.assertRegex(first_id, ID_RE)
            # An explicit ID prints as-is.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xadd", "events",
                                  "9999999999999-0", "type", "explicit"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(),
                             "9999999999999-0")
            # An explicit ID behind the top fails: rc 1, and the
            # error names the problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xadd", "events",
                                  "100-5", "type", "late"])
            self.assertEqual(rc, 1)
            self.assertIn("equal or smaller", err.getvalue())
            # A bad ID is a server error, not a write.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xadd", "events",
                                  "bogus", "type", "x"])
            self.assertEqual(rc, 1)
            self.assertIn("id", err.getvalue())
            # Pairs must come in twos: a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "xadd", "events",
                                 "*", "type"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()

    def test_xlen_xrange_cluster(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for entry_id in ("100-5", "100-6", "101-0"):
                resp = xadd(addr, pairs=(("a", entry_id),),
                            id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)
            # XLEN counts the entries on every node, and XRANGE
            # reads them back in ID order — including from a
            # follower, which redirects to the leader.
            for node_id in cluster.live:
                self.assertEqual(
                    xlen(cluster.addrs[node_id]).get("xlen"), 3)
                resp = xrange(cluster.addrs[node_id])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("entries"),
                                 [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]])
            # A partial range and a COUNT cap, from a follower.
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = xrange(cluster.addrs[follower], start="100-6",
                          end="101-0")
            self.assertEqual(resp.get("entries"),
                             [["100-6", {"a": "100-6"}],
                              ["101-0", {"a": "101-0"}]])
            resp = xrange(cluster.addrs[follower], count=2)
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["100-5", "100-6"])
            # A missing key answers 0 / an empty list.
            self.assertEqual(xlen(addr, key="nope").get("xlen"), 0)
            self.assertEqual(xrange(addr, key="nope").get("entries"),
                             [])
            # The module functions follow redirects too.
            resp = client_xlen(cluster.addrs[follower], "events")
            self.assertEqual(resp.get("xlen"), 3, resp)
            resp = client_xrange(cluster.addrs[follower], "events",
                                 start="100-6", end="+", count=1)
            self.assertEqual(resp.get("entries"),
                             [["100-6", {"a": "100-6"}]], resp)
        finally:
            cluster.stop_all()

    def test_xlen_xrange_wrongtype_and_bad_bounds(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            self.assertEqual(write(addr, key="plain",
                                   value="v").get("status"), "ok")
            resp = xlen(addr, key="plain")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("stream", resp.get("error", ""), resp)
            resp = xrange(addr, key="plain")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("stream", resp.get("error", ""), resp)
            # Bad bounds and counts are client errors, not reads.
            resp = xrange(addr, start="bogus")
            self.assertEqual(resp.get("status"), "error", resp)
            resp = xrange(addr, end="1-2-3")
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xrange", "key": "s",
                       "start": "-", "end": "+", "count": -1})
            self.assertEqual(resp.get("status"), "error", resp)
            # Reads append no log entries, bad or not.
            self.assertEqual(len(leader.log), before + 1)
        finally:
            cluster.stop_all()

    def test_session_xlen_xrange_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"type": "login"},
                                    id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                # A session read sees its own xadd's entry.
                resp = session.xlen("events")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("xlen"), 1, resp)
                resp = session.xrange("events")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("entries"),
                                 [["100-5", {"type": "login"}]],
                                 resp)
                # A session read from a follower still sees the
                # session's own xadd.
                follower = next(n for n in cluster.live
                                if n != leader.id)
                with Session(cluster.addrs[follower],
                             client_id=session.client_id) as other:
                    other.last_write_index = session.last_write_index
                    other.last_write_term = session.last_write_term
                    resp = other.xlen("events")
                    self.assertEqual(resp.get("xlen"), 1, resp)
                    resp = other.xrange("events", start="100-5",
                                        end="100-5")
                    self.assertEqual(resp.get("entries"),
                                     [["100-5", {"type": "login"}]],
                                     resp)
        finally:
            cluster.stop_all()

    def test_xlen_xrange_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            for entry_id in ("100-5", "100-6"):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = client_main(["--node", node, "xadd", "events",
                                      entry_id, "a", entry_id])
                self.assertEqual(rc, 0)
            # XLEN prints the count.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xlen", "events"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            # XRANGE prints the entries as JSON.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xrange", "events",
                                  "-", "+"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-5", {"a": "100-5"}],
                              ["100-6", {"a": "100-6"}]])
            # A range with COUNT prints just the capped page.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xrange", "events",
                                  "-", "+", "--count", "1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-5", {"a": "100-5"}]])
            # XLEN on a missing key prints 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xlen", "nope"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # A bad bound fails: rc 1, and the error names the
            # problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xrange", "events",
                                  "bogus", "+"])
            self.assertEqual(rc, 1)
            self.assertIn("bound", err.getvalue())
        finally:
            cluster.stop_all()

    def test_xread_cluster(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for entry_id in ("100-5", "100-6", "101-0"):
                resp = xadd(addr, pairs=(("a", entry_id),),
                            id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)
            for entry_id in ("200-0", "200-1"):
                resp = xadd(addr, key="alerts",
                            pairs=(("b", entry_id),), id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)
            # XREAD reads the entries after one ID per stream, in
            # ID order — including from a follower, which
            # redirects to the leader.
            for node_id in cluster.live:
                resp = xread(cluster.addrs[node_id],
                             keys=("events", "alerts"),
                             ids=("0-0", "0-0"))
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("streams"),
                                 [["events",
                                   [["100-5", {"a": "100-5"}],
                                    ["100-6", {"a": "100-6"}],
                                    ["101-0", {"a": "101-0"}]]],
                                  ["alerts",
                                   [["200-0", {"b": "200-0"}],
                                    ["200-1", {"b": "200-1"}]]]])
            # "$" means only entries after the top: nothing new
            # here, so the reply is empty — and a missing key
            # never rides along.
            resp = xread(addr, keys=("events", "nope"),
                         ids=("$", "0-0"))
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"), [])
            # A per-stream COUNT cap, from a follower.
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = xread(cluster.addrs[follower],
                         keys=("events", "alerts"), ids=("0-0", "0-0"),
                         count=1)
            self.assertEqual(resp.get("streams"),
                             [["events", [["100-5", {"a": "100-5"}]]],
                              ["alerts", [["200-0", {"b": "200-0"}]]]])
            # The module function follows redirects too.
            resp = client_xread(cluster.addrs[follower],
                                ["events"], ["100-5"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["events", [["100-6", {"a": "100-6"}],
                                         ["101-0", {"a": "101-0"}]]]],
                             resp)
        finally:
            cluster.stop_all()

    def test_xread_block_returns_when_entry_lands(self):
        """A BLOCK read with nothing yet returns as soon as an
        entry lands — it does not wait out the whole block."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]

            def _append_later():
                time.sleep(0.3)
                xadd(addr, pairs=(("a", "late"),), id="100-5")

            writer = threading.Thread(target=_append_later)
            writer.start()
            try:
                started = time.monotonic()
                resp = client_xread(addr, ["events"], ["0-0"],
                                    block=5000)
                elapsed = time.monotonic() - started
            finally:
                writer.join()
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["events",
                               [["100-5", {"a": "late"}]]]], resp)
            self.assertFalse(resp.get("timed_out"), resp)
            # It woke on the entry, well before the 5s block.
            self.assertLess(elapsed, 3.0, elapsed)
        finally:
            cluster.stop_all()

    def test_xread_block_with_dollar_sees_only_new_entries(self):
        """A BLOCK read with "$" sees only the entries appended
        after the call — the "$" is resolved once, at the read
        index, so the entry that wakes it is the one it
        returns."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for entry_id in ("100-5", "100-6"):
                resp = xadd(addr, pairs=(("a", entry_id),),
                            id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)

            def _append_later():
                time.sleep(0.3)
                xadd(addr, pairs=(("a", "new"),), id="101-0")

            writer = threading.Thread(target=_append_later)
            writer.start()
            try:
                resp = client_xread(addr, ["events"], ["$"],
                                    block=5000)
            finally:
                writer.join()
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["events",
                               [["101-0", {"a": "new"}]]]], resp)
            self.assertFalse(resp.get("timed_out"), resp)
        finally:
            cluster.stop_all()

    def test_xread_block_times_out_empty(self):
        """A BLOCK read on a stream nothing lands in answers an
        empty list with timed_out set, after about the block —
        not an error, and not a hang."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            started = time.monotonic()
            resp = client_xread(addr, ["events"], ["0-0"], block=500)
            elapsed = time.monotonic() - started
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"), [], resp)
            self.assertTrue(resp.get("timed_out"), resp)
            self.assertGreaterEqual(elapsed, 0.4, elapsed)
            self.assertLess(elapsed, 3.0, elapsed)
        finally:
            cluster.stop_all()

    def test_xread_wrongtype_and_bad_args(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            self.assertEqual(write(addr, key="plain",
                                   value="v").get("status"), "ok")
            resp = xread(addr, keys=("plain",), ids=("0-0",))
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("stream", resp.get("error", ""), resp)
            resp = xread(addr, keys=("plain",), ids=("$",))
            self.assertEqual(resp.get("status"), "error", resp)
            # Bad keys, ids, counts, and blocks are client
            # errors, not reads.
            resp = xread(addr, keys=(), ids=())
            self.assertEqual(resp.get("status"), "error", resp)
            resp = xread(addr, keys=("events",), ids=("0-0", "0-0"))
            self.assertEqual(resp.get("status"), "error", resp)
            resp = xread(addr, keys=("events",), ids=("bogus",))
            self.assertEqual(resp.get("status"), "error", resp)
            resp = xread(addr, keys=("events",), ids=("-",))
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xread",
                       "keys": ["events"], "ids": ["0-0"],
                       "count": -1})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xread",
                       "keys": ["events"], "ids": ["0-0"],
                       "block": -5})
            self.assertEqual(resp.get("status"), "error", resp)
            # Reads append no log entries, bad or not.
            self.assertEqual(len(leader.log), before + 1)
        finally:
            cluster.stop_all()

    def test_session_xread_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"type": "login"},
                                    id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                # A session read sees its own xadd's entry.
                resp = session.xread(["events"], ["0-0"])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("streams"),
                                 [["events",
                                   [["100-5", {"type": "login"}]]]],
                                 resp)
                # And with "$" after its own xadd, nothing is
                # new — the top is its own entry.
                resp = session.xread(["events"], ["$"])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("streams"), [], resp)
                # A session read from a follower still sees the
                # session's own xadd.
                follower = next(n for n in cluster.live
                                if n != leader.id)
                with Session(cluster.addrs[follower],
                             client_id=session.client_id) as other:
                    other.last_write_index = session.last_write_index
                    other.last_write_term = session.last_write_term
                    resp = other.xread(["events"], ["0-0"])
                    self.assertEqual(resp.get("streams"),
                                     [["events",
                                       [["100-5", {"type": "login"}]]]],
                                     resp)
        finally:
            cluster.stop_all()

    def test_xread_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            for entry_id in ("100-5", "100-6"):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = client_main(["--node", node, "xadd", "events",
                                      entry_id, "a", entry_id])
                self.assertEqual(rc, 0)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xadd", "alerts",
                                  "200-0", "b", "200-0"])
            self.assertEqual(rc, 0)
            # XREAD prints the streams as JSON.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xread",
                                  "events", "0-0", "alerts", "0-0"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             [["events",
                               [["100-5", {"a": "100-5"}],
                                ["100-6", {"a": "100-6"}]]],
                              ["alerts",
                               [["200-0", {"b": "200-0"}]]]])
            # "$" prints only what is new — nothing here.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xread",
                                  "events", "$"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), [])
            # COUNT caps per stream.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xread",
                                  "events", "0-0", "--count", "1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             [["events", [["100-5", {"a": "100-5"}]]]])
            # A BLOCK with nothing landing prints an empty list.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xread",
                                  "events", "$", "--block", "200"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), [])
            # An odd number of key/id pairs fails: a CLI usage
            # error (exit 2), and the error names the problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "xread",
                                 "events"])
            self.assertEqual(cm.exception.code, 2)
            self.assertIn("key id pairs", err.getvalue())
            # A bad id fails: rc 1, and the error names the
            # problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xread",
                                  "events", "bogus"])
            self.assertEqual(rc, 1)
            self.assertIn("stream", err.getvalue())
        finally:
            cluster.stop_all()


class TestXtrim(unittest.TestCase):
    def _five(self, store=None):
        """A stream with five entries, 100-1 through 100-5."""
        store = store or KVStore()
        for i in range(1, 6):
            store.apply({"op": "xadd", "key": "events",
                         "pairs": [["n", str(i)]],
                         "id": f"100-{i}"})
        return store

    def test_xtrim_maxlen_keeps_newest_and_top_stays(self):
        """MAXLEN keeps the newest N entries and answers how many
        went; the top ID never moves, so an xadd at or below the
        old top still fails after the trim."""
        store = self._five()
        result = store.apply({"op": "xtrim", "key": "events",
                              "strategy": "MAXLEN", "threshold": 2})
        self.assertEqual(result, {"type": "client_response",
                                  "status": "ok", "xtrim": 3})
        self.assertEqual(entries_of(store.get("events")),
                         [("100-4", {"n": "4"}),
                          ("100-5", {"n": "5"})])
        self.assertEqual(store.stream_top("events"), "100-5")
        result = store.apply({"op": "xadd", "key": "events",
                             "pairs": [["n", "x"]], "id": "100-5"})
        self.assertEqual(result["status"], "xadd_failed")
        self.assertEqual(result["reason"], "id_not_greater")
        self.assertEqual(entries_of(store.get("events")),
                         [("100-4", {"n": "4"}),
                          ("100-5", {"n": "5"})])

    def test_xtrim_maxlen_zero_deletes_key(self):
        """MAXLEN 0 empties the stream, and an emptied stream
        deletes the key outright, like an hdel that removes the
        last field — a later xadd starts a fresh stream."""
        store = self._five()
        result = store.apply({"op": "xtrim", "key": "events",
                              "strategy": "MAXLEN", "threshold": 0})
        self.assertEqual(result["xtrim"], 5)
        self.assertTrue(result["deleted"])
        self.assertIsNone(store.get("events"))
        self.assertEqual(store.xlen("events"), 0)
        result = store.apply({"op": "xadd", "key": "events",
                             "pairs": [["n", "6"]], "id": "*",
                             "auto_ms": 1700000000000})
        self.assertEqual(result["xadd"], "1700000000000-0")
        self.assertEqual(entries_of(store.get("events")),
                         [("1700000000000-0", {"n": "6"})])

    def test_xtrim_maxlen_above_length_is_noop(self):
        """A MAXLEN past the end removes nothing and rewrites
        nothing."""
        store = self._five()
        before = store.get("events")
        result = store.apply({"op": "xtrim", "key": "events",
                              "strategy": "MAXLEN", "threshold": 10})
        self.assertEqual(result["xtrim"], 0)
        self.assertNotIn("deleted", result)
        self.assertEqual(store.get("events"), before)

    def test_xtrim_minid_drops_below_bound(self):
        """MINID drops every entry strictly below the bound — the
        bound entry itself stays — and a bare millisecond names
        that millisecond with sequence 0, like an xrange bound."""
        store = self._five()
        result = store.apply({"op": "xtrim", "key": "events",
                              "strategy": "MINID",
                              "threshold": "100-3"})
        self.assertEqual(result["xtrim"], 2)
        self.assertEqual([e[0] for e in entries_of(
            store.get("events"))],
            ["100-3", "100-4", "100-5"])
        # A bare millisecond is (100, 0): nothing is below it.
        result = store.apply({"op": "xtrim", "key": "events",
                              "strategy": "MINID", "threshold": "100"})
        self.assertEqual(result["xtrim"], 0)
        # Past the end empties the stream and deletes the key.
        result = store.apply({"op": "xtrim", "key": "events",
                              "strategy": "MINID",
                              "threshold": "200-0"})
        self.assertEqual(result["xtrim"], 3)
        self.assertTrue(result["deleted"])
        self.assertIsNone(store.get("events"))

    def test_xtrim_missing_key_answers_zero(self):
        """An xtrim on a missing key answers 0 and writes
        nothing, like an expire on a missing key."""
        store = KVStore()
        result = store.apply({"op": "xtrim", "key": "nope",
                              "strategy": "MAXLEN", "threshold": 5})
        self.assertEqual(result["xtrim"], 0)
        self.assertIsNone(store.get("nope"))

    def test_xtrim_wrongtype(self):
        """An xtrim on a plain string fails with wrongtype and
        leaves the value untouched, like an xadd."""
        store = KVStore()
        store.apply({"op": "set", "key": "plain", "value": "v"})
        result = store.apply({"op": "xtrim", "key": "plain",
                              "strategy": "MAXLEN", "threshold": 1})
        self.assertEqual(result["status"], "xtrim_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("plain"), "v")

    def test_xtrim_bad_strategy_or_threshold_raises(self):
        """A hand-built log entry with a bad strategy or
        threshold raises rather than silently trimming
        nonsense."""
        store = self._five()
        for command in (
                {"op": "xtrim", "key": "events",
                 "strategy": "BOGUS", "threshold": 2},
                {"op": "xtrim", "key": "events",
                 "strategy": "MAXLEN", "threshold": -1},
                {"op": "xtrim", "key": "events",
                 "strategy": "MAXLEN", "threshold": True},
                {"op": "xtrim", "key": "events",
                 "strategy": "MAXLEN", "threshold": "2"},
                {"op": "xtrim", "key": "events",
                 "strategy": "MINID", "threshold": 100},
                {"op": "xtrim", "key": "events",
                 "strategy": "MINID", "threshold": "bogus"},
                {"op": "xtrim", "key": "events",
                 "strategy": "MINID", "threshold": "100-3-4"}):
            with self.assertRaises(ValueError, msg=str(command)):
                store.apply(command)
        self.assertEqual(len(entries_of(store.get("events"))), 5)

    def test_xtrim_approximate_and_mode_accepted(self):
        """The "~" flag and the KEEPREF / DELREF / ACKED modes are
        accepted: the trim is exact either way, and with no consumer
        groups on the stream the mode changes nothing — what each
        mode does to pending references is proved in
        test_xreadgroup.py."""
        store = self._five()
        for mode in ("KEEPREF", "DELREF", "ACKED"):
            result = store.apply({"op": "xtrim", "key": "events",
                                  "strategy": "MAXLEN", "threshold": 4,
                                  "approximate": True, "mode": mode})
            self.assertEqual(result["status"], "ok", mode)
            self.assertEqual(result["xtrim"], 1, mode)
            top_ms, top_seq = store.stream_top("events").split("-")
            store.apply({"op": "xadd", "key": "events",
                         "pairs": [["n", "back"]],
                         "id": f"{top_ms}-{int(top_seq) + 1}"})

    def test_xtrim_survives_snapshot_load(self):
        """A trimmed stream is a plain string in the store, so a
        snapshot load rebuilds the same entries and the same top
        ID — and an ID below the top still fails after the load."""
        store = self._five()
        store.apply({"op": "xtrim", "key": "events",
                     "strategy": "MAXLEN", "threshold": 2})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(entries_of(loaded.get("events")),
                         [("100-4", {"n": "4"}),
                          ("100-5", {"n": "5"})])
        self.assertEqual(loaded.stream_top("events"), "100-5")
        result = loaded.apply({"op": "xadd", "key": "events",
                              "pairs": [["n", "x"]], "id": "100-3"})
        self.assertEqual(result["status"], "xadd_failed")
        self.assertEqual(result["reason"], "id_not_greater")

    def test_xtrim_keeps_ttl_deadline(self):
        """An xtrim is an update, not a fresh set: the deadline
        survives it when entries remain, like an xadd — and it
        goes with the key when the trim deletes it."""
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "1-0"}, index=10)
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "2"]], "id": "2-0"}, index=11)
        store.apply({"op": "expire", "key": "s",
                     "ttl_entries": 10}, index=12)
        self.assertEqual(store.expiries(), {"s": 22})
        result = store.apply({"op": "xtrim", "key": "s",
                              "strategy": "MAXLEN", "threshold": 1},
                             index=13)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.expiries(), {"s": 22})
        result = store.apply({"op": "xtrim", "key": "s",
                              "strategy": "MAXLEN", "threshold": 0},
                             index=14)
        self.assertTrue(result["deleted"])
        self.assertEqual(store.expiries(), {})

    def test_xtrim_maxlen_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for i in range(1, 6):
                    resp = session.xadd("events", {"n": str(i)},
                                        id=f"100-{i}")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xtrim("events", "MAXLEN", 2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("xtrim"), 3, resp)
                # A session read after its own trim sees the trim.
                resp = session.xlen("events")
                self.assertEqual(resp.get("xlen"), 2, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                self.assertEqual(
                    [e[0] for e in entries_of(
                        cluster.nodes[node_id].sm.get("events"))],
                    ["100-4", "100-5"])
        finally:
            cluster.stop_all()

    def test_xtrim_minid_module_function_from_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            addr = cluster.addrs[leader.id]
            for i in range(1, 6):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            # The module function follows the leader redirect.
            resp = client_xtrim(cluster.addrs[follower], "events",
                                "MINID", "100-4")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xtrim"), 3, resp)
            self.assertEqual(
                [e[0] for e in entries_of(
                    cluster.nodes[leader.id].sm.get("events"))],
                ["100-4", "100-5"])
        finally:
            cluster.stop_all()

    def test_xtrim_wrongtype_and_bad_args(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="plain").get("status"),
                             "ok")
            resp = xtrim(addr, key="plain", threshold=1)
            self.assertEqual(resp.get("status"), "xtrim_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            self.assertEqual(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "plain"}).get("value"),
                "old")
            # Bad arguments are client errors with no log entry.
            before = len(leader.log)
            for msg in (
                    {"strategy": "BOGUS", "threshold": 2},
                    {"strategy": "MAXLEN", "threshold": -1},
                    {"strategy": "MAXLEN", "threshold": "2"},
                    {"strategy": "MINID", "threshold": 100},
                    {"strategy": "MINID", "threshold": "bogus"},
                    {"strategy": "MAXLEN", "threshold": 2,
                     "mode": "BOGUS"},
                    {"strategy": "MAXLEN", "threshold": 2,
                     "approximate": "yes"}):
                resp = client_request(
                    addr, {"type": "client", "op": "xtrim",
                           "key": "events", **msg})
                self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(len(leader.log), before)
            # A missing key answers 0, like an expire on one.
            resp = xtrim(addr, key="nope", threshold=5)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xtrim"), 0, resp)
        finally:
            cluster.stop_all()

    def test_xtrim_dedup_retry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 6):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            resp = xtrim(addr, threshold=2, client_id="c1",
                         request_id="t1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xtrim"), 3, resp)
            # The retry returns the first attempt's count instead
            # of trimming twice — and logs one entry, not two.
            resp = xtrim(addr, threshold=2, client_id="c1",
                         request_id="t1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xtrim"), 3, resp)
            self.assertEqual(count_entries(leader, "t1"), 1)
            self.assertEqual(len(entries_of(
                leader.sm.get("events"))), 2)
        finally:
            cluster.stop_all()

    def test_xtrim_to_empty_deletes_key_and_top_survives(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 4):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            resp = xtrim(addr, threshold=0)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xtrim"), 3, resp)
            self.assertIsNone(leader.sm.get("events"))
            self.assertEqual(xlen(addr).get("xlen"), 0)
            # The key is gone, so the old top is forgotten with it
            # — a new xadd starts a fresh stream, exactly as Redis
            # starts one once the stream is gone — and an auto ID
            # appends after it.
            resp = xadd(addr, id="100-3")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xadd"), "100-3", resp)
            resp = xadd(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertGreater(id_tuple(resp["xadd"]), (100, 3))
        finally:
            cluster.stop_all()

    def test_xtrim_fires_watch_events(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 4):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            events = []
            leader.watch.watch("events", events.append)
            resp = xtrim(addr, threshold=2)
            self.assertEqual(resp.get("xtrim"), 1, resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 1 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 1, events)
            self.assertEqual(events[0]["key"], "events", events)
            self.assertEqual(events[0]["op"], "set", events)
            self.assertEqual(events[0]["value"],
                             leader.sm.get("events"), events)
            # A trim that removes nothing fires nothing.
            resp = xtrim(addr, threshold=2)
            self.assertEqual(resp.get("xtrim"), 0, resp)
            time.sleep(0.3)
            self.assertEqual(len(events), 1, events)
            # A trim that empties the stream fires a del.
            resp = xtrim(addr, threshold=0)
            self.assertEqual(resp.get("xtrim"), 2, resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            self.assertEqual(events[1]["op"], "del", events)
            self.assertIsNone(events[1]["value"], events)
        finally:
            cluster.stop_all()

    def test_pipeline_xtrim_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for i in range(1, 4):
                    resp = session.xadd("events", {"n": str(i)},
                                        id=f"100-{i}")
                    self.assertEqual(resp.get("status"), "ok", resp)
                replies = session.pipeline([
                    {"op": "xtrim", "key": "events",
                     "strategy": "MAXLEN", "threshold": 2},
                    {"op": "xtrim", "key": "events",
                     "strategy": "MINID", "threshold": "100-3"},
                    {"op": "xtrim", "key": "events",
                     "strategy": "BOGUS", "threshold": 2},
                    {"op": "xtrim", "key": "events",
                     "strategy": "MAXLEN", "threshold": -1},
                    {"op": "xtrim", "key": "nope",
                     "strategy": "MAXLEN", "threshold": 5},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("xtrim"), 1, replies)
            self.assertEqual(replies[1].get("xtrim"), 1, replies)
            self.assertEqual(replies[2].get("status"), "error",
                             replies)
            self.assertEqual(replies[3].get("status"), "error",
                             replies)
            self.assertEqual(replies[4].get("xtrim"), 0, replies)
            self.assertEqual(
                [e[0] for e in entries_of(leader.sm.get("events"))],
                ["100-3"])
        finally:
            cluster.stop_all()

    def test_xtrim_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 6):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            resp = xtrim(addr, threshold=2)
            self.assertEqual(resp.get("xtrim"), 3, resp)
            # Enough writes to compact the log past the trim.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(
                [e[0] for e in entries_of(leader.sm.get("events"))],
                ["100-4", "100-5"])
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The trimmed stream survived the snapshot + restart:
            # only the kept entries are there, and an ID below the
            # top still fails.
            self.assertEqual(
                [e[0] for e in entries_of(
                    new_leader.sm.get("events"))],
                ["100-4", "100-5"])
            resp = xadd(addr, id="100-3")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            self.assertEqual(resp.get("reason"), "id_not_greater",
                             resp)
            resp = xadd(addr, id="100-6")
            self.assertEqual(resp.get("xadd"), "100-6", resp)
        finally:
            cluster.stop_all()

    def test_xtrim_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            for i in range(1, 4):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = client_main(["--node", node, "xadd", "events",
                                      f"100-{i}", "n", str(i)])
                self.assertEqual(rc, 0)
            # MAXLEN prints the number of entries removed.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xtrim", "events",
                                  "MAXLEN", "2"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # "~" asks for approximate trimming (exact here).
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xtrim", "events",
                                  "MAXLEN", "~1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # MINID drops below the bound; the bound entry stays.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xtrim", "events",
                                  "MINID", "100-3"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # A bad threshold fails: rc 1, and the error names the
            # problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xtrim", "events",
                                  "MAXLEN", "-1"])
            self.assertEqual(rc, 1)
            self.assertIn("threshold", err.getvalue())
            # A bad mode fails at the CLI, before anything is sent.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "xtrim", "events",
                                 "MAXLEN", "1", "--mode", "BOGUS"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()
class TestXdel(unittest.TestCase):
    def _five(self, store=None):
        """A stream with five entries, 100-1 through 100-5."""
        store = store or KVStore()
        for i in range(1, 6):
            store.apply({"op": "xadd", "key": "events",
                         "pairs": [["n", str(i)]],
                         "id": f"100-{i}"})
        return store

    def test_xdel_deletes_named_entries_and_counts_them(self):
        """An xdel removes exactly the entries it names — middle
        ones included — and answers how many went; the entries
        around them and the top ID are untouched."""
        store = self._five()
        result = store.apply({"op": "xdel", "key": "events",
                              "ids": ["100-2", "100-4"]})
        self.assertEqual(result, {"type": "client_response",
                                  "status": "ok", "xdel": 2})
        self.assertEqual(entries_of(store.get("events")),
                         [("100-1", {"n": "1"}),
                          ("100-3", {"n": "3"}),
                          ("100-5", {"n": "5"})])
        self.assertEqual(store.stream_top("events"), "100-5")

    def test_xdel_unknown_and_duplicate_ids_do_not_count(self):
        """An ID that is not in the stream does not count, and one
        named twice counts once — the second naming finds nothing
        left to delete."""
        store = self._five()
        result = store.apply({"op": "xdel", "key": "events",
                              "ids": ["100-2", "100-9", "100-2"]})
        self.assertEqual(result["xdel"], 1)
        self.assertEqual([e[0] for e in entries_of(
            store.get("events"))],
            ["100-1", "100-3", "100-4", "100-5"])
        # Deleting the same ID again now counts nothing.
        result = store.apply({"op": "xdel", "key": "events",
                              "ids": ["100-2"]})
        self.assertEqual(result["xdel"], 0)

    def test_xdel_to_empty_keeps_key_and_top(self):
        """Deleting the last entry leaves the key behind as an
        empty stream — Redis keeps it too — with its top ID
        remembered, so an xadd at or below the old top still fails
        after a full delete, unlike an xtrim that took the key."""
        store = self._five()
        result = store.apply({"op": "xdel", "key": "events",
                              "ids": ["100-1", "100-2", "100-3",
                                      "100-4", "100-5"]})
        self.assertEqual(result["xdel"], 5)
        self.assertNotIn("deleted", result)
        self.assertIsNotNone(store.get("events"))
        self.assertEqual(store.xlen("events"), 0)
        self.assertEqual(store.stream_top("events"), "100-5")
        result = store.apply({"op": "xadd", "key": "events",
                             "pairs": [["n", "x"]], "id": "100-5"})
        self.assertEqual(result["status"], "xadd_failed")
        self.assertEqual(result["reason"], "id_not_greater")
        result = store.apply({"op": "xadd", "key": "events",
                             "pairs": [["n", "6"]], "id": "100-6"})
        self.assertEqual(result["xadd"], "100-6")
        self.assertEqual(entries_of(store.get("events")),
                         [("100-6", {"n": "6"})])

    def test_xdel_missing_key_answers_zero(self):
        """An xdel on a missing key answers 0 and writes nothing,
        like an expire on a missing key."""
        store = KVStore()
        result = store.apply({"op": "xdel", "key": "nope",
                              "ids": ["1-0"]})
        self.assertEqual(result["xdel"], 0)
        self.assertIsNone(store.get("nope"))

    def test_xdel_wrongtype(self):
        """An xdel on a plain string fails with wrongtype and
        leaves the value untouched, like an xadd."""
        store = KVStore()
        store.apply({"op": "set", "key": "plain", "value": "v"})
        result = store.apply({"op": "xdel", "key": "plain",
                              "ids": ["1-0"]})
        self.assertEqual(result["status"], "xdel_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("plain"), "v")

    def test_xdel_bad_ids_raise(self):
        """A hand-built log entry with a bad ID list raises rather
        than silently deleting nonsense."""
        store = self._five()
        for command in (
                {"op": "xdel", "key": "events", "ids": []},
                {"op": "xdel", "key": "events",
                 "ids": ["1-0"] * 1001},
                {"op": "xdel", "key": "events", "ids": [5]},
                {"op": "xdel", "key": "events", "ids": ["bogus"]},
                {"op": "xdel", "key": "events", "ids": ["0-0"]},
                {"op": "xdel", "key": "events", "ids": ["*"]},
                {"op": "xdel", "key": "events", "ids": ["100-*"]},
                {"op": "xdel", "key": "events",
                 "ids": ["100-3-4"]}):
            with self.assertRaises(ValueError, msg=str(command)):
                store.apply(command)
        self.assertEqual(len(entries_of(store.get("events"))), 5)

    def test_xdel_drops_pending_entries(self):
        """A deleted entry stops being pending in every group, so
        XPENDING agrees with the stream — the entry is gone, not
        merely unreadable."""
        store = self._five()
        store.apply({"op": "xgroup_create", "key": "events",
                     "group": "g", "start_id": "0"})
        result = store.apply({"op": "xreadgroup", "key": "events",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-1", "100-2", "100-3", "100-4",
                          "100-5"])
        self.assertEqual(store.xpending("events", "g")["pending"], 5)
        result = store.apply({"op": "xdel", "key": "events",
                              "ids": ["100-2", "100-4"]})
        self.assertEqual(result["xdel"], 2)
        pending = store.xpending("events", "g")
        self.assertEqual(pending["pending"], 3, pending)
        self.assertEqual(pending["min_id"], "100-1", pending)
        self.assertEqual(pending["max_id"], "100-5", pending)
        self.assertEqual(pending["consumers"], [["c1", 3]], pending)

    def test_xdel_keeps_ttl_deadline(self):
        """An xdel is an update, not a fresh set: the deadline
        survives it, even when it takes the last entry — the key
        stays, so the deadline has something to hang on."""
        store = KVStore()
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "1-0"}, index=10)
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "2"]], "id": "2-0"}, index=11)
        store.apply({"op": "expire", "key": "s",
                     "ttl_entries": 10}, index=12)
        self.assertEqual(store.expiries(), {"s": 22})
        result = store.apply({"op": "xdel", "key": "s",
                              "ids": ["1-0"]}, index=13)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.expiries(), {"s": 22})
        result = store.apply({"op": "xdel", "key": "s",
                              "ids": ["2-0"]}, index=14)
        self.assertEqual(result["xdel"], 1)
        self.assertEqual(store.expiries(), {"s": 22})
        self.assertIsNotNone(store.get("s"))

    def test_xdel_survives_snapshot_load(self):
        """A stream with entries deleted is a plain string in the
        store, so a snapshot load rebuilds the same entries and the
        same top ID — and an ID at or below the top still fails
        after the load."""
        store = self._five()
        store.apply({"op": "xdel", "key": "events",
                     "ids": ["100-2", "100-4"]})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(entries_of(loaded.get("events")),
                         [("100-1", {"n": "1"}),
                          ("100-3", {"n": "3"}),
                          ("100-5", {"n": "5"})])
        self.assertEqual(loaded.stream_top("events"), "100-5")
        result = loaded.apply({"op": "xadd", "key": "events",
                              "pairs": [["n", "x"]], "id": "100-3"})
        self.assertEqual(result["status"], "xadd_failed")
        self.assertEqual(result["reason"], "id_not_greater")

    def test_xdel_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for i in range(1, 6):
                    resp = session.xadd("events", {"n": str(i)},
                                        id=f"100-{i}")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xdel("events", "100-2", "100-4")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("xdel"), 2, resp)
                # A session read after its own delete sees it.
                resp = session.xlen("events")
                self.assertEqual(resp.get("xlen"), 3, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                self.assertEqual(
                    [e[0] for e in entries_of(
                        cluster.nodes[node_id].sm.get("events"))],
                    ["100-1", "100-3", "100-5"])
        finally:
            cluster.stop_all()

    def test_xdel_module_function_from_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            addr = cluster.addrs[leader.id]
            for i in range(1, 6):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            # The module function follows the leader redirect.
            resp = client_xdel(cluster.addrs[follower], "events",
                               "100-2", "100-4", "100-9")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xdel"), 2, resp)
            self.assertEqual(
                [e[0] for e in entries_of(
                    cluster.nodes[leader.id].sm.get("events"))],
                ["100-1", "100-3", "100-5"])
        finally:
            cluster.stop_all()

    def test_xdel_wrongtype_and_bad_args(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="plain").get("status"),
                             "ok")
            resp = xdel(addr, key="plain")
            self.assertEqual(resp.get("status"), "xdel_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            self.assertEqual(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "plain"}).get("value"),
                "old")
            # Bad arguments are client errors with no log entry.
            before = len(leader.log)
            for msg in (
                    {"ids": []},
                    {"ids": ["1-0"] * 1001},
                    {"ids": [5]},
                    {"ids": ["bogus"]},
                    {"ids": ["0-0"]},
                    {"ids": ["*"]},
                    {"ids": ["100-*"]}):
                resp = client_request(
                    addr, {"type": "client", "op": "xdel",
                           "key": "events", **msg})
                self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(len(leader.log), before)
            # A missing key answers 0, like an expire on one.
            resp = xdel(addr, key="nope")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xdel"), 0, resp)
        finally:
            cluster.stop_all()

    def test_xdel_dedup_retry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 6):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            resp = xdel(addr, ids=("100-2", "100-4"), client_id="c1",
                        request_id="d1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xdel"), 2, resp)
            # The retry returns the first attempt's count instead
            # of deleting twice — and logs one entry, not two.
            resp = xdel(addr, ids=("100-2", "100-4"), client_id="c1",
                        request_id="d1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xdel"), 2, resp)
            self.assertEqual(count_entries(leader, "d1"), 1)
            self.assertEqual(len(entries_of(
                leader.sm.get("events"))), 3)
        finally:
            cluster.stop_all()

    def test_xdel_drops_pending_on_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for i in range(1, 6):
                    resp = session.xadd("events", {"n": str(i)},
                                        id=f"100-{i}")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "g", "0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "g", "c1", ">")
                self.assertEqual(len(resp.get("entries", [])), 5,
                                 resp)
                resp = session.xdel("events", "100-2", "100-4")
                self.assertEqual(resp.get("xdel"), 2, resp)
                resp = session.xpending("events", "g")
                self.assertEqual(resp.get("pending"), 3, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                self.assertEqual(
                    cluster.nodes[node_id].sm.xpending(
                        "events", "g")["pending"], 3)
        finally:
            cluster.stop_all()

    def test_xdel_fires_watch_events(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 4):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            events = []
            leader.watch.watch("events", events.append)
            resp = xdel(addr, ids=("100-2",))
            self.assertEqual(resp.get("xdel"), 1, resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 1 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 1, events)
            self.assertEqual(events[0]["key"], "events", events)
            self.assertEqual(events[0]["op"], "set", events)
            self.assertEqual(events[0]["value"],
                             leader.sm.get("events"), events)
            # An xdel that removes nothing fires nothing.
            resp = xdel(addr, ids=("100-2",))
            self.assertEqual(resp.get("xdel"), 0, resp)
            time.sleep(0.3)
            self.assertEqual(len(events), 1, events)
        finally:
            cluster.stop_all()

    def test_pipeline_xdel_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for i in range(1, 4):
                    resp = session.xadd("events", {"n": str(i)},
                                        id=f"100-{i}")
                    self.assertEqual(resp.get("status"), "ok", resp)
                replies = session.pipeline([
                    {"op": "xdel", "key": "events",
                     "ids": ["100-2"]},
                    {"op": "xdel", "key": "events",
                     "ids": ["100-1", "100-9"]},
                    {"op": "xdel", "key": "events", "ids": []},
                    {"op": "xdel", "key": "events",
                     "ids": ["bogus"]},
                    {"op": "xdel", "key": "nope",
                     "ids": ["1-0"]},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("xdel"), 1, replies)
            self.assertEqual(replies[1].get("xdel"), 1, replies)
            self.assertEqual(replies[2].get("status"), "error",
                             replies)
            self.assertEqual(replies[3].get("status"), "error",
                             replies)
            self.assertEqual(replies[4].get("xdel"), 0, replies)
            self.assertEqual(
                [e[0] for e in entries_of(leader.sm.get("events"))],
                ["100-3"])
        finally:
            cluster.stop_all()

    def test_xdel_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(1, 6):
                self.assertEqual(
                    xadd(addr, id=f"100-{i}").get("status"), "ok")
            resp = xdel(addr, ids=("100-2", "100-4"))
            self.assertEqual(resp.get("xdel"), 2, resp)
            # Enough writes to compact the log past the delete.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(
                [e[0] for e in entries_of(leader.sm.get("events"))],
                ["100-1", "100-3", "100-5"])
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The deleted entries stayed deleted through the
            # snapshot + restart, and the top is still remembered.
            self.assertEqual(
                [e[0] for e in entries_of(
                    new_leader.sm.get("events"))],
                ["100-1", "100-3", "100-5"])
            resp = xadd(addr, id="100-4")
            self.assertEqual(resp.get("status"), "xadd_failed", resp)
            self.assertEqual(resp.get("reason"), "id_not_greater",
                             resp)
            resp = xadd(addr, id="100-6")
            self.assertEqual(resp.get("xadd"), "100-6", resp)
        finally:
            cluster.stop_all()

    def test_xdel_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            for i in range(1, 4):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = client_main(["--node", node, "xadd", "events",
                                      f"100-{i}", "n", str(i)])
                self.assertEqual(rc, 0)
            # XDEL prints the number of entries removed.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xdel", "events",
                                  "100-2"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # An unknown ID prints 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xdel", "events",
                                  "100-9"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # A bad ID fails: rc 1, and the error names the
            # problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xdel", "events",
                                  "bogus"])
            self.assertEqual(rc, 1)
            self.assertIn("ids", err.getvalue())
        finally:
            cluster.stop_all()


class TestXrevrange(unittest.TestCase):
    def _four(self):
        """A stream with four entries, 100-5, 100-6, 101-0, 102-3."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0", "102-3"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        return store

    def test_xrevrange_full_range_is_xrange_reversed(self):
        """XREVRANGE with "+" / "-" returns every entry in
        reverse ID order — exactly XRANGE's answer, reversed —
        and a missing key answers an empty list."""
        store = KVStore()
        self.assertEqual(store.xrevrange("s", "+", "-"), [])
        store = self._four()
        self.assertEqual(store.xrevrange("s", "+", "-"),
                         [["102-3", {"a": "102-3"}],
                          ["101-0", {"a": "101-0"}],
                          ["100-6", {"a": "100-6"}],
                          ["100-5", {"a": "100-5"}]])
        self.assertEqual(store.xrevrange("s", "+", "-"),
                         list(reversed(store.xrange("s", "-", "+"))))

    def test_xrevrange_partial_ranges(self):
        """Explicit bounds cut the range, a bare millisecond
        names that millisecond with sequence 0, and an end
        below the start answers an empty list rather than an
        error."""
        store = self._four()
        self.assertEqual(store.xrevrange("s", "101-0", "100-6"),
                         [["101-0", {"a": "101-0"}],
                          ["100-6", {"a": "100-6"}]])
        # A bare millisecond is that millisecond, sequence 0.
        self.assertEqual(store.xrevrange("s", "101", "101"),
                         [["101-0", {"a": "101-0"}]])
        self.assertEqual(store.xrevrange("s", "100-6", "100"),
                         [["100-6", {"a": "100-6"}],
                          ["100-5", {"a": "100-5"}]])
        # "+" names the end and 0-0 the very start.
        self.assertEqual(
            [e[0] for e in store.xrevrange("s", "+", "0-0")],
            ["102-3", "101-0", "100-6", "100-5"])
        # An end below the start is empty, not an error.
        self.assertEqual(store.xrevrange("s", "100-5", "102-3"), [])
        self.assertEqual(store.xrevrange("s", "200", "+"), [])

    def test_xrevrange_count_caps_from_newest_and_zero_is_empty(self):
        """COUNT caps how many entries one call returns, taken
        from the newest end — and COUNT 0 answers an empty
        list."""
        store = self._four()
        self.assertEqual(store.xrevrange("s", "+", "-", count=2),
                         [["102-3", {"a": "102-3"}],
                          ["101-0", {"a": "101-0"}]])
        self.assertEqual(store.xrevrange("s", "+", "-", count=0), [])
        self.assertEqual(store.xrevrange("s", "+", "-", count=99),
                         [["102-3", {"a": "102-3"}],
                          ["101-0", {"a": "101-0"}],
                          ["100-6", {"a": "100-6"}],
                          ["100-5", {"a": "100-5"}]])
        # A count past the range start stops at the range start.
        self.assertEqual(store.xrevrange("s", "101-0", "-",
                                         count=99),
                         [["101-0", {"a": "101-0"}],
                          ["100-6", {"a": "100-6"}],
                          ["100-5", {"a": "100-5"}]])

    def test_xrevrange_wrongtype_leaves_value_untouched(self):
        """A plain string value fails an xrevrange with
        TypeError and leaves the value untouched."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "x"})
        with self.assertRaises(TypeError):
            store.xrevrange("k", "+", "-")
        self.assertEqual(store.get("k"), "x")

    def test_xrevrange_bad_bounds_and_count_raise(self):
        """A bad bound or count raises ValueError — even on a
        missing key, since the bounds are parsed before the key
        is looked up."""
        store = KVStore()
        bad_bounds = ["bogus", "1-2-3", "", "5-",
                      "18446744073709551616-0",
                      "5-18446744073709551616",
                      "18446744073709551616",
                      5, None]
        for bad in bad_bounds:
            with self.assertRaises(ValueError, msg=str(bad)):
                store.xrevrange("s", bad, "-")
            with self.assertRaises(ValueError, msg=str(bad)):
                store.xrevrange("s", "+", bad)
        for bad_count in (-1, True, "2", 1.5):
            with self.assertRaises(ValueError, msg=str(bad_count)):
                store.xrevrange("s", "+", "-", count=bad_count)

    def test_xrevrange_cluster(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for entry_id in ("100-5", "100-6", "101-0"):
                resp = xadd(addr, pairs=(("a", entry_id),),
                            id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)
            # XREVRANGE reads them back newest-first — including
            # from a follower, which redirects to the leader.
            for node_id in cluster.live:
                resp = xrevrange(cluster.addrs[node_id])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("entries"),
                                 [["101-0", {"a": "101-0"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["100-5", {"a": "100-5"}]])
            # A partial range and a COUNT cap from the newest
            # end, from a follower.
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = xrevrange(cluster.addrs[follower], end="100-6",
                             start="-")
            self.assertEqual(resp.get("entries"),
                             [["100-6", {"a": "100-6"}],
                              ["100-5", {"a": "100-5"}]])
            resp = xrevrange(cluster.addrs[follower], count=2)
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["101-0", "100-6"])
            # A missing key answers an empty list.
            self.assertEqual(xrevrange(addr, key="nope")
                             .get("entries"), [])
            # The module function follows redirects too.
            resp = client_xrevrange(cluster.addrs[follower],
                                    "events", end="100-6", start="-",
                                    count=1)
            self.assertEqual(resp.get("entries"),
                             [["100-6", {"a": "100-6"}]], resp)
            # A plain string value is a client error, not a
            # list, and bad bounds and counts are client errors
            # too — and reads append no log entries, bad or not.
            self.assertEqual(write(addr, key="plain",
                                   value="v").get("status"), "ok")
            before = len(leader.log)
            resp = xrevrange(addr, key="plain")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("stream", resp.get("error", ""), resp)
            resp = xrevrange(addr, end="bogus")
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "xrevrange",
                       "key": "s", "end": "+", "start": "-",
                       "count": -1})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(len(leader.log), before)
            # A session read sees its own xadd's entry, newest
            # first, and a session read from a follower still
            # sees it.
            with Session(addr) as session:
                resp = session.xadd("events", {"type": "login"},
                                    id="102-3")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xrevrange("events")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in
                                  resp.get("entries")],
                                 ["102-3", "101-0", "100-6",
                                  "100-5"], resp)
                with Session(cluster.addrs[follower],
                             client_id=session.client_id) as other:
                    other.last_write_index = session.last_write_index
                    other.last_write_term = session.last_write_term
                    resp = other.xrevrange("events", end="101-0",
                                           start="100-6")
                    self.assertEqual(resp.get("entries"),
                                     [["101-0", {"a": "101-0"}],
                                      ["100-6", {"a": "100-6"}]],
                                     resp)
        finally:
            cluster.stop_all()

    def test_xrevrange_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            for entry_id in ("100-5", "100-6"):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = client_main(["--node", node, "xadd", "events",
                                      entry_id, "a", entry_id])
                self.assertEqual(rc, 0)
            # XREVRANGE prints the entries as JSON, newest
            # first.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xrevrange",
                                  "events", "+", "-"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-6", {"a": "100-6"}],
                              ["100-5", {"a": "100-5"}]])
            # A range with COUNT prints just the newest page.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xrevrange",
                                  "events", "+", "-", "--count", "1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-6", {"a": "100-6"}]])
            # XREVRANGE on a missing key prints [].
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xrevrange",
                                  "nope", "+", "-"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), [])
            # A bad bound fails: rc 1, and the error names the
            # problem.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "xrevrange",
                                  "events", "bogus", "-"])
            self.assertEqual(rc, 1)
            self.assertIn("bound", err.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
