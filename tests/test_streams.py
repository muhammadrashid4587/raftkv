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
exactly like any other value. There is no MAXLEN trimming yet, so
a stream grows without bound, and no XREAD / XRANGE / XLEN yet, so
the only way to read entries back is the raw blob. These spin up
real nodes (real TCP, real threads, real disk) on localhost, like
the rest of the suite.
"""

import contextlib
import io
import json
import re
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import xadd as client_xadd
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


if __name__ == "__main__":
    unittest.main()
