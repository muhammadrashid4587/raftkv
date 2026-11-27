"""Tests for raftkv consumer groups: XREADGROUP (v0.67.0).

``xgroup_create key group start [MKSTREAM]`` creates a consumer
group on a stream — "$" (only entries appended after the group is
created), "0" (from the very first entry), a bare millisecond, or a
full "ms-seq" — and ``xreadgroup key group consumer [id] [COUNT n]
[NOACK]`` hands entries to one consumer: ">" delivers the entries
appended after the group's last delivery and marks each one pending
for that consumer, any other ID re-reads the consumer's own pending
entries and raises each one's delivery count. ``xack key group
id ...`` acknowledges pending entries, and ``xpending key group``
summarises the pending list. The groups ride in the stream blob
with the entries, so they survive snapshots and replay exactly
like the entries — a group created before a snapshot is still
there after one, with its pending list intact. These spin up real
nodes (real TCP, real threads, real disk) on localhost, like the
rest of the suite.
"""

import contextlib
import io
import json
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import xack as client_xack
from raftkv.client import xadd as client_xadd
from raftkv.client import xgroup_create as client_xgroup_create
from raftkv.client import xpending as client_xpending
from raftkv.client import xreadgroup as client_xreadgroup
from raftkv.client import xtrim as client_xtrim
from raftkv.store import KVStore, STREAM_PREFIX, is_stream


def xadd(addr, key="events", pairs=(("type", "login"),), id="*",
         client_id=None, request_id=None):
    msg = {"type": "client", "op": "xadd", "key": key,
           "pairs": [[f, v] for f, v in pairs], "id": id}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xgroup_create(addr, key="events", group="workers",
                  start_id="$", mkstream=None, client_id=None,
                  request_id=None):
    msg = {"type": "client", "op": "xgroup_create", "key": key,
           "group": group, "start_id": start_id}
    if mkstream is not None:
        msg["mkstream"] = mkstream
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xreadgroup(addr, key="events", group="workers",
               consumer="c1", id=">", count=None, noack=None,
               client_id=None, request_id=None):
    msg = {"type": "client", "op": "xreadgroup", "key": key,
           "group": group, "consumer": consumer, "id": id}
    if count is not None:
        msg["count"] = count
    if noack is not None:
        msg["noack"] = noack
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xack(addr, key="events", group="workers", ids=("100-0",),
         client_id=None, request_id=None):
    msg = {"type": "client", "op": "xack", "key": key,
           "group": group, "ids": list(ids)}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xpending(addr, key="events", group="workers"):
    return client_request(addr, {"type": "client", "op": "xpending",
                                 "key": key, "group": group})


def xtrim(addr, key="events", strategy="MAXLEN", threshold=0,
          mode=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xtrim", "key": key,
           "strategy": strategy, "threshold": threshold}
    if mode is not None:
        msg["mode"] = mode
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="k", value="old"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def groups_of(value):
    """The {"group": {"last_delivered": ..., "pending": {...}}}
    map behind a stream blob, or None when the key holds nothing."""
    if value is None:
        return None
    assert is_stream(value), value
    data = json.loads(value[len(STREAM_PREFIX):])
    return data.get("groups", {})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestXReadGroupStore(unittest.TestCase):
    def test_create_group_from_zero_reads_from_the_start(self):
        """A group created with "0" starts at the very first entry:
        its last_delivered is 0-0, so a ">" read hands out every
        entry the stream already holds."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        result = store.apply({"op": "xgroup_create", "key": "s",
                              "group": "g", "start_id": "0"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xgroup_create"], 1, result)
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "0-0")
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5", "100-6", "101-0"])
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "101-0")

    def test_create_group_from_dollar_reads_only_new_entries(self):
        """A group created with "$" starts at the stream's top: a
        ">" read hands out nothing already there, and only the
        entries appended after the group was created."""
        store = KVStore()
        for entry_id in ("100-5", "100-6"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        result = store.apply({"op": "xgroup_create", "key": "s",
                              "group": "g", "start_id": "$"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "100-6")
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual(result["entries"], [])
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "101-0"]], "id": "101-0"})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual(result["entries"],
                         [["101-0", {"a": "101-0"}]])

    def test_create_group_errors(self):
        """A group cannot be created twice, cannot be created on a
        missing key without MKSTREAM, and cannot be created on a
        plain string — each failure names its reason — while
        MKSTREAM creates an empty stream whose group starts with
        no last delivery, so every later entry counts as new."""
        store = KVStore()
        result = store.apply({"op": "xgroup_create", "key": "s",
                              "group": "g", "start_id": "$"})
        self.assertEqual(result["status"], "xgroup_create_failed",
                         result)
        self.assertEqual(result["reason"], "no_group", result)
        result = store.apply({"op": "xgroup_create", "key": "s",
                              "group": "g", "start_id": "$",
                              "mkstream": True})
        self.assertEqual(result["status"], "ok", result)
        self.assertIsNone(store.get("s") and
                          groups_of(store.get("s"))["g"]
                          ["last_delivered"])
        self.assertEqual(store.xlen("s"), 0)
        result = store.apply({"op": "xgroup_create", "key": "s",
                              "group": "g", "start_id": "$"})
        self.assertEqual(result["status"], "xgroup_create_failed",
                         result)
        self.assertEqual(result["reason"], "group_exists", result)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        result = store.apply({"op": "xgroup_create", "key": "plain",
                              "group": "g", "start_id": "$"})
        self.assertEqual(result["status"], "xgroup_create_failed",
                         result)
        self.assertEqual(result["reason"], "wrongtype", result)
        # A group created on an empty stream with "$" delivers
        # every entry appended after it.
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "1"]], "id": "100-5"})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual(result["entries"],
                         [["100-5", {"a": "1"}]])

    def test_readgroup_two_consumers_split_the_work(self):
        """Two consumers reading ">" with COUNT 1 split the entries
        between them: each entry is handed to exactly one consumer,
        and a second ">" read by either hands out nothing."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        first = store.apply({"op": "xreadgroup", "key": "s",
                             "group": "g", "consumer": "c1", "id": ">",
                             "count": 1})
        self.assertEqual(first["entries"],
                         [["100-5", {"a": "100-5"}]])
        second = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c2", "id": ">",
                              "count": 1})
        self.assertEqual(second["entries"],
                         [["100-6", {"a": "100-6"}]])
        third = store.apply({"op": "xreadgroup", "key": "s",
                             "group": "g", "consumer": "c1", "id": ">",
                             "count": 1})
        self.assertEqual(third["entries"],
                         [["101-0", {"a": "101-0"}]])
        self.assertEqual(
            store.apply({"op": "xreadgroup", "key": "s",
                         "group": "g", "consumer": "c1",
                         "id": ">"})["entries"], [])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["consumer"], "c1")
        self.assertEqual(pending["100-6"]["consumer"], "c2")
        self.assertEqual(pending["101-0"]["consumer"], "c1")

    def test_readgroup_explicit_id_rereads_and_counts_deliveries(self):
        """An explicit-ID read re-reads only the calling consumer's
        own pending entries at or after the ID — another consumer's
        pending entries stay invisible — and each re-read raises
        the entry's delivery count by one, so a consumer can tell
        a first delivery from a retry."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">", "count": 1})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "0"})
        self.assertEqual(result["entries"],
                         [["100-5", {"a": "100-5"}]])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["deliveries"], 2)
        self.assertEqual(pending["100-6"]["deliveries"], 1)
        # From 100-6, c1 has nothing pending at or after it.
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "100-6"})
        self.assertEqual(result["entries"], [])
        # c2 re-reads its own entry, and the count rises again.
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c2",
                              "id": "100-6"})
        self.assertEqual(result["entries"],
                         [["100-6", {"a": "100-6"}]])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-6"]["deliveries"], 2)
        # COUNT caps a pending re-read too.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "0", "count": 1})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5"])

    def test_readgroup_noack_delivers_without_pending(self):
        """NOACK hands the entries out and still advances the last
        delivery — a second ">" read hands out nothing — but
        records nothing pending, so XPENDING stays empty and the
        entries can never be re-read or acked."""
        store = KVStore()
        for entry_id in ("100-5", "100-6"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1", "id": ">",
                              "noack": True})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5", "100-6"])
        self.assertEqual(store.xpending("s", "g"),
                         {"pending": 0, "min_id": None,
                          "max_id": None, "consumers": []})
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "100-6")
        self.assertEqual(
            store.apply({"op": "xreadgroup", "key": "s",
                         "group": "g", "consumer": "c1",
                         "id": ">"})["entries"], [])
        result = store.apply({"op": "xack", "key": "s", "group": "g",
                              "ids": ["100-5"]})
        self.assertEqual(result["xack"], 0, result)

    def test_xack_acks_any_consumers_entries(self):
        """XACK removes pending entries — for any consumer, since
        an ack names the entry, not its holder — and answers how
        many actually went: an ID never delivered, or already
        acked, simply does not count."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">", "count": 1})
        result = store.apply({"op": "xack", "key": "s", "group": "g",
                              "ids": ["100-5", "100-6", "999-9"]})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xack"], 2, result)
        self.assertEqual(store.xpending("s", "g"),
                         {"pending": 0, "min_id": None,
                          "max_id": None, "consumers": []})
        # Acking the same IDs again acks nothing.
        result = store.apply({"op": "xack", "key": "s", "group": "g",
                              "ids": ["100-5", "100-6"]})
        self.assertEqual(result["xack"], 0, result)
        # A missing key or group is NOGROUP, not a zero.
        result = store.apply({"op": "xack", "key": "nope",
                              "group": "g", "ids": ["100-5"]})
        self.assertEqual(result["status"], "xack_failed", result)
        self.assertEqual(result["reason"], "no_group", result)
        result = store.apply({"op": "xack", "key": "s",
                              "group": "nope", "ids": ["100-5"]})
        self.assertEqual(result["status"], "xack_failed", result)
        self.assertEqual(result["reason"], "no_group", result)
        # A readgroup on a missing key or group is NOGROUP too.
        result = store.apply({"op": "xreadgroup", "key": "nope",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual(result["status"], "xreadgroup_failed",
                         result)
        self.assertEqual(result["reason"], "no_group", result)
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "nope", "consumer": "c1",
                              "id": ">"})
        self.assertEqual(result["status"], "xreadgroup_failed",
                         result)
        self.assertEqual(result["reason"], "no_group", result)

    def test_xpending_summary(self):
        """XPENDING answers the pending count, the smallest and
        largest pending ID, and the per-consumer counts sorted by
        consumer name — and a missing key or group, or a plain
        string value, is an error, not a summary of zeros."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0", "102-3"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">", "count": 2})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1})
        self.assertEqual(store.xpending("s", "g"),
                         {"pending": 3, "min_id": "100-5",
                          "max_id": "101-0",
                          "consumers": [["c1", 1], ["c2", 2]]})
        store.apply({"op": "xack", "key": "s", "group": "g",
                     "ids": ["100-5", "101-0"]})
        self.assertEqual(store.xpending("s", "g"),
                         {"pending": 1, "min_id": "100-6",
                          "max_id": "100-6",
                          "consumers": [["c2", 1]]})
        for bad_key, bad_group in (("nope", "g"), ("s", "nope")):
            with self.assertRaises(ValueError,
                                   msg=(bad_key, bad_group)):
                store.xpending(bad_key, bad_group)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        with self.assertRaises(TypeError):
            store.xpending("plain", "g")
        # A group with nothing pending summarises empty, not an
        # error.
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "empty", "start_id": "$"})
        self.assertEqual(store.xpending("s", "empty"),
                         {"pending": 0, "min_id": None,
                          "max_id": None, "consumers": []})

    def test_groups_survive_snapshot_load(self):
        """The groups ride in the stream blob, so a snapshot load
        rebuilds the group, its last delivery, and its pending
        list with delivery counts — and a re-read after the load
        still raises the count instead of starting over."""
        store = KVStore()
        for entry_id in ("100-5", "100-6"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">"})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(loaded.xpending("s", "g"),
                         {"pending": 2, "min_id": "100-5",
                          "max_id": "100-6",
                          "consumers": [["c1", 2]]})
        result = loaded.apply({"op": "xreadgroup", "key": "s",
                               "group": "g", "consumer": "c1",
                               "id": "0"})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5", "100-6"])
        pending = groups_of(loaded.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["deliveries"], 2)
        # A stream created before groups existed loads without a
        # groups key and reads like one with no groups.
        plain = KVStore()
        plain.apply({"op": "xadd", "key": "old",
                     "pairs": [["a", "1"]], "id": "100-5"})
        blob = plain.get("old")
        self.assertNotIn("groups", json.loads(
            blob[len(STREAM_PREFIX):]))
        with self.assertRaises(ValueError):
            plain.xpending("old", "g")

    def test_xtrim_drops_pending_by_default(self):
        """Trimming drops the pending references of the entries
        that went — DELREF, and no mode at all — so a trimmed
        entry stops being pending in every group; a trim that
        leaves entries behind keeps the rest pending."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">"})
        result = store.apply({"op": "xtrim", "key": "s",
                              "strategy": "MAXLEN", "threshold": 1})
        self.assertEqual(result["xtrim"], 2, result)
        self.assertEqual(store.xpending("s", "g"),
                         {"pending": 1, "min_id": "101-0",
                          "max_id": "101-0",
                          "consumers": [["c1", 1]]})

    def test_xtrim_keepref_keeps_pending_but_skips_data(self):
        """KEEPREF keeps the pending reference of a trimmed entry:
        it still counts in XPENDING, but a later explicit-ID
        re-read skips it, since its data is gone — and ACKED
        behaves like KEEPREF in this version."""
        for mode in ("KEEPREF", "ACKED"):
            store = KVStore()
            for entry_id in ("100-5", "100-6", "101-0"):
                store.apply({"op": "xadd", "key": "s",
                             "pairs": [["a", entry_id]],
                             "id": entry_id})
            store.apply({"op": "xgroup_create", "key": "s",
                         "group": "g", "start_id": "0"})
            store.apply({"op": "xreadgroup", "key": "s",
                         "group": "g", "consumer": "c1", "id": ">"})
            result = store.apply({"op": "xtrim", "key": "s",
                                  "strategy": "MAXLEN", "threshold": 1,
                                  "mode": mode})
            self.assertEqual(result["xtrim"], 2, result)
            self.assertEqual(store.xpending("s", "g"),
                             {"pending": 3, "min_id": "100-5",
                              "max_id": "101-0",
                              "consumers": [["c1", 3]]}, mode)
            result = store.apply({"op": "xreadgroup", "key": "s",
                                  "group": "g", "consumer": "c1",
                                  "id": "0"})
            self.assertEqual(result["entries"],
                             [["101-0", {"a": "101-0"}]], mode)
            # The skipped entries are still pending, and still
            # ackable — the reference survived the trim.
            result = store.apply({"op": "xack", "key": "s",
                                  "group": "g", "ids": ["100-5"]})
            self.assertEqual(result["xack"], 1, result)


class TestXReadGroupCluster(unittest.TestCase):
    def test_full_flow_replicates_to_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for entry_id in ("100-5", "100-6", "101-0"):
                resp = xadd(addr, pairs=(("a", entry_id),),
                            id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)
            resp = xgroup_create(addr, start_id="0")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = xreadgroup(addr, consumer="c1", count=2)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["100-5", "100-6"])
            resp = xreadgroup(addr, consumer="c2")
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["101-0"])
            resp = xpending(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("pending"), 3, resp)
            self.assertEqual(resp.get("consumers"),
                             [["c1", 2], ["c2", 1]], resp)
            resp = xack(addr, ids=["100-5"])
            self.assertEqual(resp.get("xack"), 1, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                self.assertEqual(
                    groups_of(cluster.nodes[node_id].sm.get("events"))
                    ["workers"]["last_delivered"], "101-0")
                self.assertEqual(
                    cluster.nodes[node_id].sm.xpending(
                        "events", "workers")["pending"], 2)
        finally:
            cluster.stop_all()

    def test_readgroup_retry_delivers_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            xadd(addr, id="100-5")
            xadd(addr, id="100-6")
            self.assertEqual(
                xgroup_create(addr, start_id="0").get("status"), "ok")
            first = xreadgroup(addr, client_id="c1", request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            self.assertEqual([e[0] for e in first.get("entries")],
                             ["100-5", "100-6"])
            second = xreadgroup(addr, client_id="c1", request_id="r1")
            self.assertEqual(second.get("status"), "ok", second)
            self.assertEqual(second.get("entries"),
                             first.get("entries"), second)
            self.assertEqual(count_entries(leader, "r1"), 1)
            # The retry did not deliver twice: still two pending,
            # each with one delivery.
            self.assertEqual(xpending(addr).get("pending"), 2)
            pending = groups_of(leader.sm.get("events"))["workers"][
                "pending"]
            self.assertEqual(pending["100-5"]["deliveries"], 1)
            # A retry with a different consumer still returns the
            # first attempt's entries, not a second delivery.
            third = xreadgroup(addr, consumer="c2",
                               client_id="c1", request_id="r1")
            self.assertEqual(third.get("entries"),
                             first.get("entries"), third)
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_invalid_args_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="plain",
                                   value="v").get("status"), "ok")
            before = len(leader.log)
            # Wrongtype reads and acks are apply-time failures,
            # not client errors — they commit an entry.
            resp = xreadgroup(addr, key="plain")
            self.assertEqual(resp.get("status"), "xreadgroup_failed",
                             resp)
            resp = xack(addr, key="plain")
            self.assertEqual(resp.get("status"), "xack_failed", resp)
            resp = xpending(addr, key="plain")
            self.assertEqual(resp.get("status"), "error", resp)
            # Bad arguments are client errors with no log entry.
            for msg in (
                    {"type": "client", "op": "xgroup_create",
                     "key": "events", "group": "", "start_id": "$"},
                    {"type": "client", "op": "xgroup_create",
                     "key": "events", "group": "g",
                     "start_id": "bogus"},
                    {"type": "client", "op": "xgroup_create",
                     "key": "events", "group": "g", "start_id": "$",
                     "mkstream": "yes"},
                    {"type": "client", "op": "xreadgroup",
                     "key": "events", "group": "g", "consumer": "",
                     "id": ">"},
                    {"type": "client", "op": "xreadgroup",
                     "key": "events", "group": "g", "consumer": "c1",
                     "id": "$"},
                    {"type": "client", "op": "xreadgroup",
                     "key": "events", "group": "g", "consumer": "c1",
                     "id": ">", "count": -1},
                    {"type": "client", "op": "xreadgroup",
                     "key": "events", "group": "g", "consumer": "c1",
                     "id": ">", "noack": "yes"},
                    {"type": "client", "op": "xack",
                     "key": "events", "group": "g", "ids": []},
                    {"type": "client", "op": "xack",
                     "key": "events", "group": "g",
                     "ids": ["100-5"] * 1001},
                    {"type": "client", "op": "xack",
                     "key": "events", "group": "g", "ids": ["bogus"]},
                    {"type": "client", "op": "xpending",
                     "key": "events", "group": ""},
                    {"type": "client", "op": "xpending",
                     "key": "events"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before + 2)
        finally:
            cluster.stop_all()

    def test_session_methods(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("events", {"a": "2"}, id="100-6")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("xgroup_create"), 1, resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5", "100-6"])
                # A session read sees its own readgroup's pending.
                resp = session.xpending("events", "workers")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("pending"), 2, resp)
                resp = session.xack("events", "workers", "100-5")
                self.assertEqual(resp.get("xack"), 1, resp)
                resp = session.xpending("events", "workers")
                self.assertEqual(resp.get("pending"), 1, resp)
                # Bad arguments never go on the wire.
                resp = session.xgroup_create("events", "")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xreadgroup("events", "workers", "")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xack("events", "workers")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xpending("events", "")
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_module_functions_from_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            addr = cluster.addrs[follower]
            resp = client_xadd(addr, "events", {"a": "1"}, id="100-5")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_xgroup_create(addr, "events", "workers",
                                        start_id="0")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_xreadgroup(addr, "events", "workers", "c1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"a": "1"}]], resp)
            resp = client_xpending(addr, "events", "workers")
            self.assertEqual(resp.get("pending"), 1, resp)
            resp = client_xack(addr, "events", "workers", ["100-5"])
            self.assertEqual(resp.get("xack"), 1, resp)
            resp = client_xpending(addr, "events", "workers")
            self.assertEqual(resp.get("pending"), 0, resp)
        finally:
            cluster.stop_all()

    def test_pipeline_group_ops_in_position(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                results = session.pipeline([
                    {"op": "xadd", "key": "events",
                     "pairs": [["a", "1"]], "id": "100-5"},
                    {"op": "xadd", "key": "events",
                     "pairs": [["a", "2"]], "id": "100-6"},
                    {"op": "xgroup_create", "key": "events",
                     "group": "workers", "start_id": "0"},
                    {"op": "xreadgroup", "key": "events",
                     "group": "workers", "consumer": "c1",
                     "count": 1},
                    {"op": "xack", "key": "events",
                     "group": "workers", "ids": ["100-5"]},
                ])
            self.assertEqual(len(results), 5, results)
            for resp in results:
                self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(results[2].get("xgroup_create"), 1)
            self.assertEqual(results[3].get("entries"),
                             [["100-5", {"a": "1"}]])
            self.assertEqual(results[4].get("xack"), 1)
            self.assertEqual(
                session.xpending("events", "workers").get("pending"),
                0)
            # A malformed group op in a pipeline is a per-op
            # error, not a crash.
            with Session(addr) as session:
                results = session.pipeline([
                    {"op": "xgroup_create", "key": "events",
                     "group": "", "start_id": "$"},
                    {"op": "xreadgroup", "key": "events",
                     "group": "workers", "consumer": "c1",
                     "id": "$"},
                    {"op": "xack", "key": "events",
                     "group": "workers", "ids": []},
                ])
            self.assertEqual(len(results), 3, results)
            for resp in results:
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_groups_survive_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(xadd(addr, id="100-5").get("xadd"),
                             "100-5")
            self.assertEqual(
                xgroup_create(addr, start_id="0").get("status"), "ok")
            self.assertEqual(
                xreadgroup(addr).get("entries"),
                [["100-5", {"type": "login"}]])
            # Enough writes to compact the log past the group ops.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The group survived the snapshot + restart with its
            # pending list: the entry is still pending for c1, and
            # a re-read raises its delivery count instead of
            # delivering it fresh.
            resp = xpending(addr)
            self.assertEqual(resp.get("pending"), 1, resp)
            self.assertEqual(resp.get("consumers"), [["c1", 1]], resp)
            resp = xreadgroup(addr, id="0")
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"type": "login"}]], resp)
            pending = groups_of(new_leader.sm.get("events"))[
                "workers"]["pending"]
            self.assertEqual(pending["100-5"]["deliveries"], 2)
        finally:
            cluster.stop_all()

    def test_cli_end_to_end(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            node = f"127.0.0.1:{cluster.addrs[leader.id][1]}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xadd", "events",
                                  "100-5", "a", "1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "100-5")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_create",
                                  "events", "workers", "0"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "OK")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xreadgroup",
                                  "events", "workers", "c1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-5", {"a": "1"}]])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             {"pending": 1, "min_id": "100-5",
                              "max_id": "100-5",
                              "consumers": [["c1", 1]]})
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xack",
                                  "events", "workers", "100-5"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "1")
            # MKSTREAM creates the stream and the group together.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_create",
                                  "fresh", "g", "$", "--mkstream"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "OK")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
