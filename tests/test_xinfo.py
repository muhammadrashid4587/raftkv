"""Tests for raftkv stream introspection: XINFO STREAM / GROUPS /
CONSUMERS (v0.70.0).

``xinfo_stream key`` reports the stream itself — its length, top ID,
group count, and first / last entry. ``xinfo_groups key`` lists one
dict per consumer group — its last-delivered ID, entries-read count,
lag, and pending count. ``xinfo_consumers key group`` lists one dict
per consumer — its pending count, its idle time, the milliseconds since
that consumer was last handed an entry, and its lag, the entries in
the stream past the newest entry ever handed to that consumer. The idle
time is tracked, not guessed: every xreadgroup stamps the consumer's
last-seen millisecond — fixed by the leader at log time, like an xadd
auto ID — in the group's "consumers" map in the stream blob, so it
survives snapshots and replay like the rest of the group, and a
DELCONSUMER drops the record with the consumer. A consumer only appears once it has been
handed at least one entry: a readgroup that hands out nothing writes
nothing at all, so a consumer that only ever polled an empty group is
not listed. These spin up real nodes (real TCP, real threads, real
disk) on localhost, like the rest of the suite.
"""

import contextlib
import io
import json
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import xack as client_xack
from raftkv.client import xadd as client_xadd
from raftkv.client import xgroup_create as client_xgroup_create
from raftkv.client import xgroup_delconsumer as client_xgroup_delconsumer
from raftkv.client import xgroup_destroy as client_xgroup_destroy
from raftkv.client import xgroup_setid as client_xgroup_setid
from raftkv.client import xinfo_consumers as client_xinfo_consumers
from raftkv.client import xinfo_groups as client_xinfo_groups
from raftkv.client import xinfo_stream as client_xinfo_stream
from raftkv.client import xreadgroup as client_xreadgroup
from raftkv.store import KVStore


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
               block=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xreadgroup", "key": key,
           "group": group, "consumer": consumer, "id": id}
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


def xgroup_setid(addr, key="events", group="workers", id="$",
                 entries_read=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xgroup_setid", "key": key,
           "group": group, "id": id}
    if entries_read is not None:
        msg["entries_read"] = entries_read
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xgroup_destroy(addr, key="events", group="workers",
                   client_id=None, request_id=None):
    msg = {"type": "client", "op": "xgroup_destroy", "key": key,
           "group": group}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xgroup_delconsumer(addr, key="events", group="workers",
                       consumer="c1", client_id=None, request_id=None):
    msg = {"type": "client", "op": "xgroup_delconsumer", "key": key,
           "group": group, "consumer": consumer}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def xinfo(addr, key="events", sub="STREAM", group=None):
    msg = {"type": "client", "op": "xinfo", "sub": sub, "key": key}
    if group is not None:
        msg["group"] = group
    return client_request(addr, msg)


def _seed_store():
    """A store with a stream of three entries and a group created
    from "0" that has already been handed the first two — to c1,
    stamped at a fixed millisecond so the idle is exact."""
    store = KVStore()
    for entry_id in ("100-5", "100-6", "101-0"):
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", entry_id]], "id": entry_id})
    store.apply({"op": "xgroup_create", "key": "s",
                 "group": "g", "start_id": "0"})
    store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                 "consumer": "c1", "id": ">", "count": 2,
                 "seen_ms": 1700000000000})
    return store


class TestXInfoStore(unittest.TestCase):
    def test_stream_reports_length_top_groups_and_ends(self):
        """XINFO STREAM answers the length, the top ID, how many
        groups the stream has, and the first / last entry as [id,
        {field: value}] pairs."""
        store = _seed_store()
        self.assertEqual(store.xinfo_stream("s"), {
            "length": 3,
            "last_generated_id": "101-0",
            "groups": 1,
            "first_entry": ["100-5", {"a": "100-5"}],
            "last_entry": ["101-0", {"a": "101-0"}]})

    def test_stream_empty_reports_zero_and_no_ends(self):
        """A stream created with MKSTREAM and no entries yet
        reports length 0, top "0-0" like Redis, and no first or
        last entry — but it does have its one group."""
        store = KVStore()
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "$", "mkstream": True})
        self.assertEqual(store.xinfo_stream("s"), {
            "length": 0,
            "last_generated_id": "0-0",
            "groups": 1,
            "first_entry": None,
            "last_entry": None})

    def test_groups_report_last_delivered_lag_and_pending(self):
        """XINFO GROUPS answers one dict per group, sorted by name:
        the last-delivered ID, the entries-read count (None when
        no SETID ever set it), the lag — the entries after the
        last delivery — and the pending count."""
        store = _seed_store()
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "a", "start_id": "0"})
        self.assertEqual(store.xinfo_groups("s"), [
            {"name": "a", "last_delivered_id": "0-0",
             "entries_read": None, "lag": 3, "pending": 0},
            {"name": "g", "last_delivered_id": "100-6",
             "entries_read": None, "lag": 1, "pending": 2}])
        # A SETID that rewinds and sets ENTRIESREAD moves the lag
        # and reports the count it set.
        store.apply({"op": "xgroup_setid", "key": "s", "group": "g",
                     "id": "100-5", "entries_read": 9})
        self.assertEqual(store.xinfo_groups("s")[1], {
            "name": "g", "last_delivered_id": "100-5",
            "entries_read": 9, "lag": 2, "pending": 2})

    def test_consumers_report_pending_and_idle(self):
        """XINFO CONSUMERS answers one dict per consumer, sorted by
        name: the pending count and the idle milliseconds since
        that consumer was last handed an entry — measured against
        the now the caller passes in, so the store itself never
        reads a clock."""
        store = _seed_store()
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 2, "idle": 3000, "lag": 1}])
        # A consumer handed entries by a delivery this version did
        # not stamp — a replayed pre-v0.70.0 log entry carries no
        # seen_ms — is still listed, with an idle of None rather
        # than a fabricated number, and a lag of None too: with no
        # last-delivered ID there is nothing to count past.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">"})
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 2, "idle": 3000, "lag": 1},
             {"name": "c2", "pending": 1, "idle": None,
              "lag": None}])
        # An ack drops the pending count but the consumer stays
        # listed, with its idle still counting — and its lag
        # unchanged, since acking does not move what it was
        # handed last.
        store.apply({"op": "xack", "key": "s", "group": "g",
                     "ids": ["100-5"]})
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 1, "idle": 3000, "lag": 1},
             {"name": "c2", "pending": 1, "idle": None,
              "lag": None}])

    def test_consumers_report_lag(self):
        """XINFO CONSUMERS answers each consumer's lag — the
        entries in the stream past the newest entry ever handed
        to that consumer, the per-consumer analogue of the group
        lag: 0 when the consumer read to the head, more when
        entries it never saw were appended after its last
        delivery, and None when its deliveries all came from
        replays of pre-v0.74.0 log entries, which carry no
        last-delivered ID."""
        store = _seed_store()
        # c1 was handed 100-5 and 100-6; 101-0 sits past them.
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 2, "idle": 3000, "lag": 1}])
        # c2 reads the rest: its lag is 0, and c1's is unchanged —
        # entries handed to another consumer still count as lag
        # for c1, since it has not read them.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">",
                     "seen_ms": 1700000001000})
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 2, "idle": 3000, "lag": 1},
             {"name": "c2", "pending": 1, "idle": 2000, "lag": 0}])
        # A NOACK delivery hands entries out without recording
        # them pending, but it still moves the consumer's last
        # delivery: a second group shows c3 with pending 0 and
        # lag 2 after a one-entry NOACK read.
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "h", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "h",
                     "consumer": "c3", "id": ">", "count": 1,
                     "noack": True, "seen_ms": 1700000002000})
        self.assertEqual(
            store.xinfo_consumers("s", "h", 1700000003000),
            [{"name": "c3", "pending": 0, "idle": 1000, "lag": 2}])
        # A re-read hands c1 its pending entries again — the
        # delivery counts rise — but never moves its last
        # delivery past what it was handed, so its lag stays 1.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": "0",
                     "seen_ms": 1700000002500})
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 2, "idle": 500, "lag": 1},
             {"name": "c2", "pending": 1, "idle": 2000, "lag": 0}])
        # A claim moves the claimed entry to its new consumer and
        # records it as that consumer's last delivery: a fresh
        # c4 takes 100-5 off c1, so c4's lag is 2 — every entry
        # past 100-5 — while c1's stays 1 and c2's stays 0, since
        # c2 already read to the head and a claim of an older
        # entry never moves a last delivery backwards.
        store.apply({"op": "xclaim", "key": "s", "group": "g",
                     "consumer": "c4", "ids": ["100-5"],
                     "min_idle_ms": 0, "force": True,
                     "seen_ms": 1700000002800})
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 1, "idle": 500, "lag": 1},
             {"name": "c2", "pending": 1, "idle": 2000, "lag": 0},
             {"name": "c4", "pending": 1, "idle": 200, "lag": 2}])
        # The last-delivered IDs ride in the blob, so a snapshot
        # load rebuilds the lags exactly.
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(
            loaded.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 1, "idle": 500, "lag": 1},
             {"name": "c2", "pending": 1, "idle": 2000, "lag": 0},
             {"name": "c4", "pending": 1, "idle": 200, "lag": 2}])
        self.assertEqual(
            loaded.xinfo_consumers("s", "h", 1700000003000),
            [{"name": "c3", "pending": 0, "idle": 1000, "lag": 2}])

    def test_consumer_that_only_polled_is_not_listed(self):
        """A consumer that only ever polled an empty group was
        never handed an entry, so the read wrote nothing and the
        consumer is not listed at all."""
        store = KVStore()
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "$", "mkstream": True})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": ">", "seen_ms": 1700000000000})
        self.assertEqual(result["entries"], [])
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000), [])

    def test_missing_key_group_and_wrongtype(self):
        """A missing key fails all three reports, a missing group
        fails CONSUMERS, and a plain string value fails all three
        as WRONGTYPE — a typo'd key or group must not read as an
        empty stream or group."""
        store = _seed_store()
        for fn in (lambda: store.xinfo_stream("nope"),
                   lambda: store.xinfo_groups("nope"),
                   lambda: store.xinfo_consumers("nope", "g", 0)):
            with self.assertRaises(ValueError):
                fn()
        with self.assertRaises(ValueError):
            store.xinfo_consumers("s", "nope", 0)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        for fn in (lambda: store.xinfo_stream("plain"),
                   lambda: store.xinfo_groups("plain"),
                   lambda: store.xinfo_consumers("plain", "g", 0)):
            with self.assertRaises(TypeError):
                fn()

    def test_delconsumer_drops_last_seen_record(self):
        """DELCONSUMER drops the consumer's pending entries and its
        last-seen record, so XINFO CONSUMERS stops listing it —
        and the reply says a record went, even when the consumer
        held nothing pending."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_delconsumer", "key": "s",
                              "group": "g", "consumer": "c1"})
        self.assertEqual(result["xgroup_delconsumer"], 2, result)
        self.assertEqual(result.get("consumer_dropped"), 1, result)
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000), [])
        self.assertEqual(store.xinfo_groups("s")[0]["pending"], 0)
        # A consumer that held nothing pending but was seen is
        # still dropped from the listing.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">",
                     "seen_ms": 1700000000000})
        store.apply({"op": "xack", "key": "s", "group": "g",
                     "ids": ["101-0"]})
        result = store.apply({"op": "xgroup_delconsumer", "key": "s",
                              "group": "g", "consumer": "c2"})
        self.assertEqual(result["xgroup_delconsumer"], 0, result)
        self.assertEqual(result.get("consumer_dropped"), 1, result)
        self.assertEqual(
            store.xinfo_consumers("s", "g", 1700000003000), [])

    def test_xinfo_survives_snapshot_load(self):
        """The last-seen stamps and the entries-read count ride in
        the blob, so a snapshot load rebuilds the idle times and
        the group report exactly."""
        store = _seed_store()
        store.apply({"op": "xgroup_setid", "key": "s", "group": "g",
                     "id": "100-5", "entries_read": 9})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(loaded.xinfo_stream("s")["length"], 3)
        self.assertEqual(loaded.xinfo_groups("s"), [
            {"name": "g", "last_delivered_id": "100-5",
             "entries_read": 9, "lag": 2, "pending": 2}])
        self.assertEqual(
            loaded.xinfo_consumers("s", "g", 1700000003000),
            [{"name": "c1", "pending": 2, "idle": 3000, "lag": 1}])


class TestXInfoCluster(unittest.TestCase):
    def test_full_flow_reports_replicate(self):
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
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["100-5", "100-6"])
            resp = xreadgroup(addr, consumer="c2")
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["101-0"])
            # Every node agrees on the stream report, once the
            # deliveries have replicated to each of them.
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                node = cluster.nodes[node_id]
                self.assertEqual(node.sm.xinfo_stream("events"), {
                    "length": 3,
                    "last_generated_id": "101-0",
                    "groups": 1,
                    "first_entry": ["100-5", {"a": "100-5"}],
                    "last_entry": ["101-0", {"a": "101-0"}]})
                self.assertEqual(node.sm.xinfo_groups("events"), [
                    {"name": "workers",
                     "last_delivered_id": "101-0",
                     "entries_read": None, "lag": 0,
                     "pending": 3}])
            # The consumers report rides the ReadIndex path, so it
            # is linearizable: both consumers are listed, c1 with
            # two pending and c2 with one, and both idle times are
            # real — non-negative integers that grow as time passes.
            resp = xinfo(addr, sub="CONSUMERS", group="workers")
            self.assertEqual(resp.get("status"), "ok", resp)
            consumers = resp.get("consumers")
            self.assertEqual(
                [(c["name"], c["pending"]) for c in consumers],
                [("c1", 2), ("c2", 1)], resp)
            for consumer in consumers:
                self.assertIsInstance(consumer["idle"], int, resp)
                self.assertGreaterEqual(consumer["idle"], 0, resp)
            first_idle = {c["name"]: c["idle"] for c in consumers}
            time.sleep(0.5)
            resp = xinfo(addr, sub="CONSUMERS", group="workers")
            second_idle = {c["name"]: c["idle"]
                           for c in resp.get("consumers")}
            for name in first_idle:
                self.assertGreater(second_idle[name], first_idle[name],
                                   (name, first_idle, second_idle))
            # An ack drops the pending count in the report, and a
            # SETID ENTRIESREAD shows up in the groups report.
            self.assertEqual(
                client_xack(addr, "events", "workers",
                            ["100-5"]).get("xack"), 1)
            resp = xinfo(addr, sub="GROUPS")
            self.assertEqual(resp.get("groups"), [
                {"name": "workers", "last_delivered_id": "101-0",
                 "entries_read": None, "lag": 0, "pending": 2}],
                resp)
            self.assertEqual(
                xgroup_setid(addr, id="100-5",
                             entries_read=2).get("status"), "ok")
            resp = xinfo(addr, sub="GROUPS")
            self.assertEqual(resp.get("groups"), [
                {"name": "workers", "last_delivered_id": "100-5",
                 "entries_read": 2, "lag": 2, "pending": 2}],
                resp)
            # Dropping a consumer removes it from the consumers
            # report; destroying the group removes it from the
            # groups report, and CONSUMERS then errors.
            self.assertEqual(
                xgroup_delconsumer(addr, consumer="c1")
                .get("xgroup_delconsumer"), 1)
            resp = xinfo(addr, sub="CONSUMERS", group="workers")
            self.assertEqual(
                [c["name"] for c in resp.get("consumers")], ["c2"],
                resp)
            self.assertEqual(
                xgroup_destroy(addr).get("xgroup_destroy"), 1)
            resp = xinfo(addr, sub="GROUPS")
            self.assertEqual(resp.get("groups"), [], resp)
            resp = xinfo(addr, sub="CONSUMERS", group="workers")
            self.assertEqual(resp.get("status"), "error", resp)
            # The stream itself is untouched by the destroy.
            resp = xinfo(addr, sub="STREAM")
            self.assertEqual(resp.get("length"), 3, resp)
            self.assertEqual(resp.get("groups"), 0, resp)
        finally:
            cluster.stop_all()

    def test_consumers_report_lag_replicated(self):
        """XINFO CONSUMERS reports each consumer's lag on every
        node: the entries past the newest entry that consumer was
        handed — 1 for a consumer that stopped one entry short of
        the head, 0 for one that read to it — and the lag grows
        when a new entry is appended past a consumer that has
        stopped reading."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for entry_id in ("100-5", "100-6", "101-0"):
                self.assertEqual(
                    xadd(addr, pairs=(("a", entry_id),),
                         id=entry_id).get("status"), "ok")
            self.assertEqual(
                xgroup_create(addr, start_id="0").get("status"), "ok")
            self.assertEqual(
                [e[0] for e in xreadgroup(addr, consumer="c1",
                                          count=2).get("entries")],
                ["100-5", "100-6"])
            self.assertEqual(
                [e[0] for e in xreadgroup(addr, consumer="c2")
                 .get("entries")],
                ["101-0"])
            # Both consumers' lags replicate to every node.
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                by_name = {
                    c["name"]: c
                    for c in cluster.nodes[node_id].sm
                    .xinfo_consumers("events", "workers",
                                     int(time.time() * 1000))}
                self.assertEqual(by_name["c1"]["lag"], 1, by_name)
                self.assertEqual(by_name["c2"]["lag"], 0, by_name)
            # One more entry past c1, which has stopped reading:
            # its lag grows to 2, and c2's stays 0 once it reads
            # the new entry too.
            self.assertEqual(
                xadd(addr, pairs=(("a", "102-0"),),
                     id="102-0").get("status"), "ok")
            self.assertEqual(
                [e[0] for e in xreadgroup(addr, consumer="c2")
                 .get("entries")],
                ["102-0"])
            resp = xinfo(addr, sub="CONSUMERS", group="workers")
            by_name = {c["name"]: c for c in resp.get("consumers")}
            self.assertEqual(by_name["c1"]["lag"], 2, resp)
            self.assertEqual(by_name["c2"]["lag"], 0, resp)
        finally:
            cluster.stop_all()

    def test_invalid_args_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                client_request(addr, {"type": "client", "op": "set",
                                      "key": "plain",
                                      "value": "v"}).get("status"),
                "ok")
            before = len(leader.log)
            # A bad subcommand, an empty key, or an empty group for
            # CONSUMERS is a client error with no log entry.
            for msg in (
                    {"type": "client", "op": "xinfo", "sub": "BOGUS",
                     "key": "events"},
                    {"type": "client", "op": "xinfo", "sub": "stream",
                     "key": ""},
                    {"type": "client", "op": "xinfo",
                     "sub": "CONSUMERS", "key": "events",
                     "group": ""},
                    {"type": "client", "op": "xinfo",
                     "sub": "CONSUMERS", "key": "events"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before)
            # A missing key or group, and a plain string value, are
            # errors in the payload — also with no log entry, since
            # a read never appends one.
            for msg in (
                    {"type": "client", "op": "xinfo", "sub": "STREAM",
                     "key": "nope"},
                    {"type": "client", "op": "xinfo", "sub": "GROUPS",
                     "key": "nope"},
                    {"type": "client", "op": "xinfo",
                     "sub": "CONSUMERS", "key": "nope",
                     "group": "workers"},
                    {"type": "client", "op": "xinfo", "sub": "STREAM",
                     "key": "plain"},
                    {"type": "client", "op": "xinfo", "sub": "GROUPS",
                     "key": "plain"},
                    {"type": "client", "op": "xinfo",
                     "sub": "CONSUMERS", "key": "plain",
                     "group": "workers"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before)
            # The subcommand is matched case-insensitively.
            self.assertEqual(
                client_request(addr, {"type": "client", "op": "xinfo",
                                      "sub": "stream",
                                      "key": "plain"})
                .get("status"), "error")
            xadd(addr, id="100-5")
            resp = client_request(addr, {"type": "client",
                                         "op": "xinfo", "sub": "stream",
                                         "key": "events"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 1, resp)
        finally:
            cluster.stop_all()

    def test_session_methods(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                self.assertEqual(
                    session.xadd("events", {"a": "1"},
                                 id="100-5").get("status"), "ok")
                self.assertEqual(
                    session.xgroup_create("events", "workers",
                                          "0").get("status"), "ok")
                self.assertEqual(
                    session.xreadgroup("events", "workers", "c1")
                    .get("entries"),
                    [["100-5", {"a": "1"}]])
                resp = session.xinfo_stream("events")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("length"), 1, resp)
                self.assertEqual(resp.get("groups"), 1, resp)
                self.assertEqual(resp.get("first_entry"),
                                 ["100-5", {"a": "1"}], resp)
                resp = session.xinfo_groups("events")
                self.assertEqual(resp.get("groups"), [
                    {"name": "workers",
                     "last_delivered_id": "100-5",
                     "entries_read": None, "lag": 0,
                     "pending": 1}], resp)
                resp = session.xinfo_consumers("events", "workers")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(len(resp.get("consumers")), 1, resp)
                self.assertEqual(resp["consumers"][0]["name"], "c1",
                                 resp)
                self.assertEqual(resp["consumers"][0]["pending"], 1,
                                 resp)
                self.assertIsInstance(resp["consumers"][0]["idle"],
                                      int, resp)
                # A bad group never goes on the wire, and a missing
                # key or group is an error, not an empty report.
                resp = session.xinfo_consumers("events", "")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xinfo_stream("nope")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xinfo_groups("nope")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xinfo_consumers("events", "ghost")
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_module_functions_from_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower_id = next(n for n in cluster.live
                               if n != leader.id)
            follower_addr = cluster.addrs[follower_id]
            leader_addr = cluster.addrs[leader.id]
            self.assertEqual(
                client_xadd(leader_addr, "events", {"a": "1"},
                            id="100-5").get("status"), "ok")
            self.assertEqual(
                client_xgroup_create(follower_addr, "events",
                                     "workers",
                                     "0").get("status"), "ok")
            self.assertEqual(
                client_xreadgroup(follower_addr, "events", "workers",
                                  "c1").get("entries"),
                [["100-5", {"a": "1"}]])
            resp = client_xinfo_stream(follower_addr, "events")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 1, resp)
            self.assertEqual(resp.get("last_generated_id"), "100-5",
                             resp)
            resp = client_xinfo_groups(follower_addr, "events")
            self.assertEqual(resp.get("groups"), [
                {"name": "workers", "last_delivered_id": "100-5",
                 "entries_read": None, "lag": 0, "pending": 1}],
                resp)
            resp = client_xinfo_consumers(follower_addr, "events",
                                          "workers")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(resp.get("consumers")), 1, resp)
            self.assertEqual(resp["consumers"][0]["name"], "c1", resp)
            self.assertIsInstance(resp["consumers"][0]["idle"], int,
                                  resp)
            # A bad group never goes on the wire.
            self.assertEqual(
                client_xinfo_consumers(follower_addr, "events", "")
                .get("status"), "error")
            self.assertEqual(
                client_xinfo_stream(follower_addr, "nope")
                .get("status"), "error")
        finally:
            cluster.stop_all()

    def test_pipelined_readgroup_stamps_last_seen(self):
        """A readgroup sent through a pipeline is stamped by the
        leader like any other, so its consumer shows up in XINFO
        CONSUMERS with a real idle — the stamp covers the pipeline
        path, not just the direct one."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                self.assertEqual(
                    session.xadd("events", {"a": "1"},
                                 id="100-5").get("status"), "ok")
                self.assertEqual(
                    session.xgroup_create("events", "workers",
                                          "0").get("status"), "ok")
                replies = session.pipeline([
                    {"op": "xreadgroup", "key": "events",
                     "group": "workers", "consumer": "c1"},
                ])
                self.assertEqual(len(replies), 1, replies)
                self.assertEqual(replies[0].get("entries"),
                                 [["100-5", {"a": "1"}]], replies[0])
                resp = session.xinfo_consumers("events", "workers")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(len(resp.get("consumers")), 1, resp)
                self.assertEqual(resp["consumers"][0]["name"], "c1",
                                 resp)
                self.assertIsInstance(resp["consumers"][0]["idle"],
                                      int, resp)
                self.assertGreaterEqual(resp["consumers"][0]["idle"],
                                        0, resp)
        finally:
            cluster.stop_all()

    def test_xinfo_survives_snapshot_and_restart(self):
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
            self.assertEqual(
                xgroup_setid(addr, id="0",
                             entries_read=4).get("status"), "ok")
            # Enough writes to compact the log past the group ops.
            for i in range(10):
                self.assertEqual(
                    client_request(
                        addr, {"type": "client", "op": "set",
                               "key": f"pad{i}", "value": "x"})
                    .get("status"), "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The group survived the snapshot + restart with its
            # rewound position, entries-read count, and the
            # consumer's last-seen stamp: the idle is real, not
            # None, and the entry is still pending for c1 — and
            # the lag survived too: c1 was handed 100-5, the only
            # entry, so it sits at 0.
            resp = xinfo(addr, sub="GROUPS")
            self.assertEqual(resp.get("groups"), [
                {"name": "workers", "last_delivered_id": "0-0",
                 "entries_read": 4, "lag": 1, "pending": 1}], resp)
            resp = xinfo(addr, sub="CONSUMERS", group="workers")
            self.assertEqual(len(resp.get("consumers")), 1, resp)
            self.assertEqual(resp["consumers"][0]["name"], "c1", resp)
            self.assertEqual(resp["consumers"][0]["pending"], 1, resp)
            self.assertEqual(resp["consumers"][0]["lag"], 0, resp)
            self.assertIsInstance(resp["consumers"][0]["idle"], int,
                                  resp)
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
                rc = client_main(["--node", node, "xinfo", "STREAM",
                                  "events"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), {
                "length": 1, "last_generated_id": "100-5",
                "groups": 1,
                "first_entry": ["100-5", {"a": "1"}],
                "last_entry": ["100-5", {"a": "1"}]})
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xinfo", "GROUPS",
                                  "events"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), [
                {"name": "workers", "last_delivered_id": "100-5",
                 "entries_read": None, "lag": 0, "pending": 1}])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xinfo",
                                  "CONSUMERS", "events", "workers"])
            self.assertEqual(rc, 0, out.getvalue())
            consumers = json.loads(out.getvalue())
            self.assertEqual(len(consumers), 1)
            self.assertEqual(consumers[0]["name"], "c1")
            self.assertEqual(consumers[0]["pending"], 1)
            self.assertEqual(consumers[0]["lag"], 0)
            self.assertIsInstance(consumers[0]["idle"], int)
            # A missing key errors, and CONSUMERS without a group
            # is a usage error, not a request.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xinfo", "STREAM",
                                  "nope"])
            self.assertEqual(rc, 1, out.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
