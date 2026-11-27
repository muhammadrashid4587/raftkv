"""Tests for raftkv XAUTOCLAIM (v0.73.0).

``xautoclaim key group consumer min-idle-ms [start] [--count n]
[--justid]`` claims pending entries in bulk from a dead consumer:
it walks the group's pending list in ID order from just after
``start`` ("0-0" from the very first pending entry) and takes over
every entry idle at least the minimum — the milliseconds since its
last delivery, measured against the wall-clock millisecond the
leader fixed at log time, like an xclaim — moving it to the claiming
consumer, raising its delivery count by one, and re-stamping its
delivery time to the claim's time. COUNT caps how many pending
entries one call examines (default 100), not how many it claims,
and the reply carries ``next`` — the ID to pass as ``start`` on the
next call, "0-0" when the scan reached the end of the pending list
— plus the claimed entries, or just their IDs with JUSTID, and
``deleted`` — pending entries whose data was trimmed away under
KEEPREF, dropped from the pending list. An entry with no provable
idle is skipped, like an xclaim without FORCE. These spin up real
nodes (real TCP, real threads, real disk) on localhost, like the
rest of the suite.
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
from raftkv.client import xautoclaim as client_xautoclaim
from raftkv.client import xgroup_create as client_xgroup_create
from raftkv.client import xpending as client_xpending
from raftkv.client import xpending_entries as client_xpending_entries
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


def xautoclaim(addr, key="events", group="workers", consumer="c2",
               min_idle_ms=0, start="0-0", count=None, justid=None,
               client_id=None, request_id=None):
    msg = {"type": "client", "op": "xautoclaim", "key": key,
           "group": group, "consumer": consumer,
           "min_idle_ms": min_idle_ms, "start": start}
    if count is not None:
        msg["count"] = count
    if justid is not None:
        msg["justid"] = justid
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
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


class TestXAutoClaimStore(unittest.TestCase):
    def test_claims_idle_entries_in_order_and_cursor_ends(self):
        """An autoclaim moves every entry idle long enough to the
        new consumer, in ID order, with their fields, raises each
        delivery count by one, and re-stamps the delivery time to
        the claim's time — and the cursor comes back "0-0", since
        the scan reached the end of the pending list."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 2000, "start": "0-0",
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-5", {"a": "100-5"}],
                          ["100-6", {"a": "100-6"}]])
        self.assertEqual(resp.get("deleted"), [])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000004000),
            [{"id": "100-5", "consumer": "c2", "idle": 1000,
              "deliveries": 2},
             {"id": "100-6", "consumer": "c2", "idle": 1000,
              "deliveries": 2}])

    def test_cursor_pages_through_pending_list(self):
        """COUNT 1 examines one pending entry per call, so the
        cursor walks the pending list one entry at a time: the
        first call claims the first entry and answers its ID as
        the next cursor, and the next call resumes after it."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "count": 1, "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "100-5", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-5", {"a": "100-5"}]])
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "100-5",
                            "count": 1, "seen_ms": 1700000001000})
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-6", {"a": "100-6"}]])

    def test_count_bounds_examined_not_claimed(self):
        """COUNT bounds how many pending entries a call examines,
        not how many it claims: a call whose first examined entry
        is too fresh to take returns nothing claimed, and the
        cursor still advances past the entry it examined — the
        next call resumes after it and claims the idle one."""
        store = _seed_store()
        # Re-read 100-5 a moment before the claim, so it is too
        # fresh to take while 100-6 is idle enough — the count of
        # 1 keeps the re-read to 100-5 alone, since an explicit-ID
        # re-read otherwise walks every pending entry at or after
        # the ID.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": "100-5", "count": 1,
                     "seen_ms": 1700000002900})
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 2000, "start": "0-0",
                            "count": 1, "seen_ms": 1700000003000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "100-5", resp)
        self.assertEqual(resp.get("entries"), [])
        self.assertEqual(resp.get("deleted"), [])
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 2000, "start": "100-5",
                            "count": 1, "seen_ms": 1700000003000})
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-6", {"a": "100-6"}]])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000)[0]
            ["consumer"], "c1")

    def test_too_fresh_and_unstamped_entries_skipped(self):
        """Entries idle less than the minimum stay with their
        holder — a call that takes nothing answers an empty list
        with the cursor at the end, and writes nothing — and an
        entry with no stamp cannot prove it has been idle that
        long, so it is skipped too."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 5000, "start": "0-0",
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"), [])
        self.assertEqual(resp.get("deleted"), [])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000)[0]
            ["consumer"], "c1")
        # Claim the two idle entries away first, so only the
        # unstamped one is left pending: a delivery this version
        # did not stamp — a replayed pre-v0.71.0 log entry carries
        # no seen_ms — cannot prove it has been idle that long,
        # so an autoclaim skips it.
        store.apply({"op": "xautoclaim", "key": "s", "group": "g",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0", "seen_ms": 1700000003000})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">"})
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c3",
                            "min_idle_ms": 1, "start": "0-0",
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("entries"), [])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", 1700000003000)],
            ["100-5", "100-6", "101-0"])

    def test_justid_answers_only_ids(self):
        """JUSTID answers only the claimed IDs, not the entries —
        the cheap form for a consumer that only needs to know
        which entries it now owns — and still carries the cursor
        and the deleted list."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "justid": True,
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("ids"), ["100-5", "100-6"])
        self.assertEqual(resp.get("deleted"), [])
        self.assertNotIn("entries", resp)

    def test_trimmed_keepref_entry_reported_deleted(self):
        """A pending entry whose data was trimmed away under
        KEEPREF is dropped from the pending list and reported
        under ``deleted``, while the entries that are still there
        are claimed as usual."""
        store = _seed_store()
        store.apply({"op": "xtrim", "key": "s",
                     "strategy": "MINID", "threshold": "100-6",
                     "mode": "KEEPREF"})
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-6", {"a": "100-6"}]])
        self.assertEqual(resp.get("deleted"), ["100-5"])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", 1700000001000)],
            ["100-6"])

    def test_count_zero_claims_nothing_and_cursor_stays(self):
        """COUNT 0 examines nothing, so it claims nothing and the
        cursor comes back as the start that was given — it does
        not move, and it does not jump to the end."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "count": 0, "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"), [])
        self.assertEqual(resp.get("deleted"), [])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", 1700000001000)],
            ["100-5", "100-6"])

    def test_start_after_last_pending_answers_empty(self):
        """A start past every pending entry examines nothing, so
        the call answers an empty claim with the cursor at the
        end — there is nothing left to page through."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "101-0",
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("next"), "0-0", resp)
        self.assertEqual(resp.get("entries"), [])
        self.assertEqual(resp.get("deleted"), [])

    def test_missing_key_group_wrongtype_and_bad_args(self):
        """A missing key or group fails with xautoclaim_failed /
        no_group, and a plain string value fails with wrongtype —
        a typo'd key or group must not read as an empty claim —
        and a bad group, consumer, start, minimum idle, count, or
        a missing seen_ms raises rather than claiming nonsense."""
        store = _seed_store()
        resp = store.apply({"op": "xautoclaim", "key": "nope",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "xautoclaim_failed",
                         resp)
        self.assertEqual(resp.get("reason"), "no_group", resp)
        resp = store.apply({"op": "xautoclaim", "key": "s",
                            "group": "nope", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "xautoclaim_failed",
                         resp)
        self.assertEqual(resp.get("reason"), "no_group", resp)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        resp = store.apply({"op": "xautoclaim", "key": "plain",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "start": "0-0",
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "xautoclaim_failed",
                         resp)
        self.assertEqual(resp.get("reason"), "wrongtype", resp)
        base = {"op": "xautoclaim", "key": "s", "group": "g",
                "consumer": "c2", "min_idle_ms": 0, "start": "0-0",
                "seen_ms": 1700000001000}
        bad = []
        for key, value in (("group", ""), ("consumer", ""),
                           ("start", 5), ("start", "bogus"),
                           ("min_idle_ms", -1),
                           ("min_idle_ms", "0"),
                           ("count", -1)):
            cmd = dict(base)
            cmd[key] = value
            bad.append(cmd)
        cmd = dict(base)
        del cmd["seen_ms"]
        bad.append(cmd)
        for cmd in bad:
            with self.assertRaises(ValueError, msg=str(cmd)):
                store.apply(cmd)

    def test_claim_survives_snapshot_load(self):
        """The claim rides in the blob, so a snapshot load rebuilds
        the new holder, the raised delivery count, and the
        re-stamped idle exactly."""
        store = _seed_store()
        store.apply({"op": "xautoclaim", "key": "s", "group": "g",
                     "consumer": "c2", "min_idle_ms": 2000,
                     "start": "0-0", "seen_ms": 1700000003000})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(
            loaded.xpending_entries("s", "g", 1700000004000),
            [{"id": "100-5", "consumer": "c2", "idle": 1000,
              "deliveries": 2},
             {"id": "100-6", "consumer": "c2", "idle": 1000,
              "deliveries": 2}])


class TestXAutoClaimCluster(unittest.TestCase):
    def test_full_flow_autoclaim_replicates(self):
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
            # A minimum idle of 0 claims at once: the entries were
            # delivered a moment ago, and 0 is idle enough.
            resp = xautoclaim(addr, consumer="c3")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("next"), "0-0", resp)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"a": "100-5"}],
                              ["100-6", {"a": "100-6"}],
                              ["101-0", {"a": "101-0"}]])
            self.assertEqual(resp.get("deleted"), [])
            # Every node agrees on who holds what, once the claim
            # has replicated to each of them.
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                node = cluster.nodes[node_id]
                entries = node.sm.xpending_entries(
                    "events", "workers",
                    int(time.time() * 1000))
                self.assertEqual(
                    [(e["id"], e["consumer"], e["deliveries"])
                     for e in entries],
                    [("100-5", "c3", 2), ("100-6", "c3", 2),
                     ("101-0", "c3", 2)])
                for entry in entries:
                    self.assertIsInstance(entry["idle"], int, entry)
                    self.assertGreaterEqual(entry["idle"], 0, entry)
            # The idle times are real — they grow as time passes —
            # so a claim with a minimum idle above the time since
            # the claim takes nothing, and the same claim takes
            # the entry once it has been idle long enough.
            resp = xautoclaim(addr, consumer="c1",
                              min_idle_ms=10 ** 9)
            self.assertEqual(resp.get("entries"), [], resp)
            self.assertEqual(resp.get("next"), "0-0", resp)
            time.sleep(0.5)
            resp = xautoclaim(addr, consumer="c1",
                              min_idle_ms=400)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"a": "100-5"}],
                              ["100-6", {"a": "100-6"}],
                              ["101-0", {"a": "101-0"}]], resp)
            # The cursor pages through a fresh batch one entry at
            # a time: hand two more entries to c2, then claim
            # them with COUNT 1.
            for entry_id in ("102-0", "103-0"):
                resp = xadd(addr, pairs=(("a", entry_id),),
                            id=entry_id)
                self.assertEqual(resp.get("status"), "ok", resp)
            resp = xreadgroup(addr, consumer="c2")
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["102-0", "103-0"])
            # The cursor pages through a fresh batch one entry at
            # a time, resuming after the last examined entry:
            # start past the older entries, which a min-idle of 0
            # would claim first.
            resp = xautoclaim(addr, consumer="c3", min_idle_ms=0,
                              start="101-0", count=1)
            self.assertEqual(resp.get("next"), "102-0", resp)
            self.assertEqual(resp.get("entries"),
                             [["102-0", {"a": "102-0"}]], resp)
            resp = xautoclaim(addr, consumer="c3", min_idle_ms=0,
                              start="102-0", count=1)
            self.assertEqual(resp.get("next"), "0-0", resp)
            self.assertEqual(resp.get("entries"),
                             [["103-0", {"a": "103-0"}]], resp)
            # JUSTID answers only the IDs, and an ack drops the
            # claimed entry while the summary form still answers.
            resp = xautoclaim(addr, consumer="c1", min_idle_ms=0,
                              justid=True)
            self.assertEqual(resp.get("ids"),
                             ["100-5", "100-6", "101-0", "102-0",
                              "103-0"], resp)
            self.assertNotIn("entries", resp)
            self.assertEqual(
                client_xack(addr, "events", "workers",
                            ["100-5"]).get("xack"), 1)
            resp = client_xpending(addr, "events", "workers")
            self.assertEqual(resp.get("pending"), 4, resp)
            self.assertEqual(resp.get("min_id"), "100-6", resp)
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
            # A bad group, consumer, start, minimum idle, count,
            # or flag is a client error with no log entry.
            for msg in (
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0"},
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "workers",
                     "consumer": "", "min_idle_ms": 0,
                     "start": "0-0"},
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "bogus"},
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": -1,
                     "start": "0-0"},
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0", "count": -1},
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0", "justid": "yes"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before)
            # A missing key or group, and a plain string value, are
            # errors in the payload — decided at apply time, like a
            # CAS, so they do commit their entry.
            for msg in (
                    {"type": "client", "op": "xautoclaim",
                     "key": "nope", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0"},
                    {"type": "client", "op": "xautoclaim",
                     "key": "events", "group": "ghost",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0"},
                    {"type": "client", "op": "xautoclaim",
                     "key": "plain", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "start": "0-0"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"),
                                 "xautoclaim_failed", (msg, resp))
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
                resp = session.xautoclaim("events", "workers", "c2",
                                          0)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("next"), "0-0", resp)
                self.assertEqual(resp.get("entries"),
                                 [["100-5", {"a": "1"}]])
                resp = session.xpending_entries("events", "workers")
                self.assertEqual(
                    [(e["id"], e["consumer"], e["deliveries"])
                     for e in resp.get("entries")],
                    [("100-5", "c2", 2)], resp)
                # A bad group never goes on the wire, and a missing
                # key or group is an error, not an empty list.
                resp = session.xautoclaim("events", "", "c2", 0)
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xautoclaim("nope", "workers", "c2", 0)
                self.assertEqual(resp.get("status"),
                                 "xautoclaim_failed", resp)
                resp = session.xautoclaim("events", "ghost", "c2", 0)
                self.assertEqual(resp.get("status"),
                                 "xautoclaim_failed", resp)
                resp = session.xautoclaim("events", "workers", "c2",
                                          -1)
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xautoclaim("events", "workers", "c2",
                                          0, count=-1)
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_retry_returns_first_attempt(self):
        """A retry with the same session id returns the first
        attempt's cursor and entries instead of claiming twice:
        the delivery count rises once, not once per retry."""
        cluster = Cluster(3)
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
            first = xautoclaim(addr, client_id="c1", request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            self.assertEqual(first.get("next"), "0-0", first)
            self.assertEqual(first.get("entries"),
                             [["100-5", {"type": "login"}]])
            second = xautoclaim(addr, client_id="c1",
                                request_id="r1")
            self.assertEqual(second.get("status"), "ok", second)
            self.assertEqual(second.get("next"),
                             first.get("next"), second)
            self.assertEqual(second.get("entries"),
                             first.get("entries"), second)
            # The retry did not claim twice: still one delivery
            # past the first handover.
            resp = client_xpending_entries(addr, "events", "workers")
            self.assertEqual(
                [(e["id"], e["consumer"], e["deliveries"])
                 for e in resp.get("entries")],
                [("100-5", "c2", 2)], resp)
            # A retry with a different consumer still returns the
            # first attempt's entries, not a second claim.
            third = xautoclaim(addr, consumer="c3", client_id="c1",
                               request_id="r1")
            self.assertEqual(third.get("entries"),
                             first.get("entries"), third)
            resp = client_xpending_entries(addr, "events", "workers")
            self.assertEqual(resp["entries"][0]["consumer"], "c2",
                             resp)
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
            resp = client_xautoclaim(follower_addr, "events",
                                     "workers", "c2", 0)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("next"), "0-0", resp)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"a": "1"}]])
            resp = client_xpending_entries(follower_addr, "events",
                                           "workers")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(
                [(e["id"], e["consumer"], e["deliveries"])
                 for e in resp.get("entries")],
                [("100-5", "c2", 2)], resp)
            # A bad group never goes on the wire.
            self.assertEqual(
                client_xautoclaim(follower_addr, "events", "", "c2",
                                  0).get("status"), "error")
            self.assertEqual(
                client_xautoclaim(follower_addr, "nope", "workers",
                                  "c2", 0).get("status"),
                "xautoclaim_failed")
        finally:
            cluster.stop_all()

    def test_pipelined_autoclaim(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                xadd(addr, id="100-5").get("xadd"), "100-5")
            self.assertEqual(
                xgroup_create(addr, start_id="0").get("status"), "ok")
            self.assertEqual(
                xreadgroup(addr).get("entries"),
                [["100-5", {"type": "login"}]])
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "xautoclaim", "key": "events",
                     "group": "workers", "consumer": "c2",
                     "min_idle_ms": 0, "start": "0-0"},
                    {"op": "xautoclaim", "key": "events",
                     "group": "workers", "consumer": "c3",
                     "min_idle_ms": 0, "start": "0-0",
                     "justid": True},
                ])
            self.assertEqual(len(replies), 2, replies)
            self.assertEqual(replies[0].get("status"), "ok", replies)
            self.assertEqual(replies[0].get("next"), "0-0", replies)
            self.assertEqual(replies[0].get("entries"),
                             [["100-5", {"type": "login"}]], replies)
            self.assertEqual(replies[1].get("status"), "ok", replies)
            self.assertEqual(replies[1].get("ids"), ["100-5"],
                             replies)
            resp = client_xpending_entries(addr, "events", "workers")
            self.assertEqual(
                [(e["id"], e["consumer"], e["deliveries"])
                 for e in resp.get("entries")],
                [("100-5", "c3", 3)], resp)
            # A malformed pipelined autoclaim never goes on the
            # wire.
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "xautoclaim", "key": "events",
                     "group": "workers", "consumer": "c2",
                     "min_idle_ms": -1, "start": "0-0"},
                ])
            self.assertEqual(replies[0].get("status"), "error",
                             replies)
        finally:
            cluster.stop_all()

    def test_claim_survives_snapshot_and_restart(self):
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
            resp = xautoclaim(addr)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"type": "login"}]])
            # Enough writes to compact the log past the claim.
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
            # The entry survived the snapshot + restart with its
            # new holder and raised delivery count.
            resp = client_xpending_entries(addr, "events", "workers")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(
                [(e["id"], e["consumer"], e["deliveries"])
                 for e in resp.get("entries")],
                [("100-5", "c2", 2)], resp)
            self.assertIsInstance(resp["entries"][0]["idle"], int)
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
            # The autoclaim answers the next cursor, the claimed
            # entries, and the deleted IDs as one JSON object.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xautoclaim",
                                  "events", "workers", "c2", "0"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             {"next": "0-0",
                              "entries": [["100-5", {"a": "1"}]],
                              "deleted": []})
            # JUSTID answers only the IDs, and COUNT pages the
            # scan: with nothing pending the cursor is already at
            # the end.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xautoclaim",
                                  "events", "workers", "c3", "0",
                                  "--justid", "--count", "1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             {"next": "0-0", "entries": ["100-5"],
                              "deleted": []})
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers", "-", "+"])
            self.assertEqual(rc, 0, out.getvalue())
            entries = json.loads(out.getvalue())
            self.assertEqual(entries[0]["consumer"], "c3")
            self.assertEqual(entries[0]["deliveries"], 3)
            # An autoclaim on a missing key errors.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xautoclaim",
                                  "nope", "workers", "c2", "0"])
            self.assertEqual(rc, 1, out.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
