"""Tests for raftkv XCLAIM (v0.72.0).

``xclaim key group consumer min-idle-ms id [id ...] [--justid]
[--force] [--retrycount n]`` takes pending entries over from a
dead consumer: every named ID that is pending in the group and has
been idle at least the minimum — the milliseconds since its last
delivery, measured against the wall-clock millisecond the leader
fixed at log time, like an xreadgroup stamp — moves to the claiming
consumer, its delivery count rising by one (or set to the retry
count when one is given), its delivery stamp re-set to the claim's
time. FORCE claims regardless of idle, JUSTID answers only the
claimed IDs, and an ID that is not pending is simply skipped. These
spin up real nodes (real TCP, real threads, real disk) on
localhost, like the rest of the suite.
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
from raftkv.client import xclaim as client_xclaim
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


def xclaim(addr, key="events", group="workers", consumer="c2",
           min_idle_ms=0, ids=None, justid=None, force=None,
           retrycount=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xclaim", "key": key,
           "group": group, "consumer": consumer,
           "min_idle_ms": min_idle_ms,
           "ids": ids if ids is not None else ["100-5"]}
    if justid is not None:
        msg["justid"] = justid
    if force is not None:
        msg["force"] = force
    if retrycount is not None:
        msg["retrycount"] = retrycount
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


class TestXClaimStore(unittest.TestCase):
    def test_claim_moves_idle_entries_to_new_consumer(self):
        """A claim moves the entries idle long enough to the new
        consumer, in the order the IDs were given, with their
        fields, raises each delivery count by one, and re-stamps
        the delivery time to the claim's time — so the idle
        measures since this claim, not since the first delivery."""
        store = _seed_store()
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c2", "min_idle_ms": 2000,
                            "ids": ["100-6", "100-5"],
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-6", {"a": "100-6"}],
                          ["100-5", {"a": "100-5"}]])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000004000),
            [{"id": "100-5", "consumer": "c2", "idle": 1000,
              "deliveries": 2},
             {"id": "100-6", "consumer": "c2", "idle": 1000,
              "deliveries": 2}])

    def test_too_fresh_entry_refused_and_force_takes_it(self):
        """An entry idle less than the minimum stays with its
        holder — a claim that takes nothing answers an empty list
        and writes nothing — and FORCE takes it anyway, since a
        dead consumer's entry cannot wait out the idle."""
        store = _seed_store()
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c2", "min_idle_ms": 5000,
                            "ids": ["100-5"],
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("entries"), [])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000)[0]
            ["consumer"], "c1")
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c2", "min_idle_ms": 5000,
                            "ids": ["100-5"], "force": True,
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("entries"),
                         [["100-5", {"a": "100-5"}]])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000)[0]
            ["consumer"], "c2")

    def test_unstamped_entry_refused_and_force_takes_it(self):
        """An entry handed out by a delivery this version did not
        stamp — a replayed pre-v0.71.0 log entry carries no
        seen_ms — cannot prove it has been idle that long, so a
        claim skips it, and FORCE takes it anyway."""
        store = _seed_store()
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">"})
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c3", "min_idle_ms": 1,
                            "ids": ["101-0"],
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("entries"), [])
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c3", "min_idle_ms": 1,
                            "ids": ["101-0"], "force": True,
                            "seen_ms": 1700000003000})
        self.assertEqual(resp.get("entries"),
                         [["101-0", {"a": "101-0"}]])
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000),
            [{"id": "100-5", "consumer": "c1", "idle": 3000,
              "deliveries": 1},
             {"id": "100-6", "consumer": "c1", "idle": 3000,
              "deliveries": 1},
             {"id": "101-0", "consumer": "c3", "idle": 0,
              "deliveries": 2}])

    def test_retrycount_sets_delivery_count(self):
        """RETRYCOUNT sets the claimed entry's delivery count to
        the given number instead of raising it by one, so a
        replacement consumer can take over at the count Redis
        would report for a retried entry."""
        store = _seed_store()
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c2", "min_idle_ms": 0,
                            "ids": ["100-5"], "retrycount": 10,
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000001000),
            [{"id": "100-5", "consumer": "c2", "idle": 0,
              "deliveries": 10},
             {"id": "100-6", "consumer": "c1", "idle": 1000,
              "deliveries": 1}])

    def test_justid_answers_only_ids(self):
        """JUSTID answers only the claimed IDs, not the entries —
        the cheap form for a consumer that only needs to know
        which entries it now owns."""
        store = _seed_store()
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c2", "min_idle_ms": 0,
                            "ids": ["100-5", "100-6"], "justid": True,
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("ids"), ["100-5", "100-6"])
        self.assertNotIn("entries", resp)

    def test_non_pending_id_skipped_and_ack_drops_claim(self):
        """An ID that was never delivered, or was already acked, is
        simply skipped — a claim names pending entries, and only
        those — and an ack drops a claimed entry from the pending
        list like any other."""
        store = _seed_store()
        resp = store.apply({"op": "xclaim", "key": "s", "group": "g",
                            "consumer": "c2", "min_idle_ms": 0,
                            "ids": ["101-0", "100-5"],
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("entries"),
                         [["100-5", {"a": "100-5"}]])
        store.apply({"op": "xack", "key": "s", "group": "g",
                     "ids": ["100-5"]})
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", 1700000001000)],
            ["100-6"])

    def test_missing_key_group_wrongtype_and_bad_args(self):
        """A missing key or group fails with xclaim_failed /
        no_group, and a plain string value fails with wrongtype —
        a typo'd key or group must not read as an empty claim —
        and a bad group, consumer, ID list, minimum idle, retry
        count, or a missing seen_ms raises rather than claiming
        nonsense."""
        store = _seed_store()
        resp = store.apply({"op": "xclaim", "key": "nope",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "ids": ["100-5"],
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "xclaim_failed", resp)
        self.assertEqual(resp.get("reason"), "no_group", resp)
        resp = store.apply({"op": "xclaim", "key": "s",
                            "group": "nope", "consumer": "c2",
                            "min_idle_ms": 0, "ids": ["100-5"],
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "xclaim_failed", resp)
        self.assertEqual(resp.get("reason"), "no_group", resp)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        resp = store.apply({"op": "xclaim", "key": "plain",
                            "group": "g", "consumer": "c2",
                            "min_idle_ms": 0, "ids": ["100-5"],
                            "seen_ms": 1700000001000})
        self.assertEqual(resp.get("status"), "xclaim_failed", resp)
        self.assertEqual(resp.get("reason"), "wrongtype", resp)
        base = {"op": "xclaim", "key": "s", "group": "g",
                "consumer": "c2", "min_idle_ms": 0,
                "ids": ["100-5"], "seen_ms": 1700000001000}
        bad = []
        for key, value in (("group", ""), ("consumer", ""),
                           ("ids", []), ("ids", ["bogus"]),
                           ("ids", ["0-0"]),
                           ("min_idle_ms", -1),
                           ("min_idle_ms", "0"),
                           ("retrycount", -1)):
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
        store.apply({"op": "xclaim", "key": "s", "group": "g",
                     "consumer": "c2", "min_idle_ms": 2000,
                     "ids": ["100-5"], "seen_ms": 1700000003000})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(
            loaded.xpending_entries("s", "g", 1700000004000),
            [{"id": "100-5", "consumer": "c2", "idle": 1000,
              "deliveries": 2},
             {"id": "100-6", "consumer": "c1", "idle": 4000,
              "deliveries": 1}])


class TestXClaimCluster(unittest.TestCase):
    def test_full_flow_claim_replicates(self):
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
            resp = xclaim(addr, ids=["100-5", "100-6"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"a": "100-5"}],
                              ["100-6", {"a": "100-6"}]])
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
                    [("100-5", "c2", 2), ("100-6", "c2", 2),
                     ("101-0", "c2", 1)])
                for entry in entries:
                    self.assertIsInstance(entry["idle"], int, entry)
                    self.assertGreaterEqual(entry["idle"], 0, entry)
            # The idle times are real — they grow as time passes —
            # so a claim with a minimum idle above the time since
            # the claim takes nothing, and the same claim takes
            # the entry once it has been idle long enough.
            resp = xclaim(addr, consumer="c3", min_idle_ms=10 ** 9,
                          ids=["101-0"])
            self.assertEqual(resp.get("entries"), [], resp)
            time.sleep(0.5)
            resp = xclaim(addr, consumer="c3", min_idle_ms=400,
                          ids=["101-0"])
            self.assertEqual(resp.get("entries"),
                             [["101-0", {"a": "101-0"}]], resp)
            # FORCE takes an entry delivered a moment ago, and
            # JUSTID answers only the IDs.
            resp = xclaim(addr, consumer="c1", min_idle_ms=10 ** 9,
                          ids=["100-5"], force=True)
            self.assertEqual(resp.get("entries"),
                             [["100-5", {"a": "100-5"}]], resp)
            resp = xclaim(addr, consumer="c1", min_idle_ms=0,
                          ids=["100-6"], justid=True)
            self.assertEqual(resp.get("ids"), ["100-6"], resp)
            # RETRYCOUNT sets the delivery count.
            resp = xclaim(addr, consumer="c2", min_idle_ms=0,
                          ids=["100-6"], retrycount=7)
            self.assertEqual(resp.get("entries"),
                             [["100-6", {"a": "100-6"}]], resp)
            resp = client_xpending_entries(addr, "events", "workers")
            self.assertEqual(
                {e["id"]: e["deliveries"]
                 for e in resp.get("entries")},
                {"100-5": 3, "100-6": 7, "101-0": 2}, resp)
            # An ack drops the claimed entry, and the summary form
            # still answers alongside it.
            self.assertEqual(
                client_xack(addr, "events", "workers",
                            ["100-5"]).get("xack"), 1)
            resp = client_xpending(addr, "events", "workers")
            self.assertEqual(resp.get("pending"), 2, resp)
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
            # A bad group, consumer, ID list, minimum idle,
            # retry count, or flag is a client error with no log
            # entry.
            for msg in (
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"]},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "", "min_idle_ms": 0,
                     "ids": ["100-5"]},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0, "ids": []},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["bogus"]},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": -1,
                     "ids": ["100-5"]},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"], "retrycount": -1},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"], "justid": "yes"},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"], "force": 1}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before)
            # A missing key or group, and a plain string value, are
            # errors in the payload — decided at apply time, like a
            # CAS, so they do commit their entry.
            for msg in (
                    {"type": "client", "op": "xclaim",
                     "key": "nope", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"]},
                    {"type": "client", "op": "xclaim",
                     "key": "events", "group": "ghost",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"]},
                    {"type": "client", "op": "xclaim",
                     "key": "plain", "group": "workers",
                     "consumer": "c2", "min_idle_ms": 0,
                     "ids": ["100-5"]}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "xclaim_failed",
                                 (msg, resp))
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
                resp = session.xclaim("events", "workers", "c2", 0,
                                      "100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("entries"),
                                 [["100-5", {"a": "1"}]])
                resp = session.xpending_entries("events", "workers")
                self.assertEqual(
                    [(e["id"], e["consumer"], e["deliveries"])
                     for e in resp.get("entries")],
                    [("100-5", "c2", 2)], resp)
                # A bad group never goes on the wire, and a missing
                # key or group is an error, not an empty list.
                resp = session.xclaim("events", "", "c2", 0, "100-5")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xclaim("nope", "workers", "c2", 0,
                                      "100-5")
                self.assertEqual(resp.get("status"), "xclaim_failed",
                                 resp)
                resp = session.xclaim("events", "ghost", "c2", 0,
                                      "100-5")
                self.assertEqual(resp.get("status"), "xclaim_failed",
                                 resp)
                resp = session.xclaim("events", "workers", "c2", -1,
                                      "100-5")
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_retry_returns_first_attempt(self):
        """A retry with the same session id returns the first
        attempt's entries instead of claiming twice: the delivery
        count rises once, not once per retry."""
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
            first = xclaim(addr, client_id="c1", request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            self.assertEqual(first.get("entries"),
                             [["100-5", {"type": "login"}]])
            second = xclaim(addr, client_id="c1", request_id="r1")
            self.assertEqual(second.get("status"), "ok", second)
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
            third = xclaim(addr, consumer="c3", client_id="c1",
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
            resp = client_xclaim(follower_addr, "events", "workers",
                                 "c2", 0, ["100-5"])
            self.assertEqual(resp.get("status"), "ok", resp)
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
                client_xclaim(follower_addr, "events", "", "c2", 0,
                              ["100-5"]).get("status"), "error")
            self.assertEqual(
                client_xclaim(follower_addr, "nope", "workers", "c2",
                              0, ["100-5"]).get("status"),
                "xclaim_failed")
        finally:
            cluster.stop_all()

    def test_pipelined_claim(self):
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
                    {"op": "xclaim", "key": "events",
                     "group": "workers", "consumer": "c2",
                     "min_idle_ms": 0, "ids": ["100-5"]},
                    {"op": "xclaim", "key": "events",
                     "group": "workers", "consumer": "c3",
                     "min_idle_ms": 0, "ids": ["100-5"],
                     "justid": True},
                ])
            self.assertEqual(len(replies), 2, replies)
            self.assertEqual(replies[0].get("status"), "ok", replies)
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
            # A malformed pipelined claim never goes on the wire.
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "xclaim", "key": "events",
                     "group": "workers", "consumer": "c2",
                     "min_idle_ms": -1, "ids": ["100-5"]},
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
            resp = xclaim(addr, ids=["100-5"])
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
            # The claim answers the entries as JSON.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xclaim",
                                  "events", "workers", "c2", "0",
                                  "100-5"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-5", {"a": "1"}]])
            # JUSTID answers only the IDs, and RETRYCOUNT sets the
            # delivery count.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xclaim",
                                  "events", "workers", "c3", "0",
                                  "100-5", "--justid",
                                  "--retrycount", "4"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), ["100-5"])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers", "-", "+"])
            self.assertEqual(rc, 0, out.getvalue())
            entries = json.loads(out.getvalue())
            self.assertEqual(entries[0]["consumer"], "c3")
            self.assertEqual(entries[0]["deliveries"], 4)
            # A claim on a missing key errors.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xclaim",
                                  "nope", "workers", "c2", "0",
                                  "100-5"])
            self.assertEqual(rc, 1, out.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
