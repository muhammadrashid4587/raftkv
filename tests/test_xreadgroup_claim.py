"""Tests for raftkv XREADGROUP CLAIM (v0.76.0).

Redis 8.4's XREADGROUP CLAIM: a ">" read first claims the pending
entries idle at least ``claim_min_idle_ms`` — from any consumer,
longest idle first — moving each one to the reading consumer, raising
its delivery count by one, and re-stamping its delivery time, like
an xclaim inline in the read, and then reads new entries; COUNT is
one shared budget across the claim and the new entries, and the reply
carries ``claimed`` — one dict per claimed entry, with its idle at
claim time and how many times it had been delivered before —
alongside ``entries``, claimed first, then new. With an explicit ID
the claim is ignored, like Redis, and NOACK does not apply to
claimed entries: the transfer of ownership is the point, so they
stay pending, now held by this consumer. The store half runs against
a bare KVStore with fixed stamps, so the idle arithmetic is exact;
the cluster half spins up real nodes (real TCP, real threads, real
disk) on localhost, like the rest of the suite.
"""

import contextlib
import io
import json
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import xreadgroup as client_xreadgroup
from raftkv.store import KVStore
from test_xreadgroup import groups_of

T0 = 1700000000000


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
                 "seen_ms": T0})
    return store


def _claim_xreadgroup(addr, key="events", group="workers",
                      consumer="c2", claim_min_idle_ms=100,
                      count=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xreadgroup", "key": key,
           "group": group, "consumer": consumer, "id": ">",
           "claim_min_idle_ms": claim_min_idle_ms}
    if count is not None:
        msg["count"] = count
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


class TestXReadGroupClaimStore(unittest.TestCase):
    def test_claim_moves_idle_entries_to_new_consumer(self):
        """A claim moves the entries idle long enough to the new
        consumer, raises each delivery count by one, and re-stamps
        the delivery time to the claim's time — so the idle
        measures since this claim — and the reply names them in
        ``claimed`` with each one's idle and its previous delivery
        count; the entry never delivered to anyone rides along as
        a fresh delivery, so the group's last delivery moves past
        it too."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("entries"),
                         [["100-5", {"a": "100-5"}],
                          ["100-6", {"a": "100-6"}],
                          ["101-0", {"a": "101-0"}]])
        self.assertEqual(resp.get("claimed"),
                         [{"id": "100-5", "idle_ms": 10000,
                           "deliveries": 1},
                          {"id": "100-6", "idle_ms": 10000,
                           "deliveries": 1}])
        group = groups_of(store.get("s"))["g"]
        self.assertEqual(group["last_delivered"], "101-0")
        pending = group["pending"]
        self.assertEqual(pending["100-5"]["consumer"], "c2")
        self.assertEqual(pending["100-5"]["deliveries"], 2)
        self.assertEqual(pending["100-5"]["delivered_ms"], T0 + 10000)
        self.assertEqual(pending["100-6"]["consumer"], "c2")
        self.assertEqual(pending["100-6"]["deliveries"], 2)
        self.assertEqual(pending["100-6"]["delivered_ms"], T0 + 10000)
        # The never-delivered entry is a fresh delivery: count 1.
        self.assertEqual(pending["101-0"]["consumer"], "c2")
        self.assertEqual(pending["101-0"]["deliveries"], 1)
        consumer = group["consumers"]["c2"]
        self.assertEqual(consumer["last_seen_ms"], T0 + 10000)
        self.assertEqual(consumer["last_delivered_id"], "101-0")

    def test_claim_orders_longest_idle_first(self):
        """Entries delivered at different times claim longest idle
        first, not in ID order: the entry idle 10000ms rides ahead
        of the one idle 2000ms, and each one's ``claimed`` idle is
        its own."""
        store = KVStore()
        for entry_id in ("100-5", "100-6"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0 + 8000})
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "claim_min_idle_ms": 1000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["100-5", "100-6"])
        self.assertEqual([(c["id"], c["idle_ms"])
                          for c in resp.get("claimed")],
                         [("100-5", 10000), ("100-6", 2000)])

    def test_claim_and_new_entries_share_count_budget(self):
        """COUNT is one shared budget across the claim and the new
        entries: a claim that fills it leaves no room for new
        entries, and the group's last delivery only moves when a
        new entry is actually handed out."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "count": 2,
                            "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["100-5", "100-6"])
        self.assertEqual(len(resp.get("claimed")), 2)
        group = groups_of(store.get("s"))["g"]
        self.assertEqual(group["last_delivered"], "100-6")
        # The new entry is still there for the next read, and the
        # next read hands it out as a fresh delivery.
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "seen_ms": T0 + 11000})
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["101-0"])
        self.assertNotIn("claimed", resp)
        group = groups_of(store.get("s"))["g"]
        self.assertEqual(group["last_delivered"], "101-0")
        # And with room in the budget, claimed entries ride first
        # and the new entry follows in the same reply.
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "count": 3,
                            "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["100-5", "100-6", "101-0"])
        self.assertEqual([c["id"] for c in resp.get("claimed")],
                         ["100-5", "100-6"])
        group = groups_of(store.get("s"))["g"]
        self.assertEqual(group["last_delivered"], "101-0")

    def test_claim_skips_too_fresh_and_unstamped(self):
        """An entry idle less than the minimum is skipped, and so
        is one with no provable idle — delivered by a log entry
        with no stamp, so it carries no delivery time — like an
        xclaim without FORCE: both stay with their first consumer,
        untouched."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s",
                         "pairs": [["a", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0 + 8000})
        # The third entry is delivered with no stamp at all.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">"})
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["100-5"])
        self.assertEqual([(c["id"], c["idle_ms"])
                          for c in resp.get("claimed")],
                         [("100-5", 10000)])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-6"]["consumer"], "c1")
        self.assertEqual(pending["100-6"]["deliveries"], 1)
        self.assertEqual(pending["100-6"]["delivered_ms"], T0 + 8000)
        self.assertEqual(pending["101-0"]["consumer"], "c1")
        self.assertEqual(pending["101-0"]["deliveries"], 1)
        self.assertNotIn("delivered_ms", pending["101-0"])

    def test_claim_ignored_for_explicit_id_read(self):
        """With an explicit ID the claim is ignored, like Redis:
        the read re-reads only this consumer's own pending entries
        — none here, so it answers empty — and the other
        consumer's entries stay exactly where they were, with no
        ``claimed`` in the reply at all."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": "0", "claim_min_idle_ms": 0,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("entries"), [])
        self.assertNotIn("claimed", resp)
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["consumer"], "c1")
        self.assertEqual(pending["100-5"]["deliveries"], 1)
        self.assertEqual(pending["100-6"]["consumer"], "c1")

    def test_claim_with_noack_still_records_claimed(self):
        """NOACK does not apply to claimed entries: they stay
        pending, now held by the claiming consumer, while the new
        entries ride NOACK — delivered without being recorded —
        and the last delivery still advances past them."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "noack": True,
                            "claim_min_idle_ms": 0,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["100-5", "100-6", "101-0"])
        self.assertEqual([c["id"] for c in resp.get("claimed")],
                         ["100-5", "100-6"])
        group = groups_of(store.get("s"))["g"]
        self.assertEqual(group["last_delivered"], "101-0")
        pending = group["pending"]
        self.assertEqual(sorted(pending), ["100-5", "100-6"])
        self.assertEqual(pending["100-5"]["consumer"], "c2")
        self.assertEqual(pending["100-5"]["deliveries"], 2)

    def test_claim_skips_trimmed_keepref_entries(self):
        """A pending entry whose data was trimmed away under
        KEEPREF is skipped by the claim — there is nothing left
        to return for it — and it stays pending, like an xclaim
        on it, while the entry whose data survives is claimed and
        the never-delivered one rides along as a fresh delivery."""
        store = _seed_store()
        result = store.apply({"op": "xtrim", "key": "s",
                              "strategy": "MINID",
                              "threshold": "100-6",
                              "mode": "KEEPREF"})
        self.assertEqual(result.get("status"), "ok", result)
        self.assertEqual(result.get("xtrim"), 1, result)
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([e[0] for e in resp.get("entries")],
                         ["100-6", "101-0"])
        self.assertEqual([c["id"] for c in resp.get("claimed")],
                         ["100-6"])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["consumer"], "c1")
        self.assertEqual(pending["100-5"]["deliveries"], 1)

    def test_claim_zero_takes_everything_pending(self):
        """A claim of 0 takes every pending entry, even one
        delivered at the same millisecond — idle 0 is idle at
        least 0 — so a reaper that wants the whole pending list
        does not have to wait for any of it to age."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup", "key": "s",
                            "group": "g", "consumer": "c2",
                            "id": ">", "claim_min_idle_ms": 0,
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([c["id"] for c in resp.get("claimed")],
                         ["100-5", "100-6"])
        self.assertEqual([(c["idle_ms"], c["deliveries"])
                          for c in resp.get("claimed")],
                         [(0, 1), (0, 1)])

    def test_bad_claim_rejected(self):
        """A negative claim, a boolean, or a non-integer is a
        client error at the store too — a hand-built log entry
        with one still raises rather than silently claiming
        nonsense — and the peek rejects it the same way."""
        store = _seed_store()
        for bad in (-1, True, "5000", 1.5):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.apply({"op": "xreadgroup", "key": "s",
                             "group": "g", "consumer": "c2",
                             "id": ">", "claim_min_idle_ms": bad,
                             "seen_ms": T0 + 10000})
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.xreadgroup_peek("s", "g",
                                      claim_min_idle_ms=bad,
                                      seen_ms=T0 + 10000)


class TestXReadGroupClaimCluster(unittest.TestCase):
    def test_claim_replicates_to_every_node(self):
        """A claim through a Session replicates to every node with
        the entries held by the new consumer and the delivery
        counts raised on each one — and a later XPENDING agrees
        the entries are c2's now."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for entry_id in ("100-5", "100-6"):
                    resp = session.xadd("events", {"a": entry_id},
                                        id=entry_id)
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5", "100-6"])
                # Let the deliveries age past the claim minimum.
                time.sleep(0.3)
                resp = session.xreadgroup("events", "workers", "c2",
                                          claim_min_idle_ms=100)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5", "100-6"])
                self.assertEqual(
                    [c["id"] for c in resp.get("claimed", [])],
                    ["100-5", "100-6"])
                for claimed in resp.get("claimed", []):
                    self.assertEqual(claimed["deliveries"], 1, resp)
                    self.assertGreaterEqual(claimed["idle_ms"], 100,
                                            resp)
                # The claim replicated: every node holds the same
                # blob, with both entries pending for c2, twice
                # delivered.
                for node_id in cluster.live:
                    self.assertTrue(
                        wait_for_key(cluster.nodes[node_id], "events",
                                     cluster.nodes[leader.id].sm.get(
                                         "events")))
                    pending = groups_of(
                        cluster.nodes[node_id].sm.get("events")
                    )["workers"]["pending"]
                    self.assertEqual(
                        {eid: (info["consumer"], info["deliveries"])
                         for eid, info in pending.items()},
                        {"100-5": ("c2", 2), "100-6": ("c2", 2)})
                resp = session.xpending("events", "workers")
                self.assertEqual(resp.get("pending"), 2, resp)
                self.assertEqual(resp.get("consumers"), [["c2", 2]],
                                 resp)
        finally:
            cluster.stop_all()

    def test_claim_and_new_entries_share_budget_live(self):
        """Live: a claim plus a new entry share one COUNT budget in
        a single round trip, and the entry the budget left behind
        is still new for the next read."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1",
                                          count=1)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5"])
                time.sleep(0.3)
                resp = session.xadd("events", {"a": "2"}, id="100-6")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c2",
                                          count=1,
                                          claim_min_idle_ms=100)
                self.assertEqual(resp.get("status"), "ok", resp)
                # The claim filled the budget of 1: only the
                # claimed entry rides, not the new one.
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5"])
                self.assertEqual([c["id"] for c in resp.get("claimed")],
                                 ["100-5"])
                resp = session.xreadgroup("events", "workers", "c2")
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-6"])
        finally:
            cluster.stop_all()

    def test_dedup_retry_returns_first_attempt(self):
        """A retry with the same session id returns the first
        attempt's entries instead of claiming twice: the delivery
        counts stay at 2, not 3, on every node."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
            time.sleep(0.3)
            first = _claim_xreadgroup(addr, claim_min_idle_ms=100,
                                      client_id="dedup-c",
                                      request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            self.assertEqual([e[0] for e in first.get("entries")],
                             ["100-5"])
            second = _claim_xreadgroup(addr, claim_min_idle_ms=100,
                                       client_id="dedup-c",
                                       request_id="r1")
            self.assertEqual(second, first, second)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                pending = groups_of(
                    cluster.nodes[node_id].sm.get("events")
                )["workers"]["pending"]
                self.assertEqual(pending["100-5"]["deliveries"], 2)
                self.assertEqual(pending["100-5"]["consumer"], "c2")
        finally:
            cluster.stop_all()

    def test_block_with_claimable_entries_answers_at_once(self):
        """A blocked CLAIM read with idle entries already there
        answers at once instead of waiting out the block: the
        claim is in the reply and ``timed_out`` is False, in well
        under the 5s the block would have waited."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                time.sleep(0.3)
                started = time.monotonic()
                resp = session.xreadgroup("events", "workers", "c2",
                                          block=5000,
                                          claim_min_idle_ms=100)
                elapsed = time.monotonic() - started
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("timed_out"), False, resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5"])
                self.assertEqual([c["id"] for c in resp.get("claimed")],
                                 ["100-5"])
                self.assertLess(elapsed, 4.0, elapsed)
        finally:
            cluster.stop_all()

    def test_pipelined_claim_readgroup(self):
        """A claim rides a pipelined xreadgroup too, in position,
        and a malformed claim in a pipeline is a per-op error, not
        a crash — while BLOCK still is not a pipeline op."""
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
                     "group": "workers", "consumer": "c1"},
                ])
            for resp in results:
                self.assertEqual(resp.get("status"), "ok", resp)
            with Session(addr) as session:
                results = session.pipeline([
                    {"op": "xreadgroup", "key": "events",
                     "group": "workers", "consumer": "c2",
                     "claim_min_idle_ms": 0},
                    {"op": "xreadgroup", "key": "events",
                     "group": "workers", "consumer": "c2",
                     "claim_min_idle_ms": -1},
                ])
            self.assertEqual(len(results), 2, results)
            self.assertEqual(results[0].get("status"), "ok",
                             results[0])
            self.assertEqual([e[0] for e in results[0].get("entries")],
                             ["100-5", "100-6"])
            self.assertEqual([c["id"] for c in
                              results[0].get("claimed", [])],
                             ["100-5", "100-6"])
            self.assertEqual(results[1].get("status"), "error",
                             results[1])
            pending = groups_of(leader.sm.get("events"))[
                "workers"]["pending"]
            self.assertEqual(
                {eid: info["deliveries"] for eid, info in
                 pending.items()},
                {"100-5": 2, "100-6": 2})
        finally:
            cluster.stop_all()

    def test_claim_survives_snapshot_and_restart(self):
        """A claim survives a snapshot and a leader restart: the
        entries stay pending for the claiming consumer with their
        raised delivery counts, on the restarted leader."""
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                time.sleep(0.3)
                resp = session.xreadgroup("events", "workers", "c2",
                                          claim_min_idle_ms=100)
                self.assertEqual(resp.get("status"), "ok", resp)
                # Enough writes to compact the log past the claim.
                for i in range(10):
                    self.assertEqual(
                        session.set(f"pad{i}", "x").get("status"),
                        "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            pending = groups_of(new_leader.sm.get("events"))[
                "workers"]["pending"]
            self.assertEqual(pending["100-5"]["consumer"], "c2")
            self.assertEqual(pending["100-5"]["deliveries"], 2)
        finally:
            cluster.stop_all()

    def test_module_function_from_follower(self):
        """The module-level xreadgroup follows the redirect from a
        follower and claims there too, so a caller holding any
        node's address gets the same claim."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower_id = next(n for n in cluster.live
                               if n != leader.id)
            follower_addr = cluster.addrs[follower_id]
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
            time.sleep(0.3)
            resp = client_xreadgroup(follower_addr, "events",
                                     "workers", "c2",
                                     claim_min_idle_ms=100)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["100-5"])
            self.assertEqual([c["id"] for c in resp.get("claimed")],
                             ["100-5"])
            # And a bad claim never goes on the wire.
            resp = client_xreadgroup(follower_addr, "events",
                                     "workers", "c2",
                                     claim_min_idle_ms=-1)
            self.assertEqual(resp.get("status"), "error", resp)
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
            time.sleep(0.3)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xreadgroup",
                                  "events", "workers", "c2",
                                  "--claim", "100"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             [["100-5", {"a": "1"}]])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
