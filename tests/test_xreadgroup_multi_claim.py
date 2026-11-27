"""Tests for raftkv CLAIM for multi-stream XREADGROUP (v0.79.0).

Redis 8.4's XREADGROUP CLAIM, per ">" stream of a multi-stream
read: the read first claims the pending entries idle at least
``claim_min_idle_ms`` on that stream — from any consumer, longest
idle first — moving each one to the reading consumer, raising its
delivery count by one, and re-stamping its delivery time, like an
xclaim inline in the read, and then reads new entries on that
stream; COUNT is one shared budget across the claim and the new
entries on every stream, in the order the streams were given, and
the reply carries ``claimed`` — one [key, [{id, idle_ms,
deliveries}, ...]] pair per stream that claimed at least one entry,
in the order the streams were given — alongside ``streams``,
claimed first, then new, like Redis. With an explicit ID on a
stream the claim is ignored on that stream, like Redis, and NOACK
does not apply to claimed entries: the transfer of ownership is
the point, so they stay pending, now held by this consumer. A
blocked CLAIM read answers at once when idle entries are already
there to claim on any ">" stream. The store half runs against a
bare KVStore with fixed stamps, so the idle arithmetic is exact;
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
from raftkv.client import xreadgroup_multi as client_xreadgroup_multi
from raftkv.store import KVStore
from test_xreadgroup import groups_of

T0 = 1700000000000


def _seed_store():
    """A store with two streams — s1 with three entries, s2 with
    two — and a group "g" created from "0" on each, with c1 already
    handed the first entries on both — s1's 100-5 and 100-6, s2's
    200-1 — stamped at a fixed millisecond so the idle is exact;
    s1's 101-0 and s2's 200-2 are still new to the group."""
    store = KVStore()
    for entry_id in ("100-5", "100-6", "101-0"):
        store.apply({"op": "xadd", "key": "s1",
                     "pairs": [["a", entry_id]], "id": entry_id})
    for entry_id in ("200-1", "200-2"):
        store.apply({"op": "xadd", "key": "s2",
                     "pairs": [["b", entry_id]], "id": entry_id})
    store.apply({"op": "xgroup_create", "key": "s1",
                 "group": "g", "start_id": "0"})
    store.apply({"op": "xgroup_create", "key": "s2",
                 "group": "g", "start_id": "0"})
    store.apply({"op": "xreadgroup", "key": "s1", "group": "g",
                 "consumer": "c1", "id": ">", "count": 2,
                 "seen_ms": T0})
    store.apply({"op": "xreadgroup", "key": "s2", "group": "g",
                 "consumer": "c1", "id": ">", "count": 1,
                 "seen_ms": T0})
    return store


def _claim_multi(addr, streams=(("s1", ">"), ("s2", ">")),
                 group="g", consumer="c2", claim_min_idle_ms=100,
                 count=None, client_id=None, request_id=None):
    msg = {"type": "client", "op": "xreadgroup_multi",
           "streams": [[k, i] for k, i in streams],
           "group": group, "consumer": consumer,
           "claim_min_idle_ms": claim_min_idle_ms}
    if count is not None:
        msg["count"] = count
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


class TestXReadGroupMultiClaimStore(unittest.TestCase):
    def test_claim_moves_idle_entries_across_streams(self):
        """A claim moves the entries idle long enough to the new
        consumer on every ">" stream, raises each delivery count
        by one, and re-stamps the delivery time to the claim's
        time — so the idle measures since this claim — and the
        reply names them per stream in ``claimed`` with each one's
        idle and its previous delivery count; the entries never
        delivered to anyone ride along as fresh deliveries, so
        each group's last delivery moves past them too."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        self.assertEqual(resp.get("claimed"),
                         [["s1", [{"id": "100-5", "idle_ms": 10000,
                                   "deliveries": 1},
                                  {"id": "100-6", "idle_ms": 10000,
                                   "deliveries": 1}]],
                          ["s2", [{"id": "200-1", "idle_ms": 10000,
                                   "deliveries": 1}]]])
        group1 = groups_of(store.get("s1"))["g"]
        self.assertEqual(group1["last_delivered"], "101-0")
        pending1 = group1["pending"]
        self.assertEqual(pending1["100-5"]["consumer"], "c2")
        self.assertEqual(pending1["100-5"]["deliveries"], 2)
        self.assertEqual(pending1["100-5"]["delivered_ms"], T0 + 10000)
        self.assertEqual(pending1["100-6"]["consumer"], "c2")
        self.assertEqual(pending1["100-6"]["deliveries"], 2)
        self.assertEqual(pending1["100-6"]["delivered_ms"], T0 + 10000)
        # The never-delivered entry is a fresh delivery: count 1.
        self.assertEqual(pending1["101-0"]["consumer"], "c2")
        self.assertEqual(pending1["101-0"]["deliveries"], 1)
        consumer1 = group1["consumers"]["c2"]
        self.assertEqual(consumer1["last_seen_ms"], T0 + 10000)
        self.assertEqual(consumer1["last_delivered_id"], "101-0")
        group2 = groups_of(store.get("s2"))["g"]
        self.assertEqual(group2["last_delivered"], "200-2")
        pending2 = group2["pending"]
        self.assertEqual(pending2["200-1"]["consumer"], "c2")
        self.assertEqual(pending2["200-1"]["deliveries"], 2)
        self.assertEqual(pending2["200-1"]["delivered_ms"], T0 + 10000)
        self.assertEqual(pending2["200-2"]["consumer"], "c2")
        self.assertEqual(pending2["200-2"]["deliveries"], 1)
        consumer2 = group2["consumers"]["c2"]
        self.assertEqual(consumer2["last_seen_ms"], T0 + 10000)
        self.assertEqual(consumer2["last_delivered_id"], "200-2")

    def test_claim_and_new_entries_share_one_budget_across_streams(self):
        """COUNT is one shared budget across the claim and the new
        entries on every stream, in the order the streams were
        given: a claim that fills it on the first stream leaves no
        room for anything on the later ones, and the groups' last
        deliveries only move when a new entry is actually handed
        out."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "count": 3, "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        # The first stream takes the whole budget — its two claims
        # and then its new entry — so the second stream gets
        # nothing at all, not even its claim.
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]]])
        self.assertEqual([[k, [c["id"] for c in claimed]]
                          for k, claimed in resp.get("claimed")],
                         [["s1", ["100-5", "100-6"]]])
        # The new entry moved s1's last delivery; s2's did not
        # move, and its claimable entry is still pending for c1.
        self.assertEqual(groups_of(store.get("s1"))["g"]
                         ["last_delivered"], "101-0")
        self.assertEqual(groups_of(store.get("s2"))["g"]
                         ["last_delivered"], "200-1")
        self.assertEqual(groups_of(store.get("s2"))["g"]["pending"]
                         ["200-1"]["consumer"], "c1")
        # The next read hands out what the budget left behind: s2's
        # new entry — its claimable one is still c1's, since this
        # read carries no claim.
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "seen_ms": T0 + 11000})
        self.assertEqual(resp.get("streams"),
                         [["s2", [["200-2", {"b": "200-2"}]]]])
        self.assertNotIn("claimed", resp)
        # And with room in the budget, claimed entries ride first
        # on their stream and the new entries follow in the same
        # reply, across streams in order.
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "count": 4, "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}]]]])
        self.assertEqual([[k, [c["id"] for c in claimed]]
                          for k, claimed in resp.get("claimed")],
                         [["s1", ["100-5", "100-6"]],
                          ["s2", ["200-1"]]])

    def test_claim_ignored_for_explicit_id_streams(self):
        """With an explicit ID on a stream the claim is ignored on
        that stream, like Redis: the read re-reads only this
        consumer's own pending entries there — none here, so that
        stream is absent — while the ">" stream still claims; and
        with explicit IDs on every stream there is no ``claimed``
        in the reply at all, and the other consumer's entries stay
        exactly where they were."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", "0"]],
                            "group": "g", "consumer": "c2",
                            "claim_min_idle_ms": 0,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]]])
        self.assertEqual([[k, [c["id"] for c in claimed]]
                          for k, claimed in resp.get("claimed")],
                         [["s1", ["100-5", "100-6"]]])
        pending2 = groups_of(store.get("s2"))["g"]["pending"]
        self.assertEqual(pending2["200-1"]["consumer"], "c1")
        self.assertEqual(pending2["200-1"]["deliveries"], 1)
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", "0"], ["s2", "0"]],
                            "group": "g", "consumer": "c2",
                            "claim_min_idle_ms": 0,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"), [])
        self.assertNotIn("claimed", resp)
        pending1 = groups_of(store.get("s1"))["g"]["pending"]
        self.assertEqual(pending1["100-5"]["consumer"], "c1")
        self.assertEqual(pending1["100-5"]["deliveries"], 1)

    def test_claim_with_noack_still_records_claimed(self):
        """NOACK does not apply to claimed entries: they stay
        pending, now held by the claiming consumer, on every
        stream, while the new entries ride NOACK — delivered
        without being recorded — and the last delivery still
        advances past them."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "noack": True, "claim_min_idle_ms": 0,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        self.assertEqual([[k, [c["id"] for c in claimed]]
                          for k, claimed in resp.get("claimed")],
                         [["s1", ["100-5", "100-6"]],
                          ["s2", ["200-1"]]])
        group1 = groups_of(store.get("s1"))["g"]
        self.assertEqual(group1["last_delivered"], "101-0")
        pending1 = group1["pending"]
        self.assertEqual(sorted(pending1), ["100-5", "100-6"])
        self.assertEqual(pending1["100-5"]["consumer"], "c2")
        self.assertEqual(pending1["100-5"]["deliveries"], 2)
        group2 = groups_of(store.get("s2"))["g"]
        self.assertEqual(group2["last_delivered"], "200-2")
        pending2 = group2["pending"]
        self.assertEqual(sorted(pending2), ["200-1"])
        self.assertEqual(pending2["200-1"]["consumer"], "c2")
        self.assertEqual(pending2["200-1"]["deliveries"], 2)

    def test_claim_skips_too_fresh_entries(self):
        """An entry idle less than the minimum is skipped on its
        stream, and so is one with no provable idle — delivered by
        a log entry with no stamp, so it carries no delivery time
        — like an xclaim without FORCE: both stay with their first
        consumer, untouched, while the older entries are claimed
        and the never-delivered ones ride along."""
        store = KVStore()
        for entry_id in ("100-5", "100-6", "101-0"):
            store.apply({"op": "xadd", "key": "s1",
                         "pairs": [["a", entry_id]], "id": entry_id})
        for entry_id in ("200-1", "200-2"):
            store.apply({"op": "xadd", "key": "s2",
                         "pairs": [["b", entry_id]], "id": entry_id})
        store.apply({"op": "xgroup_create", "key": "s1",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xgroup_create", "key": "s2",
                     "group": "g", "start_id": "0"})
        store.apply({"op": "xreadgroup", "key": "s1", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0})
        store.apply({"op": "xreadgroup", "key": "s1", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0 + 8000})
        # The third s1 entry is delivered with no stamp at all,
        # and the s2 entry too fresh to claim.
        store.apply({"op": "xreadgroup", "key": "s1", "group": "g",
                     "consumer": "c1", "id": ">"})
        store.apply({"op": "xreadgroup", "key": "s2", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": T0 + 9000})
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "claim_min_idle_ms": 5000,
                            "seen_ms": T0 + 10000})
        self.assertEqual(resp.get("status"), "ok", resp)
        # 100-6 was already delivered to c1, so it is not new —
        # it is pending and too fresh to claim, so it rides
        # nowhere; only the claimed 100-5 rides on s1, and only
        # the never-delivered 200-2 rides on s2.
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}]]],
                          ["s2", [["200-2", {"b": "200-2"}]]]])
        self.assertEqual([(k, [(c["id"], c["idle_ms"])
                               for c in claimed])
                          for k, claimed in resp.get("claimed")],
                         [("s1", [("100-5", 10000)])])
        pending1 = groups_of(store.get("s1"))["g"]["pending"]
        self.assertEqual(pending1["100-6"]["consumer"], "c1")
        self.assertEqual(pending1["100-6"]["deliveries"], 1)
        self.assertEqual(pending1["100-6"]["delivered_ms"], T0 + 8000)
        self.assertEqual(pending1["101-0"]["consumer"], "c1")
        self.assertEqual(pending1["101-0"]["deliveries"], 1)
        self.assertNotIn("delivered_ms", pending1["101-0"])
        pending2 = groups_of(store.get("s2"))["g"]["pending"]
        self.assertEqual(pending2["200-1"]["consumer"], "c1")
        self.assertEqual(pending2["200-1"]["deliveries"], 1)

    def test_claim_zero_takes_everything_pending(self):
        """A claim of 0 takes every pending entry on every ">"
        stream, even one delivered at the same millisecond — idle
        0 is idle at least 0 — so a reaper that wants the whole
        pending list does not have to wait for any of it to age."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c2",
                            "claim_min_idle_ms": 0,
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([[k, [c["id"] for c in claimed]]
                          for k, claimed in resp.get("claimed")],
                         [["s1", ["100-5", "100-6"]],
                          ["s2", ["200-1"]]])
        self.assertEqual([(c["idle_ms"], c["deliveries"])
                          for _k, claimed in resp.get("claimed")
                          for c in claimed],
                         [(0, 1), (0, 1), (0, 1)])

    def test_bad_claim_rejected(self):
        """A negative claim, a boolean, or a non-integer is a
        client error at the store too — a hand-built log entry
        with one still raises rather than silently claiming
        nonsense — and the peek rejects it the same way."""
        store = _seed_store()
        for bad in (-1, True, "5000", 1.5):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.apply({"op": "xreadgroup_multi",
                             "streams": [["s1", ">"], ["s2", ">"]],
                             "group": "g", "consumer": "c2",
                             "claim_min_idle_ms": bad,
                             "seen_ms": T0 + 10000})
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.xreadgroup_multi_peek(
                    [["s1", ">"], ["s2", ">"]], "g", "c2",
                    claim_min_idle_ms=bad, seen_ms=T0 + 10000)

    def test_peek_with_claim(self):
        """The peek behind a blocked multi-stream CLAIM read sees
        the claimable entries riding first, sharing the count
        budget with the new entries, exactly as the apply path
        claims them — and it changes nothing: the pending entries
        stay with their first consumer, and a second peek sees the
        same thing."""
        store = _seed_store()
        peeked = store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c2",
            claim_min_idle_ms=5000, seen_ms=T0 + 10000)
        self.assertEqual(peeked,
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        pending1 = groups_of(store.get("s1"))["g"]["pending"]
        self.assertEqual(pending1["100-5"]["consumer"], "c1")
        self.assertEqual(pending1["100-5"]["deliveries"], 1)
        self.assertEqual(pending1["100-5"]["delivered_ms"], T0)
        # The budget is shared: a count of 2 peeks only the two
        # claimed entries on the first stream.
        peeked = store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", ">"]], "g", "c2", count=2,
            claim_min_idle_ms=5000, seen_ms=T0 + 10000)
        self.assertEqual(peeked,
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}]]]])
        # With an explicit ID the peek ignores the claim on that
        # stream, like the apply path.
        peeked = store.xreadgroup_multi_peek(
            [["s1", ">"], ["s2", "0"]], "g", "c2",
            claim_min_idle_ms=5000, seen_ms=T0 + 10000)
        self.assertEqual(peeked,
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]]])


class TestXReadGroupMultiClaimCluster(unittest.TestCase):
    def test_claim_replicates_to_every_node(self):
        """A multi-stream claim through a Session replicates to
        every node with the entries held by the new consumer and
        the delivery counts raised on each one — and a later
        XPENDING agrees per stream the entries are c2's now."""
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
                resp = session.xadd("alerts", {"b": "200-1"},
                                    id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("alerts", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5", "100-6"])
                resp = session.xreadgroup("alerts", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["200-1"])
                # Let the deliveries age past the claim minimum.
                time.sleep(0.3)
                resp = session.xreadgroup_multi(
                    [["events", ">"], ["alerts", ">"]], "workers",
                    "c2", claim_min_idle_ms=100)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("streams"),
                                 [["events", [["100-5", {"a": "100-5"}],
                                             ["100-6", {"a": "100-6"}]]],
                                  ["alerts", [["200-1", {"b": "200-1"}]]]])
                self.assertEqual(
                    [[k, [c["id"] for c in claimed]]
                     for k, claimed in resp.get("claimed", [])],
                    [["events", ["100-5", "100-6"]],
                     ["alerts", ["200-1"]]])
                for _key, claimed in resp.get("claimed", []):
                    for entry in claimed:
                        self.assertEqual(entry["deliveries"], 1, resp)
                        self.assertGreaterEqual(entry["idle_ms"], 100,
                                                resp)
                # The claim replicated: every node holds the same
                # blobs, with the entries pending for c2, twice
                # delivered.
                for node_id in cluster.live:
                    self.assertTrue(
                        wait_for_key(cluster.nodes[node_id], "events",
                                     cluster.nodes[leader.id].sm.get(
                                         "events")))
                    self.assertTrue(
                        wait_for_key(cluster.nodes[node_id], "alerts",
                                     cluster.nodes[leader.id].sm.get(
                                         "alerts")))
                    pending = groups_of(
                        cluster.nodes[node_id].sm.get("events")
                    )["workers"]["pending"]
                    self.assertEqual(
                        {eid: (info["consumer"], info["deliveries"])
                         for eid, info in pending.items()},
                        {"100-5": ("c2", 2), "100-6": ("c2", 2)})
                    pending = groups_of(
                        cluster.nodes[node_id].sm.get("alerts")
                    )["workers"]["pending"]
                    self.assertEqual(
                        {eid: (info["consumer"], info["deliveries"])
                         for eid, info in pending.items()},
                        {"200-1": ("c2", 2)})
                resp = session.xpending("events", "workers")
                self.assertEqual(resp.get("pending"), 2, resp)
                self.assertEqual(resp.get("consumers"), [["c2", 2]],
                                 resp)
                resp = session.xpending("alerts", "workers")
                self.assertEqual(resp.get("pending"), 1, resp)
                self.assertEqual(resp.get("consumers"), [["c2", 1]],
                                 resp)
        finally:
            cluster.stop_all()

    def test_claim_and_new_entries_share_budget_live(self):
        """Live: a claim plus new entries share one COUNT budget
        across the streams in a single round trip, and the entries
        the budget left behind are still new for the next read."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("alerts", {"b": "1"}, id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("alerts", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1",
                                          count=1)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5"])
                resp = session.xreadgroup("alerts", "workers", "c1",
                                          count=1)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["200-1"])
                time.sleep(0.3)
                resp = session.xadd("events", {"a": "2"}, id="100-6")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("alerts", {"b": "2"}, id="200-2")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup_multi(
                    [["events", ">"], ["alerts", ">"]], "workers",
                    "c2", count=3, claim_min_idle_ms=100)
                self.assertEqual(resp.get("status"), "ok", resp)
                # The claims and the new entry share the budget of
                # 3 across the streams in order: the first stream
                # takes its claim and its new entry, the second
                # stream takes its claim, and its new entry is left
                # behind.
                self.assertEqual(resp.get("streams"),
                                 [["events", [["100-5", {"a": "1"}],
                                             ["100-6", {"a": "2"}]]],
                                  ["alerts", [["200-1", {"b": "1"}]]]])
                self.assertEqual(
                    [[k, [c["id"] for c in claimed]]
                     for k, claimed in resp.get("claimed", [])],
                    [["events", ["100-5"]], ["alerts", ["200-1"]]])
                resp = session.xreadgroup_multi(
                    [["events", ">"], ["alerts", ">"]], "workers",
                    "c2")
                self.assertEqual(resp.get("streams"),
                                 [["alerts", [["200-2", {"b": "2"}]]]])
        finally:
            cluster.stop_all()

    def test_dedup_retry_returns_first_attempt(self):
        """A retry with the same session id returns the first
        attempt's streams instead of claiming twice: the delivery
        counts stay at 2, not 3, on every node."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("alerts", {"b": "1"}, id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("alerts", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("alerts", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
            time.sleep(0.3)
            first = _claim_multi(addr, (("events", ">"),
                                        ("alerts", ">")),
                                 group="workers",
                                 claim_min_idle_ms=100,
                                 client_id="dedup-c", request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            self.assertEqual(first.get("streams"),
                             [["events", [["100-5", {"a": "1"}]]],
                              ["alerts", [["200-1", {"b": "1"}]]]])
            second = _claim_multi(addr, (("events", ">"),
                                         ("alerts", ">")),
                                  group="workers",
                                  claim_min_idle_ms=100,
                                  client_id="dedup-c", request_id="r1")
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
        """A blocked multi-stream CLAIM read with idle entries
        already there answers at once instead of waiting out the
        block: the claim is in the reply and ``timed_out`` is
        False, in well under the 5s the block would have waited."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("alerts", {"b": "1"}, id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("alerts", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("alerts", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                time.sleep(0.3)
                started = time.monotonic()
                resp = session.xreadgroup_multi(
                    [["events", ">"], ["alerts", ">"]], "workers",
                    "c2", block=5000, claim_min_idle_ms=100)
                elapsed = time.monotonic() - started
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("timed_out"), False, resp)
                self.assertEqual(resp.get("streams"),
                                 [["events", [["100-5", {"a": "1"}]]],
                                  ["alerts", [["200-1", {"b": "1"}]]]])
                self.assertEqual(
                    [[k, [c["id"] for c in claimed]]
                     for k, claimed in resp.get("claimed", [])],
                    [["events", ["100-5"]], ["alerts", ["200-1"]]])
                self.assertLess(elapsed, 4.0, elapsed)
        finally:
            cluster.stop_all()

    def test_claim_survives_snapshot_and_restart(self):
        """A multi-stream claim survives a snapshot and a leader
        restart: the entries stay pending for the claiming
        consumer with their raised delivery counts, on the
        restarted leader."""
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("events", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("alerts", {"b": "1"}, id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("alerts", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("alerts", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                time.sleep(0.3)
                resp = session.xreadgroup_multi(
                    [["events", ">"], ["alerts", ">"]], "workers",
                    "c2", claim_min_idle_ms=100)
                self.assertEqual(resp.get("status"), "ok", resp)
                # Enough writes to compact the log past the claim.
                for i in range(10):
                    self.assertEqual(
                        session.set(f"pad{i}", "x").get("status"),
                        "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            for key in ("events", "alerts"):
                pending = groups_of(new_leader.sm.get(key))[
                    "workers"]["pending"]
                self.assertEqual(
                    {eid: (info["consumer"], info["deliveries"])
                     for eid, info in pending.items()},
                    {"100-5": ("c2", 2)} if key == "events"
                    else {"200-1": ("c2", 2)})
        finally:
            cluster.stop_all()

    def test_module_function_from_follower(self):
        """The module-level xreadgroup_multi follows the redirect
        from a follower and claims there too, so a caller holding
        any node's address gets the same claim."""
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
                resp = session.xadd("alerts", {"b": "1"}, id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("alerts", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("alerts", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
            time.sleep(0.3)
            resp = client_xreadgroup_multi(
                follower_addr, [["events", ">"], ["alerts", ">"]],
                "workers", "c2", claim_min_idle_ms=100)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["events", [["100-5", {"a": "1"}]]],
                              ["alerts", [["200-1", {"b": "1"}]]]])
            self.assertEqual(
                [[k, [c["id"] for c in claimed]]
                 for k, claimed in resp.get("claimed", [])],
                [["events", ["100-5"]], ["alerts", ["200-1"]]])
            # And a bad claim never goes on the wire.
            resp = client_xreadgroup_multi(
                follower_addr, [["events", ">"], ["alerts", ">"]],
                "workers", "c2", claim_min_idle_ms=-1)
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
                rc = client_main(["--node", node, "xadd", "alerts",
                                  "200-1", "b", "1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "200-1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_create",
                                  "events", "workers", "0"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "OK")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_create",
                                  "alerts", "workers", "0"])
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
                rc = client_main(["--node", node, "xreadgroup",
                                  "alerts", "workers", "c1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             [["200-1", {"b": "1"}]])
            time.sleep(0.3)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xreadgroup-multi",
                                  "workers", "c2", "events", ">",
                                  "alerts", ">", "--claim", "100"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             [["events", [["100-5", {"a": "1"}]]],
                              ["alerts", [["200-1", {"b": "1"}]]]])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
