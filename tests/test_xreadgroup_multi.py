"""Tests for raftkv multi-stream XREADGROUP (v0.77.0).

Redis XREADGROUP with STREAMS: one group name, one consumer, one
[key, id] pair per stream — the same group must exist on every
stream — and one call hands out entries across all of them: ">"
hands out the entries appended after that group's last delivery on
that stream, any other ID re-reads the consumer's own pending
entries on that stream, COUNT is one shared budget across all the
streams in the order they were given, and the reply carries
``streams`` — the [key, [[id, {field: value}], ...]] pairs for
every stream that handed out at least one entry, in the order the
streams were given, like XREAD. The whole call is one log entry,
so the deliveries across streams are atomic. The store half runs
against a bare KVStore with fixed stamps, so the delivery stamps
are exact; the cluster half spins up real nodes (real TCP, real
threads, real disk) on localhost, like the rest of the suite.
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
    two — and a group "g" created from "0" on each, so every entry
    is new to the group."""
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
    return store


def _multi(addr, streams=(("s1", ">"), ("s2", ">")),
           group="g", consumer="c1", count=None,
           client_id=None, request_id=None):
    msg = {"type": "client", "op": "xreadgroup_multi",
           "streams": [[k, i] for k, i in streams],
           "group": group, "consumer": consumer}
    if count is not None:
        msg["count"] = count
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


class TestXReadGroupMultiStore(unittest.TestCase):
    def test_multi_stream_gt_reads_across_streams(self):
        """A ">" read across two streams hands out every new entry
        on each, in stream order, moves each group's last delivery
        to the last entry it handed out on that stream, records
        each entry pending for the consumer on its own stream, and
        stamps the consumer once per stream — with that stream's
        newest entry as its last delivery there."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        for key, last in (("s1", "101-0"), ("s2", "200-2")):
            group = groups_of(store.get(key))["g"]
            self.assertEqual(group["last_delivered"], last)
            self.assertEqual(
                {eid: (info["consumer"], info["deliveries"])
                 for eid, info in group["pending"].items()},
                {eid: ("c1", 1)
                 for eid in group["pending"]})
            for info in group["pending"].values():
                self.assertEqual(info["delivered_ms"], T0)
            consumer = group["consumers"]["c1"]
            self.assertEqual(consumer["last_seen_ms"], T0)
            self.assertEqual(consumer["last_delivered_id"], last)

    def test_shared_count_budget(self):
        """COUNT is one shared budget across the streams, in the
        order they were given: a budget of 4 hands out all of s1
        and one entry of s2, and the entry the budget left behind
        is still new for the next read."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "count": 4, "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}]]]])
        self.assertEqual(groups_of(store.get("s1"))["g"][
            "last_delivered"], "101-0")
        self.assertEqual(groups_of(store.get("s2"))["g"][
            "last_delivered"], "200-1")
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("streams"),
                         [["s2", [["200-2", {"b": "200-2"}]]]])

    def test_stream_with_nothing_new_is_absent(self):
        """A stream with nothing for this consumer is simply absent
        from the reply, like XREAD — and a read that hands out
        nothing anywhere answers an empty streams list."""
        store = _seed_store()
        store.apply({"op": "xreadgroup_multi",
                     "streams": [["s1", ">"]],
                     "group": "g", "consumer": "c1",
                     "seen_ms": T0})
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("streams"), [])

    def test_explicit_id_rereads_own_pending_per_stream(self):
        """An explicit ID per stream re-reads the consumer's own
        pending entries on that stream, at or after the ID it
        names — each re-read raising the delivery count by one —
        and never another consumer's entries."""
        store = _seed_store()
        store.apply({"op": "xreadgroup_multi",
                     "streams": [["s1", ">"], ["s2", ">"]],
                     "group": "g", "consumer": "c1",
                     "seen_ms": T0})
        store.apply({"op": "xreadgroup", "key": "s1", "group": "g",
                     "consumer": "c2", "id": ">",
                     "seen_ms": T0})
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", "100-6"],
                                        ["s2", "0"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0 + 5000})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]],
                          ["s2", [["200-1", {"b": "200-1"}],
                                  ["200-2", {"b": "200-2"}]]]])
        pending1 = groups_of(store.get("s1"))["g"]["pending"]
        self.assertEqual(pending1["100-5"]["deliveries"], 1)
        self.assertEqual(pending1["100-6"]["deliveries"], 2)
        self.assertEqual(pending1["101-0"]["deliveries"], 2)
        pending2 = groups_of(store.get("s2"))["g"]["pending"]
        self.assertEqual(pending2["200-1"]["deliveries"], 2)
        self.assertEqual(pending2["200-2"]["deliveries"], 2)

    def test_noack_multi(self):
        """NOACK delivers across streams without recording the
        entries pending, though each stream's last delivery still
        advances — and the consumer is still stamped, since it
        interacted and received entries."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "noack": True, "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual([e[0] for _k, es in resp.get("streams")
                          for e in es],
                         ["100-5", "100-6", "101-0",
                          "200-1", "200-2"])
        for key in ("s1", "s2"):
            group = groups_of(store.get(key))["g"]
            self.assertEqual(group["pending"], {})
            self.assertEqual(group["consumers"]["c1"][
                "last_seen_ms"], T0)
        self.assertEqual(groups_of(store.get("s1"))["g"][
            "last_delivered"], "101-0")
        self.assertEqual(groups_of(store.get("s2"))["g"][
            "last_delivered"], "200-2")

    def test_count_zero_hands_out_nothing(self):
        """COUNT 0 hands out nothing on any stream and writes
        nothing at all: no pending entries, no last delivery
        moved, no consumer stamped."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "count": 0, "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"), [])
        for key in ("s1", "s2"):
            group = groups_of(store.get(key))["g"]
            self.assertEqual(group["pending"], {})
            self.assertEqual(group["last_delivered"], "0-0")
            self.assertEqual(group.get("consumers", {}), {})

    def test_missing_key_fails_atomically(self):
        """A missing key on any stream fails the whole call, and
        the streams that would have delivered are untouched: no
        entry handed out, no last delivery moved, no consumer
        stamped — the failure is decided before anything is
        delivered."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["gone", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "xreadgroup_failed",
                         resp)
        self.assertEqual(resp.get("reason"), "no_group", resp)
        group = groups_of(store.get("s1"))["g"]
        self.assertEqual(group["pending"], {})
        self.assertEqual(group["last_delivered"], "0-0")
        self.assertEqual(group.get("consumers", {}), {})

    def test_missing_group_fails_atomically(self):
        """A group that exists on one stream but not the other
        fails the whole call the same way, and the stream that
        has the group is untouched."""
        store = _seed_store()
        store.apply({"op": "xgroup_create", "key": "s2",
                     "group": "other", "start_id": "0"})
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "other", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "xreadgroup_failed",
                         resp)
        self.assertEqual(resp.get("reason"), "no_group", resp)
        group = groups_of(store.get("s2"))["g"]
        self.assertEqual(group["pending"], {})
        self.assertEqual(group["last_delivered"], "0-0")

    def test_wrongtype_fails_atomically(self):
        """A plain string on any stream fails the whole call with
        WRONGTYPE, and the stream that would have delivered is
        untouched."""
        store = _seed_store()
        store.apply({"op": "set", "key": "s2", "value": "plain"})
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s2", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "xreadgroup_failed",
                         resp)
        self.assertEqual(resp.get("reason"), "wrongtype", resp)
        group = groups_of(store.get("s1"))["g"]
        self.assertEqual(group["pending"], {})
        self.assertEqual(group["last_delivered"], "0-0")

    def test_bad_arguments_raise(self):
        """A hand-built log entry with a bad stream list, group,
        consumer, count, or ID raises instead of delivering
        nonsense — the leader validates the same shapes before
        anything is logged."""
        store = _seed_store()
        base = {"op": "xreadgroup_multi",
                "streams": [["s1", ">"], ["s2", ">"]],
                "group": "g", "consumer": "c1", "seen_ms": T0}
        for bad in ([], [["s1"]], [["s1", ">", "extra"]],
                    [[1, ">"]], [["", ">"]], [["s1", 5]],
                    "s1"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.apply({**base, "streams": bad})
        with self.assertRaises(ValueError):
            store.apply({**base, "group": ""})
        with self.assertRaises(ValueError):
            store.apply({**base, "consumer": ""})
        with self.assertRaises(ValueError):
            store.apply({**base, "count": -1})
        with self.assertRaises(ValueError):
            store.apply({**base, "count": True})
        with self.assertRaises(ValueError):
            store.apply({**base, "seen_ms": -1})
        with self.assertRaises(ValueError):
            store.apply({**base, "streams": [["s1", "nope"]]})

    def test_repeated_key_reads_updated_position(self):
        """A key named twice in one call reads the first delivery's
        effects on the second pass: the second ">" finds nothing
        new, so the key rides the reply once, not twice."""
        store = _seed_store()
        resp = store.apply({"op": "xreadgroup_multi",
                            "streams": [["s1", ">"], ["s1", ">"]],
                            "group": "g", "consumer": "c1",
                            "seen_ms": T0})
        self.assertEqual(resp.get("status"), "ok", resp)
        self.assertEqual(resp.get("streams"),
                         [["s1", [["100-5", {"a": "100-5"}],
                                  ["100-6", {"a": "100-6"}],
                                  ["101-0", {"a": "101-0"}]]]])
        group = groups_of(store.get("s1"))["g"]
        self.assertEqual(group["last_delivered"], "101-0")
        self.assertEqual(len(group["pending"]), 3)


class TestXReadGroupMultiCluster(unittest.TestCase):
    def test_multi_read_replicates_to_every_node(self):
        """Live: one multi-stream read delivers on both streams
        through one log entry, and every node holds the same blobs
        — both entries pending for the consumer on each stream —
        with XPENDING agreeing per stream."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for key, entry_id in (("s1", "100-5"),
                                      ("s1", "100-6"),
                                      ("s2", "200-1")):
                    resp = session.xadd(key, {"a": entry_id},
                                        id=entry_id)
                    self.assertEqual(resp.get("status"), "ok", resp)
                for key in ("s1", "s2"):
                    resp = session.xgroup_create(key, "g",
                                                 start_id="0")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup_multi(
                    [["s1", ">"], ["s2", ">"]], "g", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("streams"),
                                 [["s1", [["100-5", {"a": "100-5"}],
                                          ["100-6", {"a": "100-6"}]]],
                                  ["s2", [["200-1", {"a": "200-1"}]]]])
                for node_id in cluster.live:
                    self.assertTrue(
                        wait_for_key(
                            cluster.nodes[node_id], "s1",
                            cluster.nodes[leader.id].sm.get("s1")))
                    self.assertTrue(
                        wait_for_key(
                            cluster.nodes[node_id], "s2",
                            cluster.nodes[leader.id].sm.get("s2")))
                    for key, want in (("s1", ["100-5", "100-6"]),
                                      ("s2", ["200-1"])):
                        pending = groups_of(
                            cluster.nodes[node_id].sm.get(key)
                        )["g"]["pending"]
                        self.assertEqual(
                            {eid: (info["consumer"],
                                   info["deliveries"])
                             for eid, info in pending.items()},
                            {eid: ("c1", 1) for eid in want})
                for key, want in (("s1", 2), ("s2", 1)):
                    resp = session.xpending(key, "g")
                    self.assertEqual(resp.get("pending"), want, resp)
                    self.assertEqual(resp.get("consumers"),
                                     [["c1", want]], resp)
        finally:
            cluster.stop_all()

    def test_shared_budget_live(self):
        """Live: a COUNT budget shared across streams in one round
        trip, and the entry the budget left behind is still new
        for the next read."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for key, entry_id in (("s1", "100-5"),
                                      ("s1", "100-6"),
                                      ("s2", "200-1")):
                    resp = session.xadd(key, {"a": entry_id},
                                        id=entry_id)
                    self.assertEqual(resp.get("status"), "ok", resp)
                for key in ("s1", "s2"):
                    resp = session.xgroup_create(key, "g",
                                                 start_id="0")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup_multi(
                    [["s1", ">"], ["s2", ">"]], "g", "c1", count=2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("streams"),
                                 [["s1", [["100-5", {"a": "100-5"}],
                                          ["100-6", {"a": "100-6"}]]]])
                resp = session.xreadgroup_multi(
                    [["s1", ">"], ["s2", ">"]], "g", "c1")
                self.assertEqual(resp.get("streams"),
                                 [["s2", [["200-1", {"a": "200-1"}]]]])
        finally:
            cluster.stop_all()

    def test_dedup_retry_returns_first_attempt(self):
        """A retry with the same session id returns the first
        attempt's streams instead of delivering twice: the delivery
        counts stay at 1 on every node."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("s1", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xadd("s2", {"b": "1"}, id="200-1")
                self.assertEqual(resp.get("status"), "ok", resp)
                for key in ("s1", "s2"):
                    resp = session.xgroup_create(key, "g",
                                                 start_id="0")
                    self.assertEqual(resp.get("status"), "ok", resp)
            first = _multi(addr, client_id="dedup-c",
                           request_id="r1")
            self.assertEqual(first.get("status"), "ok", first)
            second = _multi(addr, client_id="dedup-c",
                            request_id="r1")
            self.assertEqual(second, first, second)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "s1",
                                 cluster.nodes[leader.id].sm.get(
                                     "s1")))
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "s2",
                                 cluster.nodes[leader.id].sm.get(
                                     "s2")))
                for key in ("s1", "s2"):
                    pending = groups_of(
                        cluster.nodes[node_id].sm.get(key)
                    )["g"]["pending"]
                    self.assertEqual(
                        {eid: info["deliveries"]
                         for eid, info in pending.items()},
                        {"100-5": 1} if key == "s1"
                        else {"200-1": 1})
        finally:
            cluster.stop_all()

    def test_bad_arguments_are_client_errors(self):
        """A bad group, stream list, or count never goes on the
        wire: the leader answers a client error and appends no log
        entry, so nothing is delivered anywhere."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.xadd("s1", {"a": "1"}, id="100-5")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("s1", "g", start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
            before = leader.sm.get("s1")
            self.assertEqual(_multi(addr, group="").get("status"),
                             "error")
            self.assertEqual(_multi(addr, streams=[]).get("status"),
                             "error")
            self.assertEqual(_multi(addr, streams=[("s1", "$")]).get(
                "status"), "error")
            self.assertEqual(_multi(addr, count=-1).get("status"),
                             "error")
            self.assertEqual(_multi(addr, streams=[("s1", ">"),
                                                   ("gone", ">")]).get(
                "status"), "xreadgroup_failed")
            self.assertEqual(leader.sm.get("s1"), before)
            self.assertEqual(groups_of(before)["g"]["pending"], {})
        finally:
            cluster.stop_all()

    def test_multi_survives_snapshot_and_restart(self):
        """A multi-stream delivery survives a snapshot and a leader
        restart: the entries stay pending for the consumer on each
        stream, on the restarted leader."""
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for key, entry_id in (("s1", "100-5"),
                                      ("s2", "200-1")):
                    resp = session.xadd(key, {"a": entry_id},
                                        id=entry_id)
                    self.assertEqual(resp.get("status"), "ok", resp)
                for key in ("s1", "s2"):
                    resp = session.xgroup_create(key, "g",
                                                 start_id="0")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup_multi(
                    [["s1", ">"], ["s2", ">"]], "g", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                # Enough writes to compact the log past the read.
                for i in range(10):
                    self.assertEqual(
                        session.set(f"pad{i}", "x").get("status"),
                        "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            for key, entry_id in (("s1", "100-5"),
                                  ("s2", "200-1")):
                pending = groups_of(new_leader.sm.get(key))[
                    "g"]["pending"]
                self.assertEqual(pending[entry_id]["consumer"], "c1")
                self.assertEqual(pending[entry_id]["deliveries"], 1)
        finally:
            cluster.stop_all()

    def test_module_function_from_follower(self):
        """The module-level xreadgroup_multi follows the redirect
        from a follower and delivers there too, so a caller holding
        any node's address gets the same read."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower_id = next(n for n in cluster.live
                               if n != leader.id)
            follower_addr = cluster.addrs[follower_id]
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for key, entry_id in (("s1", "100-5"),
                                      ("s2", "200-1")):
                    resp = session.xadd(key, {"a": entry_id},
                                        id=entry_id)
                    self.assertEqual(resp.get("status"), "ok", resp)
                for key in ("s1", "s2"):
                    resp = session.xgroup_create(key, "g",
                                                 start_id="0")
                    self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_xreadgroup_multi(follower_addr,
                                           [["s1", ">"], ["s2", ">"]],
                                           "g", "c1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("streams"),
                             [["s1", [["100-5", {"a": "100-5"}]]],
                              ["s2", [["200-1", {"a": "200-1"}]]]])
            # And a bad stream list never goes on the wire.
            resp = client_xreadgroup_multi(follower_addr, [], "g",
                                           "c1")
            self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_cli_end_to_end(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            node = f"127.0.0.1:{cluster.addrs[leader.id][1]}"
            for args, want in (
                    (["xadd", "s1", "100-5", "a", "1"], "100-5"),
                    (["xadd", "s2", "200-1", "b", "1"], "200-1"),
                    (["xgroup_create", "s1", "g", "0"], "OK"),
                    (["xgroup_create", "s2", "g", "0"], "OK")):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = client_main(["--node", node] + args)
                self.assertEqual(rc, 0, out.getvalue())
                self.assertEqual(out.getvalue().strip(), want)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xreadgroup-multi",
                                  "g", "c1", "s1", ">", "s2", ">"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()),
                             [["s1", [["100-5", {"a": "1"}]]],
                              ["s2", [["200-1", {"b": "1"}]]]])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xreadgroup-multi",
                                  "g", "c1", "s1", ">", "s2", ">",
                                  "--count", "1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), [])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
