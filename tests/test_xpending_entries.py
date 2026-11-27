"""Tests for raftkv XPENDING in per-entry form (v0.71.0).

``xpending key group start end count [consumer] [--idle n]`` lists
the individual pending entries of a consumer group — one dict per
entry handed out and not yet acked, in ID order, with who holds
it, how many times it has been handed out, and how long it has
been idle. The idle time is tracked, not guessed: every xreadgroup
stamps the wall-clock millisecond of the delivery — fixed by the
leader at log time, like an xadd auto ID — on each pending entry
it creates or re-delivers, in the stream blob, so it survives
snapshots and replay like the rest of the group. A re-read raises
the delivery count and re-stamps the idle, so a consumer can tell
a first delivery from a retry and the idle measures since the
latest handover. An entry whose deliveries all came from replays
of pre-v0.71.0 log entries carries no stamp, so its idle is None
rather than a fabricated number. These spin up real nodes (real
TCP, real threads, real disk) on localhost, like the rest of the
suite.
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


def xpending_entries(addr, key="events", group="workers", start="-",
                     end="+", count=None, consumer=None,
                     min_idle_ms=None):
    msg = {"type": "client", "op": "xpending_entries", "key": key,
           "group": group, "start": start, "end": end}
    if count is not None:
        msg["count"] = count
    if consumer is not None:
        msg["consumer"] = consumer
    if min_idle_ms is not None:
        msg["min_idle_ms"] = min_idle_ms
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


class TestXPendingEntriesStore(unittest.TestCase):
    def test_entries_list_holder_idle_and_deliveries(self):
        """XPENDING in per-entry form answers one dict per pending
        entry, in ID order: the entry ID, who holds it, the idle
        milliseconds since it was last delivered — measured
        against the now the caller passes in, so the store itself
        never reads a clock — and how many times it has been
        handed out."""
        store = _seed_store()
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000),
            [{"id": "100-5", "consumer": "c1", "idle": 3000,
              "deliveries": 1},
             {"id": "100-6", "consumer": "c1", "idle": 3000,
              "deliveries": 1}])
        # A re-read raises the delivery count and re-stamps the
        # idle, so the idle measures since the latest handover.
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": "0",
                     "seen_ms": 1700000005000})
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000006000),
            [{"id": "100-5", "consumer": "c1", "idle": 1000,
              "deliveries": 2},
             {"id": "100-6", "consumer": "c1", "idle": 1000,
              "deliveries": 2}])
        # An ack drops the entry from the list, not just the count.
        store.apply({"op": "xack", "key": "s", "group": "g",
                     "ids": ["100-5"]})
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000006000),
            [{"id": "100-6", "consumer": "c1", "idle": 1000,
              "deliveries": 2}])

    def test_range_count_consumer_and_min_idle_filters(self):
        """The range bounds keep only the entries whose IDs fall
        in [start, end], the count caps the list, the consumer
        keeps only the entries one consumer holds, and the
        minimum idle keeps only the entries idle at least that
        long — each filter composes with the others."""
        store = _seed_store()
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">",
                     "seen_ms": 1700000002000})
        now = 1700000003000
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, start="100-6")],
            ["100-6", "101-0"])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, end="100-6")],
            ["100-5", "100-6"])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, start="100-5", end="100-6")],
            ["100-5", "100-6"])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, count=1)],
            ["100-5"])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, consumer="c2")],
            ["101-0"])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, consumer="nobody")],
            [])
        # c1's entries have been idle 3000ms, c2's 1000ms.
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", now, min_idle_ms=2000)],
            ["100-5", "100-6"])
        self.assertEqual(
            store.xpending_entries("s", "g", now, min_idle_ms=4000),
            [])
        # A count of 0 answers an empty list, not an error.
        self.assertEqual(
            store.xpending_entries("s", "g", now, count=0), [])

    def test_unstamped_entry_has_unknown_idle(self):
        """An entry handed out by a delivery this version did not
        stamp — a replayed pre-v0.71.0 log entry carries no
        seen_ms — is still listed, with an idle of None rather
        than a fabricated number, and a minimum idle skips it,
        since it cannot prove it has been idle that long."""
        store = _seed_store()
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">"})
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000003000),
            [{"id": "100-5", "consumer": "c1", "idle": 3000,
              "deliveries": 1},
             {"id": "100-6", "consumer": "c1", "idle": 3000,
              "deliveries": 1},
             {"id": "101-0", "consumer": "c2", "idle": None,
              "deliveries": 1}])
        self.assertEqual(
            [e["id"] for e in store.xpending_entries(
                "s", "g", 1700000003000, min_idle_ms=1)],
            ["100-5", "100-6"])
        # A stamp from a leader whose clock runs ahead never reads
        # as a negative idle: it clamps at 0.
        self.assertEqual(
            store.xpending_entries("s", "g", 1700000000000)[0]["idle"],
            0)

    def test_missing_key_group_wrongtype_and_bad_args(self):
        """A missing key fails, a missing group fails, and a plain
        string value fails as WRONGTYPE — a typo'd key or group
        must not read as an empty list — and a bad bound, count,
        consumer, or minimum idle fails too."""
        store = _seed_store()
        for fn in (lambda: store.xpending_entries("nope", "g", 0),
                   lambda: store.xpending_entries("s", "nope", 0),
                   lambda: store.xpending_entries("s", "g", 0,
                                                  start="bogus"),
                   lambda: store.xpending_entries("s", "g", 0,
                                                  end="bogus"),
                   lambda: store.xpending_entries("s", "g", 0,
                                                  count=-1),
                   lambda: store.xpending_entries("s", "g", 0,
                                                  count="2"),
                   lambda: store.xpending_entries("s", "g", 0,
                                                  consumer=""),
                   lambda: store.xpending_entries("s", "g", 0,
                                                  min_idle_ms=-1)):
            with self.assertRaises(ValueError):
                fn()
        store.apply({"op": "set", "key": "plain", "value": "v"})
        with self.assertRaises(TypeError):
            store.xpending_entries("plain", "g", 0)

    def test_entries_survive_snapshot_load(self):
        """The per-entry delivery stamps ride in the blob, so a
        snapshot load rebuilds the idle times exactly — including
        the re-stamp from a re-read."""
        store = _seed_store()
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": "0",
                     "seen_ms": 1700000005000})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertEqual(
            loaded.xpending_entries("s", "g", 1700000006000),
            [{"id": "100-5", "consumer": "c1", "idle": 1000,
              "deliveries": 2},
             {"id": "100-6", "consumer": "c1", "idle": 1000,
              "deliveries": 2}])


class TestXPendingEntriesCluster(unittest.TestCase):
    def test_full_flow_entries_replicate(self):
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
            # Every node agrees on the pending entries, once the
            # deliveries have replicated to each of them.
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
                    [("100-5", "c1", 1), ("100-6", "c1", 1),
                     ("101-0", "c2", 1)])
                for entry in entries:
                    self.assertIsInstance(entry["idle"], int, entry)
                    self.assertGreaterEqual(entry["idle"], 0, entry)
            # The per-entry list rides the ReadIndex path, so it is
            # linearizable: the idle times are real — non-negative
            # integers that grow as time passes — and the filters
            # work end to end.
            resp = xpending_entries(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            entries = resp.get("entries")
            self.assertEqual(
                [(e["id"], e["consumer"]) for e in entries],
                [("100-5", "c1"), ("100-6", "c1"),
                 ("101-0", "c2")], resp)
            first_idle = {e["id"]: e["idle"] for e in entries}
            time.sleep(0.5)
            resp = xpending_entries(addr)
            second_idle = {e["id"]: e["idle"]
                           for e in resp.get("entries")}
            for entry_id in first_idle:
                self.assertGreater(second_idle[entry_id],
                                   first_idle[entry_id],
                                   (entry_id, first_idle, second_idle))
            resp = xpending_entries(addr, consumer="c2")
            self.assertEqual(
                [e["id"] for e in resp.get("entries")], ["101-0"],
                resp)
            resp = xpending_entries(addr, count=2)
            self.assertEqual(
                [e["id"] for e in resp.get("entries")],
                ["100-5", "100-6"], resp)
            resp = xpending_entries(addr, start="100-6", end="100-6")
            self.assertEqual(
                [e["id"] for e in resp.get("entries")], ["100-6"],
                resp)
            resp = xpending_entries(addr, min_idle_ms=10 ** 9)
            self.assertEqual(resp.get("entries"), [], resp)
            # An ack drops the entry from the list, and the summary
            # form still answers alongside it.
            self.assertEqual(
                client_xack(addr, "events", "workers",
                            ["100-5"]).get("xack"), 1)
            resp = xpending_entries(addr)
            self.assertEqual(
                [e["id"] for e in resp.get("entries")],
                ["100-6", "101-0"], resp)
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
            # A bad group, bound, count, consumer, or minimum idle
            # is a client error with no log entry.
            for msg in (
                    {"type": "client", "op": "xpending_entries",
                     "key": "events", "group": ""},
                    {"type": "client", "op": "xpending_entries",
                     "key": "", "group": "workers"},
                    {"type": "client", "op": "xpending_entries",
                     "key": "events", "group": "workers",
                     "start": "bogus"},
                    {"type": "client", "op": "xpending_entries",
                     "key": "events", "group": "workers",
                     "count": -1},
                    {"type": "client", "op": "xpending_entries",
                     "key": "events", "group": "workers",
                     "consumer": ""},
                    {"type": "client", "op": "xpending_entries",
                     "key": "events", "group": "workers",
                     "min_idle_ms": -1}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before)
            # A missing key or group, and a plain string value, are
            # errors in the payload — also with no log entry, since
            # a read never appends one.
            for msg in (
                    {"type": "client", "op": "xpending_entries",
                     "key": "nope", "group": "workers"},
                    {"type": "client", "op": "xpending_entries",
                     "key": "events", "group": "ghost"},
                    {"type": "client", "op": "xpending_entries",
                     "key": "plain", "group": "workers"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before)
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
                resp = session.xpending_entries("events", "workers")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(len(resp.get("entries")), 1, resp)
                entry = resp["entries"][0]
                self.assertEqual(entry["id"], "100-5", resp)
                self.assertEqual(entry["consumer"], "c1", resp)
                self.assertEqual(entry["deliveries"], 1, resp)
                self.assertIsInstance(entry["idle"], int, resp)
                # The filters ride the session method too.
                resp = session.xpending_entries("events", "workers",
                                                consumer="nobody")
                self.assertEqual(resp.get("entries"), [], resp)
                resp = session.xpending_entries("events", "workers",
                                                count=0)
                self.assertEqual(resp.get("entries"), [], resp)
                # A bad group never goes on the wire, and a missing
                # key or group is an error, not an empty list.
                resp = session.xpending_entries("events", "")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xpending_entries("nope", "workers")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xpending_entries("events", "ghost")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xpending_entries("events", "workers",
                                                count=-1)
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
            resp = client_xpending_entries(follower_addr, "events",
                                           "workers")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(resp.get("entries")), 1, resp)
            entry = resp["entries"][0]
            self.assertEqual(entry["id"], "100-5", resp)
            self.assertEqual(entry["consumer"], "c1", resp)
            self.assertEqual(entry["deliveries"], 1, resp)
            self.assertIsInstance(entry["idle"], int, resp)
            # A bad group never goes on the wire.
            self.assertEqual(
                client_xpending_entries(follower_addr, "events", "")
                .get("status"), "error")
            self.assertEqual(
                client_xpending_entries(follower_addr, "nope",
                                        "workers")
                .get("status"), "error")
        finally:
            cluster.stop_all()

    def test_entries_survive_snapshot_and_restart(self):
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
                    client_request(
                        addr, {"type": "client", "op": "set",
                               "key": f"pad{i}", "value": "x"})
                    .get("status"), "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The entry survived the snapshot + restart with its
            # delivery stamp: the idle is real, not None.
            resp = xpending_entries(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(resp.get("entries")), 1, resp)
            entry = resp["entries"][0]
            self.assertEqual(entry["id"], "100-5", resp)
            self.assertEqual(entry["consumer"], "c1", resp)
            self.assertEqual(entry["deliveries"], 1, resp)
            self.assertIsInstance(entry["idle"], int, resp)
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
            # The summary form still answers with two arguments.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), {
                "pending": 1, "min_id": "100-5", "max_id": "100-5",
                "consumers": [["c1", 1]]})
            # The per-entry form answers with a start bound.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers", "-", "+"])
            self.assertEqual(rc, 0, out.getvalue())
            entries = json.loads(out.getvalue())
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["id"], "100-5")
            self.assertEqual(entries[0]["consumer"], "c1")
            self.assertEqual(entries[0]["deliveries"], 1)
            self.assertIsInstance(entries[0]["idle"], int)
            # The count and consumer filter ride the CLI too.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers", "-", "+", "0"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "events", "workers", "-", "+",
                                  "--consumer", "nobody"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(json.loads(out.getvalue()), [])
            # A missing key errors.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xpending",
                                  "nope", "workers", "-", "+"])
            self.assertEqual(rc, 1, out.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
