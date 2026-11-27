"""Tests for raftkv consumer-group management: XGROUP SETID / DESTROY /
DELCONSUMER (v0.69.0).

``xgroup_setid key group id [ENTRIESREAD n]`` moves a group's
last-delivered ID — "$" (the stream's top), "0" (from the very first
entry), a bare millisecond, or a full "ms-seq", even one past the
stream's top — and optionally sets its entries-read count; the pending
entries are untouched. ``xgroup_destroy key group`` drops the whole
group, pending entries and all, answering 1 when the group existed and
0 when it did not. ``xgroup_delconsumer key group consumer`` drops one
consumer and every entry it held pending, answering how many pending
entries the consumer held, 0 when it held none or did not exist. The
groups ride in the stream blob with the entries, so the management
survives snapshots and replay exactly like the entries — and a
SETID to the ID the group already holds writes nothing at all. These
spin up real nodes (real TCP, real threads, real disk) on localhost,
like the rest of the suite.
"""

import contextlib
import io
import json
import threading
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
from raftkv.client import xpending as client_xpending
from raftkv.client import xreadgroup as client_xreadgroup
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


def xpending(addr, key="events", group="workers"):
    return client_request(addr, {"type": "client", "op": "xpending",
                                 "key": key, "group": group})


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


class EventLog:
    """Thread-safe sink for watch events, with a wait helper."""

    def __init__(self):
        self.events: list[dict] = []
        self._cond = threading.Condition()

    def __call__(self, event):
        with self._cond:
            self.events.append(event)
            self._cond.notify_all()

    def wait_for(self, count: int, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self.events) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return True


def _seed_store():
    """A store with a stream of three entries and a group created
    from "0" that has already been handed the first two."""
    store = KVStore()
    for entry_id in ("100-5", "100-6", "101-0"):
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", entry_id]], "id": entry_id})
    store.apply({"op": "xgroup_create", "key": "s",
                 "group": "g", "start_id": "0"})
    store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                 "consumer": "c1", "id": ">", "count": 2})
    return store


class TestXGroupManageStore(unittest.TestCase):
    def test_setid_moves_last_delivered(self):
        """A SETID to an explicit ID rewinds the group's read
        position: the next ">" read hands out the entries after
        that ID, and a SETID past the stream's top hands out
        nothing until entries pass it."""
        store = _seed_store()
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "100-6")
        result = store.apply({"op": "xgroup_setid", "key": "s",
                              "group": "g", "id": "100-5"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xgroup_setid"], 1, result)
        self.assertEqual(result.get("changed"), 1, result)
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "100-5")
        # The pending entries are untouched by the rewind.
        self.assertEqual(
            set(groups_of(store.get("s"))["g"]["pending"]),
            {"100-5", "100-6"})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c2",
                              "id": ">"})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-6", "101-0"])
        # Past the top: nothing new until an entry passes it.
        result = store.apply({"op": "xgroup_setid", "key": "s",
                              "group": "g", "id": "200-0"})
        self.assertEqual(result["status"], "ok", result)
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c2",
                              "id": ">"})
        self.assertEqual(result["entries"], [])
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", "201-0"]], "id": "201-0"})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c2",
                              "id": ">"})
        self.assertEqual([e[0] for e in result["entries"]], ["201-0"])

    def test_setid_dollar_moves_to_top(self):
        """A SETID to "$" moves the group to the stream's top, so
        a ">" read hands out only the entries appended after it —
        and on an empty stream it leaves last_delivered None, so
        every later entry counts as new."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_setid", "key": "s",
                              "group": "g", "id": "$"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(
            groups_of(store.get("s"))["g"]["last_delivered"], "101-0")
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c2",
                              "id": ">"})
        self.assertEqual(result["entries"], [])
        store.apply({"op": "xgroup_create", "key": "empty",
                     "group": "g", "start_id": "$", "mkstream": True})
        result = store.apply({"op": "xgroup_setid", "key": "empty",
                              "group": "g", "id": "$"})
        self.assertEqual(result["status"], "ok", result)
        self.assertIsNone(
            groups_of(store.get("empty"))["g"]["last_delivered"])

    def test_setid_entries_read(self):
        """ENTRIESREAD rides in the group in the blob: it is stored
        when given, kept when a later SETID does not name it, and
        replaced when a later SETID names a new one — and a SETID
        to the ID the group already holds, with no new
        entries-read, writes nothing at all."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_setid", "key": "s",
                              "group": "g", "id": "100-6",
                              "entries_read": 7})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result.get("changed"), 1, result)
        self.assertEqual(
            groups_of(store.get("s"))["g"]["entries_read"], 7)
        # Same ID, no entries-read: nothing changes, nothing is
        # written — the blob is byte-identical.
        before = store.get("s")
        result = store.apply({"op": "xgroup_setid", "key": "s",
                              "group": "g", "id": "100-6"})
        self.assertEqual(result["status"], "ok", result)
        self.assertNotIn("changed", result, result)
        self.assertEqual(store.get("s"), before)
        # A new ID without entries-read keeps the old count.
        store.apply({"op": "xgroup_setid", "key": "s",
                     "group": "g", "id": "101-0"})
        self.assertEqual(
            groups_of(store.get("s"))["g"]["entries_read"], 7)
        # And a new entries-read replaces it.
        store.apply({"op": "xgroup_setid", "key": "s",
                     "group": "g", "id": "101-0", "entries_read": 3})
        self.assertEqual(
            groups_of(store.get("s"))["g"]["entries_read"], 3)

    def test_setid_errors(self):
        """A SETID on a missing key or group fails as no_group, on
        a plain string as wrongtype, and a bad group, ID, or
        entries-read raises instead of moving anything."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_setid", "key": "nope",
                              "group": "g", "id": "0"})
        self.assertEqual(result["status"], "xgroup_setid_failed",
                         result)
        self.assertEqual(result["reason"], "no_group", result)
        result = store.apply({"op": "xgroup_setid", "key": "s",
                              "group": "nope", "id": "0"})
        self.assertEqual(result["status"], "xgroup_setid_failed",
                         result)
        self.assertEqual(result["reason"], "no_group", result)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        result = store.apply({"op": "xgroup_setid", "key": "plain",
                              "group": "g", "id": "0"})
        self.assertEqual(result["status"], "xgroup_setid_failed",
                         result)
        self.assertEqual(result["reason"], "wrongtype", result)
        for bad in ({"group": ""}, {"group": 3},
                    {"id": "-"}, {"id": "+"}, {"id": "bogus"},
                    {"id": 5}, {"entries_read": -1},
                    {"entries_read": "3"}, {"entries_read": True}):
            cmd = {"op": "xgroup_setid", "key": "s",
                   "group": "g", "id": "0"}
            cmd.update(bad)
            with self.assertRaises(ValueError, msg=bad):
                store.apply(cmd)

    def test_destroy_group(self):
        """Destroying a group removes it whole — pending entries
        and all — answering 1, and a second destroy of the same
        name answers 0; the stream itself, and its entries, are
        untouched, so a group can be created again from scratch."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_destroy", "key": "s",
                              "group": "g"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xgroup_destroy"], 1, result)
        self.assertEqual(groups_of(store.get("s")), {})
        self.assertEqual(store.xlen("s"), 3)
        with self.assertRaises(ValueError):
            store.xpending("s", "g")
        result = store.apply({"op": "xgroup_destroy", "key": "s",
                              "group": "g"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xgroup_destroy"], 0, result)
        result = store.apply({"op": "xgroup_create", "key": "s",
                              "group": "g", "start_id": "0"})
        self.assertEqual(result["status"], "ok", result)
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": ">"})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5", "100-6", "101-0"])

    def test_destroy_errors(self):
        """Destroying a group on a missing key fails as no_group,
        on a plain string as wrongtype, and a bad group raises
        instead of destroying anything."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_destroy", "key": "nope",
                              "group": "g"})
        self.assertEqual(result["status"], "xgroup_destroy_failed",
                         result)
        self.assertEqual(result["reason"], "no_group", result)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        result = store.apply({"op": "xgroup_destroy", "key": "plain",
                              "group": "g"})
        self.assertEqual(result["status"], "xgroup_destroy_failed",
                         result)
        self.assertEqual(result["reason"], "wrongtype", result)
        for bad_group in ("", 3, None):
            with self.assertRaises(ValueError, msg=bad_group):
                store.apply({"op": "xgroup_destroy", "key": "s",
                             "group": bad_group})

    def test_delconsumer_drops_only_its_pending(self):
        """Deleting a consumer drops its pending entries and
        answers how many it held — the other consumer's pending
        entries are untouched — and deleting a consumer that held
        nothing, or never existed, answers 0."""
        store = _seed_store()
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">"})
        self.assertEqual(store.xpending("s", "g")["pending"], 3)
        result = store.apply({"op": "xgroup_delconsumer", "key": "s",
                              "group": "g", "consumer": "c1"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xgroup_delconsumer"], 2, result)
        self.assertEqual(store.xpending("s", "g"),
                         {"pending": 1, "min_id": "101-0",
                          "max_id": "101-0",
                          "consumers": [["c2", 1]]})
        result = store.apply({"op": "xgroup_delconsumer", "key": "s",
                              "group": "g", "consumer": "c1"})
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["xgroup_delconsumer"], 0, result)
        result = store.apply({"op": "xgroup_delconsumer", "key": "s",
                              "group": "g", "consumer": "ghost"})
        self.assertEqual(result["xgroup_delconsumer"], 0, result)
        # The entries themselves are still in the stream.
        self.assertEqual(store.xlen("s"), 3)

    def test_delconsumer_errors(self):
        """Deleting a consumer on a missing key or group fails as
        no_group, on a plain string as wrongtype, and a bad group
        or consumer raises instead of deleting anything."""
        store = _seed_store()
        result = store.apply({"op": "xgroup_delconsumer",
                              "key": "nope", "group": "g",
                              "consumer": "c1"})
        self.assertEqual(result["status"],
                         "xgroup_delconsumer_failed", result)
        self.assertEqual(result["reason"], "no_group", result)
        result = store.apply({"op": "xgroup_delconsumer",
                              "key": "s", "group": "nope",
                              "consumer": "c1"})
        self.assertEqual(result["status"],
                         "xgroup_delconsumer_failed", result)
        self.assertEqual(result["reason"], "no_group", result)
        store.apply({"op": "set", "key": "plain", "value": "v"})
        result = store.apply({"op": "xgroup_delconsumer",
                              "key": "plain", "group": "g",
                              "consumer": "c1"})
        self.assertEqual(result["status"],
                         "xgroup_delconsumer_failed", result)
        self.assertEqual(result["reason"], "wrongtype", result)
        for bad in ({"group": ""}, {"group": 3},
                    {"consumer": ""}, {"consumer": 3}):
            cmd = {"op": "xgroup_delconsumer", "key": "s",
                   "group": "g", "consumer": "c1"}
            cmd.update(bad)
            with self.assertRaises(ValueError, msg=bad):
                store.apply(cmd)

    def test_manage_ops_survive_snapshot_load(self):
        """A SETID with ENTRIESREAD and a DELCONSUMER ride in the
        blob, so a snapshot load rebuilds the moved read position,
        the entries-read count, and the surviving pending list."""
        store = _seed_store()
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">"})
        store.apply({"op": "xgroup_setid", "key": "s", "group": "g",
                     "id": "100-5", "entries_read": 9})
        store.apply({"op": "xgroup_delconsumer", "key": "s",
                     "group": "g", "consumer": "c1"})
        loaded = KVStore()
        loaded.load(store.items())
        group = groups_of(loaded.get("s"))["g"]
        self.assertEqual(group["last_delivered"], "100-5")
        self.assertEqual(group["entries_read"], 9)
        self.assertEqual(loaded.xpending("s", "g"),
                         {"pending": 1, "min_id": "101-0",
                          "max_id": "101-0",
                          "consumers": [["c2", 1]]})
        result = loaded.apply({"op": "xreadgroup", "key": "s",
                               "group": "g", "consumer": "c2",
                               "id": ">"})
        # The rewound position hands out the entries after 100-5
        # again — the SETID rewind survived the load.
        self.assertEqual(result["entries"],
                         [["100-6", {"a": "100-6"}],
                          ["101-0", {"a": "101-0"}]])


class TestXGroupManageCluster(unittest.TestCase):
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
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["100-5", "100-6"])
            resp = xreadgroup(addr, consumer="c2")
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["101-0"])
            # Rewind the group: the next ">" read re-hands the
            # entries after the new position.
            resp = xgroup_setid(addr, id="100-5", entries_read=2)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xgroup_setid"), 1, resp)
            resp = xreadgroup(addr, consumer="c2")
            self.assertEqual([e[0] for e in resp.get("entries")],
                             ["100-6", "101-0"])
            # Drop one consumer: its pending entries go with it,
            # the other's stay. The re-read above moved 100-6 from
            # c1 to c2 — a ">" read after a SETID rewind re-hands
            # entries past the new position, like Redis — so c1
            # holds only 100-5 now.
            resp = xgroup_delconsumer(addr, consumer="c1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xgroup_delconsumer"), 1, resp)
            resp = xpending(addr)
            self.assertEqual(resp.get("pending"), 2, resp)
            self.assertEqual(resp.get("consumers"), [["c2", 2]], resp)
            # Destroy the group: it is gone everywhere, and the
            # stream itself is untouched.
            resp = xgroup_destroy(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("xgroup_destroy"), 1, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "events",
                                 cluster.nodes[leader.id].sm.get(
                                     "events")))
                self.assertEqual(
                    groups_of(cluster.nodes[node_id].sm.get("events")),
                    {})
                self.assertEqual(
                    cluster.nodes[node_id].sm.xlen("events"), 3)
        finally:
            cluster.stop_all()

    def test_manage_retry_returns_first_result(self):
        """A retried SETID / DESTROY / DELCONSUMER returns the
        first attempt's result and appends no second log entry —
        so a retry after a successful destroy answers 0, the
        group already gone, instead of destroying it twice."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            xadd(addr, id="100-5")
            xadd(addr, id="100-6")
            self.assertEqual(
                xgroup_create(addr, start_id="0").get("status"), "ok")
            xreadgroup(addr, consumer="c1")
            first = xgroup_setid(addr, id="100-5", client_id="c1",
                                 request_id="s1")
            self.assertEqual(first.get("status"), "ok", first)
            second = xgroup_setid(addr, id="0", client_id="c1",
                                  request_id="s1")
            self.assertEqual(second.get("xgroup_setid"), 1, second)
            self.assertEqual(count_entries(leader, "s1"), 1)
            # The retry did not rewind twice: the position is
            # still the first attempt's.
            self.assertEqual(
                groups_of(leader.sm.get("events"))["workers"]
                ["last_delivered"], "100-5")
            first = xgroup_delconsumer(addr, consumer="c1",
                                       client_id="c1",
                                       request_id="d1")
            self.assertEqual(first.get("xgroup_delconsumer"), 2, first)
            second = xgroup_delconsumer(addr, consumer="c1",
                                        client_id="c1",
                                        request_id="d1")
            self.assertEqual(second.get("xgroup_delconsumer"), 2,
                             second)
            self.assertEqual(count_entries(leader, "d1"), 1)
            first = xgroup_destroy(addr, client_id="c1",
                                   request_id="x1")
            self.assertEqual(first.get("xgroup_destroy"), 1, first)
            second = xgroup_destroy(addr, client_id="c1",
                                    request_id="x1")
            self.assertEqual(second.get("xgroup_destroy"), 1, second)
            self.assertEqual(count_entries(leader, "x1"), 1)
            # A fresh destroy now answers 0: the group is gone.
            third = xgroup_destroy(addr)
            self.assertEqual(third.get("xgroup_destroy"), 0, third)
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
            # Wrongtype management ops are apply-time failures,
            # not client errors — they commit an entry.
            resp = xgroup_setid(addr, key="plain", id="0")
            self.assertEqual(resp.get("status"),
                             "xgroup_setid_failed", resp)
            resp = xgroup_destroy(addr, key="plain")
            self.assertEqual(resp.get("status"),
                             "xgroup_destroy_failed", resp)
            resp = xgroup_delconsumer(addr, key="plain")
            self.assertEqual(resp.get("status"),
                             "xgroup_delconsumer_failed", resp)
            # Bad arguments are client errors with no log entry.
            for msg in (
                    {"type": "client", "op": "xgroup_setid",
                     "key": "events", "group": "", "id": "0"},
                    {"type": "client", "op": "xgroup_setid",
                     "key": "events", "group": "g", "id": "-"},
                    {"type": "client", "op": "xgroup_setid",
                     "key": "events", "group": "g", "id": "bogus"},
                    {"type": "client", "op": "xgroup_setid",
                     "key": "events", "group": "g", "id": "0",
                     "entries_read": -1},
                    {"type": "client", "op": "xgroup_setid",
                     "key": "events", "group": "g", "id": "0",
                     "entries_read": "3"},
                    {"type": "client", "op": "xgroup_destroy",
                     "key": "events", "group": ""},
                    {"type": "client", "op": "xgroup_delconsumer",
                     "key": "events", "group": "g", "consumer": ""},
                    {"type": "client", "op": "xgroup_delconsumer",
                     "key": "events", "group": "", "consumer": "c1"}):
                resp = client_request(addr, msg)
                self.assertEqual(resp.get("status"), "error", (msg,
                                                                resp))
            self.assertEqual(len(leader.log), before + 3)
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
                resp = session.xgroup_setid("events", "workers", "0")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("xgroup_setid"), 1, resp)
                resp = session.xgroup_delconsumer("events", "workers",
                                                  "c1")
                self.assertEqual(resp.get("xgroup_delconsumer"), 1,
                                 resp)
                resp = session.xgroup_destroy("events", "workers")
                self.assertEqual(resp.get("xgroup_destroy"), 1, resp)
                # A bad group never goes on the wire.
                resp = session.xgroup_setid("events", "", "0")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xgroup_destroy("events", "")
                self.assertEqual(resp.get("status"), "error", resp)
                resp = session.xgroup_delconsumer("events", "workers",
                                                  "")
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
            self.assertEqual(
                client_xgroup_setid(follower_addr, "events",
                                    "workers", "0").get("status"),
                "ok")
            self.assertEqual(
                client_xgroup_delconsumer(follower_addr, "events",
                                          "workers",
                                          "c1").get(
                                              "xgroup_delconsumer"),
                1)
            self.assertEqual(
                client_xgroup_destroy(follower_addr, "events",
                                      "workers").get(
                                          "xgroup_destroy"), 1)
            # A bad group never goes on the wire.
            self.assertEqual(
                client_xgroup_setid(follower_addr, "events", "",
                                    "0").get("status"), "error")
            self.assertEqual(
                client_xgroup_destroy(follower_addr, "events", "")
                .get("status"), "error")
            self.assertEqual(
                client_xgroup_delconsumer(follower_addr, "events",
                                          "workers", "")
                .get("status"), "error")
        finally:
            cluster.stop_all()

    def test_pipeline_manage_ops_in_position(self):
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
                    session.xadd("events", {"a": "2"},
                                 id="100-6").get("status"), "ok")
                self.assertEqual(
                    session.xgroup_create("events", "workers",
                                          "0").get("status"), "ok")
                self.assertEqual(
                    session.xreadgroup("events", "workers", "c1")
                    .get("entries"),
                    [["100-5", {"a": "1"}],
                     ["100-6", {"a": "2"}]])
                replies = session.pipeline([
                    {"op": "xgroup_setid", "key": "events",
                     "group": "workers", "id": "100-5",
                     "entries_read": 2},
                    {"op": "xgroup_delconsumer", "key": "events",
                     "group": "workers", "consumer": "c1"},
                    {"op": "xgroup_destroy", "key": "events",
                     "group": "workers"},
                    {"op": "xgroup_destroy", "key": "events",
                     "group": "workers"},
                ])
                self.assertEqual(len(replies), 4, replies)
                self.assertEqual(replies[0].get("xgroup_setid"), 1,
                                 replies[0])
                self.assertEqual(
                    replies[1].get("xgroup_delconsumer"), 2,
                    replies[1])
                self.assertEqual(replies[2].get("xgroup_destroy"), 1,
                                 replies[2])
                self.assertEqual(replies[3].get("xgroup_destroy"), 0,
                                 replies[3])
                # A malformed manage op in a pipeline is an error
                # in position, and the ops around it still run.
                replies = session.pipeline([
                    {"op": "xgroup_create", "key": "events",
                     "group": "workers", "start_id": "0"},
                    {"op": "xgroup_setid", "key": "events",
                     "group": "", "id": "0"},
                    {"op": "xgroup_destroy", "key": "events",
                     "group": "workers"},
                ])
                self.assertEqual(len(replies), 3, replies)
                self.assertEqual(replies[0].get("status"), "ok",
                                 replies[0])
                self.assertEqual(replies[1].get("status"), "error",
                                 replies[1])
                self.assertEqual(replies[2].get("xgroup_destroy"), 1,
                                 replies[2])
        finally:
            cluster.stop_all()

    def test_manage_ops_survive_snapshot_and_restart(self):
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
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The group survived the snapshot + restart with its
            # rewound position and entries-read count: the entry
            # is still pending for c1.
            resp = xpending(addr)
            self.assertEqual(resp.get("pending"), 1, resp)
            self.assertEqual(resp.get("consumers"), [["c1", 1]], resp)
            group = groups_of(new_leader.sm.get("events"))["workers"]
            self.assertEqual(group["last_delivered"], "0-0")
            self.assertEqual(group["entries_read"], 4)
            resp = xgroup_delconsumer(addr, consumer="c1")
            self.assertEqual(resp.get("xgroup_delconsumer"), 1, resp)
            resp = xpending(addr)
            self.assertEqual(resp.get("pending"), 0, resp)
        finally:
            cluster.stop_all()

    def test_watch_events_for_manage_ops(self):
        """A SETID that moves the read position, a destroy, and a
        delete that drops pending entries each fire one `set`
        watch event with the new blob — a no-op SETID and a delete
        of a consumer holding nothing fire nothing."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            log = EventLog()
            leader.watch.watch("events", log)
            xadd(addr, id="100-5")
            self.assertTrue(log.wait_for(1))
            xgroup_create(addr, start_id="0")
            self.assertTrue(log.wait_for(2))
            xreadgroup(addr, consumer="c1")
            self.assertTrue(log.wait_for(3))
            # A SETID to the same ID fires nothing.
            self.assertEqual(
                xgroup_setid(addr, id="100-5").get("status"), "ok")
            time.sleep(0.3)
            self.assertEqual(len(log.events), 3)
            # A SETID that moves fires once.
            self.assertEqual(
                xgroup_setid(addr, id="0").get("status"), "ok")
            self.assertTrue(log.wait_for(4))
            self.assertEqual(log.events[-1]["op"], "set")
            # Deleting a consumer holding nothing fires nothing.
            self.assertEqual(
                xgroup_delconsumer(addr, consumer="ghost")
                .get("xgroup_delconsumer"), 0)
            time.sleep(0.3)
            self.assertEqual(len(log.events), 4)
            # Deleting the consumer holding the entry fires once.
            self.assertEqual(
                xgroup_delconsumer(addr, consumer="c1")
                .get("xgroup_delconsumer"), 1)
            self.assertTrue(log.wait_for(5))
            # Destroying the group fires once.
            self.assertEqual(
                xgroup_destroy(addr).get("xgroup_destroy"), 1)
            self.assertTrue(log.wait_for(6))
            self.assertEqual(log.events[-1]["op"], "set")
            # A destroy of a group that is not there fires nothing.
            self.assertEqual(
                xgroup_destroy(addr).get("xgroup_destroy"), 0)
            time.sleep(0.3)
            self.assertEqual(len(log.events), 6)
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
                rc = client_main(["--node", node, "xgroup_setid",
                                  "events", "workers", "0",
                                  "--entries-read", "1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "OK")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_delconsumer",
                                  "events", "workers", "c1"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_destroy",
                                  "events", "workers"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "xgroup_destroy",
                                  "events", "workers"])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertEqual(out.getvalue().strip(), "0")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
