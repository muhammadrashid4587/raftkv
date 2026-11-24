"""Tests for raftkv append / strlen / getdel / getrange / setrange
(Redis string ops).

``append key value`` appends to the string at the key — a missing key
is created holding just the value — and answers with the new length;
``strlen key`` answers the length of the string value, 0 for a missing
key; ``getdel key`` atomically answers with the value the key held and
deletes it, answering None on a missing key; ``getrange key start
end`` answers the substring from start to end inclusive, negative
indexes counting back from the end and out-of-range indexes clamped;
``setrange key offset value`` overwrites the string at the key from
offset, padding with zero bytes when the offset lies past the end,
and answers with the new length. Append, getdel, and setrange are
writes replicated through the Raft log like any other, their outcome
decided when the entry applies — at its exact position in the log — so
a retry with the same session id returns the first attempt's outcome
instead of applying twice. Strlen and getrange are reads through the
same ReadIndex path as a ``get``. A successful append or setrange
keeps the key's TTL deadline, like a CAS; a getdel takes the deadline
with it, since the key is gone.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import append as client_append
from raftkv.client import getdel as client_getdel
from raftkv.client import getrange as client_getrange
from raftkv.client import setrange as client_setrange
from raftkv.client import strlen as client_strlen
from raftkv.store import KVStore


def append(addr, key="k", value="ab", client_id=None, request_id=None):
    msg = {"type": "client", "op": "append", "key": key, "value": value}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def strlen(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "strlen",
                                 "key": key})


def getdel(addr, key="k", client_id=None, request_id=None):
    msg = {"type": "client", "op": "getdel", "key": key}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def getrange(addr, key="k", start=0, end=-1):
    return client_request(addr, {"type": "client", "op": "getrange",
                                 "key": key, "start": start, "end": end})


def setrange(addr, key="k", offset=0, value="x",
             client_id=None, request_id=None):
    msg = {"type": "client", "op": "setrange", "key": key,
           "offset": offset, "value": value}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def write(addr, key="k", value="old"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestStringOpsStore(unittest.TestCase):
    def test_append_on_missing_key_creates_and_returns_length(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "append", "key": "k", "value": "ab"}),
            {"type": "client_response", "status": "ok",
             "value": "ab", "length": 2})
        self.assertEqual(store.get("k"), "ab")

    def test_append_concatenates_and_returns_new_length(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "ab"})
        self.assertEqual(
            store.apply({"op": "append", "key": "k", "value": "cd"}),
            {"type": "client_response", "status": "ok",
             "value": "abcd", "length": 4})
        self.assertEqual(store.get("k"), "abcd")

    def test_append_to_empty_string(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": ""})
        resp = store.apply({"op": "append", "key": "k", "value": "x"})
        self.assertEqual(resp["length"], 1)
        self.assertEqual(resp["value"], "x")
        self.assertEqual(store.get("k"), "x")

    def test_append_keeps_ttl_deadline(self):
        # An append is an update, not a fresh set: the deadline
        # survives it, like a CAS or an incr.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "ab",
                     "ttl_entries": 10}, index=5)
        self.assertEqual(store._expiry["k"], 15)
        self.assertEqual(
            store.apply({"op": "append", "key": "k", "value": "cd"},
                        index=6)["length"], 4)
        self.assertEqual(store.get("k"), "abcd")
        self.assertEqual(store._expiry["k"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["k"])
        self.assertIsNone(store.get("k"))

    def test_append_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "append", "key": "a", "value": "y"})
        store.apply({"op": "append", "key": "b", "value": "z"})
        store.apply({"op": "append", "key": "c", "value": "w"})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))
        self.assertEqual(store.get("b"), "xz")

    def test_append_wrongtype_fails_and_writes_nothing(self):
        # Every value this store holds is a string, so a non-string
        # value can only exist in a hand-built state — but the op
        # must still refuse it with WRONGTYPE instead of appending
        # to something that has no length.
        store = KVStore()
        store._data["k"] = 123
        resp = store.apply({"op": "append", "key": "k", "value": "x"})
        self.assertEqual(resp["status"], "append_failed", resp)
        self.assertEqual(resp["reason"], "wrongtype", resp)
        self.assertEqual(store._data["k"], 123)

    def test_getdel_on_missing_key_is_noop(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "getdel", "key": "k"}),
            {"type": "client_response", "status": "ok",
             "value": None, "getdel": 0})
        self.assertEqual(store.keys(), ([], False))

    def test_getdel_returns_old_value_and_deletes(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v"})
        self.assertEqual(
            store.apply({"op": "getdel", "key": "k"}),
            {"type": "client_response", "status": "ok",
             "value": "v", "getdel": 1})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.keys(), ([], False))

    def test_getdel_after_delete_returns_none(self):
        # A key that was deleted holds nothing, so a later getdel
        # answers None and deletes nothing — the value is live
        # state, not history.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        store.apply({"op": "del", "key": "k"})
        resp = store.apply({"op": "getdel", "key": "k"})
        self.assertIsNone(resp["value"])
        self.assertEqual(resp["getdel"], 0)

    def test_getdel_drops_ttl_deadline_with_the_key(self):
        # The key is gone, so there is nothing left to expire: the
        # deadline goes with it instead of lingering for a key a
        # later set recreates.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 10}, index=5)
        self.assertEqual(store._expiry["k"], 15)
        store.apply({"op": "getdel", "key": "k"}, index=6)
        self.assertNotIn("k", store._expiry)
        self.assertEqual(store.expire_up_to(15), [])

    def test_strlen_missing_empty_and_present(self):
        store = KVStore()
        self.assertEqual(store.strlen("missing"), 0)
        store.apply({"op": "set", "key": "k", "value": "abcd"})
        self.assertEqual(store.strlen("k"), 4)
        store.apply({"op": "set", "key": "e", "value": ""})
        self.assertEqual(store.strlen("e"), 0)

    def test_strlen_wrongtype_raises(self):
        # Like append, a non-string value can only exist in a
        # hand-built state — but the read must still refuse it
        # instead of answering a length for something that has
        # none.
        store = KVStore()
        store._data["k"] = 123
        with self.assertRaises(TypeError):
            store.strlen("k")

    def test_getrange_whole_and_partial(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "hello"})
        self.assertEqual(store.getrange("k", 0, -1), "hello")
        self.assertEqual(store.getrange("k", 1, 3), "ell")
        self.assertEqual(store.getrange("k", 0, 0), "h")
        self.assertEqual(store.getrange("k", -1, -1), "o")

    def test_getrange_negative_indexes_count_from_end(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "hello"})
        self.assertEqual(store.getrange("k", -3, -1), "llo")
        self.assertEqual(store.getrange("k", -5, -4), "he")
        # A negative start past the beginning clamps to 0.
        self.assertEqual(store.getrange("k", -100, 1), "he")

    def test_getrange_clamps_out_of_range(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "hello"})
        self.assertEqual(store.getrange("k", 0, 100), "hello")
        self.assertEqual(store.getrange("k", 2, 100), "llo")
        # A range past the end, or with start past end, is empty.
        self.assertEqual(store.getrange("k", 10, 20), "")
        self.assertEqual(store.getrange("k", 3, 1), "")
        # A negative end past the beginning clamps to 0, so the
        # range holds just the first character.
        self.assertEqual(store.getrange("k", 0, -100), "h")

    def test_getrange_missing_key_and_empty_value(self):
        store = KVStore()
        self.assertEqual(store.getrange("missing", 0, -1), "")
        self.assertEqual(store.getrange("missing", 2, 5), "")
        store.apply({"op": "set", "key": "e", "value": ""})
        self.assertEqual(store.getrange("e", 0, -1), "")
        self.assertEqual(store.getrange("e", 0, 0), "")

    def test_getrange_wrongtype_raises(self):
        # Like strlen, a non-string value can only exist in a
        # hand-built state — but the read must still refuse it
        # instead of answering a substring of something that has
        # none.
        store = KVStore()
        store._data["k"] = 123
        with self.assertRaises(TypeError):
            store.getrange("k", 0, -1)

    def test_setrange_on_missing_key_creates(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "setrange", "key": "k", "offset": 0,
                         "value": "ab"}),
            {"type": "client_response", "status": "ok",
             "value": "ab", "length": 2})
        self.assertEqual(store.get("k"), "ab")

    def test_setrange_overwrites_middle_and_end(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "hello"})
        resp = store.apply({"op": "setrange", "key": "k", "offset": 1,
                            "value": "EL"})
        self.assertEqual(resp["value"], "hELlo")
        self.assertEqual(resp["length"], 5)
        # A value longer than the tail extends the string.
        resp = store.apply({"op": "setrange", "key": "k", "offset": 3,
                            "value": "LONG"})
        self.assertEqual(resp["value"], "hELLONG")
        self.assertEqual(resp["length"], 7)
        self.assertEqual(store.get("k"), "hELLONG")

    def test_setrange_past_end_pads_with_zero_bytes(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "ab"})
        resp = store.apply({"op": "setrange", "key": "k", "offset": 5,
                            "value": "x"})
        self.assertEqual(resp["value"], "ab\x00\x00\x00x")
        self.assertEqual(resp["length"], 6)
        # And on a missing key the pad starts at the offset.
        resp = store.apply({"op": "setrange", "key": "m", "offset": 2,
                            "value": "y"})
        self.assertEqual(resp["value"], "\x00\x00y")
        self.assertEqual(resp["length"], 3)

    def test_setrange_keeps_ttl_deadline(self):
        # A setrange is an update, not a fresh set: the deadline
        # survives it, like a CAS or an append.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "hello",
                     "ttl_entries": 10}, index=5)
        self.assertEqual(store._expiry["k"], 15)
        self.assertEqual(
            store.apply({"op": "setrange", "key": "k", "offset": 1,
                         "value": "EL"}, index=6)["length"], 5)
        self.assertEqual(store.get("k"), "hELlo")
        self.assertEqual(store._expiry["k"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["k"])
        self.assertIsNone(store.get("k"))

    def test_setrange_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "setrange", "key": "a", "offset": 0,
                     "value": "y"})
        store.apply({"op": "setrange", "key": "b", "offset": 1,
                     "value": "z"})
        store.apply({"op": "setrange", "key": "c", "offset": 3,
                     "value": "w"})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))
        self.assertEqual(store.get("b"), "xz")
        self.assertEqual(store.get("c"), "\x00\x00\x00w")

    def test_setrange_wrongtype_fails_and_writes_nothing(self):
        # Every value this store holds is a string, so a non-string
        # value can only exist in a hand-built state — but the op
        # must still refuse it with WRONGTYPE instead of
        # overwriting something that has no characters.
        store = KVStore()
        store._data["k"] = 123
        resp = store.apply({"op": "setrange", "key": "k", "offset": 0,
                            "value": "x"})
        self.assertEqual(resp["status"], "setrange_failed", resp)
        self.assertEqual(resp["reason"], "wrongtype", resp)
        self.assertEqual(store._data["k"], 123)

    def test_setrange_negative_offset_raises(self):
        # The leader validates the offset before logging, so a
        # negative one can only arrive in a hand-built entry —
        # but the store must still refuse it instead of slicing
        # from the end.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "hello"})
        with self.assertRaises(ValueError):
            store.apply({"op": "setrange", "key": "k", "offset": -1,
                         "value": "x"})
        self.assertEqual(store.get("k"), "hello")


class TestStringOpsCluster(unittest.TestCase):
    def test_append_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = append(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 2, resp)
            self.assertEqual(resp.get("value"), "ab", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "ab"))
            resp = append(addr, value="cd")
            self.assertEqual(resp.get("length"), 4, resp)
            self.assertEqual(resp.get("value"), "abcd", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "abcd"))
        finally:
            cluster.stop_all()

    def test_append_requires_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(
                addr, {"type": "client", "op": "append", "key": "k"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("value", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_append_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = append(cluster.addrs[follower])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 2, resp)
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "ab")
        finally:
            cluster.stop_all()

    def test_append_retry_appends_once_and_returns_first_length(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = append(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("length"), 2, resp)
            # A second append with a different id sees the first
            # one's value appended to.
            resp = append(addr, value="cd")
            self.assertEqual(resp.get("length"), 4, resp)
            self.assertEqual(resp.get("value"), "abcd", resp)
            # But the retry of the first attempt still answers the
            # first attempt's length, not the current one, and
            # appends no second entry.
            resp = append(addr, value="XX",
                          client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 2, resp)
            self.assertEqual(resp.get("value"), "ab", resp)
            self.assertEqual(read(addr).get("value"), "abcd")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_strlen_reads(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(strlen(addr).get("length"), 0)
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            resp = strlen(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 5, resp)
            self.assertEqual(append(addr, value="!").get("length"), 6)
            self.assertEqual(strlen(addr).get("length"), 6)
        finally:
            cluster.stop_all()

    def test_strlen_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = strlen(cluster.addrs[follower])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 5, resp)
        finally:
            cluster.stop_all()

    def test_strlen_module_function(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            self.assertEqual(client_strlen(addr, "k").get("length"), 5)
            self.assertEqual(client_append(addr, "k", "!").get("length"),
                             6)
            self.assertEqual(client_strlen(addr, "k").get("length"), 6)
        finally:
            cluster.stop_all()

    def test_getrange_reads(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            # A missing key holds the empty string.
            self.assertEqual(getrange(addr).get("value"), "")
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            self.assertEqual(getrange(addr).get("value"), "hello")
            self.assertEqual(getrange(addr, start=1, end=3).get("value"),
                             "ell")
            self.assertEqual(getrange(addr, start=-3, end=-1)
                             .get("value"), "llo")
            self.assertEqual(getrange(addr, start=0, end=100)
                             .get("value"), "hello")
            self.assertEqual(getrange(addr, start=3, end=1).get("value"),
                             "")
            # The reply names the read index it was measured at,
            # like any ReadIndex read.
            resp = getrange(addr, start=0, end=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNotNone(resp.get("read_index"), resp)
        finally:
            cluster.stop_all()

    def test_getrange_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = getrange(cluster.addrs[follower], start=1, end=3)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "ell", resp)
        finally:
            cluster.stop_all()

    def test_getrange_rejects_non_integer_bounds(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(
                addr, {"type": "client", "op": "getrange", "key": "k",
                       "start": "0", "end": -1})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("start", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "getrange", "key": "k",
                       "start": 0})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("end", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_setrange_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = setrange(addr, offset=0, value="hello")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("length"), 5, resp)
            self.assertEqual(resp.get("value"), "hello", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "hello"))
            resp = setrange(addr, offset=1, value="EL")
            self.assertEqual(resp.get("value"), "hELlo", resp)
            self.assertEqual(resp.get("length"), 5, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "hELlo"))
            # Past the end pads with zero bytes, on every node.
            resp = setrange(addr, offset=7, value="!")
            self.assertEqual(resp.get("value"), "hELlo\x00\x00!", resp)
            self.assertEqual(resp.get("length"), 8, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k",
                                 "hELlo\x00\x00!"))
        finally:
            cluster.stop_all()

    def test_setrange_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = setrange(cluster.addrs[follower], offset=1, value="EL")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "hELlo", resp)
            self.assertEqual(read(addr).get("value"), "hELlo")
        finally:
            cluster.stop_all()

    def test_setrange_requires_value_and_valid_offset(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(
                addr, {"type": "client", "op": "setrange", "key": "k",
                       "offset": 0})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("value", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "setrange", "key": "k",
                       "offset": -1, "value": "x"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("offset", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "setrange", "key": "k",
                       "offset": 1_000_001, "value": "x"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("offset", resp.get("error", ""), resp)
            # None of the rejected writes left a key behind.
            self.assertIsNone(read(addr).get("value"))
        finally:
            cluster.stop_all()

    def test_setrange_retry_returns_first_value_and_writes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            resp = setrange(addr, offset=1, value="EL",
                            client_id="c1", request_id="r1")
            self.assertEqual(resp.get("value"), "hELlo", resp)
            self.assertEqual(resp.get("length"), 5, resp)
            # A later write moves the value on — but the retry of
            # the first setrange still answers the first attempt's
            # value and length, and appends no second entry.
            self.assertEqual(write(addr, value="world").get("status"),
                             "ok")
            resp = setrange(addr, offset=0, value="XX",
                            client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "hELlo", resp)
            self.assertEqual(resp.get("length"), 5, resp)
            self.assertEqual(read(addr).get("value"), "world")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_setrange_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("k", events.append)
            follower.watch.watch("k", events.append)
            self.assertEqual(write(addr, value="hello").get("status"),
                             "ok")
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            self.assertEqual(setrange(addr, offset=1, value="EL")
                             .get("value"), "hELlo")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            for event in events[2:]:
                self.assertEqual(event["key"], "k", event)
                # The event carries the new (overwritten) value.
                self.assertEqual(event["value"], "hELlo", event)
                self.assertEqual(event["op"], "set", event)
        finally:
            cluster.stop_all()

    def test_getrange_setrange_continue_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(setrange(addr, offset=0, value="hello")
                             .get("length"), 5)
            self.assertEqual(getrange(addr, start=1, end=3)
                             .get("value"), "ell")
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The setranged value survived the failover, so the
            # substring still answers "ell" and a new setrange
            # still overwrites it.
            self.assertEqual(getrange(addr, start=1, end=3)
                             .get("value"), "ell")
            self.assertEqual(setrange(addr, offset=0, value="j")
                             .get("value"), "jello")
            self.assertEqual(getrange(addr).get("value"), "jello")
        finally:
            cluster.stop_all()

    def test_getrange_setrange_survive_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(setrange(addr, offset=0, value="hello")
                             .get("length"), 5)
            # Enough writes to compact the log past the setrange.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(getrange(addr, start=1, end=3)
                             .get("value"), "ell")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(getrange(addr).get("value"), "hello")
            self.assertEqual(setrange(addr, offset=0, value="j")
                             .get("value"), "jello")
            self.assertEqual(getrange(addr, start=0, end=0)
                             .get("value"), "j")
        finally:
            cluster.stop_all()

    def test_session_getrange_setrange_are_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.setrange("k", 0, "hello")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("length"), 5, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.getrange("k", 1, 3)
                                 .get("value"), "ell")
                self.assertEqual(session.getrange("k", -2, -1)
                                 .get("value"), "lo")
                # A setrange through the same session answers the
                # new value and still advances the watermark.
                resp = session.setrange("k", 1, "EL")
                self.assertEqual(resp.get("value"), "hELlo", resp)
                self.assertEqual(session.getrange("k", 0, -1)
                                 .get("value"), "hELlo")
                # A getrange on a missing key is "" through the
                # session too, and still advances the read
                # watermark.
                self.assertEqual(session.getrange("missing", 0, -1)
                                 .get("value"), "")
        finally:
            cluster.stop_all()

    def test_pipeline_setrange_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "setrange", "key": "a", "offset": 0,
                     "value": "hello"},
                    {"op": "setrange", "key": "a", "offset": 1,
                     "value": "EL"},
                    {"op": "setrange", "key": "b", "offset": 3,
                     "value": "x"},
                    {"op": "setrange", "key": "c", "offset": -1,
                     "value": "x"},
                    {"op": "setrange", "key": "c", "offset": 0},
                    {"op": "setrange", "key": "d", "offset": 2,
                     "value": "y"},
                ])
            self.assertEqual(len(replies), 6, replies)
            self.assertEqual(replies[0].get("value"), "hello", replies)
            self.assertEqual(replies[0].get("length"), 5, replies)
            self.assertEqual(replies[1].get("value"), "hELlo", replies)
            self.assertEqual(replies[2].get("value"), "\x00\x00\x00x",
                             replies)
            self.assertEqual(replies[2].get("length"), 4, replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("status"), "error", replies)
            self.assertEqual(replies[5].get("value"), "\x00\x00y",
                             replies)
            self.assertEqual(read(addr, key="a").get("value"), "hELlo")
            self.assertEqual(read(addr, key="b").get("value"),
                             "\x00\x00\x00x")
            self.assertIsNone(read(addr, key="c").get("value"))
            self.assertEqual(read(addr, key="d").get("value"), "\x00\x00y")
        finally:
            cluster.stop_all()

    def test_getrange_setrange_module_functions(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(client_setrange(addr, "k", 0, "hello")
                             .get("length"), 5)
            self.assertEqual(client_getrange(addr, "k", 1, 3)
                             .get("value"), "ell")
            self.assertEqual(client_setrange(addr, "k", 0, "j")
                             .get("value"), "jello")
            self.assertEqual(client_getrange(addr, "k", 0, -1)
                             .get("value"), "jello")
        finally:
            cluster.stop_all()

    def test_getdel_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="v").get("status"), "ok")
            resp = getdel(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v", resp)
            self.assertEqual(resp.get("getdel"), 1, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", None))
            # A second getdel finds nothing.
            resp = getdel(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("value"), resp)
            self.assertEqual(resp.get("getdel"), 0, resp)
        finally:
            cluster.stop_all()

    def test_getdel_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="v").get("status"), "ok")
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = getdel(cluster.addrs[follower])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v", resp)
            self.assertIsNone(read(addr).get("value"))
        finally:
            cluster.stop_all()

    def test_getdel_retry_returns_first_value_and_deletes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="v").get("status"), "ok")
            resp = getdel(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("value"), "v", resp)
            self.assertEqual(resp.get("getdel"), 1, resp)
            # A later write recreates the key — but the retry of the
            # first getdel still answers the first attempt's value
            # and must not delete the recreated key.
            self.assertEqual(write(addr, value="new").get("status"),
                             "ok")
            resp = getdel(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v", resp)
            self.assertEqual(read(addr).get("value"), "new")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_string_ops_continue_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(append(addr).get("length"), 2)
            self.assertEqual(strlen(addr).get("length"), 2)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The appended value survived the failover, so the
            # length still answers 2 and the getdel still finds it.
            self.assertEqual(strlen(addr).get("length"), 2)
            self.assertEqual(getdel(addr).get("value"), "ab")
            self.assertEqual(strlen(addr).get("length"), 0)
        finally:
            cluster.stop_all()

    def test_append_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("k", events.append)
            follower.watch.watch("k", events.append)
            self.assertEqual(append(addr).get("length"), 2)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "k", event)
                # The event carries the new (appended) value.
                self.assertEqual(event["value"], "ab", event)
                self.assertEqual(event["op"], "set", event)
            # A second append fires again, with the new value.
            self.assertEqual(append(addr, value="cd").get("length"), 4)
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            for event in events[2:]:
                self.assertEqual(event["value"], "abcd", event)
        finally:
            cluster.stop_all()

    def test_getdel_fires_watch_event_and_miss_fires_nothing(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("k", events.append)
            follower.watch.watch("k", events.append)
            # A getdel on a missing key deletes nothing, so it
            # fires nothing.
            self.assertEqual(getdel(addr).get("getdel"), 0)
            time.sleep(0.3)
            self.assertEqual(events, [])
            self.assertEqual(write(addr, value="v").get("status"), "ok")
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            self.assertEqual(getdel(addr).get("value"), "v")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            for event in events[2:]:
                self.assertEqual(event["key"], "k", event)
                self.assertIsNone(event["value"], event)
                self.assertEqual(event["op"], "del", event)
        finally:
            cluster.stop_all()

    def test_string_ops_survive_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(append(addr).get("length"), 2)
            # Enough writes to compact the log past the append.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(strlen(addr).get("length"), 2)
            self.assertEqual(getdel(addr).get("value"), "ab")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(strlen(addr).get("length"), 0)
            self.assertEqual(append(addr, value="zz").get("length"), 2)
            self.assertEqual(read(addr).get("value"), "zz")
        finally:
            cluster.stop_all()

    def test_session_string_ops_are_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.append("k", "ab")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("length"), 2, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.strlen("k").get("length"), 2)
                self.assertEqual(session.get("k").get("value"), "ab")
                # A second append through the same session answers
                # the new length and still advances the watermark.
                resp = session.append("k", "cd")
                self.assertEqual(resp.get("length"), 4, resp)
                self.assertEqual(session.strlen("k").get("length"), 4)
                resp = session.getdel("k")
                self.assertEqual(resp.get("value"), "abcd", resp)
                self.assertEqual(resp.get("getdel"), 1, resp)
                self.assertEqual(session.strlen("k").get("length"), 0)
                self.assertIsNone(session.get("k").get("value"))
        finally:
            cluster.stop_all()

    def test_pipeline_string_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "append", "key": "a", "value": "1"},
                    {"op": "append", "key": "a", "value": "2"},
                    {"op": "set", "key": "b", "value": "x"},
                    {"op": "append", "key": "b"},
                    {"op": "getdel", "key": "b"},
                    {"op": "getdel", "key": "b"},
                    {"op": "append", "key": "c", "value": "3"},
                ])
            self.assertEqual(len(replies), 7, replies)
            self.assertEqual(replies[0].get("length"), 1, replies)
            self.assertEqual(replies[1].get("length"), 2, replies)
            self.assertEqual(replies[2].get("status"), "ok", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("value"), "x", replies)
            self.assertEqual(replies[4].get("getdel"), 1, replies)
            self.assertEqual(replies[5].get("getdel"), 0, replies)
            self.assertIsNone(replies[5].get("value"), replies)
            self.assertEqual(replies[6].get("length"), 1, replies)
            self.assertEqual(read(addr, key="a").get("value"), "12")
            self.assertIsNone(read(addr, key="b").get("value"))
            self.assertEqual(read(addr, key="c").get("value"), "3")
        finally:
            cluster.stop_all()

    def test_string_ops_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # Strlen on a missing key prints 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "strlen", "k"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # Append prints the new length and creates the key.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "append", "k", "ab"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "append", "k", "cd"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "4")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "strlen", "k"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "4")
            # Getdel prints the value, then (nil) once it is gone.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getdel", "k"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "abcd")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getdel", "k"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            self.assertIsNone(read(cluster.addrs[leader.id])
                              .get("value"))
            # Getrange prints the substring, empty on a missing key.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getrange", "k",
                                  "0", "-1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "")
            # Setrange prints the new length.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setrange", "k",
                                  "0", "hello"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "5")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getrange", "k",
                                  "1", "3"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "ell")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setrange", "k",
                                  "1", "EL"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "5")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getrange", "k",
                                  "-2", "-1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "lo")
            # A negative offset is a CLI error, not a write.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "setrange", "k",
                                  "--", "-1", "x"])
            self.assertEqual(rc, 1)
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "hELlo")
            # A missing value is a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "append", "k"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
