"""Tests for raftkv hashes (hset / hget / hdel / hgetall).

An hset is a write replicated through the Raft log like any
other: {"op": "hset", "key": k, "pairs": [[f, v], ...]} sets fields
in the hash at k and returns the number of fields that did not
exist before. A missing key is created as an empty hash; the
outcome is decided when the entry applies — at its exact position
in the log — so every node computes the same count, a plain string
value fails distinctly with wrongtype, and a retry with the same
session id returns the first attempt's count instead of setting
twice. An hget reads one field back through the same ReadIndex
path as a get: None when the key or the field is missing, a client
error when the key holds a plain string. An hdel deletes fields
the same way — {"op": "hdel", "key": k, "fields": [f, ...]} returns
the number of fields actually removed, 0 for a missing key or a
missing field, and deleting the last field deletes the key — and
an hgetall reads the whole field map back through the ReadIndex
path: an empty dict when the key does not exist, a client error
when the key holds a plain string.
"""

import contextlib
import io
import json
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def hset(addr, key="user:1", pairs=None, client_id=None,
         request_id=None):
    msg = {"type": "client", "op": "hset", "key": key,
           "pairs": pairs if pairs is not None
           else [["name", "mo"]]}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def hget(addr, key="user:1", field="name"):
    return client_request(addr, {"type": "client", "op": "hget",
                                 "key": key, "field": field})


def hdel(addr, key="user:1", fields=None, client_id=None,
         request_id=None):
    msg = {"type": "client", "op": "hdel", "key": key,
           "fields": fields if fields is not None else ["name"]}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def hgetall(addr, key="user:1"):
    return client_request(addr, {"type": "client", "op": "hgetall",
                                 "key": key})


def hlen(addr, key="user:1"):
    return client_request(addr, {"type": "client", "op": "hlen",
                                 "key": key})


def hexists(addr, key="user:1", field="name"):
    return client_request(addr, {"type": "client", "op": "hexists",
                                 "key": key, "field": field})


def hkeys(addr, key="user:1"):
    return client_request(addr, {"type": "client", "op": "hkeys",
                                 "key": key})


def hvals(addr, key="user:1"):
    return client_request(addr, {"type": "client", "op": "hvals",
                                 "key": key})


def hmget(addr, key="user:1", fields=None):
    return client_request(addr, {"type": "client", "op": "hmget",
                                 "key": key,
                                 "fields": fields if fields is not None
                                 else ["name"]})


def write(addr, key="user:1", value="plain"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


def wait_for_field(node, key, field, value, timeout=5.0):
    """Poll a node's local state machine until the hash at key
    holds field=value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.sm.hget(key, field) == value:
            return True
        time.sleep(0.02)
    return False


def wait_for_hgetall(node, key, fields, timeout=5.0):
    """Poll a node's local state machine until the hash at key
    holds exactly ``fields`` — used after an hdel, where the
    surviving fields were already there before the delete, so
    waiting on one field would return before the delete applied."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.sm.hgetall(key) == fields:
            return True
        time.sleep(0.02)
    return False


class TestHashStore(unittest.TestCase):
    def test_hset_missing_key_creates_hash_and_counts_new_fields(self):
        store = KVStore()
        resp = store.apply({"op": "hset", "key": "u",
                            "pairs": [["name", "mo"],
                                      ["city", "phl"]]})
        self.assertEqual(resp["status"], "ok", resp)
        self.assertEqual(resp["hset"], 2, resp)
        self.assertEqual(store.hget("u", "name"), "mo")
        self.assertEqual(store.hget("u", "city"), "phl")

    def test_hset_overwrite_is_not_counted(self):
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"]]})
        resp = store.apply({"op": "hset", "key": "u",
                            "pairs": [["name", "rashid"],
                                      ["age", "22"]]})
        self.assertEqual(resp["hset"], 1, resp)
        self.assertEqual(store.hget("u", "name"), "rashid")
        self.assertEqual(store.hget("u", "age"), "22")

    def test_hset_repeated_field_in_one_call_counts_once(self):
        store = KVStore()
        resp = store.apply({"op": "hset", "key": "u",
                            "pairs": [["name", "mo"],
                                      ["name", "rashid"]]})
        self.assertEqual(resp["hset"], 1, resp)
        self.assertEqual(store.hget("u", "name"), "rashid")

    def test_hget_missing_key_and_missing_field_are_none(self):
        store = KVStore()
        self.assertIsNone(store.hget("nope", "name"))
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"]]})
        self.assertIsNone(store.hget("u", "nope"))

    def test_hset_on_plain_string_fails_wrongtype(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        resp = store.apply({"op": "hset", "key": "u",
                            "pairs": [["name", "mo"]]})
        self.assertEqual(resp["status"], "hset_failed", resp)
        self.assertEqual(resp["reason"], "wrongtype", resp)
        self.assertEqual(resp["current"], "plain", resp)
        self.assertEqual(store.get("u"), "plain")

    def test_hget_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hget("u", "name")

    def test_hset_rejects_malformed_pairs(self):
        store = KVStore()
        for bad in (None, [], [["only-field"]],
                    [["field", 1]], [["field"]], "not-a-list"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.apply({"op": "hset", "key": "u", "pairs": bad})

    def test_hset_keeps_ttl_deadline(self):
        # A successful hset is an update, not a fresh set: the
        # deadline survives it, like an incr or a CAS. The hash
        # gets its deadline from expire, since a plain set would
        # leave a string, not a hash.
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"]]}, index=5)
        self.assertEqual(
            store.apply({"op": "expire", "key": "u",
                         "ttl_entries": 10}, index=5),
            {"type": "client_response", "status": "ok",
             "expire": 1, "deadline": 15})
        self.assertEqual(store._expiry["u"], 15)
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["city", "phl"]]}, index=6)
        self.assertEqual(store.hget("u", "city"), "phl")
        self.assertEqual(store._expiry["u"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["u"])
        self.assertIsNone(store.hget("u", "name"))

    def test_hdel_removes_fields_and_counts(self):
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]})
        resp = store.apply({"op": "hdel", "key": "u",
                            "fields": ["city", "age"]})
        self.assertEqual(resp["status"], "ok", resp)
        self.assertEqual(resp["hdel"], 2, resp)
        self.assertEqual(store.hgetall("u"), {"name": "mo"})
        self.assertIsNone(store.hget("u", "city"))

    def test_hdel_repeated_field_counts_once(self):
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        resp = store.apply({"op": "hdel", "key": "u",
                            "fields": ["name", "name", "city"]})
        self.assertEqual(resp["hdel"], 2, resp)
        self.assertEqual(store.hgetall("u"), {})

    def test_hdel_missing_key_and_missing_field_answer_zero(self):
        store = KVStore()
        resp = store.apply({"op": "hdel", "key": "nope",
                            "fields": ["name"]})
        self.assertEqual(resp, {"type": "client_response",
                                "status": "ok", "hdel": 0}, resp)
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"]]})
        resp = store.apply({"op": "hdel", "key": "u",
                            "fields": ["nope"]})
        self.assertEqual(resp["hdel"], 0, resp)
        self.assertEqual(store.hgetall("u"), {"name": "mo"})

    def test_hdel_last_field_deletes_the_key(self):
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        store.apply({"op": "expire", "key": "u",
                     "ttl_entries": 100}, index=5)
        self.assertEqual(store._expiry["u"], 105)
        resp = store.apply({"op": "hdel", "key": "u",
                            "fields": ["name", "city"]}, index=6)
        self.assertEqual(resp["hdel"], 2, resp)
        self.assertTrue(resp.get("deleted"), resp)
        self.assertIsNone(store.get("u"))
        self.assertEqual(store.keys(), ([], False))
        self.assertNotIn("u", store._expiry)
        # And a later hdel on the gone key is a 0, not an error.
        self.assertEqual(store.apply(
            {"op": "hdel", "key": "u", "fields": ["name"]})["hdel"], 0)

    def test_hdel_on_plain_string_fails_wrongtype(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        resp = store.apply({"op": "hdel", "key": "u",
                            "fields": ["name"]})
        self.assertEqual(resp["status"], "hdel_failed", resp)
        self.assertEqual(resp["reason"], "wrongtype", resp)
        self.assertEqual(resp["current"], "plain", resp)
        self.assertEqual(store.get("u"), "plain")

    def test_hgetall_returns_all_fields(self):
        store = KVStore()
        self.assertEqual(store.hgetall("nope"), {})
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        self.assertEqual(store.hgetall("u"),
                         {"name": "mo", "city": "phl"})

    def test_hgetall_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hgetall("u")

    def test_hlen_counts_fields_and_missing_key_is_zero(self):
        store = KVStore()
        self.assertEqual(store.hlen("nope"), 0)
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        self.assertEqual(store.hlen("u"), 2)
        # An overwrite adds no field, so the count stays put.
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "rashid"]]})
        self.assertEqual(store.hlen("u"), 2)
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["age", "22"]]})
        self.assertEqual(store.hlen("u"), 3)

    def test_hlen_tracks_hdel_down_to_zero(self):
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]})
        self.assertEqual(store.hlen("u"), 3)
        store.apply({"op": "hdel", "key": "u",
                     "fields": ["city"]})
        self.assertEqual(store.hlen("u"), 2)
        # Removing the last fields deletes the key, so the
        # count is 0 again — a missing key, not an empty hash.
        store.apply({"op": "hdel", "key": "u",
                     "fields": ["name", "age"]})
        self.assertEqual(store.hlen("u"), 0)
        self.assertIsNone(store.get("u"))

    def test_hlen_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hlen("u")

    def test_hexists_answers_presence(self):
        store = KVStore()
        self.assertEqual(store.hexists("nope", "name"), 0)
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        self.assertEqual(store.hexists("u", "name"), 1)
        self.assertEqual(store.hexists("u", "city"), 1)
        self.assertEqual(store.hexists("u", "nope"), 0)
        store.apply({"op": "hdel", "key": "u",
                     "fields": ["name"]})
        self.assertEqual(store.hexists("u", "name"), 0)
        self.assertEqual(store.hexists("u", "city"), 1)

    def test_hexists_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hexists("u", "name")

    def test_hlen_hexists_survive_snapshot_load(self):
        # The blob is a plain string in _data, so a snapshot
        # carries it exactly like any other value — and the
        # count and the presence read back off the loaded blob.
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        state = store.items()
        store2 = KVStore()
        store2.load(state)
        self.assertEqual(store2.hlen("u"), 2)
        self.assertEqual(store2.hexists("u", "name"), 1)
        self.assertEqual(store2.hexists("u", "nope"), 0)

    def test_hkeys_lists_field_names_sorted(self):
        store = KVStore()
        self.assertEqual(store.hkeys("nope"), [])
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]})
        # Sorted, not insertion order — the names come back in
        # the order the blob was dumped in.
        self.assertEqual(store.hkeys("u"), ["age", "city", "name"])
        store.apply({"op": "hdel", "key": "u",
                     "fields": ["city"]})
        self.assertEqual(store.hkeys("u"), ["age", "name"])

    def test_hkeys_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hkeys("u")

    def test_hvals_lists_values_in_sorted_field_order(self):
        store = KVStore()
        self.assertEqual(store.hvals("nope"), [])
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]})
        # Aligned with hkeys: age, city, name.
        self.assertEqual(store.hvals("u"), ["22", "phl", "mo"])
        self.assertEqual(store.hkeys("u"), ["age", "city", "name"])
        # The two calls together rebuild the map.
        self.assertEqual(dict(zip(store.hkeys("u"),
                                  store.hvals("u"))),
                         {"name": "mo", "city": "phl", "age": "22"})
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "rashid"]]})
        self.assertEqual(store.hvals("u"), ["22", "phl", "rashid"])

    def test_hvals_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hvals("u")

    def test_hmget_answers_requested_fields_in_order(self):
        store = KVStore()
        self.assertEqual(store.hmget("nope", ["name", "city"]),
                         [None, None])
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]})
        # Requested order, not sorted order — and a missing
        # field is None in its own position, never a shift.
        self.assertEqual(store.hmget("u", ["name", "nope", "age"]),
                         ["mo", None, "22"])
        self.assertEqual(store.hmget("u", ["age", "name"]),
                         ["22", "mo"])
        # A repeated field is answered once per occurrence.
        self.assertEqual(store.hmget("u", ["name", "name"]),
                         ["mo", "mo"])
        store.apply({"op": "hdel", "key": "u",
                     "fields": ["city"]})
        self.assertEqual(store.hmget("u", ["city", "name"]),
                         [None, "mo"])

    def test_hmget_on_plain_string_raises(self):
        store = KVStore()
        store.apply({"op": "set", "key": "u", "value": "plain"})
        with self.assertRaises(TypeError):
            store.hmget("u", ["name"])

    def test_hkeys_hvals_hmget_survive_snapshot_load(self):
        # The blob is a plain string in _data, so a snapshot
        # carries it exactly like any other value — and the
        # names, values, and multi-get read back off the
        # loaded blob.
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        state = store.items()
        store2 = KVStore()
        store2.load(state)
        self.assertEqual(store2.hkeys("u"), ["city", "name"])
        self.assertEqual(store2.hvals("u"), ["phl", "mo"])
        self.assertEqual(store2.hmget("u", ["name", "nope"]),
                         ["mo", None])

    def test_hdel_rejects_malformed_fields(self):
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"]]})
        for bad in (None, [], [1], ["name", 2], "name"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store.apply({"op": "hdel", "key": "u",
                             "fields": bad})
        self.assertEqual(store.hgetall("u"), {"name": "mo"})

    def test_hdel_keeps_ttl_while_fields_remain(self):
        # An hdel that leaves fields behind is an update, not a
        # fresh set: the deadline survives it, like an hset.
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]},
                    index=5)
        self.assertEqual(
            store.apply({"op": "expire", "key": "u",
                         "ttl_entries": 10}, index=5),
            {"type": "client_response", "status": "ok",
             "expire": 1, "deadline": 15})
        resp = store.apply({"op": "hdel", "key": "u",
                            "fields": ["name"]}, index=6)
        self.assertEqual(resp["hdel"], 1, resp)
        self.assertFalse(resp.get("deleted", False), resp)
        self.assertEqual(store.hgetall("u"), {"city": "phl"})
        self.assertEqual(store._expiry["u"], 15)
        # And the key still expires at its deadline.
        self.assertEqual(store.expire_up_to(15), ["u"])
        self.assertEqual(store.hgetall("u"), {})

    def test_hgetall_survives_snapshot_load(self):
        # The blob is a plain string in _data, so a snapshot
        # carries it exactly like any other value — including
        # after an hdel shrank it.
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]})
        store.apply({"op": "hdel", "key": "u", "fields": ["age"]})
        state = store.items()
        store2 = KVStore()
        store2.load(state)
        self.assertEqual(store2.hgetall("u"),
                         {"name": "mo", "city": "phl"})

    def test_hset_new_key_joins_sorted_index(self):
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "hset", "key": "a",
                     "pairs": [["f", "v"]]})
        store.apply({"op": "hset", "key": "c",
                     "pairs": [["f", "v"]]})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))

    def test_hash_survives_snapshot_load(self):
        # The blob is a plain string in _data, so a snapshot
        # carries it exactly like any other value.
        store = KVStore()
        store.apply({"op": "hset", "key": "u",
                     "pairs": [["name", "mo"], ["city", "phl"]]})
        state = store.items()
        store2 = KVStore()
        store2.load(state)
        self.assertEqual(store2.hget("u", "name"), "mo")
        self.assertEqual(store2.hget("u", "city"), "phl")


class TestHashCluster(unittest.TestCase):
    def test_hset_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = hset(addr, pairs=[["name", "mo"],
                                     ["city", "phl"]])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hset"), 2, resp)
            resp = hset(addr, pairs=[["name", "rashid"],
                                     ["age", "22"]])
            self.assertEqual(resp.get("hset"), 1, resp)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_field(node, "user:1", "name", "rashid"))
                self.assertEqual(node.sm.hget("user:1", "age"), "22")
            self.assertEqual(
                hget(addr, field="name").get("value"), "rashid")
            self.assertEqual(
                hget(addr, field="city").get("value"), "phl")
        finally:
            cluster.stop_all()

    def test_hget_missing_key_and_field_are_none(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertIsNone(hget(addr).get("value"))
            self.assertEqual(hset(addr).get("hset"), 1)
            self.assertIsNone(hget(addr, field="nope").get("value"))
        finally:
            cluster.stop_all()

    def test_hset_on_plain_string_fails_and_writes_nothing(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hset(addr)
            self.assertEqual(resp.get("status"), "hset_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            self.assertEqual(resp.get("current"), "plain", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "user:1",
                                 "plain"))
        finally:
            cluster.stop_all()

    def test_hget_on_plain_string_is_client_error(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hget(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_hset_rejects_bad_pairs(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for bad in (None, [], [["only-field"]],
                        [["field", 1]], "not-a-list"):
                resp = client_request(
                    addr, {"type": "client", "op": "hset",
                           "key": "user:1", "pairs": bad})
                self.assertEqual(resp.get("status"), "error",
                                 (bad, resp))
            self.assertIsNone(hget(addr).get("value"))
        finally:
            cluster.stop_all()

    def test_hset_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = hset(cluster.addrs[follower],
                        pairs=[["name", "mo"]])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hset"), 1, resp)
            self.assertEqual(
                hget(cluster.addrs[leader.id]).get("value"), "mo")
        finally:
            cluster.stop_all()

    def test_hset_retry_appends_once_and_returns_same_count(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = hset(addr, pairs=[["name", "mo"],
                                     ["city", "phl"]],
                        client_id="c1", request_id="r1")
            self.assertEqual(resp.get("hset"), 2, resp)
            resp = hset(addr, pairs=[["name", "mo"],
                                     ["city", "phl"]],
                        client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hset"), 2, resp)
            self.assertEqual(
                hget(addr, field="name").get("value"), "mo")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_failed_hset_retry_returns_same_failure(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hset(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "hset_failed", resp)
            resp = hset(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "hset_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            # The failed attempt committed one entry; the retry none.
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertTrue(
                wait_for_key(cluster.nodes[leader.id], "user:1",
                             "plain"))
        finally:
            cluster.stop_all()

    def test_hset_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"]]).get("hset"), 1)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(
                hset(addr, pairs=[["city", "phl"]]).get("hset"), 1)
            self.assertEqual(
                hget(addr, field="name").get("value"), "mo")
            self.assertEqual(
                hget(addr, field="city").get("value"), "phl")
        finally:
            cluster.stop_all()

    def test_hset_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("user:1", events.append)
            follower.watch.watch("user:1", events.append)
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"]]).get("hset"), 1)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "user:1", event)
                self.assertEqual(event["op"], "set", event)
                self.assertEqual(
                    event["value"],
                    leader.sm.get("user:1"), event)
            # A failed hset fires nothing — but the plain set
            # above fires one event per node too, so wait for
            # those first, then clear, then fail.
            self.assertEqual(write(addr).get("status"), "ok")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            events.clear()
            self.assertEqual(hset(addr).get("status"), "hset_failed")
            time.sleep(0.3)
            self.assertEqual(len(events), 0, events)
        finally:
            cluster.stop_all()

    def test_hset_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"]]).get("hset"), 1)
            # Enough writes to compact the log past the hash.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(
                hset(addr, pairs=[["city", "phl"]]).get("hset"), 1)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(
                hget(addr, field="name").get("value"), "mo")
            self.assertEqual(
                hget(addr, field="city").get("value"), "phl")
            self.assertEqual(
                hset(addr, pairs=[["age", "22"]]).get("hset"), 1)
        finally:
            cluster.stop_all()

    def test_session_hset_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.hset("user:1", {"name": "mo"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hset"), 1, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(
                    session.hget("user:1", "name").get("value"), "mo")
        finally:
            cluster.stop_all()

    def test_pipeline_hset_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "hset", "key": "user:1",
                     "pairs": [["name", "mo"], ["city", "phl"]]},
                    {"op": "hset", "key": "user:1",
                     "pairs": [["name", "rashid"]]},
                    {"op": "set", "key": "other", "value": "x"},
                    {"op": "hset", "key": "user:1", "pairs": []},
                    {"op": "hset", "key": "user:1",
                     "pairs": [["age", "22"]]},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("hset"), 2, replies)
            self.assertEqual(replies[1].get("hset"), 0, replies)
            self.assertEqual(replies[2].get("status"), "ok", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("hset"), 1, replies)
            self.assertEqual(
                hget(addr, field="name").get("value"), "rashid")
            self.assertEqual(
                hget(addr, field="age").get("value"), "22")
        finally:
            cluster.stop_all()

    def test_hset_hget_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hset", "user:1",
                                  "name", "mo", "city", "phl"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hget", "user:1",
                                  "name"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "mo")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hget", "user:1",
                                  "nope"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            # A failed hset exits 1 and names the failure.
            self.assertEqual(write(cluster.addrs[leader.id],
                                   value="plain").get("status"), "ok")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hset", "user:1",
                                  "name", "mo"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
        finally:
            cluster.stop_all()


    def test_hdel_succeeds_and_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"], ["city", "phl"],
                                  ["age", "22"]]).get("hset"), 3)
            resp = hdel(addr, fields=["city", "age"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hdel"), 2, resp)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_hgetall(node, "user:1", {"name": "mo"}))
            self.assertEqual(
                hgetall(addr).get("fields"), {"name": "mo"})
            # Removing the last field deletes the key on every node.
            resp = hdel(addr, fields=["name"])
            self.assertEqual(resp.get("hdel"), 1, resp)
            self.assertTrue(resp.get("deleted"), resp)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_hgetall(node, "user:1", {}))
                self.assertIsNone(node.sm.get("user:1"))
            self.assertEqual(hgetall(addr).get("fields"), {})
        finally:
            cluster.stop_all()

    def test_hdel_missing_key_and_field_answer_zero(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(hdel(addr).get("hdel"), 0)
            self.assertEqual(hset(addr).get("hset"), 1)
            self.assertEqual(
                hdel(addr, fields=["nope"]).get("hdel"), 0)
            self.assertEqual(
                hgetall(addr).get("fields"), {"name": "mo"})
        finally:
            cluster.stop_all()

    def test_hdel_on_plain_string_fails_and_writes_nothing(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hdel(addr)
            self.assertEqual(resp.get("status"), "hdel_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            self.assertEqual(resp.get("current"), "plain", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "user:1",
                                 "plain"))
        finally:
            cluster.stop_all()

    def test_hgetall_on_plain_string_is_client_error(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hgetall(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
        finally:
            cluster.stop_all()

    def test_hdel_rejects_bad_fields(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for bad in (None, [], [1], ["name", 2], "name"):
                resp = client_request(
                    addr, {"type": "client", "op": "hdel",
                           "key": "user:1", "fields": bad})
                self.assertEqual(resp.get("status"), "error",
                                 (bad, resp))
            self.assertEqual(hgetall(addr).get("fields"), {})
        finally:
            cluster.stop_all()

    def test_hdel_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"],
                                  ["city", "phl"]]).get("hset"), 2)
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = hdel(cluster.addrs[follower], fields=["city"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hdel"), 1, resp)
            self.assertEqual(
                hgetall(addr).get("fields"), {"name": "mo"})
        finally:
            cluster.stop_all()

    def test_hdel_retry_appends_once_and_returns_same_count(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"], ["city", "phl"],
                                  ["age", "22"]]).get("hset"), 3)
            resp = hdel(addr, fields=["city", "age"],
                        client_id="c1", request_id="r1")
            self.assertEqual(resp.get("hdel"), 2, resp)
            resp = hdel(addr, fields=["city", "age"],
                        client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hdel"), 2, resp)
            self.assertEqual(
                hgetall(addr).get("fields"), {"name": "mo"})
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_failed_hdel_retry_returns_same_failure(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hdel(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "hdel_failed", resp)
            resp = hdel(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "hdel_failed", resp)
            self.assertEqual(resp.get("reason"), "wrongtype", resp)
            # The failed attempt committed one entry; the retry none.
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertTrue(
                wait_for_key(cluster.nodes[leader.id], "user:1",
                             "plain"))
        finally:
            cluster.stop_all()

    def test_hdel_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"],
                                  ["city", "phl"]]).get("hset"), 2)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(
                hdel(addr, fields=["city"]).get("hdel"), 1)
            self.assertEqual(
                hgetall(addr).get("fields"), {"name": "mo"})
        finally:
            cluster.stop_all()

    def test_hdel_fires_watch_events(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("user:1", events.append)
            follower.watch.watch("user:1", events.append)
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"],
                                  ["city", "phl"]]).get("hset"), 2)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            events.clear()
            # Removing one field fires a `set` with the new blob.
            self.assertEqual(
                hdel(addr, fields=["city"]).get("hdel"), 1)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "user:1", event)
                self.assertEqual(event["op"], "set", event)
                self.assertEqual(
                    event["value"],
                    leader.sm.get("user:1"), event)
            events.clear()
            # Removing the last field fires a `del`.
            self.assertEqual(
                hdel(addr, fields=["name"]).get("hdel"), 1)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "user:1", event)
                self.assertEqual(event["op"], "del", event)
                self.assertIsNone(event["value"], event)
            events.clear()
            # Removing nothing fires nothing.
            self.assertEqual(
                hdel(addr, fields=["nope"]).get("hdel"), 0)
            time.sleep(0.3)
            self.assertEqual(len(events), 0, events)
        finally:
            cluster.stop_all()

    def test_hdel_hgetall_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"],
                                  ["city", "phl"]]).get("hset"), 2)
            # Enough writes to compact the log past the hash.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(
                hdel(addr, fields=["city"]).get("hdel"), 1)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(
                hgetall(addr).get("fields"), {"name": "mo"})
            self.assertEqual(
                hdel(addr, fields=["name"]).get("hdel"), 1)
            self.assertEqual(hgetall(addr).get("fields"), {})
        finally:
            cluster.stop_all()

    def test_session_hdel_hgetall_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.hset("user:1", {"name": "mo",
                                               "city": "phl"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hset"), 2, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(
                    session.hgetall("user:1").get("fields"),
                    {"name": "mo", "city": "phl"})
                resp = session.hdel("user:1", "city")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hdel"), 1, resp)
                self.assertEqual(
                    session.hgetall("user:1").get("fields"),
                    {"name": "mo"})
        finally:
            cluster.stop_all()

    def test_pipeline_hdel_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "hset", "key": "user:1",
                     "pairs": [["name", "mo"], ["city", "phl"],
                               ["age", "22"]]},
                    {"op": "hdel", "key": "user:1",
                     "fields": ["city", "nope"]},
                    {"op": "hdel", "key": "user:1", "fields": []},
                    {"op": "hdel", "key": "user:1",
                     "fields": ["name", "age"]},
                    {"op": "hdel", "key": "user:1",
                     "fields": ["name"]},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("hset"), 3, replies)
            self.assertEqual(replies[1].get("hdel"), 1, replies)
            self.assertEqual(replies[2].get("status"), "error", replies)
            self.assertEqual(replies[3].get("hdel"), 2, replies)
            self.assertTrue(replies[3].get("deleted"), replies)
            self.assertEqual(replies[4].get("hdel"), 0, replies)
            self.assertEqual(hgetall(addr).get("fields"), {})
        finally:
            cluster.stop_all()

    def test_hdel_hgetall_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hset", "user:1",
                                  "name", "mo", "city", "phl"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hgetall", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             {"name": "mo", "city": "phl"})
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hdel", "user:1",
                                  "city"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hgetall", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), {"name": "mo"})
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hdel", "user:1",
                                  "name"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hgetall", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), {})
            # A failed hdel exits 1 and names the failure.
            self.assertEqual(write(cluster.addrs[leader.id],
                                   value="plain").get("status"), "ok")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hdel", "user:1",
                                  "name"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
        finally:
            cluster.stop_all()

    def test_hlen_hexists_succeed_and_replicate(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"], ["city", "phl"],
                                  ["age", "22"]]).get("hset"), 3)
            resp = hlen(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("hlen"), 3, resp)
            self.assertEqual(hexists(addr, field="name").get("hexists"),
                             1)
            self.assertEqual(hexists(addr, field="nope").get("hexists"),
                             0)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_hgetall(node, "user:1",
                                     {"name": "mo", "city": "phl",
                                      "age": "22"}))
                self.assertEqual(node.sm.hlen("user:1"), 3)
                self.assertEqual(node.sm.hexists("user:1", "age"), 1)
                self.assertEqual(node.sm.hexists("user:1", "nope"), 0)
            # The count tracks a delete on every node too.
            self.assertEqual(hdel(addr, fields=["age"]).get("hdel"), 1)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_hgetall(node, "user:1",
                                     {"name": "mo", "city": "phl"}))
                self.assertEqual(node.sm.hlen("user:1"), 2)
            self.assertEqual(hlen(addr).get("hlen"), 2)
            self.assertEqual(hexists(addr, field="age").get("hexists"),
                             0)
        finally:
            cluster.stop_all()

    def test_hlen_hexists_missing_key_answer_zero(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(hlen(addr).get("hlen"), 0)
            self.assertEqual(hexists(addr).get("hexists"), 0)
        finally:
            cluster.stop_all()

    def test_hlen_hexists_on_plain_string_are_client_errors(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hlen(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
            resp = hexists(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
            # The string is still there, untouched.
            self.assertEqual(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "user:1"}).get("value"),
                "plain")
        finally:
            cluster.stop_all()

    def test_hexists_rejects_non_string_field(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"]]).get("hset"), 1)
            resp = client_request(
                addr, {"type": "client", "op": "hexists",
                       "key": "user:1", "field": 1})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(hlen(addr).get("hlen"), 1)
        finally:
            cluster.stop_all()

    def test_hlen_hexists_from_follower_redirect(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            self.assertEqual(
                hset(cluster.addrs[leader.id],
                     pairs=[["name", "mo"]]).get("hset"), 1)
            # Reads sent to a follower are served by the leader
            # the client is redirected to, like an hget.
            self.assertEqual(
                hlen(cluster.addrs[follower]).get("hlen"), 1)
            self.assertEqual(
                hexists(cluster.addrs[follower]).get("hexists"), 1)
        finally:
            cluster.stop_all()

    def test_hlen_hexists_continue_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"]]).get("hset"), 1)
            self.assertEqual(hlen(addr).get("hlen"), 1)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(
                hset(addr, pairs=[["city", "phl"]]).get("hset"), 1)
            self.assertEqual(hlen(addr).get("hlen"), 2)
            self.assertEqual(hexists(addr, field="city").get("hexists"),
                             1)
        finally:
            cluster.stop_all()

    def test_hlen_hexists_survive_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"],
                                  ["city", "phl"]]).get("hset"), 2)
            # Enough writes to compact the log past the hash.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(hlen(addr).get("hlen"), 2)
            self.assertEqual(hexists(addr, field="name").get("hexists"),
                             1)
            self.assertEqual(hexists(addr, field="nope").get("hexists"),
                             0)
        finally:
            cluster.stop_all()

    def test_session_hlen_hexists_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.hset("user:1", {"name": "mo",
                                               "city": "phl"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hset"), 2, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(
                    session.hlen("user:1").get("hlen"), 2)
                self.assertEqual(
                    session.hexists("user:1", "name").get("hexists"),
                    1)
                self.assertEqual(
                    session.hexists("user:1", "nope").get("hexists"),
                    0)
                resp = session.hdel("user:1", "city")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hdel"), 1, resp)
                self.assertEqual(
                    session.hlen("user:1").get("hlen"), 1)
                self.assertEqual(
                    session.hexists("user:1", "city").get("hexists"),
                    0)
        finally:
            cluster.stop_all()

    def test_hlen_hexists_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hlen", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hexists", "user:1",
                                  "name"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hset", "user:1",
                                  "name", "mo", "city", "phl"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hlen", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hexists", "user:1",
                                  "name"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hexists", "user:1",
                                  "nope"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hdel", "user:1",
                                  "city"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hlen", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # A failed hlen exits 1 and names the failure.
            self.assertEqual(write(cluster.addrs[leader.id],
                                   value="plain").get("status"), "ok")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hlen", "user:1"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hexists", "user:1",
                                  "name"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
        finally:
            cluster.stop_all()

    def test_hkeys_hvals_hmget_succeed_and_replicate(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"], ["city", "phl"],
                                  ["age", "22"]]).get("hset"), 3)
            resp = hkeys(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("fields"),
                             ["age", "city", "name"], resp)
            resp = hvals(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("values"),
                             ["22", "phl", "mo"], resp)
            resp = hmget(addr, fields=["name", "nope", "age"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("values"),
                             ["mo", None, "22"], resp)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_hgetall(node, "user:1",
                                     {"name": "mo", "city": "phl",
                                      "age": "22"}))
                self.assertEqual(node.sm.hkeys("user:1"),
                                 ["age", "city", "name"])
                self.assertEqual(node.sm.hvals("user:1"),
                                 ["22", "phl", "mo"])
                self.assertEqual(node.sm.hmget(
                    "user:1", ["name", "nope", "age"]),
                    ["mo", None, "22"])
            # The listings track a delete on every node too.
            self.assertEqual(hdel(addr, fields=["age"]).get("hdel"), 1)
            for node_id in cluster.live:
                node = cluster.nodes[node_id]
                self.assertTrue(
                    wait_for_hgetall(node, "user:1",
                                     {"name": "mo", "city": "phl"}))
                self.assertEqual(node.sm.hkeys("user:1"),
                                 ["city", "name"])
            self.assertEqual(hkeys(addr).get("fields"),
                             ["city", "name"])
            self.assertEqual(hvals(addr).get("values"),
                             ["phl", "mo"])
            self.assertEqual(hmget(addr, fields=["age", "name"])
                             .get("values"), [None, "mo"])
        finally:
            cluster.stop_all()

    def test_hkeys_hvals_hmget_missing_key_answer_empty(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(hkeys(addr).get("fields"), [])
            self.assertEqual(hvals(addr).get("values"), [])
            self.assertEqual(hmget(addr, fields=["name", "city"])
                             .get("values"), [None, None])
        finally:
            cluster.stop_all()

    def test_hkeys_hvals_hmget_on_plain_string_are_client_errors(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = hkeys(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
            resp = hvals(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
            resp = hmget(addr)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a hash", resp.get("error", ""), resp)
            # The string is still there, untouched.
            self.assertEqual(client_request(
                addr, {"type": "client", "op": "get",
                       "key": "user:1"}).get("value"), "plain")
        finally:
            cluster.stop_all()

    def test_hmget_malformed_fields_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            node = cluster.nodes[leader.id]
            before = len(node.log)
            for bad in (None, [], ["name", 2], "name",
                        ["f"] * 1001):
                resp = client_request(
                    addr, {"type": "client", "op": "hmget",
                           "key": "user:1", "fields": bad})
                self.assertEqual(resp.get("status"), "error", bad)
            # Nothing was logged for any of them.
            self.assertEqual(len(node.log), before)
        finally:
            cluster.stop_all()

    def test_hkeys_hvals_hmget_continue_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                hset(addr, pairs=[["name", "mo"]]).get("hset"), 1)
            self.assertEqual(hkeys(addr).get("fields"), ["name"])
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(
                hset(addr, pairs=[["city", "phl"]]).get("hset"), 1)
            self.assertEqual(hkeys(addr).get("fields"),
                             ["city", "name"])
            self.assertEqual(hvals(addr).get("values"),
                             ["phl", "mo"])
            self.assertEqual(hmget(addr, fields=["name", "city"])
                             .get("values"), ["mo", "phl"])
        finally:
            cluster.stop_all()

    def test_session_hkeys_hvals_hmget_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.hset("user:1", {"name": "mo",
                                               "city": "phl"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hset"), 2, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(
                    session.hkeys("user:1").get("fields"),
                    ["city", "name"])
                self.assertEqual(
                    session.hvals("user:1").get("values"),
                    ["phl", "mo"])
                self.assertEqual(
                    session.hmget("user:1", ["name", "nope"])
                    .get("values"), ["mo", None])
                resp = session.hdel("user:1", "city")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("hdel"), 1, resp)
                self.assertEqual(
                    session.hkeys("user:1").get("fields"), ["name"])
                self.assertEqual(
                    session.hvals("user:1").get("values"), ["mo"])
                self.assertEqual(
                    session.hmget("user:1", ["city", "name"])
                    .get("values"), [None, "mo"])
        finally:
            cluster.stop_all()

    def test_hkeys_hvals_hmget_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hkeys", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hvals", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hmget", "user:1",
                                  "name", "nope"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()), [None, None])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hset", "user:1",
                                  "name", "mo", "city", "phl"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hkeys", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             ["city", "name"])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hvals", "user:1"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             ["phl", "mo"])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "hmget", "user:1",
                                  "name", "nope", "city"])
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads(out.getvalue()),
                             ["mo", None, "phl"])
            # A failed hkeys exits 1 and names the failure.
            self.assertEqual(write(cluster.addrs[leader.id],
                                   value="plain").get("status"), "ok")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hkeys", "user:1"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hvals", "user:1"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "hmget", "user:1",
                                  "name"])
            self.assertEqual(rc, 1)
            self.assertIn("not a hash", err.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
