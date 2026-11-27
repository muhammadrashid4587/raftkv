"""Tests for raftkv SETBIT / GETBIT (Redis bit operations, v0.46.0)
and BITOP (v0.60.0).

``setbit key offset bit`` sets one bit in the string at the key to 0
or 1 and answers with the old bit value (``old_bit``) and the new
value; ``getbit key offset`` answers the bit at the offset. Bit 0 is
the most significant bit of the first byte, bit 7 the least
significant, bit 8 the most significant bit of the second byte, and
so on. A missing key counts as an empty string, so a setbit creates
it and a getbit on it answers 0; when the offset lies past the end
the gap is padded with zero bytes, and a getbit past the end answers
0. A setbit is a write replicated through the Raft log like any
other, its outcome decided when the entry applies — at its exact
position in the log — so a retry with the same session id returns
the first attempt's old bit instead of setting twice. A getbit is a
read through the same ReadIndex path as a ``get``. A successful
setbit keeps the key's TTL deadline, like a CAS, and fires a ``set``
watch event with the new value. A value that is not a byte string —
one holding a character above U+00FF — fails with ``setbit_failed``
/ ``wrongtype`` instead of writing.

``bitop operation destkey key [key ...]`` combines the string values
at the source keys bitwise — AND, OR, XOR, or NOT (NOT takes exactly
one source key) — and stores the result at the destination, answering
with the result length (``bitop``) and the result value. The result
is as long as the longest input, missing keys count as empty strings,
and when every input is missing or empty the destination is deleted
instead of storing an empty value, answering ``bitop`` 0. A bitop is
a fresh set of its destination, so it clears the destination's TTL
deadline, like a plain set, and fires a ``set`` watch event with the
result — or a ``del`` event when the empty result deletes the
destination. Like a setbit, its outcome is decided at apply time, so
a retry returns the first attempt's length instead of combining
twice, and a non-byte-string source fails with ``bitop_failed`` /
``wrongtype``. These spin up real nodes (real TCP, real threads,
real disk) on localhost, like the rest of the suite.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import bitop as client_bitop
from raftkv.client import getbit as client_getbit
from raftkv.client import setbit as client_setbit
from raftkv.store import KVStore


def setbit(addr, key="k", offset=0, bit=1, client_id=None,
           request_id=None):
    msg = {"type": "client", "op": "setbit", "key": key,
           "offset": offset, "bit": bit}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def getbit(addr, key="k", offset=0):
    return client_request(addr, {"type": "client", "op": "getbit",
                                 "key": key, "offset": offset})


def bitop(addr, operation="AND", dest="dest", keys=("a", "b"),
          client_id=None, request_id=None):
    msg = {"type": "client", "op": "bitop", "key": dest,
           "operation": operation, "keys": list(keys)}
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


class TestBitOpsStore(unittest.TestCase):
    def test_setbit_on_missing_key_sets_msb(self):
        """A setbit at offset 0 on a missing key creates it holding
        one byte with only the most significant bit set — "\\x80" —
        and answers the old bit 0."""
        store = KVStore()
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 0, "bit": 1})
        self.assertEqual(result, {"type": "client_response",
                                  "status": "ok", "old_bit": 0,
                                  "value": "\x80"})
        self.assertEqual(store.get("k"), "\x80")
        self.assertEqual(store.getbit("k", 0), 1)
        self.assertEqual(store.getbit("k", 1), 0)
        self.assertEqual(store.getbit("k", 7), 0)

    def test_setbit_on_missing_key_sets_lsb(self):
        """Offset 7 is the least significant bit of the first byte:
        setting it on a missing key creates "\\x01"."""
        store = KVStore()
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 7, "bit": 1})
        self.assertEqual(result["old_bit"], 0)
        self.assertEqual(result["value"], "\x01")
        self.assertEqual(store.getbit("k", 7), 1)

    def test_setbit_returns_old_bit_and_clears(self):
        """Setting a bit that is already set answers the old bit 1
        and changes nothing; clearing it answers 1 the first time
        and 0 the second."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "a"})
        # "a" is 0x61 = 0110 0001: bits 1, 2, and 7 are set.
        self.assertEqual(store.getbit("k", 0), 0)
        self.assertEqual(store.getbit("k", 1), 1)
        self.assertEqual(store.getbit("k", 7), 1)
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 0, "bit": 1})
        self.assertEqual(result["old_bit"], 0)
        self.assertEqual(result["value"], "\xe1")
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 1, "bit": 1})
        self.assertEqual(result["old_bit"], 1)
        self.assertEqual(result["value"], "\xe1")
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 1, "bit": 0})
        self.assertEqual(result["old_bit"], 1)
        self.assertEqual(result["value"], "\xa1")
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 1, "bit": 0})
        self.assertEqual(result["old_bit"], 0)
        self.assertEqual(result["value"], "\xa1")

    def test_setbit_across_byte_boundary(self):
        """Offset 8 is the most significant bit of the second byte:
        setting it on "a" appends a second byte "\\x80", and
        clearing bit 7 of the first byte leaves the second byte
        alone."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "a"})
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 8, "bit": 1})
        self.assertEqual(result["old_bit"], 0)
        self.assertEqual(result["value"], "a\x80")
        self.assertEqual(store.getbit("k", 8), 1)
        self.assertEqual(store.getbit("k", 15), 0)
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 7, "bit": 0})
        self.assertEqual(result["old_bit"], 1)
        self.assertEqual(result["value"], "`\x80")

    def test_setbit_past_end_pads_with_zero_bytes(self):
        """A setbit past the end pads the gap with zero bytes: on
        "a", offset 15 sets the last bit of the second byte, and
        every bit in between reads 0."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "a"})
        result = store.apply({"op": "setbit", "key": "k",
                              "offset": 15, "bit": 1})
        self.assertEqual(result["old_bit"], 0)
        self.assertEqual(result["value"], "a\x01")
        for offset in range(8, 15):
            self.assertEqual(store.getbit("k", offset), 0, offset)
        self.assertEqual(store.getbit("k", 15), 1)

    def test_getbit_on_missing_key_and_past_end_is_zero(self):
        """A getbit on a missing key is 0 at any offset, and a
        getbit past the end of an existing value is 0 too — never
        an error."""
        store = KVStore()
        self.assertEqual(store.getbit("missing", 0), 0)
        self.assertEqual(store.getbit("missing", 100), 0)
        store.apply({"op": "set", "key": "k", "value": "a"})
        self.assertEqual(store.getbit("k", 8), 0)
        self.assertEqual(store.getbit("k", 1000), 0)

    def test_setbit_keeps_ttl_deadline(self):
        # A setbit is an update, not a fresh set: the deadline
        # survives it, like a CAS or an append.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "a",
                     "ttl_entries": 10}, index=10)
        self.assertEqual(store.expiries(), {"k": 20})
        result = store.apply({"op": "setbit", "key": "k",
                             "offset": 0, "bit": 1}, index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.expiries(), {"k": 20})
        self.assertEqual(store.get("k"), "\xe1")
        self.assertEqual(store.expire_up_to(19), [])
        self.assertEqual(store.expire_up_to(20), ["k"])

    def test_setbit_new_key_joins_sorted_index(self):
        """A setbit that creates a key inserts it in the sorted
        index, so a scan finds it in order."""
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "setbit", "key": "a", "offset": 0,
                     "bit": 1})
        store.apply({"op": "setbit", "key": "c", "offset": 7,
                     "bit": 1})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))

    def test_setbit_wrongtype_leaves_value_untouched(self):
        """A value holding a character above U+00FF has no byte
        form: setbit fails with setbit_failed / wrongtype and
        writes nothing, and getbit on it raises TypeError."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "€"})
        result = store.apply({"op": "setbit", "key": "k",
                             "offset": 0, "bit": 1})
        self.assertEqual(result["status"], "setbit_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("k"), "€")
        with self.assertRaises(TypeError):
            store.getbit("k", 0)

    def test_setbit_bad_offset_and_bit_raise(self):
        """A negative offset or a bit other than 0 or 1 is a
        programming error at apply time — the leader validates
        both before logging, so a hand-built entry with either
        raises instead of setting a nonsense bit."""
        store = KVStore()
        with self.assertRaises(ValueError):
            store.apply({"op": "setbit", "key": "k",
                         "offset": -1, "bit": 1})
        with self.assertRaises(ValueError):
            store.apply({"op": "setbit", "key": "k",
                         "offset": 0, "bit": 2})
        with self.assertRaises(ValueError):
            store.apply({"op": "setbit", "key": "k",
                         "offset": 0, "bit": True})


class TestBitOpsCluster(unittest.TestCase):
    def test_setbit_replicates_to_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = setbit(addr, offset=0, bit=1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("old_bit"), 0, resp)
            self.assertEqual(resp.get("value"), "\x80", resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "\x80"))
                self.assertEqual(
                    cluster.nodes[node_id].sm.getbit("k", 0), 1)
                self.assertEqual(
                    cluster.nodes[node_id].sm.getbit("k", 7), 0)
            resp = setbit(addr, offset=7, bit=1)
            self.assertEqual(resp.get("old_bit"), 0, resp)
            self.assertEqual(resp.get("value"), "\x81", resp)
            self.assertEqual(getbit(addr, offset=7).get("bit"), 1)
            self.assertEqual(getbit(addr, offset=0).get("bit"), 1)
        finally:
            cluster.stop_all()

    def test_setbit_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = client_setbit(cluster.addrs[follower], "k", 0, 1)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("old_bit"), 0, resp)
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "\x80")
            self.assertEqual(
                client_getbit(cluster.addrs[follower], "k", 0)
                .get("bit"), 1)
        finally:
            cluster.stop_all()

    def test_invalid_setbit_and_getbit_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = setbit(addr, offset=-1, bit=1)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("offset", resp.get("error", ""), resp)
            resp = setbit(addr, offset=1_000_001, bit=1)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("offset", resp.get("error", ""), resp)
            resp = setbit(addr, offset=0, bit=2)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("bit", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "setbit", "key": "k",
                       "offset": 0, "bit": "1"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("bit", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "setbit", "key": "k",
                       "offset": 0, "bit": True})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("bit", resp.get("error", ""), resp)
            resp = getbit(addr, offset=-1)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("offset", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "getbit", "key": "k",
                       "offset": "0"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("offset", resp.get("error", ""), resp)
            # None of the rejected writes left a key behind, and
            # none appended a log entry.
            self.assertIsNone(read(addr).get("value"))
            self.assertEqual(len(leader.log), before)
        finally:
            cluster.stop_all()

    def test_setbit_retry_returns_first_old_bit_and_writes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, value="a").get("status"),
                             "ok")
            resp = setbit(addr, offset=0, bit=1,
                          client_id="c1", request_id="r1")
            self.assertEqual(resp.get("old_bit"), 0, resp)
            self.assertEqual(resp.get("value"), "\xe1", resp)
            # A later write moves the value on — but the retry of
            # the first setbit still answers the first attempt's
            # old bit and value, and appends no second entry.
            self.assertEqual(write(addr, value="b").get("status"),
                             "ok")
            resp = setbit(addr, offset=0, bit=0,
                          client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("old_bit"), 0, resp)
            self.assertEqual(resp.get("value"), "\xe1", resp)
            self.assertEqual(read(addr).get("value"), "b")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_setbit_fires_watch_event_with_new_value(self):
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
            self.assertEqual(write(addr, value="a").get("status"),
                             "ok")
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            self.assertEqual(setbit(addr, offset=0, bit=1)
                             .get("value"), "\xe1")
            deadline = time.monotonic() + 5.0
            while len(events) < 4 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 4, events)
            for event in events[2:]:
                self.assertEqual(event["key"], "k", event)
                # The event carries the new value, with the bit set.
                self.assertEqual(event["value"], "\xe1", event)
                self.assertEqual(event["op"], "set", event)
            # A failed setbit — wrongtype on a non-byte-string
            # value — writes nothing, so it fires nothing.
            w_events = []
            leader.watch.watch("w", w_events.append)
            follower.watch.watch("w", w_events.append)
            self.assertEqual(write(addr, key="w", value="€")
                             .get("status"), "ok")
            deadline = time.monotonic() + 5.0
            while len(w_events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(w_events), 2, w_events)
            resp = setbit(addr, key="w", offset=0, bit=1)
            self.assertEqual(resp.get("status"), "setbit_failed", resp)
            time.sleep(0.3)
            self.assertEqual(len(w_events), 2, w_events)
        finally:
            cluster.stop_all()

    def test_setbit_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(setbit(addr, offset=0, bit=1)
                             .get("value"), "\x80")
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The set bit survived the failover, so the bit still
            # answers 1 and a new setbit still flips it.
            self.assertEqual(getbit(addr, offset=0).get("bit"), 1)
            self.assertEqual(setbit(addr, offset=0, bit=0)
                             .get("old_bit"), 1)
            self.assertEqual(getbit(addr, offset=0).get("bit"), 0)
            self.assertEqual(read(addr).get("value"), "\x00")
        finally:
            cluster.stop_all()

    def test_setbit_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(setbit(addr, offset=0, bit=1)
                             .get("value"), "\x80")
            self.assertEqual(setbit(addr, offset=15, bit=1)
                             .get("value"), "\x80\x01")
            # Enough writes to compact the log past the setbits.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(getbit(addr, offset=15).get("bit"), 1)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr).get("value"), "\x80\x01")
            self.assertEqual(getbit(addr, offset=0).get("bit"), 1)
            self.assertEqual(getbit(addr, offset=15).get("bit"), 1)
            self.assertEqual(setbit(addr, offset=8, bit=1)
                             .get("value"), "\x80\x81")
        finally:
            cluster.stop_all()

    def test_session_setbit_getbit_are_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.setbit("k", 0, 1)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("old_bit"), 0, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.getbit("k", 0).get("bit"), 1)
                self.assertEqual(session.getbit("k", 7).get("bit"), 0)
                # A setbit through the same session answers the
                # old bit and still advances the watermark.
                resp = session.setbit("k", 7, 1)
                self.assertEqual(resp.get("old_bit"), 0, resp)
                self.assertEqual(resp.get("value"), "\x81", resp)
                self.assertEqual(session.getbit("k", 7).get("bit"), 1)
                self.assertEqual(session.get("k").get("value"), "\x81")
                # A getbit on a missing key is 0 through the
                # session too, and still advances the read
                # watermark.
                self.assertEqual(session.getbit("missing", 0)
                                 .get("bit"), 0)
                # A failed setbit still committed its entry, so
                # its index is a valid watermark too.
                resp = session.set("w", "€")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.setbit("w", 0, 1)
                self.assertEqual(resp.get("status"), "setbit_failed",
                                 resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("w").get("value"), "€")
        finally:
            cluster.stop_all()

    def test_pipeline_setbit_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "setbit", "key": "a", "offset": 0,
                     "bit": 1},
                    {"op": "setbit", "key": "a", "offset": 7,
                     "bit": 1},
                    {"op": "setbit", "key": "b", "offset": 15,
                     "bit": 1},
                    {"op": "setbit", "key": "c", "offset": -1,
                     "bit": 1},
                    {"op": "setbit", "key": "c", "offset": 0,
                     "bit": 2},
                    {"op": "setbit", "key": "d", "offset": 2,
                     "bit": 0},
                ])
            self.assertEqual(len(replies), 6, replies)
            self.assertEqual(replies[0].get("old_bit"), 0, replies)
            self.assertEqual(replies[0].get("value"), "\x80", replies)
            self.assertEqual(replies[1].get("old_bit"), 0, replies)
            self.assertEqual(replies[1].get("value"), "\x81", replies)
            self.assertEqual(replies[2].get("old_bit"), 0, replies)
            self.assertEqual(replies[2].get("value"), "\x00\x01",
                             replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("status"), "error", replies)
            self.assertEqual(replies[5].get("old_bit"), 0, replies)
            self.assertEqual(replies[5].get("value"), "\x00", replies)
            self.assertEqual(read(addr, key="a").get("value"), "\x81")
            self.assertEqual(read(addr, key="b").get("value"), "\x00\x01")
            self.assertIsNone(read(addr, key="c").get("value"))
            self.assertEqual(read(addr, key="d").get("value"), "\x00")
        finally:
            cluster.stop_all()

    def test_setbit_getbit_module_functions(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(client_setbit(addr, "k", 0, 1)
                             .get("old_bit"), 0)
            self.assertEqual(client_getbit(addr, "k", 0).get("bit"), 1)
            self.assertEqual(client_setbit(addr, "k", 0, 0)
                             .get("old_bit"), 1)
            self.assertEqual(client_getbit(addr, "k", 0).get("bit"), 0)
            self.assertEqual(client_getbit(addr, "missing", 3)
                             .get("bit"), 0)
        finally:
            cluster.stop_all()

    def test_bit_ops_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # Getbit on a missing key prints 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getbit", "k", "0"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # Setbit prints the old bit and creates the key.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setbit", "k",
                                  "0", "1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getbit", "k", "0"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # Setting the same bit again prints the old bit 1.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setbit", "k",
                                  "0", "1"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # Clearing it prints the old bit 1, then getbit is 0.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "setbit", "k",
                                  "0", "0"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "getbit", "k", "0"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "\x00")
            # A bit other than 0 or 1 is a server error, not a
            # write, and a negative offset is too.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "setbit", "k",
                                  "0", "2"])
            self.assertEqual(rc, 1)
            self.assertIn("bit", err.getvalue())
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "setbit", "k",
                                  "--", "-1", "1"])
            self.assertEqual(rc, 1)
            self.assertIn("offset", err.getvalue())
            self.assertEqual(read(cluster.addrs[leader.id])
                             .get("value"), "\x00")
            # A missing bit is a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "setbit", "k", "0"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()


class TestBitopStore(unittest.TestCase):
    def _two_keys(self):
        # "a" is 0x61 = 0110 0001, "A" is 0x41 = 0100 0001.
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "a"})
        store.apply({"op": "set", "key": "b", "value": "A"})
        return store

    def test_bitop_and_or_xor(self):
        """AND, OR, and XOR of "a" (0x61) and "A" (0x41) are "A"
        (0x41), "a" (0x61), and " " (0x20) — bit by bit, with the
        reply carrying the result length and value."""
        store = self._two_keys()
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "AND", "keys": ["a", "b"]})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bitop"], 1)
        self.assertEqual(result["value"], "A")
        self.assertEqual(store.get("dest"), "A")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "OR", "keys": ["a", "b"]})
        self.assertEqual(result["value"], "a")
        self.assertEqual(store.get("dest"), "a")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "XOR", "keys": ["a", "b"]})
        self.assertEqual(result["value"], " ")
        self.assertEqual(store.get("dest"), " ")

    def test_bitop_not(self):
        """NOT of "a" (0x61) is 0x9E — every bit flipped — and NOT
        of a missing key is the empty result, which deletes the
        destination instead of storing an empty value."""
        store = self._two_keys()
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "NOT", "keys": ["a"]})
        self.assertEqual(result["bitop"], 1)
        self.assertEqual(result["value"], "\x9e")
        self.assertEqual(store.get("dest"), "\x9e")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "NOT", "keys": ["missing"]})
        self.assertEqual(result["bitop"], 0)
        self.assertTrue(result["deleted"])
        self.assertIsNone(store.get("dest"))

    def test_bitop_missing_key_counts_as_empty(self):
        """A missing source counts as an all-zero string: AND with
        one is all zeros, OR with one is the other value, and XOR
        with one is the other value too."""
        store = self._two_keys()
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "AND",
                              "keys": ["a", "missing"]})
        self.assertEqual(result["bitop"], 1)
        self.assertEqual(result["value"], "\x00")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "OR",
                              "keys": ["a", "missing"]})
        self.assertEqual(result["value"], "a")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "XOR",
                              "keys": ["missing", "b"]})
        self.assertEqual(result["value"], "A")

    def test_bitop_different_lengths_pad_with_zeros(self):
        """The result is as long as the longest input, and a
        shorter input is padded with zero bytes — so AND keeps
        only the long key's low byte zeroed, OR keeps it whole,
        and XOR keeps it whole too."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "\xff\xff"})
        store.apply({"op": "set", "key": "b", "value": "\x0f"})
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "AND", "keys": ["a", "b"]})
        self.assertEqual(result["bitop"], 2)
        self.assertEqual(result["value"], "\x0f\x00")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "OR", "keys": ["a", "b"]})
        self.assertEqual(result["value"], "\xff\xff")
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "XOR", "keys": ["a", "b"]})
        self.assertEqual(result["value"], "\xf0\xff")

    def test_bitop_all_missing_deletes_destination(self):
        """When every input is missing or empty the result is the
        empty string: the destination is deleted outright — it
        leaves the sorted index and takes its deadline with it —
        and the reply answers ``bitop`` 0 with ``deleted`` set."""
        store = KVStore()
        store.apply({"op": "set", "key": "dest", "value": "old",
                     "ttl_entries": 10}, index=10)
        store.apply({"op": "set", "key": "b", "value": "x"})
        self.assertEqual(store.expiries(), {"dest": 20})
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "AND",
                              "keys": ["missing1", "missing2"]},
                       index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bitop"], 0)
        self.assertTrue(result["deleted"])
        self.assertIsNone(store.get("dest"))
        self.assertEqual(store.keys(), (["b"], False))
        self.assertEqual(store.expiries(), {})

    def test_bitop_clears_dest_ttl(self):
        """A bitop is a fresh set of its destination, not an
        update: the deadline the destination had is cleared, like
        a plain set — unlike a setbit, which keeps it."""
        store = KVStore()
        store.apply({"op": "set", "key": "dest", "value": "old",
                     "ttl_entries": 10}, index=10)
        store.apply({"op": "set", "key": "a", "value": "a"})
        self.assertEqual(store.expiries(), {"dest": 20})
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "OR", "keys": ["a"]},
                       index=11)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.get("dest"), "a")
        self.assertEqual(store.expiries(), {})

    def test_bitop_dest_as_source(self):
        """The destination may also be a source: it is read as a
        source first, then overwritten with the result — AND of a
        key with itself is itself."""
        store = self._two_keys()
        store.apply({"op": "set", "key": "dest", "value": "\x0f"})
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "AND",
                              "keys": ["dest", "a"]})
        self.assertEqual(result["value"], "\x01")
        self.assertEqual(store.get("dest"), "\x01")

    def test_bitop_wrongtype_leaves_dest_untouched(self):
        """A source holding a character above U+00FF has no byte
        form: bitop fails with bitop_failed / wrongtype and writes
        nothing — the destination keeps its old value."""
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": "€"})
        store.apply({"op": "set", "key": "dest", "value": "old"})
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "OR", "keys": ["a", "b"]})
        self.assertEqual(result["status"], "bitop_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("dest"), "old")

    def test_bitop_new_key_joins_sorted_index(self):
        """A bitop that creates its destination inserts it in the
        sorted index, so a scan finds it in order."""
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "set", "key": "a", "value": "\xff"})
        store.apply({"op": "bitop", "key": "c",
                      "operation": "NOT", "keys": ["a"]})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))

    def test_bitop_operation_is_case_insensitive(self):
        """The operation is matched case-insensitively and
        normalized — "and" combines like AND."""
        store = self._two_keys()
        result = store.apply({"op": "bitop", "key": "dest",
                              "operation": "and", "keys": ["a", "b"]})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["value"], "A")

    def test_bitop_bad_operation_and_keys_raise(self):
        """A bad operation, NOT with two keys, an empty key list,
        a key list past the cap, or a non-string key is a
        programming error at apply time — the leader validates
        all of them before logging, so a hand-built entry with
        any of them raises instead of combining nonsense."""
        store = self._two_keys()
        with self.assertRaises(ValueError):
            store.apply({"op": "bitop", "key": "dest",
                         "operation": "NAND", "keys": ["a"]})
        with self.assertRaises(ValueError):
            store.apply({"op": "bitop", "key": "dest",
                         "operation": "NOT", "keys": ["a", "b"]})
        with self.assertRaises(ValueError):
            store.apply({"op": "bitop", "key": "dest",
                         "operation": "AND", "keys": []})
        with self.assertRaises(ValueError):
            store.apply({"op": "bitop", "key": "dest",
                         "operation": "AND",
                         "keys": ["k"] * 1001})
        with self.assertRaises(ValueError):
            store.apply({"op": "bitop", "key": "dest",
                         "operation": "AND", "keys": ["a", 7]})


class TestBitopCluster(unittest.TestCase):
    def test_bitop_replicates_to_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="a", value="a")
                             .get("status"), "ok")
            self.assertEqual(write(addr, key="b", value="A")
                             .get("status"), "ok")
            with Session(addr) as session:
                resp = session.bitop("dest", "AND", ["a", "b"])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("bitop"), 1, resp)
                self.assertEqual(resp.get("value"), "A", resp)
                self.assertGreater(session.last_write_index, 0)
                # Read-your-writes through the same session: the
                # get sees the bitop's result.
                self.assertEqual(session.get("dest").get("value"),
                                 "A")
                resp = session.bitop("dest", "XOR", ["a", "b"])
                self.assertEqual(resp.get("value"), " ", resp)
                self.assertEqual(session.get("dest").get("value"),
                                 " ")
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "dest",
                                 " "))
        finally:
            cluster.stop_all()

    def test_invalid_bitop_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = bitop(addr, operation="NAND", keys=["a"])
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("operation", resp.get("error", ""), resp)
            resp = bitop(addr, operation="NOT", keys=["a", "b"])
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("NOT", resp.get("error", ""), resp)
            resp = bitop(addr, operation="AND", keys=[])
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("keys", resp.get("error", ""), resp)
            resp = bitop(addr, operation="AND",
                         keys=["k"] * 1001)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("keys", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "bitop", "key": "dest",
                       "operation": "AND", "keys": ["a", 7]})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("keys", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "bitop", "key": "dest",
                       "operation": 7, "keys": ["a"]})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("operation", resp.get("error", ""), resp)
            # None of the rejected writes left a key behind, and
            # none appended a log entry.
            self.assertIsNone(read(addr, key="dest").get("value"))
            self.assertEqual(len(leader.log), before)
        finally:
            cluster.stop_all()

    def test_bitop_retry_returns_first_result_and_writes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="a", value="a")
                             .get("status"), "ok")
            self.assertEqual(write(addr, key="b", value="A")
                             .get("status"), "ok")
            resp = bitop(addr, operation="AND", dest="dest",
                         keys=["a", "b"],
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("bitop"), 1, resp)
            self.assertEqual(resp.get("value"), "A", resp)
            # A later write moves a source on — but the retry of
            # the first bitop still answers the first attempt's
            # length and value, and appends no second entry.
            self.assertEqual(write(addr, key="a", value="b")
                             .get("status"), "ok")
            resp = bitop(addr, operation="XOR", dest="dest",
                         keys=["a", "b"],
                         client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("bitop"), 1, resp)
            self.assertEqual(resp.get("value"), "A", resp)
            self.assertEqual(read(addr, key="dest").get("value"),
                             "A")
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_bitop_fires_watch_events(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            self.assertEqual(write(addr, key="a", value="a")
                             .get("status"), "ok")
            self.assertEqual(write(addr, key="b", value="A")
                             .get("status"), "ok")
            events = []
            leader.watch.watch("dest", events.append)
            follower.watch.watch("dest", events.append)
            resp = bitop(addr, operation="AND", dest="dest",
                         keys=["a", "b"])
            self.assertEqual(resp.get("value"), "A", resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "dest", event)
                self.assertEqual(event["value"], "A", event)
                self.assertEqual(event["op"], "set", event)
            # An empty result deletes the destination, so the
            # watch fires a del event instead.
            events.clear()
            resp = bitop(addr, operation="AND", dest="dest",
                         keys=["missing1", "missing2"])
            self.assertEqual(resp.get("bitop"), 0, resp)
            self.assertTrue(resp.get("deleted"), resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "dest", event)
                self.assertIsNone(event["value"], event)
                self.assertEqual(event["op"], "del", event)
            # A failed bitop — wrongtype on a non-byte-string
            # source — writes nothing, so it fires nothing.
            w_events = []
            leader.watch.watch("w", w_events.append)
            follower.watch.watch("w", w_events.append)
            self.assertEqual(write(addr, key="w", value="€")
                             .get("status"), "ok")
            deadline = time.monotonic() + 5.0
            while len(w_events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(w_events), 2, w_events)
            resp = bitop(addr, operation="OR", dest="w",
                         keys=["w", "a"])
            self.assertEqual(resp.get("status"), "bitop_failed", resp)
            time.sleep(0.3)
            self.assertEqual(len(w_events), 2, w_events)
        finally:
            cluster.stop_all()

    def test_bitop_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="a", value="a")
                             .get("status"), "ok")
            self.assertEqual(write(addr, key="b", value="A")
                             .get("status"), "ok")
            self.assertEqual(bitop(addr, operation="XOR", dest="dest",
                                   keys=["a", "b"]).get("value"), " ")
            # Enough writes to compact the log past the bitop.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertEqual(read(addr, key="dest").get("value"), " ")
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr, key="dest").get("value"), " ")
            self.assertEqual(bitop(addr, operation="NOT", dest="dest",
                                   keys=["dest"]).get("value"), "\xdf")
        finally:
            cluster.stop_all()

    def test_pipeline_bitop_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                self.assertEqual(session.set("a", "a")
                                 .get("status"), "ok")
                self.assertEqual(session.set("b", "A")
                                 .get("status"), "ok")
                replies = session.pipeline([
                    {"op": "bitop", "key": "d1",
                     "operation": "AND", "keys": ["a", "b"]},
                    {"op": "bitop", "key": "d2",
                     "operation": "NOT", "keys": ["a"]},
                    {"op": "bitop", "key": "d3",
                     "operation": "NAND", "keys": ["a"]},
                    {"op": "bitop", "key": "d4",
                     "operation": "NOT", "keys": ["a", "b"]},
                    {"op": "bitop", "key": "d5",
                     "operation": "XOR", "keys": ["a", "b"]},
                ])
            self.assertEqual(len(replies), 5, replies)
            self.assertEqual(replies[0].get("bitop"), 1, replies)
            self.assertEqual(replies[0].get("value"), "A", replies)
            self.assertEqual(replies[1].get("bitop"), 1, replies)
            self.assertEqual(replies[1].get("value"), "\x9e", replies)
            self.assertEqual(replies[2].get("status"), "error", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("bitop"), 1, replies)
            self.assertEqual(replies[4].get("value"), " ", replies)
            self.assertEqual(read(addr, key="d1").get("value"), "A")
            self.assertEqual(read(addr, key="d2").get("value"), "\x9e")
            self.assertIsNone(read(addr, key="d3").get("value"))
            self.assertIsNone(read(addr, key="d4").get("value"))
            self.assertEqual(read(addr, key="d5").get("value"), " ")
        finally:
            cluster.stop_all()

    def test_bitop_module_function(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="a", value="a")
                             .get("status"), "ok")
            self.assertEqual(write(addr, key="b", value="A")
                             .get("status"), "ok")
            # Sent to a follower, it follows the redirect to the
            # leader like any other module-level write.
            resp = client_bitop(cluster.addrs[follower], "dest",
                                "AND", ["a", "b"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("bitop"), 1, resp)
            self.assertEqual(resp.get("value"), "A", resp)
            self.assertEqual(read(addr, key="dest").get("value"), "A")
        finally:
            cluster.stop_all()

    def test_bitop_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "a", "a"])
            self.assertEqual(rc, 0)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "b", "A"])
            self.assertEqual(rc, 0)
            # AND prints the result length; the destination holds
            # the combined value.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "bitop", "AND",
                                  "dest", "a", "b"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "get", "dest"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "A")
            # XOR of the same two keys is a space.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "bitop", "XOR",
                                  "dest", "a", "b"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "get", "dest"])
            self.assertEqual(rc, 0)
            # The value is one space: compare un-stripped, since
            # strip() would eat the value itself.
            self.assertEqual(out.getvalue(), " \n")
            # An empty result prints 0 and deletes the
            # destination.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "bitop", "AND",
                                  "dest", "missing1", "missing2"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "get", "dest"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            # A bad operation is a server error, not a write.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "bitop", "NAND",
                                  "dest", "a"])
            self.assertEqual(rc, 1)
            self.assertIn("operation", err.getvalue())
            # NOT with two keys is a server error too.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "bitop", "NOT",
                                  "dest", "a", "b"])
            self.assertEqual(rc, 1)
            self.assertIn("NOT", err.getvalue())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
