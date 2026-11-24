"""Tests for raftkv SETBIT / GETBIT (Redis bit operations, v0.46.0).

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
/ ``wrongtype`` instead of writing. These spin up real nodes (real
TCP, real threads, real disk) on localhost, like the rest of the
suite.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
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


if __name__ == "__main__":
    unittest.main()
