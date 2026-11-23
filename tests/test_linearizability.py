"""Tests for the linearizability checker (raftkv/linearizability.py).

Most tests build synthetic histories by hand — the interesting cases
are timing relationships, not the cluster — plus one live test that
records a real cluster's concurrent reads and writes and checks them.
"""

import random
import threading
import time
import unittest

from harness import Cluster, client_request
from raftkv.linearizability import History, Op, record_client_op


def op(client, seq, kind, key, value=None, invoke=0.0, respond=1.0,
       status="ok", returned=None, expected=None):
    return Op(client=client, seq=seq, kind=kind, key=key, value=value,
              invoke=invoke, respond=respond, status=status,
              returned=returned, expected=expected)


def read(client, seq, key, returned, invoke, respond):
    return op(client, seq, "get", key, invoke=invoke, respond=respond,
              returned=returned)


def write(client, seq, key, value, invoke, respond):
    return op(client, seq, "set", key, value=value, invoke=invoke,
              respond=respond)


class TestLinearizabilitySynthetic(unittest.TestCase):
    def test_empty_history_is_ok(self):
        self.assertTrue(History().check().ok)

    def test_sequential_write_then_read_sees_it(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(read("r", 0, "k", "a", 2.0, 3.0))
        result = h.check()
        self.assertTrue(result.ok, result)
        self.assertEqual(result.reads_checked, 1)

    def test_read_before_any_write_must_return_none(self):
        h = History()
        h.record(read("r", 0, "k", None, 0.0, 1.0))
        h.record(write("w", 0, "k", "a", 2.0, 3.0))
        self.assertTrue(h.check().ok)

    def test_read_before_any_write_returning_a_value_is_impossible(self):
        h = History()
        h.record(read("r", 0, "k", "a", 0.0, 1.0))
        h.record(write("w", 0, "k", "a", 2.0, 3.0))
        result = h.check()
        self.assertFalse(result.ok)
        self.assertEqual(len(result.violations), 1)
        self.assertIn("stale read", result.violations[0])

    def test_stale_read_after_overwrite_is_flagged(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(write("w", 1, "k", "b", 2.0, 3.0))
        # Read invoked after "b" completed, but returned "a".
        h.record(read("r", 0, "k", "a", 4.0, 5.0))
        result = h.check()
        self.assertFalse(result.ok)
        self.assertIn("'a'", result.violations[0])
        self.assertIn("'b'", result.violations[0])

    def test_concurrent_read_may_see_old_or_new_value(self):
        # The overwrite overlaps the read: either value linearizes.
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(write("w", 1, "k", "b", 2.0, 4.0))
        h.record(read("r", 0, "k", "a", 3.0, 5.0))
        self.assertTrue(h.check().ok)
        h2 = History()
        h2.record(write("w", 0, "k", "a", 0.0, 1.0))
        h2.record(write("w", 1, "k", "b", 2.0, 4.0))
        h2.record(read("r", 0, "k", "b", 3.0, 5.0))
        self.assertTrue(h2.check().ok)

    def test_read_returning_a_future_value_is_flagged(self):
        # The read finished before "b" was even invoked.
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(read("r", 0, "k", "b", 2.0, 3.0))
        h.record(write("w", 1, "k", "b", 4.0, 5.0))
        self.assertFalse(h.check().ok)

    def test_delete_then_read_returns_none(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(op("w", 1, "del", "k", invoke=2.0, respond=3.0))
        h.record(read("r", 0, "k", None, 4.0, 5.0))
        self.assertTrue(h.check().ok)

    def test_read_after_delete_returning_old_value_is_flagged(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(op("w", 1, "del", "k", invoke=2.0, respond=3.0))
        h.record(read("r", 0, "k", "a", 4.0, 5.0))
        self.assertFalse(h.check().ok)

    def test_successful_cas_is_a_write(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(op("w", 1, "cas", "k", value="b", expected="a",
                   invoke=2.0, respond=3.0))
        h.record(read("r", 0, "k", "b", 4.0, 5.0))
        self.assertTrue(h.check().ok)
        h2 = History()
        h2.record(write("w", 0, "k", "a", 0.0, 1.0))
        h2.record(op("w", 1, "cas", "k", value="b", expected="a",
                    invoke=2.0, respond=3.0))
        h2.record(read("r", 0, "k", "a", 4.0, 5.0))
        self.assertFalse(h2.check().ok)

    def test_failed_cas_counts_as_a_read_of_current_value(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        # Failed CAS reporting current "a" is a consistent read.
        h.record(op("w", 1, "cas", "k", value="b", expected="zzz",
                   invoke=2.0, respond=3.0, status="cas_failed",
                   returned="a"))
        self.assertTrue(h.check().ok)
        # ... but reporting a value the key never held is not.
        h2 = History()
        h2.record(write("w", 0, "k", "a", 0.0, 1.0))
        h2.record(op("w", 1, "cas", "k", value="b", expected="zzz",
                    invoke=2.0, respond=3.0, status="cas_failed",
                    returned="nope"))
        self.assertFalse(h2.check().ok)

    def test_errored_ops_carry_no_observation(self):
        h = History()
        h.record(write("w", 0, "k", "a", 0.0, 1.0))
        h.record(op("r", 0, "get", "k", invoke=2.0, respond=3.0,
                   status="error", returned="a"))
        self.assertTrue(h.check().ok)
        self.assertEqual(h.check().reads_checked, 0)

    def test_keys_are_checked_independently(self):
        h = History()
        h.record(write("w", 0, "k1", "a", 0.0, 1.0))
        h.record(write("w", 0, "k2", "x", 0.0, 1.0))
        h.record(read("r", 0, "k1", "a", 2.0, 3.0))
        h.record(read("r", 1, "k2", "stale", 2.0, 3.0))
        result = h.check()
        self.assertFalse(result.ok)
        self.assertEqual(len(result.violations), 1)
        self.assertIn("k2", result.violations[0])
        self.assertEqual(result.keys_checked, 2)

    def test_multi_writer_key_uses_weak_check(self):
        # Two writers to the same key: a read returning either
        # written value is legal (their order is not fixed), but a
        # value nobody wrote is still impossible.
        h = History()
        h.record(write("w1", 0, "k", "a", 0.0, 1.0))
        h.record(write("w2", 0, "k", "b", 0.5, 1.5))
        h.record(read("r", 0, "k", "a", 2.0, 3.0))
        result = h.check()
        self.assertTrue(result.ok, result)
        self.assertEqual(result.multi_writer_keys, ["k"])
        h2 = History()
        h2.record(write("w1", 0, "k", "a", 0.0, 1.0))
        h2.record(write("w2", 0, "k", "b", 0.5, 1.5))
        h2.record(read("r", 0, "k", "never-written", 2.0, 3.0))
        self.assertFalse(h2.check().ok)

    def test_record_client_op_maps_responses(self):
        h = History()
        record_client_op(h, "c", 0,
                         {"op": "set", "key": "k", "value": "v"},
                         {"status": "ok"}, 1.0, 2.0)
        record_client_op(h, "c", 1,
                         {"op": "get", "key": "k"},
                         {"status": "ok", "value": "v"}, 3.0, 4.0)
        record_client_op(h, "c", 2,
                         {"op": "cas", "key": "k", "expected": "x",
                          "value": "y"},
                         {"status": "cas_failed", "reason": "mismatch",
                          "current": "v"}, 5.0, 6.0)
        # Errored responses are not recorded at all.
        record_client_op(h, "c", 3,
                         {"op": "get", "key": "k"},
                         {"status": "error", "error": "timeout"}, 7.0, 8.0)
        self.assertEqual(len(h), 3)
        self.assertTrue(h.check().ok)


class TestLinearizabilityLive(unittest.TestCase):
    def test_concurrent_reads_and_writes_on_a_live_cluster(self):
        """Two writers and two readers hammering a real 3-node
        cluster: every read must be linearizable — no stale reads,
        no values from the future."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            history = History()
            stop = threading.Event()

            def writer(thread_id: int) -> None:
                for i in range(20):
                    key = f"lin:t{thread_id}"
                    value = f"v{thread_id}-{i}"
                    msg = {"type": "client", "op": "set", "key": key,
                           "value": value,
                           "client_id": f"lin-{thread_id}",
                           "request_id": str(i)}
                    invoke = time.monotonic()
                    resp = client_request(addr, msg, timeout=5.0)
                    record_client_op(history, f"lin-{thread_id}", i,
                                     msg, resp, invoke, time.monotonic())

            def reader(thread_id: int) -> None:
                rng = random.Random(thread_id)
                seq = 0
                while not stop.is_set():
                    key = f"lin:t{rng.randrange(2)}"
                    msg = {"type": "client", "op": "get", "key": key}
                    invoke = time.monotonic()
                    resp = client_request(addr, msg, timeout=5.0)
                    record_client_op(history, f"reader-{thread_id}",
                                     seq, msg, resp, invoke,
                                     time.monotonic())
                    seq += 1
                    time.sleep(0.005)

            writers = [threading.Thread(target=writer, args=(t,))
                       for t in range(2)]
            readers = [threading.Thread(target=reader, args=(t,))
                       for t in range(2)]
            for w in writers:
                w.start()
            for r in readers:
                r.start()
            for w in writers:
                w.join()
            stop.set()
            for r in readers:
                r.join()
            result = history.check()
            self.assertTrue(result.ok, str(result))
            self.assertGreaterEqual(result.reads_checked, 10)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
