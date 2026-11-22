"""Tests for raftkv ReadIndex reads (Raft dissertation §6.4).

A ReadIndex read records the leader's commit index at read time,
confirms leadership with a heartbeat round to a majority, and only
returns the value once the state machine has applied up to that index —
so the read is linearizable without writing a log entry.

These tests mix real clusters (to prove the behaviour end to end) with
direct calls on live nodes (to isolate the pending-read registry and the
deposed-leader case), the same mix test_prevote.py uses.
"""

import threading
import unittest

from harness import Cluster, client_request, wait_for_key


class TestReadIndexReads(unittest.TestCase):
    def test_readindex_returns_latest_committed_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v"})
            self.assertEqual(resp.get("status"), "ok", resp)
            # Reads must not append log entries: that is the point of
            # ReadIndex — a read is not a write.
            log_before = leader._last_log_index()
            for _ in range(5):
                resp = client_request(addr, {"type": "client", "op": "get",
                                             "key": "k"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), "v")
            self.assertEqual(leader._last_log_index(), log_before)
            missing = client_request(addr, {"type": "client", "op": "get",
                                            "key": "nope"})
            self.assertEqual(missing.get("status"), "ok", missing)
            self.assertIsNone(missing.get("value"))
        finally:
            cluster.stop_all()

    def test_read_after_write_sees_the_write(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for value in ("v1", "v2", "v3"):
                resp = client_request(addr, {"type": "client", "op": "set",
                                             "key": "k", "value": value})
                self.assertEqual(resp.get("status"), "ok", resp)
                # A read issued after the write committed must see it:
                # the read index is the commit index, and the state
                # machine has applied up to it by the time we read.
                resp = client_request(addr, {"type": "client", "op": "get",
                                             "key": "k"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), value)
            resp = client_request(addr, {"type": "client", "op": "del",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("value"))
        finally:
            cluster.stop_all()

    def test_read_across_leader_change_never_returns_stale_data(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v1"})
            self.assertEqual(resp.get("status"), "ok", resp)

            # Kill the leader; the survivors elect a new one.
            old_leader_id = leader.id
            cluster.stop_node(old_leader_id)
            new_leader = cluster.wait_for_leader(timeout=10.0)
            self.assertNotEqual(new_leader.id, old_leader_id)

            # A write under the new leader, then a read: it must see
            # v2, never the pre-change value v1.
            new_addr = cluster.addrs[new_leader.id]
            resp = client_request(new_addr, {"type": "client", "op": "set",
                                             "key": "k", "value": "v2"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_request(new_addr, {"type": "client", "op": "get",
                                             "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")

            # The old leader rejoins as a follower; a read sent to it
            # redirects to the new leader and still returns v2.
            restarted = cluster.restart_node(old_leader_id)
            self.assertTrue(wait_for_key(restarted, "k", "v2"))
            resp = client_request(cluster.addrs[old_leader_id],
                                  {"type": "client", "op": "get", "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_deposed_leader_does_not_serve_stale_reads(self):
        """A read aimed at a deposed (but still alive) ex-leader must
        redirect or error — never return a value from its old state.

        The old leader is stopped (server down) but its node object is
        alive and still thinks it is leader in the old term. Calling
        its read path directly exercises exactly the case a partitioned
        ex-leader would hit: its heartbeat round is rejected by peers
        in the newer term, so it steps down and redirects.
        """
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v1"})
            self.assertEqual(resp.get("status"), "ok", resp)

            old_leader_id = leader.id
            old_leader = cluster.nodes[old_leader_id]
            cluster.stop_node(old_leader_id)
            new_leader = cluster.wait_for_leader(timeout=10.0)
            self.assertNotEqual(new_leader.id, old_leader_id)
            # The new leader overwrites the key; the ex-leader's state
            # still holds v1.
            resp = client_request(cluster.addrs[new_leader.id],
                                  {"type": "client", "op": "set",
                                   "key": "k", "value": "v2"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(old_leader.sm.get("k"), "v1")

            resp = old_leader._serve_read("k")
            self.assertEqual(resp.get("status"), "not_leader", resp)
            self.assertNotIn("value", resp)
            # The failed heartbeat round stepped it down to the newer
            # term it learned from the peers.
            self.assertEqual(old_leader.role, "follower")
            self.assertGreaterEqual(old_leader.current_term,
                                    new_leader.current_term)
        finally:
            cluster.stop_all()

    def test_concurrent_reads_are_all_correct(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v1"})
            self.assertEqual(resp.get("status"), "ok", resp)

            seen: list[str | None] = []
            seen_lock = threading.Lock()
            stop_reading = threading.Event()

            def reader() -> None:
                while not stop_reading.is_set():
                    resp = client_request(addr, {"type": "client",
                                                 "op": "get", "key": "k"})
                    self.assertEqual(resp.get("status"), "ok", resp)
                    with seen_lock:
                        seen.append(resp.get("value"))

            readers = [threading.Thread(target=reader, daemon=True)
                       for _ in range(8)]
            for t in readers:
                t.start()
            # A concurrent write flips v1 -> v2 while readers are in
            # flight. Every read must return one of the two committed
            # values — never None, never a torn or stale value.
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v2"})
            self.assertEqual(resp.get("status"), "ok", resp)
            stop_reading.set()
            for t in readers:
                t.join(timeout=15.0)
            self.assertTrue(seen, "no concurrent reads completed")
            self.assertTrue(set(seen) <= {"v1", "v2"},
                            f"unexpected read values: {set(seen)}")
            # And a read after the write completed sees v2.
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_read_served_with_one_follower_down(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            victim = next(n for n in cluster.nodes.values() if n is not leader)
            cluster.stop_node(victim.id)
            # A majority of two (leader + one follower) still acks the
            # heartbeat round, so reads keep working.
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v")
        finally:
            cluster.stop_all()


class TestPendingReadRegistry(unittest.TestCase):
    def test_pending_read_resolves_when_apply_catches_up(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            with leader.lock:
                applied = leader.last_applied
                # A read for an already-applied index resolves at once.
                event = leader._register_pending_read(applied,
                                                      leader.current_term)
                leader._resolve_pending_reads()
                self.assertTrue(event.is_set())
                self.assertEqual(leader._pending_reads, [])
                # A read ahead of the applied prefix stays pending.
                ahead = leader._register_pending_read(
                    applied + 50, leader.current_term)
                leader._resolve_pending_reads()
                self.assertFalse(ahead.is_set())
                self.assertEqual(len(leader._pending_reads), 1)
                # Losing leadership fails it instead of leaving it
                # waiting: the waiter wakes and redirects.
                leader._fail_pending_reads()
                self.assertTrue(ahead.is_set())
                self.assertEqual(leader._pending_reads, [])
        finally:
            cluster.stop_all()

    def test_step_down_fails_pending_reads(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            with leader.lock:
                event = leader._register_pending_read(
                    leader.last_applied + 50, leader.current_term)
                self.assertEqual(len(leader._pending_reads), 1)
                leader._step_down(leader.current_term + 1)
                self.assertTrue(event.is_set())
                self.assertEqual(leader._pending_reads, [])
            self.assertEqual(leader.role, "follower")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
