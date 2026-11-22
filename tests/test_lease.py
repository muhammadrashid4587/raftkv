"""Tests for raftkv leader-lease reads (Raft dissertation §6.4).

A leader that heard acks from a majority within LEADER_LEASE_DURATION
(100ms, shorter than any follower's 150ms minimum election timeout)
knows no new leader can have been elected since — any election
majority overlaps the acking majority in at least one node, and that
node cannot time out before its election timeout. So the leader may
serve reads straight from its applied state, skipping the heartbeat
round ReadIndex otherwise runs per read. Heartbeats go out every 50ms,
so a healthy leader's lease is almost always fresh.

These tests mix real clusters with direct calls on live nodes, the
same mix test_readindex.py uses. ``_confirm_rounds`` (exposed in
status()) counts explicit leadership-confirmation rounds, so a test
can tell a lease-served read from a ReadIndex round apart.
"""

import time
import unittest

from harness import Cluster, client_request


class TestLeaderLeaseReads(unittest.TestCase):
    def test_lease_read_skips_heartbeat_round(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v"})
            self.assertEqual(resp.get("status"), "ok", resp)
            # The write's replication acks established the lease:
            # the leader heard from a majority within the last 100ms.
            self.assertTrue(leader._lease_valid())
            rounds_before = leader._confirm_rounds
            log_before = leader._last_log_index()
            for _ in range(5):
                resp = client_request(addr, {"type": "client", "op": "get",
                                             "key": "k"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), "v")
            # Five reads, zero confirmation rounds: every one was
            # served from the lease, with no heartbeat round at all.
            self.assertEqual(leader._confirm_rounds, rounds_before)
            # Reads still must not append log entries.
            self.assertEqual(leader._last_log_index(), log_before)
        finally:
            cluster.stop_all()

    def test_lease_expires_without_majority_acks(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(leader._lease_valid())
            # Kill both followers: no more acks can arrive, so the
            # lease goes stale after LEADER_LEASE_DURATION (100ms).
            for node_id in list(cluster.live):
                if node_id != leader.id:
                    cluster.stop_node(node_id)
            time.sleep(0.3)
            self.assertFalse(leader._lease_valid())
            # The read falls back to the heartbeat round, which cannot
            # reach a majority — so it errors instead of serving from
            # a leader that can no longer prove it still leads.
            rounds_before = leader._confirm_rounds
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertEqual(resp.get("error"),
                             "could not confirm leadership")
            self.assertEqual(leader._confirm_rounds, rounds_before + 1)
        finally:
            cluster.stop_all()

    def test_step_down_clears_lease(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(leader._lease_valid())
            # A deposed leader holds no lease, even if its acks were
            # fresh a moment ago: stepping down clears them.
            with leader.lock:
                leader._step_down(leader.current_term + 1)
                self.assertEqual(leader._last_ack, {})
                self.assertFalse(leader._lease_valid())
            self.assertEqual(leader.role, "follower")
        finally:
            cluster.stop_all()

    def test_lease_counts_only_voting_members(self):
        """A non-voting server's ack must not extend the lease: only
        a majority of *voters* can keep a leader leased."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            followers = [n for n in cluster.nodes.values()
                         if n is not leader]
            with leader.lock:
                now = time.monotonic()
                # Self + one fresh voting ack = 2 of 3: leased.
                leader._last_ack = {followers[0].id: now,
                                    followers[1].id: now - 10.0}
                self.assertTrue(leader._lease_valid())
                # Both voting acks stale: not leased.
                leader._last_ack = {followers[0].id: now - 10.0,
                                    followers[1].id: now - 10.0}
                self.assertFalse(leader._lease_valid())
                # A fresh ack from a non-voter does not help: only
                # voting members count toward the majority.
                leader._last_ack = {"node4": now,
                                    followers[0].id: now - 10.0,
                                    followers[1].id: now - 10.0}
                self.assertFalse(leader._lease_valid())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
