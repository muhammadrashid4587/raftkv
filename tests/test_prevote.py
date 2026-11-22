"""Tests for raftkv pre-vote (Raft dissertation §4.2.3).

Pre-vote is the gate in front of every election: a candidate first asks
whether a majority *would* vote for it, without bumping its term. Only
with a majority of pre-votes does it start a real election. The point is
that a partitioned or stale node can never inflate its term, so when it
rejoins it cannot force the live leader to step down or the cluster to
re-elect.

These tests mix direct handler calls (to isolate the voter rules) with
real clusters (to prove the behaviour end to end).
"""

import tempfile
import time
import unittest
from pathlib import Path

from harness import Cluster, client_request, wait_for_key
from raftkv.raft import RaftNode
from raftkv.store import KVStore


def _bare_node() -> RaftNode:
    """A node that has never started: no server, no leader contact."""
    tmp = tempfile.TemporaryDirectory(prefix="raftkv-prevote-")
    node = RaftNode("solo", {}, Path(tmp.name), KVStore())
    # Keep the temp dir alive for the node's lifetime.
    node._test_tmp = tmp  # type: ignore[attr-defined]
    return node


class TestPreVoteHandler(unittest.TestCase):
    def test_stale_candidate_prevote_rejected(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(3):
                resp = client_request(addr, {"type": "client", "op": "set",
                                             "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertTrue(wait_for_key(follower, "k2", "v2"))
            # Isolate the log check: pretend this follower has not heard
            # from a leader in a long time, so only the stale log can
            # cause the rejection.
            follower._last_leader_contact = time.monotonic() - 10.0
            term_before = follower.current_term
            voted_before = follower.voted_for
            resp = follower._on_request_pre_vote({
                "type": "request_pre_vote",
                "term": term_before + 1,
                "candidate_id": "stale-candidate",
                # The follower's log holds 3 entries; this candidate
                # claims an empty one.
                "last_log_index": 0,
                "last_log_term": 0,
            })
            self.assertFalse(resp.get("pre_vote_granted"), resp)
            # A pre-vote changes nothing on the voter.
            self.assertEqual(follower.current_term, term_before)
            self.assertEqual(follower.voted_for, voted_before)
        finally:
            cluster.stop_all()

    def test_prevote_granted_when_log_current_and_no_leader(self):
        node = _bare_node()
        try:
            # Never heard from a leader, empty log, candidate's log
            # matches ours exactly.
            resp = node._on_request_pre_vote({
                "type": "request_pre_vote",
                "term": node.current_term + 1,
                "candidate_id": "candidate",
                "last_log_index": 0,
                "last_log_term": 0,
            })
            self.assertTrue(resp.get("pre_vote_granted"), resp)
            self.assertEqual(node.current_term, 0)
            self.assertIsNone(node.voted_for)
            self.assertEqual(node.role, "follower")
        finally:
            node._test_tmp.cleanup()

    def test_prevote_has_no_side_effects(self):
        node = _bare_node()
        try:
            resp = node._on_request_pre_vote({
                "type": "request_pre_vote",
                "term": 1,
                "candidate_id": "candidate",
                "last_log_index": 0,
                "last_log_term": 0,
            })
            self.assertTrue(resp.get("pre_vote_granted"), resp)
            self.assertEqual(node.current_term, 0)
            self.assertIsNone(node.voted_for)
            # The pre-vote did not consume the vote: the same candidate
            # can still win a real RequestVote in that term.
            vote = node._on_request_vote({
                "type": "request_vote",
                "term": 1,
                "candidate_id": "candidate",
                "last_log_index": 0,
                "last_log_term": 0,
            })
            self.assertTrue(vote.get("vote_granted"), vote)
            self.assertEqual(node.voted_for, "candidate")
        finally:
            node._test_tmp.cleanup()

    def test_prevote_rejected_when_leader_recently_heard(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            # Healthy follower, heartbeats arriving every 50 ms, and the
            # candidate's log is fully up to date — the only reason to
            # reject is the live leader.
            resp = follower._on_request_pre_vote({
                "type": "request_pre_vote",
                "term": follower.current_term + 1,
                "candidate_id": "some-candidate",
                "last_log_index": follower._last_log_index(),
                "last_log_term": follower._last_log_term(),
            })
            self.assertFalse(resp.get("pre_vote_granted"), resp)
            self.assertEqual(follower.current_term, leader.current_term)
        finally:
            cluster.stop_all()


class TestPreVoteBehaviour(unittest.TestCase):
    def test_partitioned_node_rejoin_does_not_disrupt_leader(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(3):
                resp = client_request(addr, {"type": "client", "op": "set",
                                             "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            leader_id = leader.id
            term_before = leader.current_term

            # Partition by taking a follower down; the leader must stay
            # leader among the two survivors.
            victim = next(n for n in cluster.nodes.values() if n is not leader)
            cluster.stop_node(victim.id)
            time.sleep(0.5)
            self.assertTrue(leader.is_leader)
            self.assertEqual(leader.current_term, term_before)

            # The partitioned node rejoins. Its timer will fire and it
            # will run a pre-vote round — but the live leader and the
            # healthy follower must both refuse it, so there is no
            # re-election and no term jump.
            restarted = cluster.restart_node(victim.id)
            time.sleep(1.0)
            self.assertTrue(leader.is_leader)
            self.assertEqual(leader.current_term, term_before)
            self.assertEqual(cluster.leader().id, leader_id)
            # The rejoined node catches up instead of forking.
            self.assertTrue(wait_for_key(restarted, "k2", "v2"))
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "after", "value": "rejoin"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(wait_for_key(restarted, "after", "rejoin"))
        finally:
            cluster.stop_all()

    def test_election_still_works_after_leader_failure(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(3):
                resp = client_request(addr, {"type": "client", "op": "set",
                                             "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            old_leader_id = leader.id
            cluster.stop_node(old_leader_id)
            # Pre-vote must not stop a genuine failover: the two
            # survivors have heard from no leader for a while, so they
            # grant each other's pre-votes and elect a new leader.
            new_leader = cluster.wait_for_leader(timeout=10.0)
            self.assertNotEqual(new_leader.id, old_leader_id)
            new_addr = cluster.addrs[new_leader.id]
            for i in range(3, 6):
                resp = client_request(new_addr, {"type": "client", "op": "set",
                                                 "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            for node in cluster.nodes.values():
                if node.id == old_leader_id:
                    continue
                self.assertTrue(wait_for_key(node, "k5", "v5"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
