"""Pipelined replication: bounded in-flight window, correct retry,
commit rule.

These spin up real nodes (real TCP, real threads, real disk) on
localhost, like the rest of the suite. The window is observable
through leader.pipeline_stats(): the high-water mark of concurrent
in-flight batches must stay above 1 (the window is doing work) and
never above the configured window.
"""

import time
import unittest

from harness import Cluster, client_request, wait_for_key


def _log_signature(node):
    """(index, term, command) per log entry, in order — the exact
    replication fingerprint. Two nodes with equal signatures hold
    identical logs: no missing, duplicated, or reordered entries."""
    return [(e["index"], e["term"], e["command"]) for e in node.log]


class TestPipelinedReplication(unittest.TestCase):
    def test_many_entries_replicate_with_pipelining(self):
        """A follower down for 60 writes catches up once it restarts:
        every entry lands, in order, with no duplicates."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            victim = next(n for n in cluster.nodes.values()
                          if n is not leader)
            cluster.stop_node(victim.id)
            for i in range(60):
                resp = client_request(
                    addr, {"type": "client", "op": "set",
                           "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            restarted = cluster.restart_node(victim.id)
            self.assertTrue(wait_for_key(restarted, "k59", "v59",
                                         timeout=10.0))
            leader = cluster.leader()
            self.assertEqual(restarted.sm.items(), leader.sm.items())
            self.assertEqual(_log_signature(restarted),
                             _log_signature(leader))
        finally:
            cluster.stop_all()

    def test_rejected_batch_backs_off_and_retries_without_duplicates(self):
        """A new leader starts with next_index = last + 1 for a
        follower that is 40 entries behind, so its first pipelined
        batch is rejected (prev_log_index past the follower's log
        end). The leader must back next_index down and re-dispatch
        from there — the follower ends up with exactly the leader's
        log, no duplicates, and the commit index advances to the end
        (only current-term entries advance it; committing them
        transitively commits the older ones)."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader1 = cluster.wait_for_leader()
            addr1 = cluster.addrs[leader1.id]
            victim = next(n for n in cluster.nodes.values()
                          if n is not leader1)
            cluster.stop_node(victim.id)
            for i in range(40):
                resp = client_request(
                    addr1, {"type": "client", "op": "set",
                            "key": f"a{i}", "value": f"{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            # Kill the leader while the victim is down. The survivor
            # with the longer log wins the election; its first batch
            # to the victim is rejected and must back off.
            cluster.stop_node(leader1.id)
            cluster.restart_node(victim.id)
            leader2 = cluster.wait_for_leader()
            self.assertNotEqual(leader2.id, leader1.id)
            addr2 = cluster.addrs[leader2.id]
            for i in range(40, 50):
                resp = client_request(
                    addr2, {"type": "client", "op": "set",
                            "key": f"a{i}", "value": f"{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            victim_node = cluster.nodes[victim.id]
            self.assertTrue(wait_for_key(victim_node, "a49", "49",
                                         timeout=10.0))
            leader2 = cluster.leader()
            self.assertEqual(victim_node.sm.items(), leader2.sm.items())
            self.assertEqual(_log_signature(victim_node),
                             _log_signature(leader2))
            self.assertEqual(leader2.commit_index,
                             leader2._last_log_index())
        finally:
            cluster.stop_all()

    def test_pipeline_window_bounds_in_flight_batches(self):
        """Catching up 200 entries dispatches several batches at once
        (more than 1 in flight) and never more than the window (10).
        The high-water mark is recorded at dispatch time under the
        lock, so polling it after the fact still sees the peak."""
        cluster = Cluster(3, pipeline_window=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            victim = next(n for n in cluster.nodes.values()
                          if n is not leader)
            cluster.stop_node(victim.id)
            for i in range(200):
                resp = client_request(
                    addr, {"type": "client", "op": "set",
                           "key": f"w{i}", "value": f"{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            restarted = cluster.restart_node(victim.id)
            max_seen = 0
            deadline = time.monotonic() + 10.0
            caught_up = False
            while time.monotonic() < deadline:
                stats = leader.pipeline_stats()
                max_seen = max(max_seen,
                               stats[victim.id]["max_in_flight"])
                if restarted.sm.get("w199") == "199":
                    caught_up = True
                    break
                time.sleep(0.005)
            self.assertTrue(caught_up, "victim never caught up")
            self.assertGreater(
                max_seen, 1,
                "pipelining never had more than 1 batch in flight")
            self.assertLessEqual(
                max_seen, 10, "in-flight window exceeded its bound")
            self.assertEqual(restarted.sm.items(), leader.sm.items())
            self.assertEqual(_log_signature(restarted),
                             _log_signature(leader))
        finally:
            cluster.stop_all()

    def test_pipeline_window_one_is_sequential(self):
        """With window=1 the driver never has more than one batch in
        flight — the sequential path — and replication is still
        correct, just one batch at a time."""
        cluster = Cluster(3, pipeline_window=1)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            victim = next(n for n in cluster.nodes.values()
                          if n is not leader)
            cluster.stop_node(victim.id)
            for i in range(60):
                resp = client_request(
                    addr, {"type": "client", "op": "set",
                           "key": f"s{i}", "value": f"{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            restarted = cluster.restart_node(victim.id)
            self.assertTrue(wait_for_key(restarted, "s59", "59",
                                         timeout=10.0))
            stats = leader.pipeline_stats()
            self.assertEqual(stats[victim.id]["max_in_flight"], 1)
            self.assertEqual(restarted.sm.items(), leader.sm.items())
            self.assertEqual(_log_signature(restarted),
                             _log_signature(leader))
        finally:
            cluster.stop_all()

    def test_lagging_follower_catches_up_quickly(self):
        """200 entries to a follower that was down the whole time
        replicate and apply well inside a generous bound — the
        pipeline streams them in a few concurrent batches instead of
        one round trip per batch."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            victim = next(n for n in cluster.nodes.values()
                          if n is not leader)
            cluster.stop_node(victim.id)
            for i in range(200):
                resp = client_request(
                    addr, {"type": "client", "op": "set",
                           "key": f"q{i}", "value": f"{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            start = time.monotonic()
            restarted = cluster.restart_node(victim.id)
            self.assertTrue(wait_for_key(restarted, "q199", "199",
                                         timeout=10.0))
            elapsed = time.monotonic() - start
            self.assertLess(elapsed, 5.0,
                            f"200-entry catch-up took {elapsed:.2f}s")
            self.assertEqual(restarted.sm.items(), leader.sm.items())
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
