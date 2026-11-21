"""Unit tests for raftkv: election, replication, redirects, persistence.

These spin up real nodes (real TCP transport, real threads, real disk
persistence) on localhost, so they exercise the actual code paths rather
than mocks. The longer leader-kill scenario lives in test_integration.py.
"""

import unittest

from harness import Cluster, client_request, wait_for_key


class TestElection(unittest.TestCase):
    def test_exactly_one_leader_is_elected(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            followers = [n for n in cluster.nodes.values() if n is not leader]
            self.assertEqual(len(followers), 2)
            for f in followers:
                self.assertEqual(f.role, "follower")
                self.assertEqual(f.leader_id, leader.id)
            # The leader stays leader while the cluster is healthy.
            import time
            time.sleep(0.5)
            self.assertTrue(leader.is_leader)
        finally:
            cluster.stop_all()

    def test_leader_stays_stable_with_no_failures(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            first = cluster.wait_for_leader()
            import time
            time.sleep(1.0)
            second = cluster.leader()
            self.assertIsNotNone(second)
            self.assertEqual(first.id, second.id)
            self.assertEqual(first.current_term, second.current_term)
        finally:
            cluster.stop_all()


class TestReplication(unittest.TestCase):
    def test_write_replicates_to_all_nodes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            resp = client_request(cluster.addrs[leader.id],
                                  {"type": "client", "op": "set",
                                   "key": "a", "value": "1"})
            self.assertEqual(resp.get("status"), "ok", resp)
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "a", "1"),
                                f"{node.id} never replicated key a")
        finally:
            cluster.stop_all()

    def test_write_to_follower_redirects_to_leader(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values() if n is not leader)
            resp = client_request(cluster.addrs[follower.id],
                                  {"type": "client", "op": "set",
                                   "key": "x", "value": "y"})
            self.assertEqual(resp.get("status"), "ok", resp)
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "x", "y"))
        finally:
            cluster.stop_all()

    def test_delete_replicates(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(client_request(
                addr, {"type": "client", "op": "set", "key": "d", "value": "v"}
            ).get("status"), "ok")
            self.assertEqual(client_request(
                addr, {"type": "client", "op": "del", "key": "d"}
            ).get("status"), "ok")
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "d", None))
        finally:
            cluster.stop_all()

    def test_leader_serves_reads(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            client_request(addr, {"type": "client", "op": "set",
                                  "key": "r", "value": "hello"})
            resp = client_request(addr, {"type": "client", "op": "get", "key": "r"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "hello")
            missing = client_request(addr, {"type": "client", "op": "get",
                                            "key": "nope"})
            self.assertEqual(missing.get("status"), "ok", missing)
            self.assertIsNone(missing.get("value"))
        finally:
            cluster.stop_all()


class TestPersistence(unittest.TestCase):
    def test_follower_restart_replays_committed_log(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for i in range(5):
                resp = client_request(addr, {"type": "client", "op": "set",
                                             "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            victim = next(n for n in cluster.nodes.values() if n is not leader)
            self.assertTrue(wait_for_key(victim, "k4", "v4"))
            cluster.stop_node(victim.id)
            # Writes keep flowing while the follower is down.
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k5", "value": "v5"})
            self.assertEqual(resp.get("status"), "ok", resp)
            restarted = cluster.restart_node(victim.id)
            # On restart the node replays its committed log from disk,
            # then catches up on anything committed while it was down.
            self.assertTrue(wait_for_key(restarted, "k4", "v4"))
            self.assertTrue(wait_for_key(restarted, "k5", "v5"))
            self.assertEqual(restarted.sm.items(),
                             {f"k{i}": f"v{i}" for i in range(6)})
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
