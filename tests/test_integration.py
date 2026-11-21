"""Integration test: kill the leader, re-elect, keep writing, verify.

This is the scenario Raft exists for: a 3-node cluster loses its leader,
elects a new one from the remaining two, keeps serving writes, and when
the old leader comes back it catches up instead of diverging. Every node
must end up with exactly the same data.
"""

import unittest

from harness import Cluster, client_request, wait_for_key


class TestLeaderFailover(unittest.TestCase):
    def test_kill_leader_reelect_write_and_rejoin(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]

            # Phase 1: write keys under the first leader.
            for i in range(5):
                resp = client_request(addr, {"type": "client", "op": "set",
                                             "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            for node in cluster.nodes.values():
                for i in range(5):
                    self.assertTrue(wait_for_key(node, f"k{i}", f"v{i}"))

            # Phase 2: kill the leader. The remaining two must elect a new one.
            old_leader_id = leader.id
            cluster.stop_node(old_leader_id)
            survivors = [n for i, n in cluster.nodes.items() if i != old_leader_id]
            new_leader = cluster.wait_for_leader(timeout=10.0)
            self.assertNotEqual(new_leader.id, old_leader_id)

            # Phase 3: writes keep working under the new leader.
            new_addr = cluster.addrs[new_leader.id]
            for i in range(5, 10):
                resp = client_request(new_addr, {"type": "client", "op": "set",
                                                 "key": f"k{i}", "value": f"v{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            expected = {f"k{i}": f"v{i}" for i in range(10)}
            for node in survivors:
                self.assertTrue(wait_for_key(node, "k9", "v9"))
                self.assertEqual(node.sm.items(), expected)

            # Phase 4: the old leader restarts and must catch up, not fork.
            restarted = cluster.restart_node(old_leader_id)
            self.assertTrue(wait_for_key(restarted, "k9", "v9"))
            self.assertEqual(restarted.sm.items(), expected)

            # Phase 5: the cluster is whole again and still serves reads.
            final_leader = cluster.wait_for_leader()
            resp = client_request(cluster.addrs[final_leader.id],
                                  {"type": "client", "op": "get", "key": "k3"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v3")
            for node in cluster.nodes.values():
                self.assertEqual(node.sm.items(), expected)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
