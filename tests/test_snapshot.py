"""Snapshot tests: log compaction, InstallSnapshot, restart from snapshot.

These spin up real nodes (real TCP, real threads, real disk) with a
tiny snapshot threshold (10 entries) so compaction triggers quickly.
The point of each test is a different half of snapshotting:

- compacting the log and surviving a restart from the snapshot,
- a lagging follower catching up via InstallSnapshot when the leader
  has already compacted away the entries it is missing,
- a node restarted alone still holding its snapshotted state.
"""

import time
import unittest
from pathlib import Path

from harness import Cluster, client_request, wait_for_key
from raftkv.raft import RaftNode
from raftkv.store import KVStore


def write_many(addr: tuple[str, int], start: int, count: int) -> None:
    for i in range(start, start + count):
        resp = client_request(addr, {"type": "client", "op": "set",
                                     "key": f"s{i}", "value": f"v{i}"})
        assert resp.get("status") == "ok", resp


def wait_for_snapshot(node: RaftNode, timeout: float = 5.0) -> bool:
    """Poll until the node has taken a snapshot (snapshot_index > 0).

    A follower applies entries and snapshots in the same locked step,
    but the test thread reads the state machine without the lock, so it
    can observe an applied key a moment before the snapshot lands.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.snapshot_index > 0:
            return True
        time.sleep(0.02)
    return False


class TestSnapshotting(unittest.TestCase):
    def test_snapshot_compacts_log_and_survives_restart(self):
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            write_many(addr, 0, 15)
            expected = {f"s{i}": f"v{i}" for i in range(15)}
            for node in cluster.nodes.values():
                self.assertTrue(wait_for_key(node, "s14", "v14"))
                self.assertTrue(wait_for_snapshot(node),
                                f"{node.id} never snapshotted")
                # The on-disk log holds only the post-snapshot suffix.
                self.assertLess(len(node.log), 15)
                self.assertTrue((node.data_dir / "snapshot.json").exists())

            # Kill a follower, write more, restart it: it resumes from
            # its snapshot + remaining log and catches up.
            victim = next(n for n in cluster.nodes.values() if n is not leader)
            cluster.stop_node(victim.id)
            write_many(addr, 15, 5)
            restarted = cluster.restart_node(victim.id)
            full = expected | {f"s{i}": f"v{i}" for i in range(15, 20)}
            self.assertTrue(wait_for_key(restarted, "s19", "v19"))
            self.assertEqual(restarted.sm.items(), full)
            self.assertGreater(restarted.snapshot_index, 0)
        finally:
            cluster.stop_all()

    def test_lagging_follower_catches_up_via_install_snapshot(self):
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            write_many(addr, 0, 5)
            lagger = next(n for n in cluster.nodes.values() if n is not leader)
            self.assertTrue(wait_for_key(lagger, "s4", "v4"))
            cluster.stop_node(lagger.id)

            # While the follower is down, the leader compacts past it:
            # its log no longer reaches back to the follower's position,
            # so AppendEntries alone cannot catch it up.
            write_many(addr, 5, 15)
            self.assertGreater(leader.snapshot_index, 10)

            restarted = cluster.restart_node(lagger.id)
            expected = {f"s{i}": f"v{i}" for i in range(20)}
            self.assertTrue(wait_for_key(restarted, "s19", "v19", timeout=10.0))
            self.assertEqual(restarted.sm.items(), expected)
            # It caught up via a snapshot, not AppendEntries: the
            # leader's log no longer contains the entries it was
            # missing, so the follower must hold a snapshot now.
            self.assertGreater(restarted.snapshot_index, 0)
        finally:
            cluster.stop_all()

    def test_restart_loads_snapshot_without_a_cluster(self):
        # A node restarted alone (no peers up) still has its
        # snapshotted state on disk and resumes from it.
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            write_many(addr, 0, 12)
            victim = next(n for n in cluster.nodes.values() if n is not leader)
            self.assertTrue(wait_for_key(victim, "s11", "v11"))
            self.assertTrue(wait_for_snapshot(victim))

            # Stop every node individually (stop_all would delete the
            # temp data dir) and reload the victim from disk alone.
            for node_id in list(cluster.nodes):
                cluster.stop_node(node_id)
            peers = {p: cluster.addrs[p] for p in cluster.nodes
                     if p != victim.id}
            node = RaftNode(victim.id, peers, cluster.base / victim.id,
                            KVStore(), snapshot_threshold=10)
            self.assertEqual(node.sm.items(),
                             {f"s{i}": f"v{i}" for i in range(12)})
            self.assertGreater(node.snapshot_index, 0)
            # All committed entries are applied: the 12 writes plus
            # the leader's no-op entry on election (13 total). The
            # snapshot covers the early prefix and the tail replays
            # from the log.
            self.assertEqual(node.last_applied, 13)
            self.assertEqual(node.commit_index, 13)
            self.assertLess(len(node.log), 13)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
