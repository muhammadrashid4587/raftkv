"""Chunked InstallSnapshot tests: snapshot streaming.

Since v0.19.0 the leader streams a snapshot in bounded chunks instead
of one RPC carrying the whole state machine. These tests prove the
three properties that make streaming safe:

- a lagging follower still catches up end to end, even when the
  snapshot is forced through many tiny chunks;
- a partial transfer installs nothing — chunks buffer until every
  piece has arrived, in whatever order, and a duplicate chunk changes
  nothing;
- a superseded transfer never completes from mixed pieces: chunks for
  a newer snapshot drop the buffered prefix of the older one.
"""

import json
import tempfile
import unittest
from pathlib import Path

from harness import Cluster, client_request, wait_for_key
from raftkv.raft import RaftNode
from raftkv.store import KVStore
from test_snapshot import wait_for_snapshot, write_many


def chunked_messages(term, leader_id, last_included_index,
                     last_included_term, payload, chunk_size):
    """Split a snapshot payload dict into install_snapshot chunk msgs."""
    blob = json.dumps(payload)
    chunks = [blob[i:i + chunk_size]
              for i in range(0, len(blob), chunk_size)] or [""]
    return [{
        "type": "install_snapshot",
        "term": term,
        "leader_id": leader_id,
        "last_included_index": last_included_index,
        "last_included_term": last_included_term,
        "chunk_index": i,
        "chunk_total": len(chunks),
        "chunk": chunk,
    } for i, chunk in enumerate(chunks)]


class TestSnapshotStreaming(unittest.TestCase):
    def test_lagging_follower_catches_up_via_chunked_snapshot(self):
        # 64-byte chunks force a ~20-key snapshot through dozens of
        # RPCs; the follower must still end up with exactly the
        # leader's state.
        cluster = Cluster(3, snapshot_threshold=10, snapshot_chunk_size=64)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            write_many(addr, 0, 5)
            lagger = next(n for n in cluster.nodes.values() if n is not leader)
            self.assertTrue(wait_for_key(lagger, "s4", "v4"))
            cluster.stop_node(lagger.id)

            write_many(addr, 5, 15)
            self.assertGreater(leader.snapshot_index, 10)

            restarted = cluster.restart_node(lagger.id)
            expected = {f"s{i}": f"v{i}" for i in range(20)}
            self.assertTrue(wait_for_key(restarted, "s19", "v19", timeout=10.0))
            self.assertEqual(restarted.sm.items(), expected)
            self.assertGreater(restarted.snapshot_index, 0)
        finally:
            cluster.stop_all()

    def test_partial_transfer_installs_nothing(self):
        # Drive chunk messages straight at an unstarted node: no
        # cluster, no timing, just the buffering rules.
        with tempfile.TemporaryDirectory(prefix="raftkv-stream-") as d:
            node = RaftNode("node1", {}, Path(d) / "node1", KVStore())
            payload = {"data": {"streamed": "yes", "k": "v"},
                       "config": None, "dedup": []}
            msgs = chunked_messages(1, "node1", 5, 1, payload, 7)
            self.assertGreater(len(msgs), 2)  # genuinely multi-chunk

            # First chunk only: buffered, nothing installed.
            resp = node._on_install_snapshot(msgs[0])
            self.assertTrue(resp["success"])
            self.assertEqual(node.sm.items(), {})
            self.assertEqual(node.snapshot_index, 0)

            # Last chunk out of order, middle still missing: still
            # nothing installed.
            resp = node._on_install_snapshot(msgs[-1])
            self.assertTrue(resp["success"])
            self.assertEqual(node.sm.items(), {})
            self.assertEqual(node.snapshot_index, 0)

            # The first chunk again (a leader retry re-sends from
            # chunk 0): a duplicate, acked, still nothing installed.
            resp = node._on_install_snapshot(msgs[0])
            self.assertTrue(resp["success"])
            self.assertEqual(node.snapshot_index, 0)

            # The missing middle chunks arrive: now complete, install.
            for msg in msgs[1:-1]:
                resp = node._on_install_snapshot(msg)
                self.assertTrue(resp["success"])
            self.assertEqual(node.sm.items(), {"streamed": "yes", "k": "v"})
            self.assertEqual(node.snapshot_index, 5)
            self.assertEqual(node.snapshot_term, 1)

            # A stale-term chunk is rejected outright.
            stale = dict(msgs[0], term=0)
            resp = node._on_install_snapshot(stale)
            self.assertFalse(resp["success"])

            # Re-sending the whole transfer for the snapshot this node
            # already holds is a no-op success, not a second install.
            for msg in msgs:
                resp = node._on_install_snapshot(msg)
                self.assertTrue(resp["success"])
            self.assertEqual(node.sm.items(), {"streamed": "yes", "k": "v"})
            self.assertEqual(node.snapshot_index, 5)

    def test_superseded_transfer_never_mixes_pieces(self):
        with tempfile.TemporaryDirectory(prefix="raftkv-stream-") as d:
            node = RaftNode("node1", {}, Path(d) / "node1", KVStore())
            old = chunked_messages(
                1, "node1", 5, 1,
                {"data": {"which": "old"}, "config": None, "dedup": []}, 7)
            new = chunked_messages(
                1, "node1", 9, 1,
                {"data": {"which": "new"}, "config": None, "dedup": []}, 7)

            # One chunk of the old snapshot, then the new snapshot
            # starts: the old prefix is dropped.
            self.assertTrue(node._on_install_snapshot(old[0])["success"])
            self.assertTrue(node._on_install_snapshot(new[0])["success"])
            self.assertEqual(node.snapshot_index, 0)

            # Finishing the old transfer now must not install the old
            # snapshot: its remaining chunks start a fresh transfer
            # for the old index, which is incomplete without chunk 0
            # re-sent — and even completed it would be older than
            # nothing installed yet, so send it fully and check the
            # new snapshot still wins. First complete the new one.
            for msg in new[1:]:
                self.assertTrue(node._on_install_snapshot(msg)["success"])
            self.assertEqual(node.sm.items(), {"which": "new"})
            self.assertEqual(node.snapshot_index, 9)

            # Now the old transfer's remaining chunks arrive: they
            # buffer under the old key, and completing them is a
            # no-op — index 5 is already covered by snapshot 9.
            for msg in old[1:]:
                self.assertTrue(node._on_install_snapshot(msg)["success"])
            self.assertEqual(node.sm.items(), {"which": "new"})
            self.assertEqual(node.snapshot_index, 9)


if __name__ == "__main__":
    unittest.main()
