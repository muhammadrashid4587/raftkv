"""Tests for group commit (batched fsyncs) in raftkv.

The first slice of group commit: when a follower receives an
AppendEntries batch, the entries that are actually new are written to
log.jsonl in one open/write/flush/fsync instead of one fsync per
entry, so a batch of N entries costs one fsync. The batch is written
and flushed before the single fsync, so a crash can never leave a
later entry durable while an earlier one is lost.

These tests count real os.fsync calls during a direct handler call on
a bare node (no server, no cluster), then prove the batch is durable
by reloading the node from the same data dir.
"""

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from raftkv.raft import RaftNode
from raftkv.store import KVStore
from tests.harness import Cluster, wait_for_key


def _bare_node() -> RaftNode:
    """A node that has never started: no server, no leader contact."""
    tmp = tempfile.TemporaryDirectory(prefix="raftkv-groupcommit-")
    node = RaftNode("solo", {}, Path(tmp.name), KVStore())
    # Keep the temp dir alive for the node's lifetime.
    node._test_tmp = tmp  # type: ignore[attr-defined]
    return node


def _entries(term: int, count: int, start: int = 0) -> list:
    return [
        {"term": term,
         "command": {"op": "set", "key": f"k{start + i}", "value": f"v{start + i}"}}
        for i in range(count)
    ]


def _append_msg(term: int, prev_index: int, prev_term: int,
                entries: list, leader_commit: int = 0) -> dict:
    return {
        "type": "append_entries",
        "term": term,
        "leader_id": "leader",
        "prev_log_index": prev_index,
        "prev_log_term": prev_term,
        "entries": entries,
        "leader_commit": leader_commit,
    }


def _counted_fsync(calls: list):
    """Wrap the real os.fsync so every call is recorded and still runs."""
    real_fsync = os.fsync

    def counting(fd):
        calls.append(fd)
        return real_fsync(fd)

    return counting


def _counted_fsync_on_file(calls: list, path: Path):
    """Like _counted_fsync, but only records fsyncs on ``path``: the
    fd is resolved through /proc/self/fd, so fsyncs on other files
    (meta.json, the followers' logs) do not pollute the count."""
    real_fsync = os.fsync
    target = str(path)

    def counting(fd):
        try:
            if os.readlink(f"/proc/self/fd/{fd}") == target:
                calls.append(fd)
        except OSError:
            pass
        return real_fsync(fd)

    return counting


def _set_via_leader(leader: RaftNode, key: str, value: str) -> dict:
    """One client write straight into the leader's handler, the way a
    connection thread would deliver it."""
    return leader._on_client({"op": "set", "key": key, "value": value})


class TestGroupCommitBatch(unittest.TestCase):
    def test_append_entries_batch_shares_one_fsync(self):
        node = _bare_node()
        node.current_term = 1
        calls: list = []
        with mock.patch("os.fsync", _counted_fsync(calls)):
            resp = node._on_append_entries(
                _append_msg(1, 0, 0, _entries(1, 5)))
        self.assertTrue(resp["success"])
        self.assertEqual(resp["match_index"], 5)
        self.assertEqual(len(node.log), 5)
        self.assertEqual([e["index"] for e in node.log], [1, 2, 3, 4, 5])
        # Five entries, one fsync: that is the group commit.
        self.assertEqual(len(calls), 1)

        # The batch is on disk, not just in memory: log.jsonl holds
        # all five lines with their indices.
        lines = (node.data_dir / "log.jsonl").read_text(
            encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 5)
        on_disk = [json.loads(line) for line in lines]
        self.assertEqual([e["index"] for e in on_disk], [1, 2, 3, 4, 5])
        self.assertEqual(on_disk[4]["command"]["key"], "k4")

        # And a restarted node reloads the whole batch from disk.
        node2 = RaftNode("solo", {}, node.data_dir, KVStore())
        self.assertEqual(len(node2.log), 5)
        self.assertEqual([e["index"] for e in node2.log], [1, 2, 3, 4, 5])

    def test_conflicting_batch_truncates_then_appends_in_one_fsync(self):
        node = _bare_node()
        node.current_term = 1
        node._on_append_entries(_append_msg(1, 0, 0, _entries(1, 3)))
        self.assertEqual([e["term"] for e in node.log], [1, 1, 1])

        # A batch whose second entry conflicts (term 2 vs term 1):
        # truncate at index 2, then append entries 2-3 together.
        node.current_term = 2
        calls: list = []
        with mock.patch("os.fsync", _counted_fsync(calls)):
            resp = node._on_append_entries(
                _append_msg(2, 1, 1, _entries(2, 2, start=1)))
        self.assertTrue(resp["success"])
        self.assertEqual([e["index"] for e in node.log], [1, 2, 3])
        self.assertEqual([e["term"] for e in node.log], [1, 2, 2])
        self.assertEqual(node.log[1]["command"]["key"], "k1")
        # One fsync for the log rewrite after truncation, one for the
        # batch append — not one per appended entry.
        self.assertEqual(len(calls), 2)

        lines = (node.data_dir / "log.jsonl").read_text(
            encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 3)
        self.assertEqual([json.loads(l)["term"] for l in lines], [1, 2, 2])

    def test_duplicate_batch_appends_nothing_and_fsyncs_nothing(self):
        node = _bare_node()
        node.current_term = 1
        node._on_append_entries(_append_msg(1, 0, 0, _entries(1, 3)))
        calls: list = []
        with mock.patch("os.fsync", _counted_fsync(calls)):
            resp = node._on_append_entries(
                _append_msg(1, 0, 0, _entries(1, 3)))
        self.assertTrue(resp["success"])
        self.assertEqual(len(node.log), 3)
        self.assertEqual(len(calls), 0)


class TestLeaderGroupCommit(unittest.TestCase):
    """The leader side: concurrent client writes in the same window
    share one fsync on the leader's log, instead of one per write."""

    def test_concurrent_writes_share_one_leader_fsync(self):
        cluster = Cluster()
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            log_path = leader.data_dir / "log.jsonl"
            results: list = []
            barrier = threading.Barrier(5)

            def _write(i: int) -> None:
                barrier.wait(timeout=5.0)
                results.append(
                    _set_via_leader(leader, f"gk{i}", f"gv{i}"))

            calls: list = []
            with mock.patch("os.fsync",
                            _counted_fsync_on_file(calls, log_path)):
                threads = [threading.Thread(target=_write, args=(i,),
                                            daemon=True)
                           for i in range(5)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=15.0)
            self.assertEqual(len(results), 5)
            self.assertTrue(all(r.get("status") == "ok" for r in results),
                            f"some writes failed: {results}")
            # Five concurrent writes, one fsync on the leader's log:
            # that is the leader-side group commit.
            self.assertEqual(len(calls), 1)

            # Every write committed and replicated: all five keys read
            # back on every node.
            for i in range(5):
                for node in cluster.nodes.values():
                    self.assertTrue(
                        wait_for_key(node, f"gk{i}", f"gv{i}"),
                        f"gk{i} missing on {node.id}")
        finally:
            cluster.stop_all()

    def test_sequential_writes_each_fsync_once(self):
        cluster = Cluster()
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            log_path = leader.data_dir / "log.jsonl"
            calls: list = []
            with mock.patch("os.fsync",
                            _counted_fsync_on_file(calls, log_path)):
                r1 = _set_via_leader(leader, "sk1", "sv1")
                r2 = _set_via_leader(leader, "sk2", "sv2")
            self.assertEqual(r1.get("status"), "ok")
            self.assertEqual(r2.get("status"), "ok")
            # No concurrency, no batching: each write pays its own
            # fsync, exactly like before group commit.
            self.assertEqual(len(calls), 2)
            self.assertTrue(wait_for_key(leader, "sk1", "sv1"))
            self.assertTrue(wait_for_key(leader, "sk2", "sv2"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
