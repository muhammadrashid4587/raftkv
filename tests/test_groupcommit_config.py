"""Tests for configurable group commit (v0.26.0) in raftkv.

Group commit itself landed in v0.7.0 with a hardcoded 2ms window; this
slice makes it configurable and observable, and proves the three
properties the knob has to keep:

1. Concurrent writes are batched into fewer fsyncs than writes, and
   the status RPC reports the batching honestly (batches, entries,
   fsyncs, largest batch).
2. Acked writes survive a crash/restart: the batch fsync happens
   before any waiter is woken, so everything the client heard back
   about is on disk.
3. Disabling batching (window 0) behaves exactly as before group
   commit: every write is its own batch and pays its own fsync.

A fourth test covers the other knob: a max batch size caps the batch
and flushes early instead of waiting out the window.
"""

import threading
import unittest

from raftkv.raft import RaftNode
from tests.harness import Cluster, wait_for_key


def _set_via_leader(leader: RaftNode, key: str, value: str) -> dict:
    """One client write straight into the leader's handler, the way a
    connection thread would deliver it."""
    return leader._on_client({"op": "set", "key": key, "value": value})


def _stats_delta(before: dict, after: dict) -> dict:
    return {k: after[k] - before[k] for k in ("batches", "entries", "fsyncs")}


def _concurrent_writes(leader: RaftNode, prefix: str, count: int) -> list:
    """Fire ``count`` writes at once from a barrier; return replies."""
    results: list = []
    barrier = threading.Barrier(count)

    def _write(i: int) -> None:
        barrier.wait(timeout=5.0)
        results.append(_set_via_leader(leader, f"{prefix}{i}", f"v{i}"))

    threads = [threading.Thread(target=_write, args=(i,), daemon=True)
               for i in range(count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)
    return results


class TestGroupCommitStats(unittest.TestCase):
    def test_concurrent_writes_share_one_fsync_in_stats(self):
        cluster = Cluster()
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            before = dict(leader._group_commit)
            results = _concurrent_writes(leader, "gs", 5)
            after = dict(leader._group_commit)
            self.assertEqual(len(results), 5)
            self.assertTrue(all(r.get("status") == "ok" for r in results),
                            f"some writes failed: {results}")
            delta = _stats_delta(before, after)
            # Five concurrent writes, one batch, one fsync — and the
            # stats say so, not just a mocked fsync count.
            self.assertEqual(delta["batches"], 1)
            self.assertEqual(delta["entries"], 5)
            self.assertEqual(delta["fsyncs"], 1)
            self.assertEqual(after["largest_batch"], 5)

            # The status RPC reports the same numbers and the config.
            status = cluster.status(leader.id)
            gc = status["group_commit"]
            self.assertEqual(gc["window_ms"], 2.0)
            self.assertEqual(gc["max_batch"], 0)
            self.assertEqual(gc["batches"], after["batches"])
            self.assertEqual(gc["entries"], after["entries"])
            self.assertEqual(gc["fsyncs"], after["fsyncs"])
            self.assertEqual(gc["largest_batch"], 5)

            for i in range(5):
                for node in cluster.nodes.values():
                    self.assertTrue(
                        wait_for_key(node, f"gs{i}", f"v{i}"),
                        f"gs{i} missing on {node.id}")
        finally:
            cluster.stop_all()

    def test_acked_writes_survive_crash_restart(self):
        cluster = Cluster()
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            before = dict(leader._group_commit)
            # Ten writes at once: batched, so fewer fsyncs than writes.
            results = _concurrent_writes(leader, "cr", 10)
            after = dict(leader._group_commit)
            self.assertEqual(len(results), 10)
            self.assertTrue(all(r.get("status") == "ok" for r in results),
                            f"some writes failed: {results}")
            delta = _stats_delta(before, after)
            self.assertEqual(delta["entries"], 10)
            self.assertLess(delta["fsyncs"], 10)

            # Crash the leader: stop it with no graceful flush (there
            # is nothing left to flush — every acked write's batch was
            # fsynced before its waiter was woken) and restart it from
            # the same data dir, which is a fresh node object loading
            # the log off disk.
            leader_id = leader.id
            cluster.stop_node(leader_id)
            restarted = cluster.restart_node(leader_id)
            cluster.wait_for_stable_leader()
            for i in range(10):
                self.assertTrue(
                    wait_for_key(restarted, f"cr{i}", f"v{i}"),
                    f"cr{i} lost across the restart on {leader_id}")
            # And every other node holds them too: no acked write was
            # lost anywhere in the cluster.
            for node in cluster.nodes.values():
                for i in range(10):
                    self.assertTrue(
                        wait_for_key(node, f"cr{i}", f"v{i}"),
                        f"cr{i} missing on {node.id}")
        finally:
            cluster.stop_all()


class TestGroupCommitDisabled(unittest.TestCase):
    def test_window_zero_disables_batching(self):
        cluster = Cluster(group_commit_window_ms=0)
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            self.assertEqual(leader.group_commit_window_ms, 0)
            before = dict(leader._group_commit)
            results = _concurrent_writes(leader, "nw", 5)
            after = dict(leader._group_commit)
            self.assertEqual(len(results), 5)
            self.assertTrue(all(r.get("status") == "ok" for r in results),
                            f"some writes failed: {results}")
            delta = _stats_delta(before, after)
            # Window 0 behaves exactly as before group commit: five
            # concurrent writes, five batches, five fsyncs — no
            # batching at all, and every write still acks.
            self.assertEqual(delta["batches"], 5)
            self.assertEqual(delta["entries"], 5)
            self.assertEqual(delta["fsyncs"], 5)
            self.assertEqual(after["largest_batch"], 1)

            # Sequential writes are identical either way: one fsync
            # each, as the pre-group-commit test already asserts for
            # the default config.
            before = dict(leader._group_commit)
            r1 = _set_via_leader(leader, "nw-seq1", "v1")
            r2 = _set_via_leader(leader, "nw-seq2", "v2")
            after = dict(leader._group_commit)
            self.assertEqual(r1.get("status"), "ok")
            self.assertEqual(r2.get("status"), "ok")
            self.assertEqual(_stats_delta(before, after)["fsyncs"], 2)

            for i in range(5):
                for node in cluster.nodes.values():
                    self.assertTrue(
                        wait_for_key(node, f"nw{i}", f"v{i}"),
                        f"nw{i} missing on {node.id}")
        finally:
            cluster.stop_all()


class TestGroupCommitMaxBatch(unittest.TestCase):
    def test_max_batch_caps_batch_and_flushes_early(self):
        # A generous window (50ms) so the cap, not the clock, decides
        # when a batch flushes.
        cluster = Cluster(group_commit_window_ms=50,
                          group_commit_max_batch=3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            before = dict(leader._group_commit)
            results = _concurrent_writes(leader, "mb", 6)
            after = dict(leader._group_commit)
            self.assertEqual(len(results), 6)
            self.assertTrue(all(r.get("status") == "ok" for r in results),
                            f"some writes failed: {results}")
            delta = _stats_delta(before, after)
            # Six writes in batches of at most three: two batches, two
            # fsyncs, no batch over the cap — and both flushed as soon
            # as they filled, not after the 50ms window.
            self.assertEqual(delta["batches"], 2)
            self.assertEqual(delta["entries"], 6)
            self.assertEqual(delta["fsyncs"], 2)
            self.assertEqual(after["largest_batch"], 3)

            status = cluster.status(leader.id)
            self.assertEqual(status["group_commit"]["max_batch"], 3)
            self.assertEqual(status["group_commit"]["window_ms"], 50)
            for i in range(6):
                for node in cluster.nodes.values():
                    self.assertTrue(
                        wait_for_key(node, f"mb{i}", f"v{i}"),
                        f"mb{i} missing on {node.id}")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
