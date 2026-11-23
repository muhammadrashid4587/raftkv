"""Jepsen-style partition test: split the cluster and prove the
majority keeps working, the minority cannot commit, and the cluster
heals with no lost acked write and a linearizable history.

A real network partition is simulated with RaftNode.partition():
peer RPCs are dropped in both directions while client and status
traffic still flow — so a partitioned node still answers status and
still claims leadership to a client that asks it directly, exactly
like a real split. This is the fault the kill/restart chaos test
cannot produce: the nodes are all *up*, they just cannot hear each
other, so the cluster must elect a new leader on the majority side
while the isolated old leader sits on uncommitted entries it can
never commit.

Two scenarios, each with writer threads (session ids, retry until
acked) and reader threads recording every observation in a shared
history, the same shape as test_chaos:

* the leader is isolated from both followers — the two followers
  elect a new leader, writes keep acking through it, the isolated
  leader's commit index does not move and a write sent to it
  directly never acks; after the heal the old leader rejoins, its
  uncommitted tail is overwritten, and all three nodes converge;
* one follower is cut off (2-vs-1) — the majority keeps working
  with the same leader, the minority falls behind, and after the
  heal it catches up.

In both cases every acked write must read back on every node, all
three state machines must hold exactly the acked writes, and the
recorded history must be linearizable.
"""

import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.linearizability import History, record_client_op


def _leader_addr(cluster: Cluster, excluded: set[str]):
    """Address of a node that currently thinks it is leader and is
    not in ``excluded`` — or, if no such node is visible yet, any
    live node (it redirects to the leader, or errors and the caller
    retries). Writers and readers re-resolve on every attempt, so a
    client pinned to a leader that gets isolated finds the new one
    instead of retrying into the partition forever."""
    for node_id, node in cluster.nodes.items():
        if node_id not in excluded and node.is_leader:
            return cluster.addrs[node_id]
    for node_id in cluster.live:
        if node_id not in excluded:
            return cluster.addrs[node_id]
    return next(iter(cluster.addrs.values()))


class _Workload:
    """Writer + reader threads recording a shared linearizability
    history, as in test_chaos but resolving the leader per attempt."""

    def __init__(self, cluster: Cluster, excluded: set[str],
                 min_writes: int = 15, threads: int = 2):
        self.cluster = cluster
        self.excluded = excluded
        self.min_writes = min_writes
        self.threads = threads
        self.acked: dict[str, str] = {}
        self.lock = threading.Lock()
        self.errors: list[str] = []
        self.history = History()
        self.written_counts = [0] * threads
        self.done = threading.Event()
        self.stop_readers = threading.Event()
        self._writers: list[threading.Thread] = []
        self._readers: list[threading.Thread] = []

    def _writer(self, thread_id: int) -> None:
        i = 0
        while not self.done.is_set() or i < self.min_writes:
            key = f"part:t{thread_id}:{i}"
            value = f"v{thread_id}-{i}"
            msg = {"type": "client", "op": "set", "key": key,
                   "value": value,
                   "client_id": f"part-{thread_id}",
                   "request_id": str(i)}
            deadline = time.monotonic() + 60.0
            first_invoke = time.monotonic()
            while True:
                resp = client_request(
                    _leader_addr(self.cluster, self.excluded), msg,
                    timeout=3.0)
                if resp.get("status") == "ok":
                    with self.lock:
                        self.acked[key] = value
                        self.written_counts[thread_id] = i + 1
                    record_client_op(self.history, f"part-{thread_id}",
                                     i, msg, resp, first_invoke,
                                     time.monotonic())
                    break
                if time.monotonic() > deadline:
                    self.errors.append(
                        f"{key}: never acked, last reply {resp}")
                    break
                time.sleep(0.1)
            i += 1

    def _reader(self, thread_id: int) -> None:
        import random
        rng = random.Random(3000 + thread_id)
        seq = 0
        while not self.stop_readers.is_set():
            with self.lock:
                counts = list(self.written_counts)
            candidates = [(t, i) for t in range(self.threads)
                          for i in range(counts[t])]
            if not candidates:
                time.sleep(0.05)
                continue
            t, i = rng.choice(candidates)
            msg = {"type": "client", "op": "get",
                   "key": f"part:t{t}:{i}"}
            invoke = time.monotonic()
            resp = client_request(
                _leader_addr(self.cluster, self.excluded), msg,
                timeout=3.0)
            record_client_op(self.history, f"reader-{thread_id}", seq,
                             msg, resp, invoke, time.monotonic())
            seq += 1
            time.sleep(0.005)

    def start(self) -> None:
        self._writers = [
            threading.Thread(target=self._writer, args=(t,))
            for t in range(self.threads)
        ]
        self._readers = [
            threading.Thread(target=self._reader, args=(t,))
            for t in range(2)
        ]
        for w in self._writers:
            w.start()
        for r in self._readers:
            r.start()

    def finish(self) -> None:
        """Stop the writers, then the readers, and join them."""
        self.done.set()
        for w in self._writers:
            w.join()
        self.stop_readers.set()
        for r in self._readers:
            r.join(timeout=5.0)

    def verify(self, test: unittest.TestCase, cluster: Cluster) -> None:
        """Every acked write present on every node, all nodes in the
        same state, and the recorded history linearizable."""
        test.assertEqual(self.errors, [], "writes that never acked")
        test.assertGreaterEqual(len(self.acked), 2 * self.min_writes,
                                "every write should have acked")
        lin = self.history.check()
        test.assertTrue(lin.ok, str(lin))
        test.assertGreaterEqual(lin.reads_checked, 10,
                                "readers should have observed reads")
        cluster.wait_for_stable_leader()
        for node in cluster.nodes.values():
            for key, value in self.acked.items():
                test.assertTrue(wait_for_key(node, key, value),
                                f"{node.id} missing {key}={value}")
        states = [node.sm.items() for node in cluster.nodes.values()]
        for state in states[1:]:
            test.assertEqual(state, states[0],
                             "nodes diverged after partition healed")
        test.assertEqual(states[0], self.acked)


def _wait_for_convergence(cluster: Cluster,
                          timeout: float = 15.0) -> None:
    """Poll status until all three nodes report the same commit
    index and last applied index."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stats = [cluster.status(i) for i in cluster.nodes]
        if all(stats) and len({(s["commit_index"], s["last_applied"])
                              for s in stats}) == 1:
            return
        time.sleep(0.1)
    raise AssertionError(
        "nodes did not converge: " +
        ", ".join(f"{i}=commit {s.get('commit_index')}/"
                  f"applied {s.get('last_applied')}" if s else f"{i}=down"
                  for i, s in
                  ((n, cluster.status(n)) for n in cluster.nodes)))


class TestPartition(unittest.TestCase):
    def test_partition_api(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            cluster.partition("node1", "node2")
            self.assertEqual(cluster.nodes["node1"].partitioned_peers(),
                             {"node2"})
            self.assertEqual(cluster.nodes["node2"].partitioned_peers(),
                             {"node1"})
            self.assertEqual(cluster.nodes["node3"].partitioned_peers(),
                             set())
            cluster.heal_partition("node1", "node2")
            self.assertEqual(cluster.nodes["node1"].partitioned_peers(),
                             set())
            cluster.isolate("node3")
            self.assertEqual(cluster.nodes["node3"].partitioned_peers(),
                             {"node1", "node2"})
            cluster.heal_all()
            for node in cluster.nodes.values():
                self.assertEqual(node.partitioned_peers(), set())
        finally:
            cluster.stop_all()

    def test_leader_isolated_majority_keeps_working_and_heals(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            old_leader = cluster.wait_for_leader()
            excluded: set[str] = set()
            work = _Workload(cluster, excluded)
            work.start()
            try:
                # Let some writes ack before the split, so the
                # history has a pre-partition prefix.
                time.sleep(1.0)
                acked_before = len(work.acked)
                self.assertGreater(acked_before, 0,
                                   "writes should ack pre-partition")

                # Isolate the leader from both followers.
                cluster.isolate(old_leader.id)
                excluded.add(old_leader.id)

                # The two followers must elect a new leader.
                deadline = time.monotonic() + 10.0
                new_leader = None
                while time.monotonic() < deadline:
                    for node_id, node in cluster.nodes.items():
                        if (node_id != old_leader.id
                                and node.is_leader):
                            new_leader = node
                            break
                    if new_leader is not None:
                        break
                    time.sleep(0.05)
                self.assertIsNotNone(
                    new_leader, "majority side elects a new leader")
                assert new_leader is not None
                self.assertNotEqual(new_leader.id, old_leader.id)

                # The isolated leader cannot commit: a write sent to
                # it directly never acks, and its commit index does
                # not move while the majority keeps committing.
                before = cluster.status(old_leader.id)["commit_index"]
                probe = client_request(
                    cluster.addrs[old_leader.id],
                    {"type": "client", "op": "set",
                     "key": "partition:probe", "value": "x",
                     "client_id": "probe", "request_id": "1"},
                    timeout=3.0)
                self.assertNotEqual(probe.get("status"), "ok",
                                    "isolated leader must not ack writes")
                self.assertEqual(
                    cluster.status(old_leader.id)["commit_index"],
                    before, "isolated leader's commit index must not move")

                # Writes keep acking through the new leader while the
                # split lasts.
                time.sleep(3.0)
                self.assertGreater(len(work.acked), acked_before,
                                   "writes must keep acking on the "
                                   "majority side during the partition")

                # Heal: the old leader rejoins, its uncommitted tail
                # is overwritten, and the cluster converges.
                cluster.heal_all()
                excluded.clear()
                _wait_for_convergence(cluster)
            finally:
                work.finish()
            work.verify(self, cluster)
        finally:
            cluster.stop_all()

    def test_minority_split_majority_keeps_working(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            minority = next(n for n in cluster.nodes
                            if n != leader.id)
            excluded: set[str] = set()
            work = _Workload(cluster, excluded)
            work.start()
            try:
                time.sleep(1.0)
                acked_before = len(work.acked)
                self.assertGreater(acked_before, 0,
                                   "writes should ack pre-partition")
                majority_commit = cluster.status(
                    leader.id)["commit_index"]

                # Cut one follower off (2-vs-1).
                cluster.isolate(minority)
                excluded.add(minority)

                # The same leader stays leader — the majority is
                # intact, so no election is needed — and writes keep
                # acking through it.
                time.sleep(1.0)
                self.assertTrue(cluster.nodes[leader.id].is_leader,
                                "majority leader must stay leader")
                time.sleep(2.0)
                self.assertGreater(len(work.acked), acked_before,
                                   "writes must keep acking on the "
                                   "majority side during the split")
                self.assertGreater(
                    cluster.status(leader.id)["commit_index"],
                    majority_commit,
                    "majority must keep committing during the split")
                # The minority falls behind and stays there.
                self.assertLess(
                    cluster.status(minority)["commit_index"],
                    cluster.status(leader.id)["commit_index"],
                    "minority must fall behind during the split")

                # Heal: the minority catches up and the cluster
                # converges.
                cluster.heal_all()
                excluded.clear()
                _wait_for_convergence(cluster)
            finally:
                work.finish()
            work.verify(self, cluster)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
