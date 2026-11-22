"""Tests for raftkv client session deduplication (at-most-once writes).

A client write may carry a (client_id, request_id) session id. The
leader records the pair when the entry applies, and a retry with the
same pair — after a lost response, after a leader change, or while the
first attempt is still in flight — returns the first attempt's result
instead of appending a second log entry. The table is derived from the
applied log, so a new leader rebuilds it by replay and a snapshot
carries it; writes without a session id behave exactly as before.
"""

import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key


def write(addr, op="set", key="k", value="v", client_id=None,
          request_id=None):
    msg = {"type": "client", "op": op, "key": key}
    if op == "set":
        msg["value"] = value
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestClientDedup(unittest.TestCase):
    def test_retry_with_same_request_id_appends_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = write(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            # The retry is a no-op: same result, no second entry.
            resp = write(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertTrue(wait_for_key(leader, "k", "v"))
        finally:
            cluster.stop_all()

    def test_concurrent_duplicate_writes_append_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            results = []
            threads = [
                threading.Thread(
                    target=lambda: results.append(
                        write(addr, client_id="c1", request_id="r1")))
                for _ in range(5)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            self.assertEqual(len(results), 5)
            for resp in results:
                self.assertEqual(resp.get("status"), "ok", resp)
            # Five identical concurrent submissions, one log entry:
            # the later ones joined the first attempt's waiter.
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertTrue(wait_for_key(leader, "k", "v"))
        finally:
            cluster.stop_all()

    def test_different_request_ids_are_distinct_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                write(addr, value="one", client_id="c1",
                      request_id="r1").get("status"), "ok")
            self.assertEqual(
                write(addr, value="two", client_id="c1",
                      request_id="r2").get("status"), "ok")
            self.assertEqual(count_entries(leader, "r1"), 1)
            self.assertEqual(count_entries(leader, "r2"), 1)
            self.assertTrue(wait_for_key(leader, "k", "two"))
        finally:
            cluster.stop_all()

    def test_retry_after_leader_change_is_deduped(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                write(addr, client_id="c1", request_id="r1")
                .get("status"), "ok")
            old_leader_id = leader.id
            cluster.stop_node(old_leader_id)
            new_leader = cluster.wait_for_stable_leader()
            self.assertNotEqual(new_leader.id, old_leader_id)
            # The new leader rebuilt the dedup table by replaying the
            # log, so the retry is still a no-op — even though the
            # first attempt's response may never have reached the
            # client (the old leader is dead).
            addr = cluster.addrs[new_leader.id]
            resp = write(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            for node_id in cluster.live:
                self.assertLessEqual(count_entries(cluster.nodes[node_id],
                                                   "r1"), 1)
            self.assertTrue(wait_for_key(new_leader, "k", "v"))
        finally:
            cluster.stop_all()

    def test_dedup_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(
                write(addr, key="keep", value="v", client_id="c1",
                      request_id="r1").get("status"), "ok")
            # Enough further writes to compact the log past the
            # threshold, so the r1 entry leaves the log and lives only
            # in the snapshot's dedup table.
            for i in range(10):
                resp = write(addr, key=f"k{i}", value="x")
                self.assertEqual(resp.get("status"), "ok", resp)
            self.assertTrue(wait_for_key(leader, "k9", "x"))
            deadline = time.monotonic() + 5.0
            while (time.monotonic() < deadline
                   and leader.snapshot_index == 0):
                time.sleep(0.05)
            self.assertGreater(leader.snapshot_index, 0)
            self.assertIn(("c1", "r1"), leader._dedup)
            # Restart the leader: the snapshot reloads the dedup table,
            # so the retry still appends nothing.
            leader_id = leader.id
            cluster.stop_node(leader_id)
            cluster.restart_node(leader_id)
            new_leader = cluster.wait_for_stable_leader()
            addr = cluster.addrs[new_leader.id]
            resp = write(addr, key="keep", value="v", client_id="c1",
                         request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIn(("c1", "r1"), new_leader._dedup)
            self.assertTrue(wait_for_key(new_leader, "keep", "v"))
        finally:
            cluster.stop_all()

    def test_delete_with_request_id_is_deduped(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            self.assertEqual(
                write(addr, op="del", client_id="c1",
                      request_id="d1").get("status"), "ok")
            resp = write(addr, op="del", client_id="c1",
                         request_id="d1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(count_entries(leader, "d1"), 1)
            self.assertTrue(wait_for_key(leader, "k", None))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
