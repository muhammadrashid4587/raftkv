"""Tests for read-your-writes client sessions.

A session is one client under one ``client_id``. Every write it acks
advances its watermark to the log index (and term) of that write's
entry, and every read it issues carries the watermark — so a client
that wrote through the leader and then reads from any node, or from a
new leader after a failover, never sees its own write missing: the
serving node waits until it has applied up to the watermark before
answering, and a session read never falls back to serving whatever
happens to be applied.
"""

import unittest

from harness import Cluster, client_request
from raftkv.client import Session


class TestSessionReadYourWrites(unittest.TestCase):
    def test_write_response_carries_log_index_and_term(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            session = Session(cluster.addrs[leader.id], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            # The write names the log position it committed at.
            self.assertIsInstance(resp.get("index"), int, resp)
            self.assertGreaterEqual(resp["index"], 1)
            self.assertIsInstance(resp.get("term"), int, resp)
            self.assertGreaterEqual(resp["term"], 1)
            # The session's watermark tracks the ack.
            self.assertEqual(session.last_write_index, resp["index"])
            self.assertEqual(session.last_write_term, resp["term"])
            # A later write commits at a later index.
            resp2 = session.set("k", "v2")
            self.assertEqual(resp2.get("status"), "ok", resp2)
            self.assertGreater(resp2["index"], resp["index"])
            self.assertEqual(session.last_write_index, resp2["index"])
        finally:
            cluster.stop_all()

    def test_session_read_after_write_sees_write_from_any_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            cluster.wait_for_leader()
            # The session talks to a follower first: writes redirect
            # to the leader, reads come back through it too.
            session = Session(cluster.addrs["node2"], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            # Same session, now talking to the other follower: the
            # read carries the watermark, so it must see v1.
            session.node_addr = cluster.addrs["node3"]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            # Overwrite, then read from the third node: still no
            # stale value, even though the client keeps moving.
            session.set("k", "v2")
            session.node_addr = cluster.addrs["node1"]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
            # A delete through the session is visible the same way.
            session.delete("k")
            session.node_addr = cluster.addrs["node2"]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(resp.get("value"))
        finally:
            cluster.stop_all()

    def test_read_your_writes_across_leader_failover(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            session = Session(cluster.addrs["node2"], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            # Kill the leader the write went through, then read
            # immediately from a surviving node — before the new
            # leader has necessarily applied the write. The session
            # read must wait for the watermark, never answer with
            # the pre-write state.
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            self.assertNotEqual(new_leader.id, leader.id)
            survivors = [i for i in cluster.live if i != leader.id]
            session.node_addr = cluster.addrs[survivors[0]]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            # Writes through the new leader advance the watermark,
            # and the next read still sees them.
            resp = session.set("k", "v2")
            self.assertEqual(resp.get("status"), "ok", resp)
            session.node_addr = cluster.addrs[survivors[1]]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_session_read_with_unknown_index_errors_not_stale(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v1"})
            self.assertEqual(resp.get("status"), "ok", resp)
            # An index no node can have is an error, never a read
            # served from whatever happens to be applied.
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k",
                                         "min_commit_index": 10 ** 9})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not in this node's log", resp.get("error", ""))
            # Same for an index just past the end of the log.
            last = leader._last_log_index()
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k",
                                         "min_commit_index": last + 5})
            self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_session_read_with_wrong_term_errors(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            session = Session(cluster.addrs[leader.id], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            # The right index with the wrong term names a different
            # log history than this cluster's: refuse, don't serve.
            resp = client_request(
                cluster.addrs[leader.id],
                {"type": "client", "op": "get", "key": "k",
                 "min_commit_index": session.last_write_index,
                 "session_term": session.last_write_term + 99})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("term mismatch", resp.get("error", ""))
            # The right index and term serve normally.
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
        finally:
            cluster.stop_all()

    def test_dedup_retry_returns_same_index(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = leader._last_log_index()
            msg = {"type": "client", "op": "set", "key": "k", "value": "v",
                   "client_id": "s1", "request_id": "r1"}
            first = client_request(addr, msg)
            self.assertEqual(first.get("status"), "ok", first)
            # The retry is a no-op that reports the first attempt's
            # index — so the session watermark does not move, and no
            # second entry is appended.
            retry = client_request(addr, msg)
            self.assertEqual(retry.get("status"), "ok", retry)
            self.assertEqual(retry.get("index"), first.get("index"))
            self.assertEqual(retry.get("term"), first.get("term"))
            self.assertEqual(leader._last_log_index(), before + 1)
        finally:
            cluster.stop_all()

    def test_session_get_without_writes_reads_normally(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v"})
            self.assertEqual(resp.get("status"), "ok", resp)
            # A fresh session has watermark 0: its reads are plain
            # ReadIndex reads and still see committed values.
            session = Session(addr, "fresh")
            self.assertEqual(session.last_write_index, 0)
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v")
            # A CAS through the session advances the watermark too:
            # the entry committed even though the read side is what
            # matters next.
            resp = session.cas("k", "v", "v2")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertGreater(session.last_write_index, 0)
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
