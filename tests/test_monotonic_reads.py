"""Tests for monotonic-reads client sessions.

A session remembers the highest log index it has *read* — the
applied index the returned value reflected — and every later read
carries the higher of its write watermark and its read watermark,
so the serving node waits until it has applied at least that far.
Reads therefore never go backwards to an older state, even across
a leader failover or when the client keeps moving between nodes.
"""

import io
import unittest
from contextlib import redirect_stdout

from harness import Cluster, client_request
from raftkv.client import Session, main as client_main


class TestMonotonicReads(unittest.TestCase):
    def test_read_after_write_returns_the_write(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            cluster.wait_for_leader()
            session = Session(cluster.addrs["node2"], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
        finally:
            cluster.stop_all()

    def test_read_response_carries_applied_index_and_term(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v1"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = client_request(addr, {"type": "client", "op": "get",
                                         "key": "k"})
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            # The read names the applied index its value reflects,
            # and the term of the entry at that index.
            self.assertIsInstance(resp.get("read_index"), int, resp)
            self.assertGreaterEqual(resp["read_index"], 1)
            self.assertIsInstance(resp.get("read_term"), int, resp)
            self.assertGreaterEqual(resp["read_term"], 1)
            self.assertEqual(resp["read_term"],
                             leader._term_at(resp["read_index"]))
        finally:
            cluster.stop_all()

    def test_session_read_advances_read_watermark(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            cluster.wait_for_leader()
            session = Session(cluster.addrs["node1"], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(session.last_read_index, 0)
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            # The served read raised the read watermark at least as
            # far as the write it returned.
            self.assertGreaterEqual(session.last_read_index,
                                    session.last_write_index)
            self.assertGreater(session.last_read_index, 0)
            self.assertGreater(session.last_read_term, 0)
            self.assertEqual(session.token["last_read_index"],
                             session.last_read_index)
            self.assertEqual(session.token["last_read_term"],
                             session.last_read_term)
            # A second read keeps the watermark (it cannot move
            # backwards) and still sees the write.
            before = session.last_read_index
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            self.assertGreaterEqual(session.last_read_index, before)
        finally:
            cluster.stop_all()

    def test_read_never_goes_backwards_after_higher_index_read(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            # Another client writes twice; the session has not read
            # anything yet, so simulate having read the newer state
            # by advancing its read watermark to the second write.
            w1 = client_request(addr, {"type": "client", "op": "set",
                                       "key": "k", "value": "v1"})
            self.assertEqual(w1.get("status"), "ok", w1)
            w2 = client_request(addr, {"type": "client", "op": "set",
                                       "key": "k", "value": "v2"})
            self.assertEqual(w2.get("status"), "ok", w2)
            session = Session(addr, "s1")
            session.last_read_index = int(w2["index"])
            session.last_read_term = int(w2["term"])
            # The next read must wait for the watermark and return
            # the newer value, never the older one the session has
            # already passed.
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
            self.assertGreaterEqual(session.last_read_index,
                                    int(w2["index"]))
            # And a live read after a third write moves forward
            # again, never back to v2.
            w3 = client_request(addr, {"type": "client", "op": "set",
                                       "key": "k", "value": "v3"})
            self.assertEqual(w3.get("status"), "ok", w3)
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v3")
            self.assertGreaterEqual(session.last_read_index,
                                    int(w3["index"]))
        finally:
            cluster.stop_all()

    def test_advanced_read_watermark_is_enforced_not_ignored(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            session = Session(addr, "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            # A watermark past the end of the log is an error, not a
            # stale read — proving the watermark is really sent and
            # checked, not silently dropped.
            session.last_read_index = 10 ** 9
            session.last_read_term = 1
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not in this node's log",
                          resp.get("error", ""))
            # A watermark with the wrong term for its index errors
            # too, rather than serving from a different history. The
            # read watermark has to win over the write watermark for
            # its term to be the one sent, so advance it with a
            # second write from another client first.
            w2 = client_request(addr, {"type": "client", "op": "set",
                                       "key": "k", "value": "v2"})
            self.assertEqual(w2.get("status"), "ok", w2)
            session.last_read_index = int(w2["index"])
            session.last_read_term = int(w2["term"]) + 99
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("term mismatch", resp.get("error", ""))
        finally:
            cluster.stop_all()

    def test_session_read_on_follower_waits_for_watermark(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            # The session talks only to followers: writes redirect
            # to the leader, reads come back through the follower
            # too — and the read watermark is still honoured.
            session = Session(cluster.addrs["node2"], "s1")
            resp = session.set("k", "v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            session.node_addr = cluster.addrs["node3"]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            watermark = session.last_read_index
            self.assertGreaterEqual(watermark, session.last_write_index)
            # Kill the leader: the next read goes through a follower
            # to a brand-new leader that may not have applied the
            # watermark yet. It must wait and return v1, never the
            # pre-write state.
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            self.assertNotEqual(new_leader.id, leader.id)
            survivors = [i for i in cluster.live if i != leader.id]
            session.node_addr = cluster.addrs[survivors[0]]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v1")
            self.assertGreaterEqual(session.last_read_index, watermark)
            # Writes through the new leader advance both watermarks,
            # and the next read — from the other follower — still
            # sees them.
            resp = session.set("k", "v2")
            self.assertEqual(resp.get("status"), "ok", resp)
            session.node_addr = cluster.addrs[survivors[1]]
            resp = session.get("k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_cli_min_read_index_flags(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            node = f"{host}:{port}"
            resp = client_request(addr, {"type": "client", "op": "set",
                                         "key": "k", "value": "v1"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp2 = client_request(addr, {"type": "client", "op": "set",
                                          "key": "k", "value": "v2"})
            self.assertEqual(resp2.get("status"), "ok", resp2)
            # A CLI read carrying the read watermark returns the
            # newer value.
            out = io.StringIO()
            with redirect_stdout(out):
                rc = client_main(["--node", node, "get", "k",
                                  "--min-read-index",
                                  str(resp2["index"]),
                                  "--read-term", str(resp2["term"])])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "v2")
            # A read watermark past the end of the log fails.
            out = io.StringIO()
            with redirect_stdout(out):
                rc = client_main(["--node", node, "get", "k",
                                  "--min-read-index", str(10 ** 9),
                                  "--read-term", "1"])
            self.assertEqual(rc, 1)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
