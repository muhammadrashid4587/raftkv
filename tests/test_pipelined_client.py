"""Pipelined client reads: framing/order correctness against a real
local server.

``get_many`` sends many gets over one persistent connection without
waiting between them; the server dispatches frames from one
connection in order, so the replies must come back aligned with the
keys — same order, same values, including missing keys. These spin
up real nodes (real TCP, real threads, real disk) on localhost, like
the rest of the suite.
"""

import unittest

from harness import Cluster, client_request, free_port
from raftkv.client import get_many
from raftkv.transport import PipelinedConnection


def _write_keys(cluster: Cluster, addr, n: int) -> None:
    for i in range(n):
        resp = client_request(
            addr, {"type": "client", "op": "set",
                   "key": f"pk{i}", "value": f"v{i}"})
        assert resp.get("status") == "ok", resp


class TestPipelinedClient(unittest.TestCase):
    def test_get_many_returns_values_in_order(self):
        """50 gets pipelined over one connection come back in key
        order with the right values — framing and reply alignment
        hold across a multi-frame batch."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(cluster, addr, 50)
            replies = get_many(addr, [f"pk{i}" for i in range(50)])
            self.assertEqual(len(replies), 50)
            for i, resp in enumerate(replies):
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), f"v{i}", resp)
        finally:
            cluster.stop_all()

    def test_get_many_includes_missing_keys(self):
        """A missing key in the middle of a batch answers ok with
        value None and does not shift the replies after it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(cluster, addr, 3)
            replies = get_many(addr, ["pk0", "nope", "pk2"])
            self.assertEqual(len(replies), 3)
            self.assertEqual(replies[0].get("value"), "v0")
            self.assertEqual(replies[1].get("status"), "ok")
            self.assertIsNone(replies[1].get("value"))
            self.assertEqual(replies[2].get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_get_many_follows_redirect_from_follower(self):
        """Pipelining at a follower re-sends the batch to the leader
        it names, and the replies still come back in order."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(cluster, addr, 10)
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            replies = get_many(cluster.addrs[follower.id],
                               [f"pk{i}" for i in range(10)])
            self.assertEqual(len(replies), 10)
            for i, resp in enumerate(replies):
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), f"v{i}", resp)
        finally:
            cluster.stop_all()

    def test_get_many_large_batch(self):
        """A 200-request batch — deeper than the default depth of 50,
        so it splits into several batches on one connection — still
        returns every value in order."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(cluster, addr, 200)
            replies = get_many(addr, [f"pk{i}" for i in range(200)],
                               depth=50)
            self.assertEqual(len(replies), 200)
            for i, resp in enumerate(replies):
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("value"), f"v{i}", resp)
        finally:
            cluster.stop_all()

    def test_get_many_empty(self):
        """No keys means no requests and no connection — an empty
        list back, without touching the server."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            self.assertEqual(
                get_many(cluster.addrs[leader.id], []), [])
        finally:
            cluster.stop_all()

    def test_pipelined_connection_unreachable_returns_nones(self):
        """A batch to a dead port returns one None per message
        instead of raising — the caller sees the failure in the
        reply positions, like a failed one-shot RPC returns None."""
        port = free_port()
        conn = PipelinedConnection("127.0.0.1", port, timeout=1.0)
        try:
            replies = conn.call_many(
                [{"type": "client", "op": "get", "key": "k"},
                 {"type": "client", "op": "get", "key": "k2"}])
            self.assertEqual(replies, [None, None])
        finally:
            conn.close()

    def test_pipelined_connection_reconnects_after_close(self):
        """Closing the connection and calling again re-opens it —
        a failed or finished batch does not retire the client."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(cluster, addr, 2)
            conn = PipelinedConnection(addr[0], addr[1], timeout=5.0)
            try:
                first = conn.call_many(
                    [{"type": "client", "op": "get", "key": "pk0"}])
                self.assertEqual(first[0].get("value"), "v0")
                conn.close()
                second = conn.call_many(
                    [{"type": "client", "op": "get", "key": "pk1"}])
                self.assertEqual(second[0].get("value"), "v1")
            finally:
                conn.close()
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
