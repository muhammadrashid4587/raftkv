"""Multi-get reads: one ``mget`` RPC serving many keys.

One request goes through the leader's ReadIndex/lease path once and
comes back with ``values`` aligned with ``keys`` (None for a missing
key), so the per-key cost is one state machine lookup instead of one
dispatch each. These spin up real nodes (real TCP, real threads, real
disk) on localhost, like the rest of the suite.
"""

import unittest

from harness import Cluster, client_request
from raftkv.client import Session, mget


def _write_keys(addr, n: int) -> None:
    for i in range(n):
        resp = client_request(
            addr, {"type": "client", "op": "set",
                   "key": f"mk{i}", "value": f"v{i}"})
        assert resp.get("status") == "ok", resp


class TestMget(unittest.TestCase):
    def test_mget_returns_values_in_order(self):
        """50 keys in one mget come back in key order with the right
        values — one dispatch serving the whole batch."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, 50)
            resp = mget(addr, [f"mk{i}" for i in range(50)])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("values"),
                             [f"v{i}" for i in range(50)])
            self.assertIsNotNone(resp.get("read_index"))
            self.assertIsNotNone(resp.get("read_term"))
        finally:
            cluster.stop_all()

    def test_mget_includes_missing_keys(self):
        """A missing key in the middle of a batch answers ok with
        None in that position and does not shift the values after
        it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, 3)
            resp = mget(addr, ["mk0", "nope", "mk2"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("values"), ["v0", None, "v2"])
        finally:
            cluster.stop_all()

    def test_mget_from_follower_redirects(self):
        """An mget sent to a follower is redirected to the leader and
        still answers with the right values — the client follows it
        like any other read."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            addr = cluster.addrs[follower.id]
            _write_keys(cluster.addrs[leader.id], 3)
            resp = mget(addr, ["mk0", "mk1", "mk2"])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("values"), ["v0", "v1", "v2"])
        finally:
            cluster.stop_all()

    def test_mget_stale_served_from_follower_lease(self):
        """An mget with stale_ok is served by a follower holding a
        fresh leader lease, without a redirect hop — the reply
        carries the follower lease marker like a single stale get."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            _write_keys(cluster.addrs[leader.id], 2)
            # A stale lease read is bounded-stale by design: a write
            # committed after the follower's last heartbeat may
            # legitimately be missing. Wait until the follower has
            # applied both keys before asserting it serves them —
            # otherwise the test races replication lag, not the code.
            import time
            deadline = time.monotonic() + 5.0
            resp = {}
            while time.monotonic() < deadline:
                resp = client_request(
                    cluster.addrs[follower.id],
                    {"type": "client", "op": "mget",
                     "keys": ["mk0", "mk1"], "stale_ok": True})
                if resp.get("values") == ["v0", "v1"]:
                    break
                time.sleep(0.05)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("values"), ["v0", "v1"])
            self.assertEqual(resp.get("lease"), "follower")
        finally:
            cluster.stop_all()

    def test_mget_rejects_bad_key_lists(self):
        """An empty list, a non-list, a non-string key, or more than
        MAX_MGET_KEYS keys is an error, not a read — the batch is
        bounded so one frame can never hold an unbounded list."""
        from raftkv.raft import MAX_MGET_KEYS
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for bad in ([], "mk0", [1], ["mk0", None],
                        [f"k{i}" for i in range(MAX_MGET_KEYS + 1)]):
                resp = client_request(
                    addr, {"type": "client", "op": "mget",
                           "keys": bad})
                self.assertEqual(resp.get("status"), "error", (bad, resp))
            self.assertEqual(mget(addr, []), {
                "type": "client_response", "status": "error",
                "error": "mget takes a list of 1-1000 keys"})
        finally:
            cluster.stop_all()

    def test_mget_rejects_bad_read_params(self):
        """A non-integer min_commit_index or a negative one is an
        error, exactly as for a single get — both ride the same
        ReadIndex path, so both validate the same way."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for params in ({"min_commit_index": "x"},
                          {"min_commit_index": -1}):
                resp = client_request(
                    addr, {"type": "client", "op": "mget",
                           "keys": ["mk0"], **params})
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_session_mget_is_read_your_writes(self):
        """A session that just wrote a key reads it back through
        mget — the batch carries the session watermark and the
        serving node waits until the write is applied — and the
        reply advances the session's read watermark, so a later
        plain read never goes backwards past it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("mk0", "v0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.mget(["mk0", "missing"])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("values"), ["v0", None])
                self.assertGreater(session.last_read_index, 0)
                # The watermark the mget returned is enforced: a
                # plain session read after it sees the same state.
                resp = session.get("mk0")
                self.assertEqual(resp.get("value"), "v0", resp)
        finally:
            cluster.stop_all()

    def test_mget_matches_pipelined_get_many(self):
        """The same 50 keys read by one mget and by pipelined
        get_many agree exactly — two ways of reading a batch must
        never disagree about the state."""
        from raftkv.client import get_many
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, 50)
            keys = [f"mk{i}" for i in range(50)]
            keys[25] = "missing"
            resp = mget(addr, keys)
            self.assertEqual(resp.get("status"), "ok", resp)
            replies = get_many(addr, keys)
            self.assertEqual(
                resp.get("values"),
                [r.get("value") for r in replies])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
