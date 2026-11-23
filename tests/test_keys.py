"""Key enumeration: one ``keys`` RPC listing the store's keys.

A scan goes through the leader's ReadIndex/lease path once and comes
back with every matching key, sorted, capped at MAX_KEYS_SCAN — so it
reflects the committed state as of one read index, exactly as
linearizable as a single ``get``. These spin up real nodes (real TCP,
real threads, real disk) on localhost, like the rest of the suite.
"""

import time
import unittest

from harness import Cluster, client_request
from raftkv.client import Session, keys
from raftkv.raft import MAX_KEYS_SCAN


def _write_keys(addr, keys_values) -> None:
    for k, v in keys_values:
        resp = client_request(
            addr, {"type": "client", "op": "set", "key": k, "value": v})
        assert resp.get("status") == "ok", resp


class TestKeys(unittest.TestCase):
    def test_keys_returns_all_keys_sorted(self):
        """A scan with no prefix returns every key in sorted order,
        with truncated False and the read index/term a session
        would use to advance its watermark."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("zebra", "1"), ("apple", "2"),
                               ("mango", "3")])
            resp = keys(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"),
                             ["apple", "mango", "zebra"])
            self.assertFalse(resp.get("truncated"))
            self.assertIsNotNone(resp.get("read_index"))
            self.assertIsNotNone(resp.get("read_term"))
        finally:
            cluster.stop_all()

    def test_keys_prefix_filters(self):
        """A prefix scan returns only keys under that prefix, still
        sorted — and a prefix matching nothing returns an empty
        list, not an error."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("user:1", "a"), ("user:2", "b"),
                               ("order:1", "c"), ("user:10", "d")])
            resp = keys(addr, prefix="user:")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"),
                             ["user:1", "user:10", "user:2"])
            self.assertFalse(resp.get("truncated"))
            resp = keys(addr, prefix="nope:")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), [])
        finally:
            cluster.stop_all()

    def test_keys_limit_truncates(self):
        """A limit below the number of matching keys returns the
        first that many in sorted order and says truncated, so the
        caller knows more matched; a limit at or above the count
        returns everything untruncated."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [(f"k{i}", "v") for i in range(5)])
            resp = keys(addr, limit=2)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), ["k0", "k1"])
            self.assertTrue(resp.get("truncated"))
            resp = keys(addr, limit=5)
            self.assertEqual(resp.get("keys"),
                             ["k0", "k1", "k2", "k3", "k4"])
            self.assertFalse(resp.get("truncated"))
        finally:
            cluster.stop_all()

    def test_keys_empty_store(self):
        """A scan of an empty store returns an empty list — a scan
        is a read, not an error, even when there is nothing to
        list."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            resp = keys(cluster.addrs[leader.id])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), [])
            self.assertFalse(resp.get("truncated"))
        finally:
            cluster.stop_all()

    def test_keys_reflects_deletes(self):
        """A deleted key is gone from the next scan — the scan reads
        the applied state machine, so it sees exactly the committed
        state as of its read index."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("a", "1"), ("b", "2"), ("c", "3")])
            resp = client_request(
                addr, {"type": "client", "op": "del", "key": "b"})
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = keys(addr)
            self.assertEqual(resp.get("keys"), ["a", "c"])
        finally:
            cluster.stop_all()

    def test_keys_from_follower_redirects(self):
        """A scan sent to a follower is redirected to the leader and
        still answers with the right keys — the client follows it
        like any other read."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("a", "1"), ("b", "2")])
            resp = keys(cluster.addrs[follower.id])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), ["a", "b"])
        finally:
            cluster.stop_all()

    def test_keys_stale_served_from_follower_lease(self):
        """A scan with stale_ok is served by a follower holding a
        fresh leader lease, without a redirect hop — the reply
        carries the follower lease marker like a single stale get."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            _write_keys(cluster.addrs[leader.id], [("a", "1"),
                                                   ("b", "2")])
            # A stale lease read is bounded-stale by design: a write
            # committed after the follower's last heartbeat may
            # legitimately be missing. Wait until the follower has
            # applied both keys before asserting it serves them —
            # otherwise the test races replication lag, not the code.
            deadline = time.monotonic() + 5.0
            resp = {}
            while time.monotonic() < deadline:
                resp = client_request(
                    cluster.addrs[follower.id],
                    {"type": "client", "op": "keys", "stale_ok": True})
                if resp.get("keys") == ["a", "b"]:
                    break
                time.sleep(0.05)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), ["a", "b"])
            self.assertEqual(resp.get("lease"), "follower")
        finally:
            cluster.stop_all()

    def test_keys_rejects_bad_params(self):
        """A non-string prefix, a limit below 1, above the cap, or
        not an integer is an error, not a scan — the reply is
        bounded so one frame can never hold an unbounded list."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            for params in ({"prefix": 123},
                          {"limit": 0},
                          {"limit": -1},
                          {"limit": MAX_KEYS_SCAN + 1},
                          {"limit": "ten"},
                          {"limit": True}):
                resp = client_request(
                    addr, {"type": "client", "op": "keys", **params})
                self.assertEqual(resp.get("status"), "error",
                                 (params, resp))
            self.assertEqual(keys(addr, prefix=123), {
                "type": "client_response", "status": "error",
                "error": "keys prefix must be a string"})
            self.assertEqual(keys(addr, limit=0), {
                "type": "client_response", "status": "error",
                "error": "keys limit must be between 1 and 1000"})
        finally:
            cluster.stop_all()

    def test_keys_rejects_bad_read_params(self):
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
                    addr, {"type": "client", "op": "keys", **params})
                self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_session_keys_is_read_your_writes(self):
        """A session that just wrote a key sees it in a scan — the
        scan carries the session watermark and the serving node
        waits until the write is applied — and the reply advances
        the session's read watermark, so a later plain read never
        goes backwards past it."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("sk0", "v0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.keys(prefix="sk")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("keys"), ["sk0"])
                self.assertGreater(session.last_read_index, 0)
                # The watermark the scan returned is enforced: a
                # plain session read after it sees the same state.
                resp = session.get("sk0")
                self.assertEqual(resp.get("value"), "v0", resp)
        finally:
            cluster.stop_all()

    def test_keys_cli(self):
        """The ``keys`` CLI subcommand prints the keys as one JSON
        list, honoring --prefix and --limit."""
        import json
        import subprocess
        import sys
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("user:1", "a"), ("user:2", "b"),
                               ("order:1", "c")])
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "keys",
                 "--prefix", "user:"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(json.loads(out.stdout),
                             ["user:1", "user:2"])
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "keys", "--limit", "1"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(json.loads(out.stdout), ["order:1"])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
