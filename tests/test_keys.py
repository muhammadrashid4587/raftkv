"""Key enumeration: one ``keys`` RPC listing the store's keys.

A scan goes through the leader's ReadIndex/lease path once and comes
back with every matching key, sorted, capped at MAX_KEYS_SCAN — so it
reflects the committed state as of one read index, exactly as
linearizable as a single ``get``. These spin up real nodes (real TCP,
real threads, real disk) on localhost, like the rest of the suite.
"""

import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, keys
from raftkv.raft import MAX_KEYS_SCAN
from raftkv.store import KVStore


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

    def test_keys_start_after_pages(self):
        """A scan with start_after resumes strictly after the
        cursor: paging limit=2 through five keys returns each page
        in order with no repeats and no gaps, and the last page is
        untruncated. The reply's next_start_after is the cursor
        for the next page, and passing the previous page's last key
        by hand works the same way."""
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
            self.assertEqual(resp.get("next_start_after"), "k1")
            resp = keys(addr, limit=2,
                        start_after=resp["next_start_after"])
            self.assertEqual(resp.get("keys"), ["k2", "k3"])
            self.assertTrue(resp.get("truncated"))
            self.assertEqual(resp.get("next_start_after"), "k3")
            resp = keys(addr, limit=2, start_after="k3")
            self.assertEqual(resp.get("keys"), ["k4"])
            self.assertFalse(resp.get("truncated"))
            self.assertIsNone(resp.get("next_start_after"))
        finally:
            cluster.stop_all()

    def test_keys_start_after_with_prefix(self):
        """A cursor composes with a prefix: paging under one prefix
        never leaks keys from another, and the cursor still bounds
        the sorted order within that prefix."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("user:1", "a"), ("user:2", "b"),
                               ("user:3", "c"), ("order:1", "d")])
            resp = keys(addr, prefix="user:", limit=2)
            self.assertEqual(resp.get("keys"), ["user:1", "user:2"])
            self.assertTrue(resp.get("truncated"))
            resp = keys(addr, prefix="user:", limit=2,
                        start_after="user:2")
            self.assertEqual(resp.get("keys"), ["user:3"])
            self.assertFalse(resp.get("truncated"))
        finally:
            cluster.stop_all()

    def test_keys_start_after_beyond_last_key(self):
        """A cursor past every matching key returns an empty list,
        not an error — the page is just empty — and a cursor equal
        to a key excludes that key itself: the bound is
        exclusive."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("a", "1"), ("b", "2")])
            resp = keys(addr, start_after="zzz")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("keys"), [])
            self.assertFalse(resp.get("truncated"))
            resp = keys(addr, start_after="a")
            self.assertEqual(resp.get("keys"), ["b"])
        finally:
            cluster.stop_all()

    def test_keys_start_after_pages_whole_keyspace(self):
        """Paging one key at a time walks the whole keyspace in
        sorted order — the cursor protocol is exact, not
        approximate."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [(f"k{i}", "v") for i in range(5)])
            seen: list[str] = []
            cursor: str | None = None
            for _ in range(10):
                resp = keys(addr, limit=1, start_after=cursor)
                self.assertEqual(resp.get("status"), "ok", resp)
                seen.extend(resp.get("keys") or [])
                if not resp.get("truncated"):
                    break
                cursor = resp.get("next_start_after")
            else:
                self.fail("paging did not terminate")
            self.assertEqual(seen, ["k0", "k1", "k2", "k3", "k4"])
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
                          {"limit": True},
                          {"start_after": 123},
                          {"start_after": ["k0"]},
                          {"start_after": True}):
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
            self.assertEqual(keys(addr, start_after=123), {
                "type": "client_response", "status": "error",
                "error": "keys start_after must be a string"})
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

    def test_session_keys_start_after_and_keys_all(self):
        """A session scan takes a cursor like any other scan, and
        ``keys_all`` walks the whole keyspace in pages and returns
        every key sorted, with the page count — a session scan
        still advances the read watermark."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for i in range(5):
                    resp = session.set(f"k{i}", "v")
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.keys(limit=2, start_after="k1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("keys"), ["k2", "k3"])
                self.assertTrue(resp.get("truncated"))
                resp = session.keys_all(limit=2)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("keys"),
                                 ["k0", "k1", "k2", "k3", "k4"])
                self.assertFalse(resp.get("truncated"))
                self.assertEqual(resp.get("pages"), 3)
                self.assertGreater(session.last_read_index, 0)
                resp = session.keys_all(prefix="k1", limit=1)
                self.assertEqual(resp.get("keys"), ["k1"])
                self.assertEqual(resp.get("pages"), 1)
        finally:
            cluster.stop_all()

    def test_keys_pages_large_keyspace(self):
        """Paging through 120 keys ten at a time returns every key
        in sorted order with no repeats and no gaps, and
        ``keys_all`` walks the same keyspace in 12 pages — the
        index serves each page off a bisect seek, not a fresh
        sort of the whole keyspace."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            expected = [f"k{i:03d}" for i in range(120)]
            _write_keys(addr, [(k, "v") for k in expected])
            seen: list[str] = []
            cursor: str | None = None
            for _ in range(20):
                resp = keys(addr, limit=10, start_after=cursor)
                self.assertEqual(resp.get("status"), "ok", resp)
                seen.extend(resp.get("keys") or [])
                if not resp.get("truncated"):
                    break
                cursor = resp.get("next_start_after")
            else:
                self.fail("paging did not terminate")
            self.assertEqual(seen, expected)
            resp = keys(addr, limit=10)
            self.assertTrue(resp.get("truncated"))
            self.assertEqual(resp.get("next_start_after"), "k009")
            with Session(addr) as session:
                resp = session.keys_all(limit=10)
                self.assertEqual(resp.get("keys"), expected)
                self.assertEqual(resp.get("pages"), 12)
        finally:
            cluster.stop_all()

    def test_keys_index_survives_snapshot_and_restart(self):
        """A follower restarted after the cluster snapshotted
        (threshold 10, 25 keys written) reloads its snapshot and
        replays the suffix, rebuilding the sorted index — a stale
        scan served from that follower's own state machine pages
        the whole keyspace in order, so the scan proves the
        rebuilt index, not just the leader's."""
        cluster = Cluster(3, snapshot_threshold=10)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            expected = [f"k{i:03d}" for i in range(25)]
            _write_keys(addr, [(k, "v") for k in expected])
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            cluster.stop_node(follower.id)
            node = cluster.restart_node(follower.id)
            self.assertTrue(wait_for_key(node, "k024", "v"),
                            "restarted follower did not re-apply")
            # The restarted follower serves the scan itself only
            # while its leader lease is fresh; poll until it does
            # and the page is right, so a redirect to the leader
            # cannot mask a broken index on this node.
            deadline = time.monotonic() + 5.0
            resp = {}
            while time.monotonic() < deadline:
                resp = client_request(
                    cluster.addrs[follower.id],
                    {"type": "client", "op": "keys",
                     "limit": 10, "stale_ok": True})
                if (resp.get("lease") == "follower"
                        and resp.get("keys") == expected[:10]
                        and resp.get("truncated")):
                    break
                time.sleep(0.05)
            self.assertEqual(resp.get("lease"), "follower", resp)
            self.assertEqual(resp.get("keys"), expected[:10])
            self.assertTrue(resp.get("truncated"))
            self.assertEqual(resp.get("next_start_after"), "k009")
            resp = client_request(
                cluster.addrs[follower.id],
                {"type": "client", "op": "keys", "limit": 10,
                 "start_after": "k009", "stale_ok": True})
            self.assertEqual(resp.get("keys"), expected[10:20], resp)
            resp = client_request(
                cluster.addrs[follower.id],
                {"type": "client", "op": "keys", "limit": 10,
                 "start_after": "k019", "stale_ok": True})
            self.assertEqual(resp.get("keys"), expected[20:], resp)
            self.assertFalse(resp.get("truncated"))
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

    def test_keys_cli_start_after(self):
        """The ``keys`` CLI subcommand honors --start-after: it
        resumes past the given key, exclusive."""
        import json
        import subprocess
        import sys
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            _write_keys(addr, [("a", "1"), ("b", "2"), ("c", "3")])
            host, port = addr
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "keys",
                 "--start-after", "a"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(json.loads(out.stdout), ["b", "c"])
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "keys",
                 "--limit", "1", "--start-after", "b"],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(json.loads(out.stdout), ["c"])
        finally:
            cluster.stop_all()


class TestSortedIndex(unittest.TestCase):
    """The sorted index inside KVStore, on its own: no cluster,
    no RPC — the apply path maintains it and ``keys`` reads
    pages off it. Every test also checks the invariant the index
    exists to keep: the index is exactly the store's keys,
    sorted."""

    def _check_invariant(self, store: KVStore) -> None:
        self.assertEqual(store._keys, sorted(store._data))

    def test_index_tracks_sets_deletes_and_rewrites(self):
        """Sets insert in sorted order, overwriting a key adds no
        second index entry, a delete removes its entry, and
        re-setting a deleted key puts it back — the index always
        equals the store's keys, sorted."""
        store = KVStore()
        store.apply({"op": "set", "key": "mango", "value": "1"})
        store.apply({"op": "set", "key": "apple", "value": "2"})
        store.apply({"op": "set", "key": "zebra", "value": "3"})
        self._check_invariant(store)
        store.apply({"op": "set", "key": "apple", "value": "9"})
        self.assertEqual(store._keys, ["apple", "mango", "zebra"])
        self._check_invariant(store)
        store.apply({"op": "del", "key": "mango"})
        self.assertEqual(store._keys, ["apple", "zebra"])
        self._check_invariant(store)
        store.apply({"op": "del", "key": "missing"})
        self.assertEqual(store._keys, ["apple", "zebra"])
        self._check_invariant(store)
        store.apply({"op": "set", "key": "mango", "value": "4"})
        self.assertEqual(store._keys, ["apple", "mango", "zebra"])
        self._check_invariant(store)

    def test_index_page_returns_only_the_page(self):
        """A page returns at most ``limit`` keys plus the
        truncated flag — the caller never receives the whole
        matching keyspace, which is the point of the index: the
        page is sought and walked, not sorted fresh."""
        store = KVStore()
        for i in range(50):
            store.apply({"op": "set", "key": f"k{i:02d}",
                         "value": "v"})
        page, truncated = store.keys(limit=5)
        self.assertEqual(page, ["k00", "k01", "k02", "k03", "k04"])
        self.assertTrue(truncated)
        page, truncated = store.keys(limit=50)
        self.assertEqual(len(page), 50)
        self.assertFalse(truncated)
        page, truncated = store.keys(limit=100)
        self.assertEqual(len(page), 50)
        self.assertFalse(truncated)

    def test_index_prefix_cursor_and_limit_compose(self):
        """Prefix, cursor, and limit compose on the index: a page
        under one prefix never leaks another's keys, the cursor
        is an exclusive lower bound inside the prefix, and the
        walk stops at the end of the prefix range."""
        store = KVStore()
        for k in ["user:1", "user:2", "user:10", "user:3",
                  "order:1", "order:2"]:
            store.apply({"op": "set", "key": k, "value": "v"})
        page, truncated = store.keys(prefix="user:", limit=2)
        self.assertEqual(page, ["user:1", "user:10"])
        self.assertTrue(truncated)
        page, truncated = store.keys(prefix="user:", limit=2,
                                     start_after="user:10")
        self.assertEqual(page, ["user:2", "user:3"])
        self.assertFalse(truncated)
        # A cursor past the prefix range is an empty page.
        page, truncated = store.keys(prefix="user:",
                                     start_after="user:3")
        self.assertEqual(page, [])
        self.assertFalse(truncated)
        # A cursor from another prefix still bounds the sorted
        # order: "order:2" sorts before every "user:" key, so it
        # excludes nothing under "user:".
        page, truncated = store.keys(prefix="user:",
                                     start_after="order:2")
        self.assertEqual(page, ["user:1", "user:10", "user:2",
                                "user:3"])
        self.assertFalse(truncated)
        page, truncated = store.keys(prefix="order:",
                                     start_after="order:1")
        self.assertEqual(page, ["order:2"])
        self.assertFalse(truncated)
        # A cursor below the prefix still starts at the prefix.
        page, truncated = store.keys(prefix="user:",
                                     start_after="aaa")
        self.assertEqual(page, ["user:1", "user:10", "user:2",
                                "user:3"])
        self.assertFalse(truncated)
        self._check_invariant(store)

    def test_index_cas_does_not_disturb_it(self):
        """A CAS — successful, failed on mismatch, or failed on
        a missing key — never adds or removes an index entry: a
        successful CAS writes a key that is already indexed, and
        the failures write nothing."""
        store = KVStore()
        store.apply({"op": "set", "key": "counter", "value": "41"})
        store.apply({"op": "cas", "key": "counter", "expected": "41",
                     "value": "42"})
        self.assertEqual(store._keys, ["counter"])
        self._check_invariant(store)
        store.apply({"op": "cas", "key": "counter", "expected": "41",
                     "value": "43"})
        self.assertEqual(store._keys, ["counter"])
        store.apply({"op": "cas", "key": "ghost", "expected": "x",
                     "value": "y"})
        self.assertEqual(store._keys, ["counter"])
        self._check_invariant(store)

    def test_index_survives_load(self):
        """Loading snapshot state rebuilds the index from the
        state itself, so a node that installs a snapshot scans
        exactly like one that replayed the log to the same
        state — including paging past a cursor under a prefix."""
        store = KVStore()
        store.apply({"op": "set", "key": "stale", "value": "1"})
        store.load({"user:2": "b", "user:1": "a", "order:1": "c",
                    "user:10": "d"})
        self.assertEqual(store._keys, ["order:1", "user:1",
                                       "user:10", "user:2"])
        self._check_invariant(store)
        page, truncated = store.keys(prefix="user:", limit=2)
        self.assertEqual(page, ["user:1", "user:10"])
        self.assertTrue(truncated)
        page, truncated = store.keys(prefix="user:", limit=2,
                                     start_after="user:10")
        self.assertEqual(page, ["user:2"])
        self.assertFalse(truncated)
        # Writes after a load keep maintaining the rebuilt index.
        store.apply({"op": "set", "key": "user:3", "value": "e"})
        store.apply({"op": "del", "key": "order:1"})
        self.assertEqual(store._keys, ["user:1", "user:10",
                                       "user:2", "user:3"])
        self._check_invariant(store)


if __name__ == "__main__":
    unittest.main()
