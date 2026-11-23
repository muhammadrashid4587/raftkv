"""Tests for raftkv follower-lease reads (v0.20.0).

A follower that heard from the current term's leader within
FOLLOWER_LEASE_DURATION (100ms, shorter than any follower's 150ms
minimum election timeout) knows no new leader can have been elected
since — so it may serve an explicit stale read (``"stale_ok": true``,
``get --stale`` on the CLI) straight from its applied state instead
of redirecting to the leader. Its applied state is always a
committed prefix of that leader's log, never divergent state.

The stale read is bounded-stale, not linearizable: it reflects the
leader's committed state as of the follower's last leader contact,
so a write committed after that contact may be missing. In a
healthy cluster heartbeats arrive every 50ms, so the staleness is
at most ~one heartbeat interval plus apply lag, and a partitioned
follower stops serving within 100ms of losing contact and redirects
instead. The flag is opt-in on purpose: a plain read without it
still redirects to the leader and is served linearizably, so the
chaos harness's linearizability checks keep passing. Session reads
never take this path either: they carry a watermark the follower
may not have applied, so they still redirect to the leader.

These tests send RPCs directly (``send_rpc``), not via the harness's
``client_request`` — that helper follows redirects, which would hide
whether the follower served the read itself or sent the client away.
"""

import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.transport import send_rpc


def _set(addr, key, value):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def _get_direct(addr, key, **extra):
    msg = {"type": "client", "op": "get", "key": key}
    msg.update(extra)
    return send_rpc(addr[0], addr[1], msg, timeout=5.0)


def _get_stale(addr, key):
    """A stale read: the follower may serve it from its lease."""
    return _get_direct(addr, key, stale_ok=True)


class TestFollowerLeaseReads(unittest.TestCase):
    def test_follower_serves_plain_read_from_lease(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            self.assertTrue(wait_for_key(follower, "k", "v"),
                            "follower never applied the write")
            # The follower heard from the leader within the last 100ms
            # (heartbeats go out every 50ms), so it holds a lease.
            self.assertTrue(follower._follower_lease_valid())
            rounds_before = leader._confirm_rounds
            log_before = follower._last_log_index()
            resp = _get_stale(cluster.addrs[follower.id], "k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v")
            self.assertEqual(resp.get("lease"), "follower")
            # Served locally: the leader ran no confirmation round,
            # and the read appended no log entry anywhere.
            self.assertEqual(leader._confirm_rounds, rounds_before)
            self.assertEqual(follower._last_log_index(), log_before)
        finally:
            cluster.stop_all()

    def test_follower_lease_read_sees_latest_committed_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v1").get("status"), "ok")
            self.assertTrue(wait_for_key(follower, "k", "v1"))
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v2").get("status"), "ok")
            self.assertTrue(wait_for_key(follower, "k", "v2"),
                            "follower never applied the overwrite")
            resp = _get_stale(cluster.addrs[follower.id], "k")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "v2")
            self.assertEqual(resp.get("lease"), "follower")
        finally:
            cluster.stop_all()

    def test_follower_lease_read_carries_applied_index(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            self.assertTrue(wait_for_key(follower, "k", "v"))
            resp = _get_stale(cluster.addrs[follower.id], "k")
            self.assertEqual(resp.get("status"), "ok", resp)
            # The reply names the applied index the value reflects,
            # so a client can treat it like any other read reply.
            self.assertEqual(resp.get("read_index"), follower.last_applied)
            self.assertEqual(resp.get("read_term"),
                             follower._term_at(follower.last_applied))
        finally:
            cluster.stop_all()

    def test_plain_read_still_redirects_from_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            self.assertTrue(wait_for_key(follower, "k", "v"))
            # The lease is fresh — but without the stale flag the
            # follower must still redirect: a plain read stays
            # linearizable through the leader. This is what keeps
            # the chaos harness's linearizability checks passing.
            self.assertTrue(follower._follower_lease_valid())
            resp = _get_direct(cluster.addrs[follower.id], "k")
            self.assertEqual(resp.get("status"), "not_leader", resp)
            self.assertEqual(resp.get("leader"), leader.id)
            self.assertNotIn("lease", resp)
        finally:
            cluster.stop_all()

    def test_follower_lease_expires_after_isolation(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            self.assertTrue(wait_for_key(follower, "k", "v"))
            self.assertTrue(follower._follower_lease_valid())
            # Isolate the follower: no more heartbeats arrive. After
            # 120ms the 100ms lease has expired, but the 150ms minimum
            # election timeout has not — so the node is still a
            # follower, and only the stale lease makes it redirect.
            cluster.isolate(follower.id)
            time.sleep(0.12)
            self.assertEqual(follower.role, "follower")
            self.assertFalse(follower._follower_lease_valid())
            self.assertFalse(
                cluster.status(follower.id).get("follower_lease_valid"))
            resp = _get_stale(cluster.addrs[follower.id], "k")
            self.assertEqual(resp.get("status"), "not_leader", resp)
            self.assertNotIn("lease", resp)
        finally:
            cluster.stop_all()

    def test_session_read_still_redirects_from_follower(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            written = _set(cluster.addrs[leader.id], "k", "v")
            self.assertEqual(written.get("status"), "ok", written)
            self.assertTrue(wait_for_key(follower, "k", "v"))
            self.assertTrue(follower._follower_lease_valid())
            # A session read carries a watermark: even with a fresh
            # lease the follower must not serve it — it may not have
            # applied that far, and the session's guarantee is that
            # its own write is visible. It redirects to the leader,
            # which waits for the watermark.
            resp = _get_direct(cluster.addrs[follower.id], "k",
                               min_commit_index=written["index"],
                               session_term=written["term"])
            self.assertEqual(resp.get("status"), "not_leader", resp)
            self.assertEqual(resp.get("leader"), leader.id)
        finally:
            cluster.stop_all()

    def test_follower_without_leader_contact_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            cluster.wait_for_leader()
            # A node that never heard from a leader holds no lease
            # and must redirect, not serve from empty state.
            node4 = cluster.add_node("node4")
            self.assertIsNone(node4._last_leader_contact)
            self.assertFalse(node4._follower_lease_valid())
            resp = _get_direct(cluster.addrs["node4"], "k")
            self.assertEqual(resp.get("status"), "not_leader", resp)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
