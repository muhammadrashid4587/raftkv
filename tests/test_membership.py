"""Tests for single-server membership changes (Raft dissertation §6).

These spin up real nodes (real TCP transport, real threads, real disk
persistence) on localhost, so they exercise the actual code paths rather
than mocks. Adding a server means: replicate the log to it as a
non-voting peer, wait for it to catch up, then commit a config entry
that makes it a voter. Removing one means: commit a config entry that
drops it from the voting config; a removed server stops participating
(and a leader that removes itself steps down).
"""

import threading
import time
import unittest

from harness import Cluster, client_request, free_port, wait_for_key


def _set(addr, key, value):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def _get(addr, key):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def _add_server(addr, server_id, server_addr):
    return client_request(addr, {"type": "client", "op": "add_server",
                                 "server_id": server_id,
                                 "addr": [server_addr[0], server_addr[1]]})


def _remove_server(addr, server_id):
    return client_request(addr, {"type": "client", "op": "remove_server",
                                 "server_id": server_id})


class TestAddServer(unittest.TestCase):
    def test_add_fourth_node_catches_up_and_serves_reads(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            for i in range(5):
                self.assertEqual(
                    _set(cluster.addrs[leader.id], f"k{i}", f"v{i}").get("status"),
                    "ok")
            node4 = cluster.add_node("node4")
            resp = _add_server(cluster.addrs[leader.id], "node4",
                               cluster.addrs["node4"])
            self.assertEqual(resp.get("status"), "ok", resp)
            # The new node caught up: it holds every key written before
            # the add, and the config on every node now has 4 members.
            for i in range(5):
                self.assertTrue(wait_for_key(node4, f"k{i}", f"v{i}"),
                                f"node4 never caught up key k{i}")
            for nid in ("node1", "node2", "node3", "node4"):
                self.assertTrue(
                    cluster.wait_for_config(
                        nid, ["node1", "node2", "node3", "node4"]),
                    f"{nid} config is not the 4-node config: "
                    f"{cluster.status(nid).get('config')}")
            # A write after the add replicates to the new node, and a
            # read sent to it (redirected to the leader) returns it.
            self.assertEqual(
                _set(cluster.addrs[leader.id], "after", "yes").get("status"), "ok")
            self.assertTrue(wait_for_key(node4, "after", "yes"))
            self.assertEqual(
                _get(cluster.addrs["node4"], "after").get("value"), "yes")
        finally:
            cluster.stop_all()

    def test_add_server_already_a_member_is_rejected(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            node4 = cluster.add_node("node4")
            self.assertEqual(
                _add_server(cluster.addrs[leader.id], "node4",
                            cluster.addrs["node4"]).get("status"), "ok")
            resp = _add_server(cluster.addrs[leader.id], "node4",
                               cluster.addrs["node4"])
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("already a member", resp.get("error", ""))
        finally:
            cluster.stop_all()


class TestRemoveServer(unittest.TestCase):
    def test_remove_node_cluster_keeps_working(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            resp = _remove_server(cluster.addrs[leader.id], follower.id)
            self.assertEqual(resp.get("status"), "ok", resp)
            # The remaining two nodes form the new config, and writes
            # still commit (leader + one follower is a majority of 2).
            self.assertTrue(cluster.wait_for_config(
                leader.id, [n for n in ("node1", "node2", "node3")
                            if n != follower.id]))
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k2", "v2").get("status"), "ok")
            self.assertEqual(
                _get(cluster.addrs[leader.id], "k2").get("value"), "v2")
            # The removed node knows it is out, and it does not disrupt
            # the cluster: the same leader stays leader for a second.
            self.assertTrue(cluster.wait_for_removed(follower.id),
                            "removed follower never reported removed")
            self.assertFalse(cluster.nodes[follower.id].is_leader)
            time.sleep(1.0)
            self.assertTrue(leader.is_leader)
            self.assertIs(cluster.leader(), leader)
        finally:
            cluster.stop_all()

    def test_remove_leader_triggers_new_election(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            old_leader = cluster.wait_for_leader()
            self.assertEqual(
                _set(cluster.addrs[old_leader.id], "k", "v").get("status"), "ok")
            resp = _remove_server(cluster.addrs[old_leader.id], old_leader.id)
            self.assertEqual(resp.get("status"), "ok", resp)
            # The old leader stepped down and reports itself removed.
            self.assertTrue(cluster.wait_for_removed(old_leader.id),
                            "old leader never reported removed")
            self.assertFalse(old_leader.is_leader)
            # The two survivors elect a new leader (majority of 2) and
            # keep serving writes. Stable, not just sighted: a
            # candidate that already bumped its term can still depose
            # a freshly elected leader during churn.
            new_leader = cluster.wait_for_stable_leader()
            self.assertIsNot(new_leader, old_leader)
            self.assertEqual(
                _set(cluster.addrs[new_leader.id], "k2", "v2").get("status"),
                "ok")
            self.assertEqual(
                _get(cluster.addrs[new_leader.id], "k2").get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_add_then_remove_keeps_three_nodes_and_survives_leader_loss(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            cluster.add_node("node4")
            self.assertEqual(
                _add_server(cluster.addrs[leader.id], "node4",
                            cluster.addrs["node4"]).get("status"), "ok")
            self.assertTrue(cluster.wait_for_config(
                "node1", ["node1", "node2", "node3", "node4"]))
            # Remove a follower: back to three voting members.
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader and n.id != "node4")
            self.assertEqual(
                _remove_server(cluster.addrs[leader.id], follower.id)
                .get("status"), "ok")
            remaining = [n for n in ("node1", "node2", "node3", "node4")
                         if n != follower.id]
            self.assertTrue(cluster.wait_for_config(leader.id, remaining))
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            # Kill the leader: the three survivors re-elect and writes
            # continue — the removal did not leave a stale majority.
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_stable_leader()
            self.assertEqual(
                _set(cluster.addrs[new_leader.id], "k2", "v2").get("status"),
                "ok")
            self.assertEqual(
                _get(cluster.addrs[new_leader.id], "k2").get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_remove_unknown_server_is_rejected(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            resp = _remove_server(cluster.addrs[leader.id], "node99")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("not a member", resp.get("error", ""))
        finally:
            cluster.stop_all()


class TestMembershipSerialization(unittest.TestCase):
    def test_concurrent_membership_changes_rejected(self):
        # A down server makes the first add sit in its catch-up wait
        # (1s with the short timeout), so a second change arriving
        # meanwhile must be rejected, not run alongside it.
        cluster = Cluster(3, membership_catchup_timeout=1.0)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            for i in range(3):
                self.assertEqual(
                    _set(cluster.addrs[leader.id], f"k{i}", f"v{i}")
                    .get("status"), "ok")
            down_addr = ("127.0.0.1", free_port())
            results: dict = {}

            def _slow_add():
                results["slow"] = _add_server(cluster.addrs[leader.id],
                                              "node5", down_addr)

            t = threading.Thread(target=_slow_add, daemon=True)
            t.start()
            time.sleep(0.3)  # let the slow add reach the leader and set its flag
            node4 = cluster.add_node("node4")
            resp_add = _add_server(cluster.addrs[leader.id], "node4",
                                   cluster.addrs["node4"])
            self.assertEqual(resp_add.get("status"), "error", resp_add)
            self.assertIn("already in progress", resp_add.get("error", ""))
            resp_remove = _remove_server(cluster.addrs[leader.id], "node2")
            self.assertEqual(resp_remove.get("status"), "error", resp_remove)
            self.assertIn("already in progress", resp_remove.get("error", ""))
            t.join(timeout=10.0)
            self.assertEqual(results["slow"].get("status"), "error",
                             results["slow"])
            self.assertIn("did not catch up", results["slow"].get("error", ""))
            # The flag cleared when the failed change ended: the same
            # add now succeeds, proving the cluster is not wedged.
            self.assertEqual(
                _add_server(cluster.addrs[leader.id], "node4",
                            cluster.addrs["node4"]).get("status"), "ok")
            self.assertTrue(cluster.wait_for_config(
                "node1", ["node1", "node2", "node3", "node4"]))
        finally:
            cluster.stop_all()


class TestMembershipPersistence(unittest.TestCase):
    def test_membership_survives_restart(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            cluster.add_node("node4")
            self.assertEqual(
                _add_server(cluster.addrs[leader.id], "node4",
                            cluster.addrs["node4"]).get("status"), "ok")
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            # Restart every node one by one: the config is persisted
            # (meta.json), so the restarted cluster still has 4 members
            # and keeps serving — no node falls back to a stale config.
            for nid in ("node1", "node2", "node3", "node4"):
                cluster.stop_node(nid)
                cluster.restart_node(nid)
            # Four rapid restarts churn elections; wait for the cluster
            # to settle on one leader instead of trusting the first
            # sighting.
            new_leader = cluster.wait_for_stable_leader()
            self.assertTrue(cluster.wait_for_config(
                new_leader.id, ["node1", "node2", "node3", "node4"]))
            self.assertEqual(
                _get(cluster.addrs[new_leader.id], "k").get("value"), "v")
            self.assertEqual(
                _set(cluster.addrs[new_leader.id], "k2", "v2").get("status"),
                "ok")
            self.assertTrue(wait_for_key(cluster.nodes["node4"], "k2", "v2"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
