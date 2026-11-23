"""Tests for joint-consensus membership changes (Raft dissertation §6).

A membership change now runs in two phases: a ``joint_config`` entry
opens the transition (C-old,new — a majority of BOTH configs must agree
from here on), and a ``final_config`` entry closes it by adopting C-new.
These spin up real nodes (real TCP transport, real threads, real disk
persistence) on localhost, so they exercise the actual code paths rather
than mocks. They prove:

- an old-majority-only quorum cannot commit anything while the joint
  phase is open (the safety point of joint consensus);
- an add through the client completes both phases and the new node
  serves;
- a leader change mid-transition still completes it (the new leader
  drives the final entry from _become_leader);
- a snapshot taken mid-transition carries the joint config, and a node
  restarted mid-transition resumes in the joint phase.
"""

import json
import time
import unittest
from pathlib import Path

from harness import Cluster, client_request, wait_for_key
from raftkv.raft import _config_list_of


def _set(addr, key, value):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def _get(addr, key):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def _add_server(addr, server_id, server_addr):
    return client_request(addr, {"type": "client", "op": "add_server",
                                 "server_id": server_id,
                                 "addr": [server_addr[0], server_addr[1]]})


def _joint_command(leader, change_op, server_id, addr):
    """The joint_config entry for one change, built the way
    _submit_joint_change builds it — used to drive a transition
    directly, without the client thread, so a test can hold the
    cluster in the joint phase and kill the leader there."""
    with leader.lock:
        new_config = dict(leader.config)
        if change_op == "add_server":
            new_config[server_id] = tuple(addr) if addr else None
        else:  # remove_server
            new_config.pop(server_id, None)
        return {"op": "joint_config", "change_op": change_op,
                "server_id": server_id,
                "addr": [addr[0], addr[1]] if addr else None,
                "old": _config_list_of(leader.config),
                "new": _config_list_of(new_config)}


def _catch_up_as_nonvoting(leader, server_id, addr, timeout=5.0):
    """Replicate the log to a not-yet-member as a non-voting peer, the
    way _add_server does before submitting the joint entry."""
    with leader.lock:
        target = leader._last_log_index()
        leader.peers[server_id] = addr
        leader._non_voting.add(server_id)
        leader._init_peer_state(server_id, 1)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with leader.lock:
            if leader.match_index.get(server_id, 0) >= target:
                return True
        time.sleep(0.02)
    return False


def _wait_for_joint(node, members, timeout=5.0):
    """Poll until a node's in-memory joint config is ``members``."""
    deadline = time.monotonic() + timeout
    want = sorted(members)
    while time.monotonic() < deadline:
        with node.lock:
            joint = node._joint_new
        if joint is not None and sorted(joint) == want:
            return True
        time.sleep(0.02)
    return False


class TestJointQuorum(unittest.TestCase):
    def test_old_majority_alone_cannot_commit_during_joint(self):
        # The safety point of joint consensus, checked directly on
        # _advance_commit: with C-old={node1,node2,node3} and
        # C-new={node1,node2,node3,node4}, the holders {leader,
        # node2} are a majority of C-old but only 2 of 4 in C-new —
        # so a current-term entry must NOT commit. Once node4 acks
        # too, both majorities hold and it commits.
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_stable_leader()
            # The leader's election no-op must already be committed
            # before the test appends its own entry: if it is still
            # in flight, the test entry lands at index 2 and
            # _advance_commit can legitimately commit index 1
            # through the same holders (others[1] holds index 1),
            # which is correct Raft behaviour but not what this test
            # is checking. Wait until the log is fully committed, so
            # the appended entry is the only uncommitted one.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                with leader.lock:
                    if leader.commit_index >= leader._last_log_index():
                        break
                time.sleep(0.02)
            else:
                self.fail("leader's log did not commit before the test")
            others = [n for n in ("node1", "node2", "node3")
                      if n != leader.id]
            with leader.lock:
                idx = leader._append_entry(
                    {"term": leader.current_term,
                     "command": {"op": "noop"}})
                before = leader.commit_index
                self.assertGreater(idx, before)
                leader._joint_new = {
                    "node1": cluster.addrs["node1"],
                    "node2": cluster.addrs["node2"],
                    "node3": cluster.addrs["node3"],
                    "node4": ("127.0.0.1", 59999),
                }
                # Holders {leader, others[0]}: a majority of C-old
                # but only 2 of 4 in C-new.
                leader.match_index = {others[0]: idx,
                                      others[1]: idx - 1,
                                      "node4": 0}
                leader._advance_commit()
                self.assertEqual(
                    leader.commit_index, before,
                    "old-majority-only holders committed during joint")
                leader.match_index["node4"] = idx
                leader._advance_commit()
                self.assertEqual(
                    leader.commit_index, idx,
                    "both-majority holders did not commit during joint")
                leader._joint_new = None
        finally:
            cluster.stop_all()


class TestJointAdd(unittest.TestCase):
    def test_add_server_completes_joint_then_final(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            for i in range(3):
                self.assertEqual(
                    _set(cluster.addrs[leader.id], f"k{i}", f"v{i}")
                    .get("status"), "ok")
            node4 = cluster.add_node("node4")
            resp = _add_server(cluster.addrs[leader.id], "node4",
                               cluster.addrs["node4"])
            self.assertEqual(resp.get("status"), "ok", resp)
            # The transition closed: every node reports the 4-node
            # config and no joint phase in flight.
            for nid in ("node1", "node2", "node3", "node4"):
                self.assertTrue(
                    cluster.wait_for_config(
                        nid, ["node1", "node2", "node3", "node4"]),
                    f"{nid} config is not the 4-node config: "
                    f"{cluster.status(nid).get('config')}")
                self.assertIsNone(cluster.status(nid).get("joint_new"),
                                  f"{nid} still in joint phase")
            # A write after the add replicates to the new node, and a
            # read sent to it returns it.
            self.assertEqual(
                _set(cluster.addrs[leader.id], "after", "yes").get("status"),
                "ok")
            self.assertTrue(wait_for_key(node4, "after", "yes"))
            self.assertEqual(
                _get(cluster.addrs["node4"], "after").get("value"), "yes")
        finally:
            cluster.stop_all()


class TestLeaderChangeMidTransition(unittest.TestCase):
    def test_leader_change_mid_transition_completes(self):
        # Drive the cluster into the joint phase directly (no client
        # thread to finish it), freeze it there by partitioning two
        # members — a majority of C-new can no longer form, so the
        # final entry cannot commit — and prove the snapshot taken
        # in that window carries the joint config and a restarted
        # follower resumes in it. Then kill the leader (still
        # partitioned, so its stuck completion thread cannot finish
        # the change first), heal, and the new leader must complete
        # the transition on its own (the _become_leader hook): the
        # cluster ends in C-new and keeps serving writes.
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            node4 = cluster.add_node("node4")
            self.assertTrue(
                _catch_up_as_nonvoting(leader, "node4",
                                       cluster.addrs["node4"]),
                "node4 never caught up as a non-voting peer")
            joint = _joint_command(leader, "add_server", "node4",
                                   cluster.addrs["node4"])
            self.assertEqual(
                leader._submit_config_entry(joint).get("status"), "ok")
            # Freeze the transition: partition the leader from node3
            # and node4 — in-process flag flips, microseconds, so
            # they land before the completion thread the joint apply
            # spawned can get acks for the final entry. (Stopping
            # the nodes instead would NOT work: stop() blocks in the
            # server shutdown and the transition completes first.)
            # With node3 and node4 cut off, only {leader, node2} can
            # ack — a majority of C-old but only 2 of 4 in C-new, so
            # the final entry stalls and the cluster sits in joint.
            cluster.partition(leader.id, "node3")
            cluster.partition(leader.id, "node4")
            self.assertTrue(
                _wait_for_joint(leader, ["node1", "node2", "node3",
                                         "node4"]),
                "leader never entered the joint phase")
            with leader.lock:
                self.assertIsNotNone(leader._joint_new)
                leader._take_snapshot(leader.last_applied)
            snap = json.loads(
                (Path(leader.data_dir) / "snapshot.json").read_text())
            self.assertIsNotNone(snap.get("joint_new"),
                                 "snapshot taken mid-transition has no "
                                 "joint config")
            self.assertEqual(sorted(m for m, _ in snap["joint_new"]),
                             ["node1", "node2", "node3", "node4"])
            # A follower restarted mid-transition resumes in the
            # joint phase: it replays the committed joint entry (and
            # its meta.json carries the joint config too).
            cluster.stop_node("node2")
            node2 = cluster.restart_node("node2")
            self.assertTrue(
                _wait_for_joint(node2, ["node1", "node2", "node3",
                                        "node4"]),
                "restarted follower did not resume in joint phase")
            # Bring the partitioned members back only after the old
            # leader is dead: healing first would let its stuck
            # completion thread finish the transition itself, and
            # this test must prove the NEW leader finishes it.
            old_leader_id = leader.id
            cluster.stop_node(old_leader_id)
            cluster.heal_all()
            new_leader = cluster.wait_for_stable_leader()
            self.assertIsNot(new_leader, leader)
            self.assertTrue(cluster.wait_for_config(
                new_leader.id, ["node1", "node2", "node3", "node4"]),
                "new leader did not complete the joint transition: "
                f"{cluster.status(new_leader.id).get('config')}")
            self.assertIsNone(cluster.status(new_leader.id)
                              .get("joint_new"))
            self.assertEqual(
                _set(cluster.addrs[new_leader.id], "k2", "v2")
                .get("status"), "ok")
            self.assertEqual(
                _get(cluster.addrs[new_leader.id], "k2").get("value"),
                "v2")
            self.assertTrue(wait_for_key(cluster.nodes["node4"],
                                         "k2", "v2"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
