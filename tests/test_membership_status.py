"""Tests for membership-change status reporting (v0.23.0).

The status RPC now carries a ``membership`` object — the current
voting config, the joint config while a transition is open, which
phase the transition is in, and the change itself (op, server, log
indexes) — and the client exposes it as ``membership-status``. These
spin up real nodes (real TCP transport, real threads, real disk
persistence) on localhost and check the report at each point of a
transition: stable, a joint entry appended but uncommitted, a joint
entry committed with the final entry still open, and back to stable
once the change completes — plus the CLI printing it, which also
covers the add-server / remove-server CLI paths.
"""

import contextlib
import io
import json
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import main as client_main
from test_joint_consensus import (
    _catch_up_as_nonvoting,
    _joint_command,
    _wait_for_joint,
)


def _membership(addr):
    return client_request(addr, {"type": "status"}).get("membership") or {}


class TestStableMembershipStatus(unittest.TestCase):
    def test_status_reports_stable_membership(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            for nid in ("node1", "node2", "node3"):
                m = cluster.status(nid).get("membership") or {}
                self.assertEqual(m.get("phase"), "stable", m)
                self.assertEqual(m.get("config"),
                                 ["node1", "node2", "node3"], m)
                self.assertIsNone(m.get("joint_new"), m)
                self.assertFalse(m.get("change_in_progress"), m)
                self.assertIsNone(m.get("pending_change"), m)
                self.assertFalse(m.get("joint_committed"), m)
                self.assertEqual(m.get("non_voting"), [], m)
                self.assertEqual(m.get("removal_pending"), {}, m)
            self.assertEqual(
                (cluster.status(leader.id).get("membership") or {})
                .get("config"),
                ["node1", "node2", "node3"])
        finally:
            cluster.stop_all()


class TestUncommittedJointStatus(unittest.TestCase):
    def test_status_reports_uncommitted_joint_entry(self):
        # Append a joint entry directly to an isolated leader: with
        # no follower reachable it can never commit, so the report
        # must show the change as open but uncommitted — and a
        # follower that never saw the entry still reports stable,
        # because the report is each node's own view.
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            others = [n for n in ("node1", "node2", "node3")
                      if n != leader.id]
            cluster.isolate(leader.id)
            joint = _joint_command(leader, "add_server", "node4",
                                   ("127.0.0.1", 59999))
            with leader.lock:
                idx = leader._append_entry(
                    {"term": leader.current_term, "command": joint})
            m = cluster.status(leader.id).get("membership") or {}
            self.assertEqual(m.get("phase"), "joint_pending", m)
            self.assertTrue(m.get("change_in_progress"), m)
            self.assertFalse(m.get("joint_committed"), m)
            pending = m.get("pending_change") or {}
            self.assertEqual(pending.get("op"), "add_server", m)
            self.assertEqual(pending.get("server_id"), "node4", m)
            self.assertEqual(pending.get("joint_index"), idx, m)
            self.assertIsNone(pending.get("final_index"), m)
            # C-old is still the voting config: the joint entry has
            # not committed, so nothing has changed yet.
            self.assertEqual(m.get("config"),
                             ["node1", "node2", "node3"], m)
            self.assertIsNone(m.get("joint_new"), m)
            follower_status = cluster.status(others[0]).get("membership")
            self.assertEqual((follower_status or {}).get("phase"),
                             "stable", follower_status)
        finally:
            cluster.stop_all()


class TestJointCommittedStatus(unittest.TestCase):
    def test_status_reports_joint_phase_mid_transition(self):
        # Freeze the cluster in the joint phase deterministically:
        # mark a completion driver as already running before the
        # joint entry applies, so the thread _apply_config_entry
        # spawns waits instead of appending the final entry — the
        # transition then sits in joint_committed until the test
        # drives it to its end itself. (Partitioning after the joint
        # submit, the way the leader-change test does it, races the
        # completion thread under load.)
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            self.assertEqual(
                client_request(cluster.addrs[leader.id],
                               {"type": "client", "op": "set",
                                "key": "k", "value": "v"}).get("status"),
                "ok")
            node4 = cluster.add_node("node4")
            self.assertTrue(
                _catch_up_as_nonvoting(leader, "node4",
                                       cluster.addrs["node4"]),
                "node4 never caught up as a non-voting peer")
            joint = _joint_command(leader, "add_server", "node4",
                                   cluster.addrs["node4"])
            with leader.lock:
                leader._joint_completion_running = True
                idx = leader._append_entry(
                    {"term": leader.current_term, "command": joint})
            # The joint entry commits and applies; the spawned
            # completion thread finds a driver already running and
            # waits, so the cluster stays in the joint phase.
            self.assertTrue(
                _wait_for_joint(leader, ["node1", "node2", "node3",
                                         "node4"]),
                "leader never entered the joint phase")
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                m = cluster.status(leader.id).get("membership") or {}
                if m.get("phase") == "joint_committed":
                    break
                time.sleep(0.02)
            self.assertEqual(m.get("phase"), "joint_committed", m)
            self.assertTrue(m.get("change_in_progress"), m)
            self.assertTrue(m.get("joint_committed"), m)
            self.assertEqual(m.get("config"),
                             ["node1", "node2", "node3"], m)
            self.assertEqual(m.get("joint_new"),
                             ["node1", "node2", "node3", "node4"], m)
            pending = m.get("pending_change") or {}
            self.assertEqual(pending.get("op"), "add_server", m)
            self.assertEqual(pending.get("server_id"), "node4", m)
            self.assertEqual(pending.get("joint_index"), idx, m)
            self.assertIsNone(pending.get("final_index"), m)
            # The added server is still non-voting until the final
            # entry applies.
            self.assertIn("node4", m.get("non_voting") or [], m)
            # A follower that applied the joint entry reports the
            # same transition from its own log. It learns the new
            # commit index a heartbeat behind the leader, so poll
            # its status rather than reading it once.
            follower_id = "node2" if "node2" != leader.id else "node3"
            deadline = time.monotonic() + 5.0
            fm: dict = {}
            while time.monotonic() < deadline:
                fm = cluster.status(follower_id).get("membership") or {}
                if fm.get("phase") == "joint_committed":
                    break
                time.sleep(0.02)
            self.assertEqual(fm.get("phase"), "joint_committed", fm)
            self.assertEqual((fm.get("pending_change") or {})
                             .get("server_id"), "node4", fm)
            self.assertEqual((fm.get("pending_change") or {})
                             .get("joint_index"), idx, fm)
            # Drive the transition to its end: release the fake
            # driver (its waiter then gives up harmlessly) and run
            # the real completion.
            with leader.lock:
                leader._joint_completion_running = False
            slot: dict = {}

            def _complete():
                slot["resp"] = leader._complete_joint_transition()

            t = threading.Thread(target=_complete, daemon=True)
            t.start()
            t.join(timeout=10.0)
            self.assertFalse(t.is_alive(),
                             "completion thread did not return")
            self.assertEqual(slot["resp"].get("status"), "ok",
                             slot["resp"])
            self.assertTrue(cluster.wait_for_config(
                leader.id, ["node1", "node2", "node3", "node4"]),
                "cluster did not reach the 4-node config: "
                f"{cluster.status(leader.id).get('config')}")
            m = cluster.status(leader.id).get("membership") or {}
            self.assertEqual(m.get("phase"), "stable", m)
            self.assertIsNone(m.get("pending_change"), m)
            self.assertIsNone(m.get("joint_new"), m)
            self.assertEqual(m.get("non_voting"), [], m)
            self.assertTrue(wait_for_key(node4, "k", "v"))
        finally:
            cluster.stop_all()


class TestMembershipStatusCli(unittest.TestCase):
    def test_membership_status_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"

            def _run_cli(argv):
                out, err = io.StringIO(), io.StringIO()
                slot: dict = {}

                def _call():
                    with contextlib.redirect_stdout(out), \
                            contextlib.redirect_stderr(err):
                        slot["rc"] = client_main(argv)

                t = threading.Thread(target=_call, daemon=True)
                t.start()
                t.join(timeout=15.0)
                self.assertFalse(t.is_alive(), f"CLI {argv} hung")
                return slot["rc"], out.getvalue(), err.getvalue()

            rc, out, err = _run_cli(["--node", node,
                                     "membership-status"])
            self.assertEqual(rc, 0, err)
            self.assertIn("membership: stable", out)
            self.assertIn("config: node1, node2, node3", out)
            rc, out, err = _run_cli(["--node", node,
                                     "membership-status", "--json"])
            self.assertEqual(rc, 0, err)
            m = json.loads(out)
            self.assertEqual(m.get("phase"), "stable", m)
            self.assertEqual(m.get("config"),
                             ["node1", "node2", "node3"], m)
            # The add-server / remove-server CLI paths: before this
            # change the hyphenated subcommand names never matched
            # the underscored comparisons, so these sent a malformed
            # request and errored instead of driving the change.
            node4 = cluster.add_node("node4")
            host4, port4 = cluster.addrs["node4"]
            rc, out, err = _run_cli(
                ["--node", node, "add-server", "node4",
                 f"{host4}:{port4}"])
            self.assertEqual(rc, 0, err)
            self.assertEqual(out.strip().splitlines()[-1], "OK", out)
            self.assertTrue(cluster.wait_for_config(
                leader.id, ["node1", "node2", "node3", "node4"]),
                "CLI add-server did not reach the 4-node config")
            rc, out, err = _run_cli(["--node", node,
                                     "membership-status"])
            self.assertEqual(rc, 0, err)
            self.assertIn("config: node1, node2, node3, node4", out)
            rc, out, err = _run_cli(["--node", node,
                                     "remove-server", "node4"])
            self.assertEqual(rc, 0, err)
            self.assertEqual(out.strip().splitlines()[-1], "OK", out)
            self.assertTrue(cluster.wait_for_config(
                leader.id, ["node1", "node2", "node3"]),
                "CLI remove-server did not return to 3 nodes")
            self.assertTrue(cluster.wait_for_removed("node4"))
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
