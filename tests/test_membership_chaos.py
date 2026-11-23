"""Membership changes under chaos faults.

The membership tests in test_membership.py prove add/remove work on a
healthy cluster, and test_chaos.py proves kill/restart faults lose no
acked writes — but neither drives a membership change *while* faults
are active. That is the dangerous combination: a joint-consensus
transition needs a majority of BOTH configs to commit anything, so a
partition or a dead follower during the transition can stall the
change, and a leader dying mid-transition hands the half-open joint
phase to the next leader. These tests drive the change through the
faults and verify the cluster converges afterwards: every acked write
still reads back, and the new membership is durable — the config every
node reports, after restarts, is the changed one.
"""

import sys
import threading
import time
import unittest
from pathlib import Path

# The chaos harness lives in scripts/, not on the test path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import chaos as chaos_script  # noqa: E402

from harness import Cluster, client_request, wait_for_key


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


def _add_server_retry(addr, server_id, server_addr, timeout=30.0):
    """Add a server, retrying until it acks ok or the deadline passes.
    A leader dying mid-add fails the attempt ("lost leadership while
    adding server"), and a joint entry that cannot commit yet returns
    a timeout — either way the change is retried, not lost: the next
    leader either finishes the open transition or takes a fresh add."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = _add_server(addr, server_id, server_addr)
        if last.get("status") == "ok":
            return last
        time.sleep(0.2)
    return last


class TestMembershipUnderPartition(unittest.TestCase):
    def test_add_node_under_partition(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            for i in range(5):
                self.assertEqual(
                    _set(cluster.addrs[leader.id], f"k{i}", f"v{i}")
                    .get("status"), "ok")
            # Split node3 off from the leader and node2. The majority
            # side must still be able to commit the joint and final
            # entries: a majority of C-old (leader + node2) and a
            # majority of C-new (leader + node2 + the catching-up
            # node4) — the isolated node3's ack is not needed.
            cluster.isolate("node3")
            node4 = cluster.add_node("node4")
            resp = _add_server(cluster.addrs[leader.id], "node4",
                               cluster.addrs["node4"])
            self.assertEqual(resp.get("status"), "ok", resp)
            # The new node caught up while the split was up, and a
            # write after the add replicates to it.
            for i in range(5):
                self.assertTrue(wait_for_key(node4, f"k{i}", f"v{i}"),
                                f"node4 never caught up key k{i}")
            self.assertEqual(
                _set(cluster.addrs[leader.id], "after", "yes")
                .get("status"), "ok")
            self.assertTrue(wait_for_key(node4, "after", "yes"))
            # Heal: the isolated node rejoins, catches up, and every
            # node reports the same 4-node config — no node is stuck
            # in the old config or in the joint phase.
            cluster.heal_all()
            for nid in ("node1", "node2", "node3", "node4"):
                self.assertTrue(
                    cluster.wait_for_config(
                        nid, ["node1", "node2", "node3", "node4"]),
                    f"{nid} config is not the 4-node config: "
                    f"{cluster.status(nid).get('config')}")
                self.assertIsNone(cluster.status(nid).get("joint_new"),
                                  f"{nid} still in a joint transition")
            self.assertTrue(wait_for_key(cluster.nodes["node3"], "after",
                                         "yes"),
                            "rejoined node3 never caught up")
            self.assertEqual(
                _get(cluster.addrs["node3"], "after").get("value"), "yes")
        finally:
            cluster.stop_all()


class TestMembershipUnderKillRestart(unittest.TestCase):
    def test_remove_node_while_follower_killed_and_restarted(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            followers = [n for n in cluster.nodes.values() if n is not leader]
            victim, removed = followers[0], followers[1]
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k", "v").get("status"), "ok")
            # Kill one follower, then remove the other one. The joint
            # entry cannot commit while the victim is down — a majority
            # of C-new needs it — so the remove RPC must wait out the
            # restart and complete afterwards, not fail or wedge.
            cluster.stop_node(victim.id)
            result: dict = {}

            def _do_remove():
                result["resp"] = _remove_server(cluster.addrs[leader.id],
                                                removed.id)

            t = threading.Thread(target=_do_remove, daemon=True)
            t.start()
            time.sleep(1.0)
            cluster.restart_node(victim.id)
            t.join(timeout=30.0)
            self.assertFalse(t.is_alive(), "remove never returned")
            self.assertEqual(result["resp"].get("status"), "ok",
                             result["resp"])
            # Back to two voting members, and writes still commit.
            remaining = [n for n in ("node1", "node2", "node3")
                         if n != removed.id]
            for nid in remaining:
                self.assertTrue(cluster.wait_for_config(nid, remaining),
                                f"{nid} config is not {remaining}: "
                                f"{cluster.status(nid).get('config')}")
            self.assertTrue(cluster.wait_for_removed(removed.id),
                            "removed node never reported removed")
            self.assertEqual(
                _set(cluster.addrs[leader.id], "k2", "v2").get("status"),
                "ok")
            self.assertEqual(
                _get(cluster.addrs[leader.id], "k2").get("value"), "v2")
            # Every acked write is on every surviving node.
            for nid in remaining:
                self.assertTrue(wait_for_key(cluster.nodes[nid], "k", "v"))
                self.assertTrue(wait_for_key(cluster.nodes[nid], "k2", "v2"))
        finally:
            cluster.stop_all()


class TestMembershipSurvivesLeaderRestartMidChaos(unittest.TestCase):
    def test_membership_survives_leader_restart_mid_chaos(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]

            acked: dict[str, str] = {}
            lock = threading.Lock()
            errors: list[str] = []
            faults_done = threading.Event()
            membership_done = threading.Event()
            # One lock around stop/restart: the fault thread and the
            # membership thread both kill and restart nodes, and a
            # second restart of a live node would fail to bind its
            # port. The check and the action happen under the lock.
            lifecycle_lock = threading.Lock()

            def stop_node(node_id: str) -> None:
                with lifecycle_lock:
                    if node_id in cluster.live:
                        cluster.stop_node(node_id)

            def restart_node(node_id: str) -> None:
                with lifecycle_lock:
                    if node_id not in cluster.live:
                        cluster.restart_node(node_id)

            def writer(thread_id: int) -> None:
                i = 0
                while not faults_done.is_set() or i < 10:
                    key = f"mchaos:t{thread_id}:{i}"
                    value = f"v{thread_id}-{i}"
                    msg = {"type": "client", "op": "set", "key": key,
                           "value": value,
                           "client_id": f"mchaos-{thread_id}",
                           "request_id": str(i)}
                    deadline = time.monotonic() + 60.0
                    first_invoke = time.monotonic()
                    while True:
                        resp = client_request(addr, msg, timeout=3.0)
                        if resp.get("status") == "ok":
                            with lock:
                                acked[key] = value
                            break
                        if time.monotonic() > deadline:
                            errors.append(
                                f"{key}: never acked, last reply {resp}")
                            break
                        time.sleep(0.1)
                    i += 1

            def faults() -> None:
                # Three kill/restart cycles on a fixed schedule while
                # the writers and the membership change are running.
                for victim_id in ("node2", "node1", "node3"):
                    time.sleep(0.5)
                    stop_node(victim_id)
                    time.sleep(0.8)
                    restart_node(victim_id)
                faults_done.set()

            def membership() -> None:
                # Add node4 mid-chaos, then kill the leader and bring
                # it back: the new membership must survive the leader
                # restart, not just the add.
                time.sleep(0.5)
                node4 = cluster.add_node("node4")
                resp = _add_server_retry(cluster.addrs[leader.id], "node4",
                                         cluster.addrs["node4"])
                if resp.get("status") != "ok":
                    errors.append(f"add node4 never acked: {resp}")
                    membership_done.set()
                    return
                current = cluster.wait_for_stable_leader()
                stop_node(current.id)
                time.sleep(0.5)
                restart_node(current.id)
                membership_done.set()

            fault_thread = threading.Thread(target=faults, daemon=True)
            membership_thread = threading.Thread(target=membership,
                                                 daemon=True)
            writers = [threading.Thread(target=writer, args=(t,))
                       for t in range(2)]
            fault_thread.start()
            membership_thread.start()
            for w in writers:
                w.start()
            for w in writers:
                w.join()
            fault_thread.join(timeout=15.0)
            membership_thread.join(timeout=40.0)
            self.assertTrue(membership_done.is_set(),
                            "membership phase never finished")
            self.assertEqual(errors, [], "writes/membership that never acked")
            self.assertGreaterEqual(len(acked), 20,
                                    "every write should have acked")

            # The cluster settles on one leader, every node reports
            # the 4-node config with no joint phase left, and every
            # acked write is present on every node.
            cluster.wait_for_stable_leader()
            for nid in ("node1", "node2", "node3", "node4"):
                self.assertTrue(
                    cluster.wait_for_config(
                        nid, ["node1", "node2", "node3", "node4"]),
                    f"{nid} config is not the 4-node config: "
                    f"{cluster.status(nid).get('config')}")
                self.assertIsNone(cluster.status(nid).get("joint_new"),
                                  f"{nid} still in a joint transition")
            for node in cluster.nodes.values():
                for key, value in acked.items():
                    self.assertTrue(wait_for_key(node, key, value),
                                    f"{node.id} missing {key}={value}")
            states = [node.sm.items() for node in cluster.nodes.values()]
            for state in states[1:]:
                self.assertEqual(state, states[0],
                                 "nodes diverged after membership chaos")
            self.assertEqual(states[0], acked)
            # Writes still work after everything settles.
            new_leader = cluster.wait_for_leader()
            self.assertEqual(
                _set(cluster.addrs[new_leader.id], "after", "yes")
                .get("status"), "ok")
            self.assertEqual(
                _get(cluster.addrs[new_leader.id], "after").get("value"),
                "yes")
        finally:
            cluster.stop_all()


class TestMembershipPhasePolling(unittest.TestCase):
    def test_summarize_phases_dedupes_and_keeps_order(self):
        samples = [
            (0.0, "node1", "stable"),
            (0.05, "node1", "catching_up"),
            (0.1, "node2", "catching_up"),  # same phase, other node
            (0.15, "node1", "joint_pending"),
            (0.2, "node1", "joint_pending"),  # repeat, no new step
            (0.25, "node1", "stable"),
        ]
        self.assertEqual(
            chaos_script.summarize_phases(samples),
            "stable -> catching_up -> joint_pending -> stable")
        self.assertEqual(chaos_script.summarize_phases([]), "")

    def test_chaos_harness_polls_phases_during_add(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            # Give node4 a log worth catching up on, so the
            # catching_up phase lasts long enough for the 50ms
            # poller to see it.
            for i in range(50):
                self.assertEqual(
                    _set(cluster.addrs[leader.id], f"k{i}", f"v{i}")
                    .get("status"), "ok")
            node4 = cluster.add_node("node4")
            samples: list = []
            stop = threading.Event()
            poller = threading.Thread(
                target=chaos_script.poll_membership_phases,
                args=(cluster.status, lambda: list(cluster.nodes),
                      stop, samples),
                daemon=True)
            poller.start()
            try:
                resp = _add_server_retry(cluster.addrs[leader.id],
                                         "node4", cluster.addrs["node4"])
                # The change committed; let the poller run a little
                # longer so the post-add stable phase is sampled
                # too and the timeline ends where the cluster
                # settled, not mid-transition.
                time.sleep(0.5)
            finally:
                stop.set()
                poller.join(timeout=5.0)
            self.assertEqual(resp.get("status"), "ok", resp)
            timeline = chaos_script.summarize_phases(samples)
            phases = timeline.split(" -> ") if timeline else []
            valid = {"stable", "catching_up", "joint_pending",
                     "joint_committed", "final_pending"}
            self.assertTrue(phases, "poller recorded no phases at all")
            self.assertTrue(set(phases) <= valid,
                            f"unknown phase in timeline: {timeline}")
            self.assertEqual(phases[-1], "stable",
                             f"timeline does not end stable: {timeline}")
            # The add went through a transition, not straight from
            # stable to stable: at least one intermediate phase
            # must have been observed.
            self.assertTrue(any(p != "stable" for p in phases),
                            f"no intermediate phase observed: {timeline}")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
