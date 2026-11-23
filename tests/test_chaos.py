"""Chaos test: kill and restart nodes mid-write, verify nothing is lost.

This is the fault-injection counterpart of test_integration's single
planned failover: a seeded random schedule stops and restarts nodes
while two writer threads keep writing, and at the end every acked
write must be present on every node and every node's state machine
must hold exactly the same data. An acked write is a promise — a
write the cluster confirmed — so losing one, or letting two nodes
diverge, is a correctness bug, not flakiness.

The fault schedule is seeded (seed 7), so the same kill/restart
sequence runs every time; a failure is reproducible, not a one-off.
The writers keep writing until the fault thread has finished all
four cycles, so the faults genuinely land mid-write instead of
racing the writers to the finish.
"""

import random
import threading
import time
import unittest

from harness import Cluster, client_request, wait_for_key


class TestChaos(unittest.TestCase):
    def test_random_kill_restart_mid_write_loses_nothing(self):
        rng = random.Random(7)
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]

            acked: dict[str, str] = {}
            lock = threading.Lock()
            errors: list[str] = []
            faults_done = threading.Event()
            fault_count = [0]

            def writer(thread_id: int) -> None:
                # Keep writing until the fault thread has run all its
                # cycles (and at least 15 writes each, so the cluster
                # has real work to protect).
                i = 0
                while not faults_done.is_set() or i < 15:
                    key = f"chaos:t{thread_id}:{i}"
                    value = f"v{thread_id}-{i}"
                    msg = {"type": "client", "op": "set", "key": key,
                           "value": value,
                           "client_id": f"chaos-{thread_id}",
                           "request_id": str(i)}
                    deadline = time.monotonic() + 60.0
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
                # Four kill/restart cycles on a seeded schedule, while
                # the writers are running: never kill a node that is
                # already down, and always bring the victim back.
                for _ in range(4):
                    time.sleep(rng.uniform(0.2, 1.0))
                    live = [i for i in cluster.live]
                    if not live:
                        continue
                    victim = rng.choice(sorted(live))
                    cluster.stop_node(victim)
                    time.sleep(rng.uniform(0.5, 1.5))
                    cluster.restart_node(victim)
                    fault_count[0] += 1
                faults_done.set()

            fault_thread = threading.Thread(target=faults, daemon=True)
            fault_thread.start()
            writers = [
                threading.Thread(target=writer, args=(t,))
                for t in range(2)
            ]
            for w in writers:
                w.start()
            for w in writers:
                w.join()
            fault_thread.join(timeout=5.0)

            self.assertEqual(errors, [], "writes that never acked")
            self.assertEqual(fault_count[0], 4,
                             "all four kill/restart cycles should run")
            self.assertGreaterEqual(len(acked), 30,
                                    "every write should have acked")

            # The cluster settles, then every node must hold every
            # acked write — and nothing else.
            cluster.wait_for_stable_leader()
            for node in cluster.nodes.values():
                for key, value in acked.items():
                    self.assertTrue(wait_for_key(node, key, value),
                                    f"{node.id} missing {key}={value}")
            states = [node.sm.items() for node in cluster.nodes.values()]
            for state in states[1:]:
                self.assertEqual(state, states[0],
                                 "nodes diverged after chaos")
            self.assertEqual(states[0], acked)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
