"""Chaos test: kill and restart nodes mid-write, verify nothing is lost.

This is the fault-injection counterpart of test_integration's single
planned failover: a seeded random schedule stops and restarts nodes
while two writer threads keep writing, and at the end every acked
write must be present on every node and every node's state machine
must hold exactly the same data. An acked write is a promise — a
write the cluster confirmed — so losing one, or letting two nodes
diverge, is a correctness bug, not flakiness.

Two reader threads read acked keys concurrently with the writes and
the faults and record every observation in a shared history; the
linearizability check at the end proves no read ever returned a stale
value or a value from the future, so the chaos run checks the
observable consistency property, not just durability. Two CAS threads
do read-modify-write cycles at the same time — each owns a counter
key it increments with CAS, and both contend on one shared counter —
so the check also verifies the precondition of every successful CAS:
the key must actually have held ``expected`` when the CAS linearized.

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
from raftkv.linearizability import History, record_client_op

SHARED_CAS_KEY = "chaos:cas:shared"


def attempt(addr, msg, timeout=60.0):
    """Send one logical op until it acks (``ok`` or ``cas_failed``)
    or ``timeout`` passes, retrying transport errors with the same
    session id, so a lost response is deduped, not applied twice.
    Returns ``(resp, first_invoke)``; ``resp`` is None on deadline."""
    first_invoke = time.monotonic()
    deadline = first_invoke + timeout
    while True:
        resp = client_request(addr, msg, timeout=3.0)
        if resp.get("status") in ("ok", "cas_failed"):
            return resp, first_invoke
        if time.monotonic() > deadline:
            return None, first_invoke
        time.sleep(0.1)


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
            history = History()
            written_counts = [0, 0]
            stop_readers = threading.Event()
            cas_acked: dict[str, str] = {}
            shared_successes = [0]

            # Seed the shared CAS counter before the CAS threads
            # start: their first read must see "0", and the init set
            # is recorded so the checker knows where "0" came from.
            shared_msg = {"type": "client", "op": "set",
                          "key": SHARED_CAS_KEY, "value": "0",
                          "client_id": "cas-init", "request_id": "0"}
            resp, first_invoke = attempt(addr, shared_msg)
            self.assertIsNotNone(resp, "shared CAS counter init timed out")
            self.assertEqual(resp.get("status"), "ok",
                             f"shared CAS counter init failed: {resp}")
            record_client_op(history, "cas-init", 0, shared_msg, resp,
                             first_invoke, time.monotonic())

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
                    first_invoke = time.monotonic()
                    while True:
                        resp = client_request(addr, msg, timeout=3.0)
                        if resp.get("status") == "ok":
                            with lock:
                                acked[key] = value
                                written_counts[thread_id] = i + 1
                            record_client_op(
                                history, f"chaos-{thread_id}", i, msg,
                                resp, first_invoke, time.monotonic())
                            break
                        if time.monotonic() > deadline:
                            errors.append(
                                f"{key}: never acked, last reply {resp}")
                            break
                        time.sleep(0.1)
                    i += 1

            def reader(thread_id: int) -> None:
                # Read keys the writers have already acked, concurrently
                # with the writes and the faults, and record every
                # observation: the linearizability check at the end
                # proves no read ever saw a stale value or a value
                # from the future.
                rng = random.Random(1000 + thread_id)
                seq = 0
                while not stop_readers.is_set():
                    with lock:
                        counts = list(written_counts)
                    candidates = [(t, i) for t in range(2)
                                  for i in range(counts[t])]
                    if not candidates:
                        time.sleep(0.05)
                        continue
                    t, i = rng.choice(candidates)
                    msg = {"type": "client", "op": "get",
                           "key": f"chaos:t{t}:{i}"}
                    invoke = time.monotonic()
                    resp = client_request(addr, msg, timeout=3.0)
                    record_client_op(history, f"reader-{thread_id}",
                                     seq, msg, resp, invoke,
                                     time.monotonic())
                    seq += 1
                    time.sleep(0.005)

            def cas_worker(thread_id: int) -> None:
                # CAS-heavy workload: increment an owned counter key
                # with CAS, and contend with the other CAS thread on
                # one shared counter (read, then CAS expected = the
                # value just read). A lost race fails the CAS and the
                # thread re-reads and retries. Every CAS op is
                # recorded in the shared history, so the
                # linearizability check verifies each successful
                # CAS's precondition, not just what reads returned.
                client_id = f"cas-{thread_id}"
                owned = f"chaos:cas:t{thread_id}"
                seq = [0]

                def get(key: str) -> dict | None:
                    seq[0] += 1
                    msg = {"type": "client", "op": "get", "key": key}
                    resp, first_invoke = attempt(addr, msg)
                    if resp is None:
                        return None
                    record_client_op(history, client_id, seq[0], msg,
                                     resp, first_invoke, time.monotonic())
                    return resp

                def cas(key: str, expected: str, value: str) -> dict | None:
                    seq[0] += 1
                    msg = {"type": "client", "op": "cas", "key": key,
                           "expected": expected, "value": value,
                           "client_id": client_id,
                           "request_id": f"cas-{seq[0]}"}
                    resp, first_invoke = attempt(addr, msg)
                    if resp is None:
                        return None
                    record_client_op(history, client_id, seq[0], msg,
                                     resp, first_invoke, time.monotonic())
                    return resp

                last = f"c{thread_id}-0"
                msg = {"type": "client", "op": "set", "key": owned,
                       "value": last, "client_id": client_id,
                       "request_id": "init"}
                resp, first_invoke = attempt(addr, msg)
                if resp is None or resp.get("status") != "ok":
                    errors.append(
                        f"{owned}: initial set never acked: {resp}")
                    return
                record_client_op(history, client_id, 0, msg, resp,
                                 first_invoke, time.monotonic())
                with lock:
                    cas_acked[owned] = last

                n = 0
                successes = 0
                while not faults_done.is_set() or successes < 15:
                    n += 1
                    value = f"c{thread_id}-{n}"
                    while True:
                        resp = cas(owned, last, value)
                        if resp is None:
                            errors.append(
                                f"{owned}: CAS {n} never acked")
                            return
                        if resp.get("status") == "ok":
                            last = value
                            successes += 1
                            with lock:
                                cas_acked[owned] = value
                            break
                        cur = get(owned)
                        if cur is None or cur.get("status") != "ok":
                            errors.append(
                                f"{owned}: re-read after failed CAS "
                                f"never acked: {cur}")
                            return
                        if cur.get("value") is None:
                            errors.append(
                                f"{owned}: key missing after failed CAS")
                            return
                        last = cur["value"]
                    while True:
                        cur = get(SHARED_CAS_KEY)
                        if cur is None or cur.get("status") != "ok":
                            errors.append(
                                f"{SHARED_CAS_KEY}: get never acked: "
                                f"{cur}")
                            return
                        current = cur.get("value")
                        if current is None:
                            errors.append(
                                f"{SHARED_CAS_KEY}: key missing mid-run")
                            return
                        resp = cas(SHARED_CAS_KEY, current,
                                   str(int(current) + 1))
                        if resp is None:
                            errors.append(
                                f"{SHARED_CAS_KEY}: CAS never acked")
                            return
                        if resp.get("status") == "ok":
                            with lock:
                                shared_successes[0] += 1
                            break

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
            readers = [
                threading.Thread(target=reader, args=(t,))
                for t in range(2)
            ]
            cas_threads = [
                threading.Thread(target=cas_worker, args=(t,))
                for t in range(2)
            ]
            for w in writers:
                w.start()
            for r in readers:
                r.start()
            for c in cas_threads:
                c.start()
            for w in writers:
                w.join()
            for c in cas_threads:
                c.join()
            stop_readers.set()
            for r in readers:
                r.join(timeout=5.0)
            fault_thread.join(timeout=5.0)

            self.assertEqual(errors, [], "writes that never acked")
            self.assertEqual(fault_count[0], 4,
                             "all four kill/restart cycles should run")
            self.assertGreaterEqual(len(acked), 30,
                                    "every write should have acked")

            # Linearizability across the whole fault schedule: every
            # concurrent read must have returned a value some
            # linearization could produce, and every successful CAS
            # must have seen its expected value when it linearized.
            lin = history.check()
            self.assertTrue(lin.ok, str(lin))
            self.assertGreaterEqual(lin.reads_checked, 10,
                                    "readers should have observed reads")
            self.assertGreaterEqual(lin.cas_checked, 10,
                                    "CAS threads should have completed "
                                    "CAS ops")

            # The cluster settles, then every node must hold every
            # acked write and every CAS counter at its final value —
            # and nothing else.
            expected = dict(acked)
            expected.update(cas_acked)
            expected[SHARED_CAS_KEY] = str(shared_successes[0])
            cluster.wait_for_stable_leader()
            for node in cluster.nodes.values():
                for key, value in expected.items():
                    self.assertTrue(wait_for_key(node, key, value),
                                    f"{node.id} missing {key}={value}")
            states = [node.sm.items() for node in cluster.nodes.values()]
            for state in states[1:]:
                self.assertEqual(state, states[0],
                                 "nodes diverged after chaos")
            self.assertEqual(states[0], expected)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
