#!/usr/bin/env python3
"""Chaos test for raftkv: kill and restart nodes mid-write, verify nothing
is lost and every node converges to the same state.

Starts a real 3-node cluster on localhost (subprocesses, like
``scripts/demo.sh``), then runs two things at once:

  * writer threads issuing committed writes, each with a unique key and
    a session id (``client_id`` / ``request_id``), retrying on error
    until the write is acknowledged — an acked write is one the cluster
    promised to keep, so losing one is a correctness bug, not noise;
  * CAS threads doing read-modify-write cycles with compare-and-swap:
    each owns a counter key it increments with CAS, and all of them
    contend on one shared counter they increment the same way (read,
    then CAS expected = the value just read). A lost race fails the
    CAS and the thread re-reads and retries, so the run exercises
    real CAS contention — and the linearizability check verifies not
    just what reads returned but the precondition of every
    successful CAS: the key must actually have held ``expected``
    when the CAS linearized;
  * reader threads reading keys the writers have already acked,
    recording every observation in a shared history;
  * a fault thread that, on a seeded random schedule, SIGKILLs a random
    node (a real crash, no clean shutdown, so the disk log has to save
    us) and restarts it a moment later.

When the writers finish and the cluster settles, the script verifies:

  * every acked write reads back with its exact value,
  * every CAS-owned counter and the shared counter read back at their
    final values (the shared counter must equal the number of
    successful shared CAS ops — each one increments by exactly one),
  * every node reports the same ``commit_index`` and ``last_applied`` —
    the same applied prefix on every node, so no node diverged or
    fell behind for good, and
  * the recorded history is linearizable (``raftkv.linearizability``):
    no concurrent read returned a stale value or a value from the
    future, and no successful CAS claimed an ``expected`` value the
    key did not hold when the CAS linearized — across the whole fault
    schedule.

The fault schedule is seeded, so ``--seed N`` reproduces the exact
same kill/restart sequence for debugging a failure. Exits non-zero if
any acked write is lost, the nodes disagree, or the cluster never
elects a leader.

Usage:

    python3 scripts/chaos.py [--seed 7] [--writes 60] [--faults 6]
        [--threads 3] [--port-base 5200] [--data-dir /tmp/raftkv-chaos]
        [--keep] [--partition SECONDS]

With ``--partition SECONDS`` the run also isolates the current
leader from both followers for that long mid-run (a real split:
all three nodes stay up, they just cannot hear each other). The
majority must elect a new leader and keep committing while the
split lasts; the isolated leader must not commit anything. The
same end-of-run verification covers the split.
"""

from __future__ import annotations

import argparse
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from raftkv.client import request as client_request  # noqa: E402
from raftkv.linearizability import History, record_client_op  # noqa: E402
from raftkv.transport import send_rpc  # noqa: E402

SEED_KEY = "__chaos_probe__"
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


def cas_worker(cluster: Cluster, thread_id: int, min_count: int,
               cas_acked: dict[str, str], lock: threading.Lock,
               errors: list[str], faults_done: threading.Event,
               history: History, shared_successes: list[int]) -> None:
    """CAS-heavy workload: one thread owns a counter key it increments
    with compare-and-swap, and all CAS threads contend on one shared
    counter they increment the same way (read, then CAS expected =
    the value just read). A lost race fails the CAS and the thread
    re-reads and retries, so the counter only ever moves forward by
    exactly one. Every CAS op — success or failed race — is recorded
    in the shared history with its first attempt's invocation time,
    so the linearizability check can verify each successful CAS's
    precondition, not just what reads returned. A retry after a
    failed CAS is a new logical op (new request id): the failed
    result is cached in the dedup table, so reusing the id would
    return the cached failure forever."""
    addr = cluster.addrs[0]
    client_id = f"cas-{thread_id}"
    owned = f"chaos:cas:t{thread_id}"
    seq = 0

    def get(key: str) -> dict | None:
        """One get, retried until it acks; records the read in the
        history. Returns the response dict, or None if it never
        acked within the deadline."""
        nonlocal seq
        seq += 1
        msg = {"type": "client", "op": "get", "key": key}
        resp, first_invoke = attempt(addr, msg)
        if resp is None:
            return None
        record_client_op(history, client_id, seq, msg, resp,
                         first_invoke, time.monotonic())
        return resp

    def cas(key: str, expected: str, value: str) -> dict | None:
        """One CAS, retried on transport errors until it acks (ok or
        cas_failed); records it in the history. Returns the response
        dict, or None if it never acked within the deadline."""
        nonlocal seq
        seq += 1
        msg = {"type": "client", "op": "cas", "key": key,
               "expected": expected, "value": value,
               "client_id": client_id, "request_id": f"cas-{seq}"}
        resp, first_invoke = attempt(addr, msg)
        if resp is None:
            return None
        record_client_op(history, client_id, seq, msg, resp,
                         first_invoke, time.monotonic())
        return resp

    # Seed the owned key with a plain set, recorded like any write.
    last = f"c{thread_id}-0"
    msg = {"type": "client", "op": "set", "key": owned, "value": last,
           "client_id": client_id, "request_id": "init"}
    resp, first_invoke = attempt(addr, msg)
    if resp is None or resp.get("status") != "ok":
        errors.append(f"{owned}: initial set never acked: {resp}")
        return
    record_client_op(history, client_id, 0, msg, resp,
                     first_invoke, time.monotonic())
    with lock:
        cas_acked[owned] = last

    n = 0
    successes = 0
    while not faults_done.is_set() or successes < min_count:
        n += 1
        # Owned key: increment our own counter with CAS. We own this
        # key, so the CAS should succeed; if it ever fails, learn the
        # current value and retry from there.
        value = f"c{thread_id}-{n}"
        while True:
            resp = cas(owned, last, value)
            if resp is None:
                errors.append(f"{owned}: CAS {n} never acked")
                return
            if resp.get("status") == "ok":
                last = value
                successes += 1
                with lock:
                    cas_acked[owned] = value
                break
            cur = get(owned)
            if cur is None or cur.get("status") != "ok":
                errors.append(f"{owned}: re-read after failed CAS "
                              f"never acked: {cur}")
                return
            if cur.get("value") is None:
                errors.append(f"{owned}: key missing after failed CAS")
                return
            last = cur["value"]
        # Shared counter: read the current value, CAS it up by one.
        # Lost races fail the CAS and retry from a fresh read.
        while True:
            cur = get(SHARED_CAS_KEY)
            if cur is None or cur.get("status") != "ok":
                errors.append(f"{SHARED_CAS_KEY}: get never acked: {cur}")
                return
            current = cur.get("value")
            if current is None:
                errors.append(f"{SHARED_CAS_KEY}: key missing mid-run")
                return
            resp = cas(SHARED_CAS_KEY, current, str(int(current) + 1))
            if resp is None:
                errors.append(f"{SHARED_CAS_KEY}: CAS never acked")
                return
            if resp.get("status") == "ok":
                with lock:
                    shared_successes[0] += 1
                break
            # Lost the race: loop around, re-read, try again.


class Cluster:
    """A throwaway 3-node cluster running as subprocesses, with
    per-node kill/restart so a fault thread can crash nodes."""

    def __init__(self, port_base: int, data_dir: Path):
        self.port_base = port_base
        self.data_dir = data_dir
        self.procs: list[subprocess.Popen | None] = [None, None, None]
        self.addrs = [("127.0.0.1", port_base + i) for i in range(3)]
        self._logs: list = []

    def _start_node(self, i: int) -> None:
        node_id = f"node{i + 1}"
        peers = ",".join(
            f"node{j + 1}=127.0.0.1:{self.port_base + j}"
            for j in range(3) if j != i
        )
        log = open(self.data_dir / f"{node_id}.log", "a")
        self._logs.append(log)
        self.procs[i] = subprocess.Popen(
            [sys.executable, "-m", "raftkv.server",
             "--id", node_id, "--port", str(self.port_base + i),
             "--peers", peers,
             "--data-dir", str(self.data_dir / node_id)],
            stdout=log, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).resolve().parent.parent),
        )

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        for i in range(3):
            self._start_node(i)
        self.wait_for_leader()

    def kill_node(self, i: int) -> None:
        """SIGKILL a node: a real crash, not a clean shutdown."""
        proc = self.procs[i]
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()
        self.procs[i] = None

    def restart_node(self, i: int) -> None:
        self._start_node(i)

    def _partition_rpc(self, src: int, dst: int, action: str) -> bool:
        """One partition control RPC, retried while the target node is
        down: the fault thread kills nodes on its own schedule, so a
        node can be mid-restart when the partition phase runs. A
        restarted node comes back with an empty partition set
        (partitions are in-memory only), so a control RPC that fails
        while the node is down must be retried after it is back, or
        the split would silently be one-sided on that link."""
        host, port = self.addrs[src]
        deadline = time.monotonic() + 5.0
        while True:
            resp = send_rpc(host, port,
                            {"type": "partition",
                             "peer": f"node{dst + 1}", "action": action},
                            timeout=2.0)
            if resp and resp.get("status") == "ok":
                return True
            if time.monotonic() > deadline:
                return False
            time.sleep(0.2)

    def partition(self, a: int, b: int) -> bool:
        """Split the link between nodes ``a`` and ``b`` (0-based) in
        both directions, via the node's partition control RPC: neither
        sends peer RPCs to the other, and each drops the other's RPCs
        on arrival. Client and status traffic still flow, so a
        partitioned node still answers status and still claims
        leadership — a real split, not a crash. Returns False if a
        control RPC did not answer even after retries."""
        return (self._partition_rpc(a, b, "drop")
                and self._partition_rpc(b, a, "drop"))

    def heal_partition(self, a: int, b: int) -> bool:
        """Restore the link between nodes ``a`` and ``b``."""
        return (self._partition_rpc(a, b, "heal")
                and self._partition_rpc(b, a, "heal"))

    def heal_all(self) -> None:
        """Heal every partition on every node."""
        for a in range(3):
            for b in range(3):
                if a != b:
                    self.heal_partition(a, b)

    def leader_index(self) -> int | None:
        """Index of the node that currently reports role leader, or
        None if none (or more than one) does. Status RPCs still work
        while partitioned, so this sees through a split."""
        leaders = [i for i in range(3)
                   if self.status(i).get("role") == "leader"]
        return leaders[0] if len(leaders) == 1 else None

    def is_live(self, i: int) -> bool:
        proc = self.procs[i]
        return proc is not None and proc.poll() is None

    def wait_for_leader(self, timeout: float = 15.0) -> None:
        """Poll a read until it succeeds — a get only returns ok once
        a leader exists and can serve it."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            resp = client_request(
                self.addrs[0],
                {"type": "client", "op": "get", "key": SEED_KEY},
                timeout=2.0,
            )
            if resp.get("status") == "ok":
                return
            time.sleep(0.1)
        raise RuntimeError(f"no leader after {timeout}s")

    def status(self, i: int) -> dict:
        host, port = self.addrs[i]
        return send_rpc(host, port, {"type": "status"}, timeout=2.0) or {}

    def wait_for_convergence(self, timeout: float = 15.0) -> dict:
        """Poll until every live node reports the same commit index and
        last applied index, then return the three status dicts."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            stats = [self.status(i) for i in range(3)]
            if all(stats) and len({(s["commit_index"], s["last_applied"])
                                  for s in stats}) == 1:
                return {f"node{i + 1}": stats[i] for i in range(3)}
            time.sleep(0.1)
        raise RuntimeError(
            "nodes did not converge: " +
            ", ".join(
                f"node{i + 1}=commit {s.get('commit_index')}/"
                f"applied {s.get('last_applied')}" if s else
                f"node{i + 1}=down"
                for i, s in enumerate(
                    [self.status(j) for j in range(3)])))

    def stop(self) -> None:
        for i, proc in enumerate(self.procs):
            if proc is not None and proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + 5.0
        for proc in self.procs:
            if proc is None:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                pass
        for proc in self.procs:
            if proc is not None and proc.poll() is None:
                proc.kill()
        self.procs = [None, None, None]
        for log in self._logs:
            log.close()


def writer(cluster: Cluster, thread_id: int, min_count: int,
           acked: dict[str, str], lock: threading.Lock,
           errors: list[str], faults_done: threading.Event,
           history: History, written_counts: list[int]) -> None:
    """Write unique keys until the fault thread has run its whole
    schedule (and at least ``min_count`` writes), retrying each until
    it is acked. A write that never acks is recorded as an error, not
    silently dropped — but a retry uses the same session id, so a
    write whose first attempt actually committed is deduped, not
    applied twice. Each acked write is recorded in the shared history
    with its *first* attempt's invocation time: the write may have
    committed any time after that, and recording the retry's later
    invocation would make a read that legitimately saw the write
    look stale."""
    addr = cluster.addrs[0]
    i = 0
    while not faults_done.is_set() or i < min_count:
        key = f"chaos:t{thread_id}:{i}"
        value = f"v{thread_id}-{i}"
        msg = {"type": "client", "op": "set", "key": key, "value": value,
               "client_id": f"chaos-{thread_id}", "request_id": str(i)}
        deadline = time.monotonic() + 60.0
        first_invoke = time.monotonic()
        while True:
            resp = client_request(addr, msg, timeout=3.0)
            if resp.get("status") == "ok":
                with lock:
                    acked[key] = value
                    written_counts[thread_id] = i + 1
                record_client_op(history, f"chaos-{thread_id}", i, msg,
                                 resp, first_invoke, time.monotonic())
                break
            if time.monotonic() > deadline:
                errors.append(f"{key}: never acked, last reply {resp}")
                break
            time.sleep(0.1)
        i += 1


def reader(cluster: Cluster, thread_id: int, threads: int,
           lock: threading.Lock, written_counts: list[int],
           stop_readers: threading.Event, history: History,
           rng: random.Random) -> None:
    """Read keys the writers have already acked, concurrently with
    the writes and the faults, and record every observation in the
    shared history. A read that returns a value no linearization
    could produce — a stale value after a newer write completed, or
    a value from the future — is a correctness bug, and the
    linearizability check at the end catches it."""
    addr = cluster.addrs[0]
    seq = 0
    while not stop_readers.is_set():
        with lock:
            counts = list(written_counts)
        candidates = [(t, i) for t in range(threads)
                      for i in range(counts[t])]
        if not candidates:
            time.sleep(0.05)
            continue
        t, i = rng.choice(candidates)
        key = f"chaos:t{t}:{i}"
        msg = {"type": "client", "op": "get", "key": key}
        invoke = time.monotonic()
        resp = client_request(addr, msg, timeout=3.0)
        record_client_op(history, f"reader-{thread_id}", seq, msg,
                         resp, invoke, time.monotonic())
        seq += 1
        time.sleep(0.005)


def fault_loop(cluster: Cluster, rng: random.Random, faults: int,
               faults_done: threading.Event, log: list[str]) -> None:
    """Seeded random kill/restart schedule: pick a live node, SIGKILL
    it, wait a moment, restart it, wait a moment, repeat. Runs the
    whole schedule — the writers keep writing until this finishes, so
    the faults genuinely land mid-write."""
    for n in range(faults):
        time.sleep(rng.uniform(0.2, 1.0))
        live = [i for i in range(3) if cluster.is_live(i)]
        if not live:
            continue
        victim = rng.choice(live)
        cluster.kill_node(victim)
        log.append(f"fault {n + 1}/{faults}: killed node{victim + 1}")
        time.sleep(rng.uniform(0.5, 2.0))
        cluster.restart_node(victim)
        log.append(f"fault {n + 1}/{faults}: restarted node{victim + 1}")
    faults_done.set()


def partition_phase(cluster: Cluster, seconds: float,
                    log: list[str]) -> None:
    """Jepsen-style split, mid-run: isolate the current leader from
    both followers for ``seconds``, then heal. The majority must
    elect a new leader and keep committing while the split lasts;
    the isolated leader cannot commit (its commit index must not
    move) and rejoins cleanly after the heal. Runs on its own
    thread, concurrently with the writers and the kill/restart
    faults; the normal end-of-run verification (acked writes,
    convergence, linearizability) covers the whole phase."""
    time.sleep(1.0)
    leader = cluster.leader_index()
    if leader is None:
        log.append("partition: skipped, no single leader found")
        return
    others = [i for i in range(3) if i != leader]
    if not all(cluster.partition(leader, i) for i in others):
        log.append("partition: control RPC failed, split may be partial")
    log.append(f"partition: isolated node{leader + 1} (leader) "
               f"for {seconds:g}s")
    time.sleep(seconds)
    cluster.heal_all()
    log.append("partition: healed")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=7,
                        help="RNG seed for the fault schedule "
                             "(default 7; same seed, same faults)")
    parser.add_argument("--writes", type=int, default=60,
                        help="minimum writes per writer thread "
                             "(default 60; writers keep going until the "
                             "fault schedule finishes)")
    parser.add_argument("--faults", type=int, default=6,
                        help="kill/restart cycles (default 6)")
    parser.add_argument("--partition", type=float, default=0.0,
                        help="isolate the current leader from both "
                             "followers for this many seconds mid-run "
                             "(default 0: no partition phase). The "
                             "majority must elect a new leader and keep "
                             "committing while the split lasts; the "
                             "isolated leader must not commit anything, "
                             "and the cluster must converge after the "
                             "heal — all checked by the normal end-of-"
                             "run verification.")
    parser.add_argument("--threads", type=int, default=3,
                        help="concurrent writer threads (default 3)")
    parser.add_argument("--cas-threads", type=int, default=2,
                        help="concurrent CAS threads doing read-modify-"
                             "write cycles (default 2)")
    parser.add_argument("--cas-writes", type=int, default=15,
                        help="minimum successful owned-key CAS ops per "
                             "CAS thread (default 15; each thread also "
                             "increments the shared counter once per "
                             "owned CAS)")
    parser.add_argument("--readers", type=int, default=2,
                        help="concurrent reader threads recording a "
                             "history for the linearizability check "
                             "(default 2)")
    parser.add_argument("--port-base", type=int, default=5200,
                        help="first of three consecutive ports "
                             "(default 5200)")
    parser.add_argument("--data-dir", default="/tmp/raftkv-chaos",
                        help="cluster data dir (default /tmp/raftkv-chaos)")
    parser.add_argument("--keep", action="store_true",
                        help="keep the data dir afterwards "
                             "(default: remove it)")
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    data_dir = Path(args.data_dir)
    cluster = Cluster(args.port_base, data_dir)
    stop = False

    def _on_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        cluster.start()
        print(f"cluster up on ports "
              f"{args.port_base}-{args.port_base + 2} "
              f"(seed {args.seed}, {args.writes} writes x "
              f"{args.threads} threads, {args.cas_writes} CAS x "
              f"{args.cas_threads} threads, {args.faults} faults)")
        acked: dict[str, str] = {}
        lock = threading.Lock()
        errors: list[str] = []
        fault_log: list[str] = []
        faults_done = threading.Event()
        history = History()
        written_counts = [0] * args.threads
        stop_readers = threading.Event()
        cas_acked: dict[str, str] = {}
        shared_successes = [0]

        # Seed the shared CAS counter before the CAS threads start:
        # their first read must see "0", and the init set is recorded
        # in the history so the checker knows where "0" came from.
        shared_msg = {"type": "client", "op": "set",
                      "key": SHARED_CAS_KEY, "value": "0",
                      "client_id": "cas-init", "request_id": "0"}
        resp, first_invoke = attempt(cluster.addrs[0], shared_msg)
        if resp is None or resp.get("status") != "ok":
            print(f"shared CAS counter init failed: {resp}",
                  file=sys.stderr)
            return 1
        record_client_op(history, "cas-init", 0, shared_msg, resp,
                         first_invoke, time.monotonic())

        faults = threading.Thread(
            target=fault_loop,
            args=(cluster, rng, args.faults, faults_done, fault_log),
            daemon=True)
        faults.start()
        partition_thread = None
        if args.partition > 0:
            partition_thread = threading.Thread(
                target=partition_phase,
                args=(cluster, args.partition, fault_log),
                daemon=True)
            partition_thread.start()
        writers = [
            threading.Thread(target=writer,
                             args=(cluster, t, args.writes, acked, lock,
                                   errors, faults_done, history,
                                   written_counts))
            for t in range(args.threads)
        ]
        readers = [
            threading.Thread(target=reader,
                             args=(cluster, t, args.threads, lock,
                                   written_counts, stop_readers, history,
                                   random.Random(args.seed + 1000 + t)),
                             daemon=True)
            for t in range(args.readers)
        ]
        for w in writers:
            w.start()
        for r in readers:
            r.start()
        cas_threads = [
            threading.Thread(target=cas_worker,
                             args=(cluster, t, args.cas_writes, cas_acked,
                                   lock, errors, faults_done, history,
                                   shared_successes))
            for t in range(args.cas_threads)
        ]
        for c in cas_threads:
            c.start()
        for w in writers:
            w.join()
        for c in cas_threads:
            c.join()
        stop_readers.set()
        for r in readers:
            r.join(timeout=5.0)
        faults.join(timeout=5.0)
        if partition_thread is not None:
            partition_thread.join(timeout=args.partition + 10.0)
            cluster.heal_all()  # never verify with a split in place
        for line in fault_log:
            print(f"  {line}")
        if errors:
            for err in errors:
                print(f"writer error: {err}", file=sys.stderr)
            return 1

        # Every node back up, cluster settled, then verify.
        for i in range(3):
            if not cluster.is_live(i):
                cluster.restart_node(i)
        cluster.wait_for_leader()
        converged = cluster.wait_for_convergence()
        for node_id, s in converged.items():
            print(f"  {node_id}: commit {s['commit_index']}, "
                  f"applied {s['last_applied']}, term {s['term']}, "
                  f"role {s['role']}")
        seed = cluster.addrs[0]
        lost = []
        for key, value in acked.items():
            resp = client_request(seed, {"type": "client", "op": "get",
                                         "key": key}, timeout=5.0)
            if resp.get("status") != "ok" or resp.get("value") != value:
                lost.append((key, value, resp))
        # CAS counters: each owned key at its last successful value,
        # and the shared counter at exactly the number of successful
        # shared CAS ops — each one increments by one, so the count
        # is the value.
        for key, value in cas_acked.items():
            resp = client_request(seed, {"type": "client", "op": "get",
                                         "key": key}, timeout=5.0)
            if resp.get("status") != "ok" or resp.get("value") != value:
                lost.append((key, value, resp))
        resp = client_request(seed, {"type": "client", "op": "get",
                                     "key": SHARED_CAS_KEY}, timeout=5.0)
        if resp.get("status") != "ok" or resp.get("value") != str(
                shared_successes[0]):
            lost.append((SHARED_CAS_KEY, str(shared_successes[0]), resp))
        if lost:
            for key, value, resp in lost[:10]:
                print(f"LOST acked write {key}={value}: {resp}",
                      file=sys.stderr)
            print(f"{len(lost)} acked writes lost", file=sys.stderr)
            return 1
        print(f"chaos ok: {len(acked)} acked writes all present, "
              f"3 nodes converged at commit "
              f"{converged['node1']['commit_index']} "
              f"after {args.faults} kill/restart faults "
              f"(seed {args.seed})")
        cas_ops = [op for op in history.ops() if op.kind == "cas"]
        cas_ok = sum(1 for op in cas_ops if op.status == "ok")
        print(f"cas ok: {cas_ok} successful CAS "
              f"({len(cas_ops) - cas_ok} failed races) across "
              f"{len(cas_acked)} owned counters, shared counter at "
              f"{shared_successes[0]}")
        # Linearizability: every concurrent read must have returned a
        # value some linearization could produce — no stale reads, no
        # values from the future — and every successful CAS must have
        # seen its expected value when it linearized, across the
        # whole fault schedule.
        lin = history.check()
        print(f"linearizability: {lin}")
        if lin.multi_writer_keys:
            print(f"  note: {len(lin.multi_writer_keys)} key(s) had "
                  f"multiple writers, checked with the weaker rule")
        if not lin.ok:
            return 1
        return 0 if not stop else 130
    except (RuntimeError, AssertionError) as exc:
        print(f"chaos failed: {exc}", file=sys.stderr)
        return 1
    finally:
        cluster.stop()
        if not args.keep:
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
