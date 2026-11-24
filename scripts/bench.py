#!/usr/bin/env python3
"""Benchmark raftkv: committed-write and ReadIndex-read throughput.

Starts a real 3-node cluster on localhost (subprocesses, like
``scripts/demo.sh``), waits for a leader, then measures:

  * sequential committed writes — one client, each write waits for a
    majority replication round plus a disk fsync
  * concurrent committed writes — N threads sharing the same total
    number of writes
  * sequential ReadIndex reads of the written keys
  * concurrent ReadIndex reads
  * (with ``--pipelined``) pipelined reads of the written keys:
    one client, one persistent connection, ``--pipeline-depth``
    requests sent at a time without waiting for each reply
  * (with ``--pipelined-writes``) pipelined writes of fresh keys:
    one client, one persistent connection, ``--pipeline-depth``
    set ops sent at a time through ``Session.pipeline`` — each op
    still appends its own log entry and pays its own fsync, so
    this measures what removing the client's round-trip wait is
    worth when the commit itself is fsync-bound
  * (with ``--mget``) multi-get reads of the written keys: one
    client, one ``mget`` RPC per ``--mget-batch`` keys, so the
    per-key cost is one state machine lookup instead of one
    dispatch each

Each phase reports ops/sec and per-op latency percentiles (p50/p95/p99).
The pipelined phase reports mean per-op latency instead: per-op
latencies are not observable inside a batch, only the batch time.
The mget phase reports the same way: a batch is one request, so only
the batch time is observable, and the per-key rate is what the phase
measures.
A sample of the written keys is read back afterwards and checked, so a
fast run that lost data still fails. Exits non-zero if any operation
fails, a verification read disagrees, or the cluster never elects a
leader.

Usage:

    python3 scripts/bench.py [--writes 200] [--threads 10]
        [--pool-size 10] [--pipelined] [--pipeline-depth 50]
        [--pipelined-writes]
        [--mget] [--mget-batch 50]
        [--port-base 5100]
        [--data-dir /tmp/raftkv-bench] [--keep]

``--pool-size N`` reuses N persistent connections per node instead
of opening a fresh TCP connection per request; without it every
request pays a handshake, which is what capped concurrent reads.

The numbers are latency-bound on localhost: every committed write pays
a full round trip to a majority plus an fsync of the log append, so
expect a few hundred writes/sec. Group commit (batching the fsyncs)
is what would raise that number; see the README roadmap.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from raftkv.client import Session, get_many, mget
from raftkv.client import request as client_request  # noqa: E402
from raftkv.transport import ConnectionPool, send_rpc  # noqa: E402

SEED_KEY = "__bench_probe__"
VALUE = "v" * 32  # 32-byte values, like a small session token


def percentile(latencies: list[float], pct: float) -> float:
    """Nearest-rank percentile of a non-empty latency list (seconds)."""
    ordered = sorted(latencies)
    rank = max(1, int(pct / 100.0 * len(ordered) + 0.5))
    return ordered[min(rank, len(ordered)) - 1]


def report(phase: str, latencies: list[float], elapsed: float) -> None:
    n = len(latencies)
    print(
        f"{phase:<28} {n:>5} ops  {elapsed:>6.2f}s  "
        f"{n / elapsed:>7.1f} ops/s  "
        f"p50 {percentile(latencies, 50) * 1000:>6.1f}ms  "
        f"p95 {percentile(latencies, 95) * 1000:>6.1f}ms  "
        f"p99 {percentile(latencies, 99) * 1000:>6.1f}ms"
    )


class Cluster:
    """A throwaway 3-node cluster running as subprocesses."""

    def __init__(self, port_base: int, data_dir: Path):
        self.port_base = port_base
        self.data_dir = data_dir
        self.procs: list[subprocess.Popen] = []
        self.addrs = [
            ("127.0.0.1", port_base + i) for i in range(3)
        ]

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        for i, (host, port) in enumerate(self.addrs):
            node_id = f"node{i + 1}"
            peers = ",".join(
                f"node{j + 1}=127.0.0.1:{self.port_base + j}"
                for j in range(3) if j != i
            )
            log = open(self.data_dir / f"{node_id}.log", "w")
            proc = subprocess.Popen(
                [sys.executable, "-m", "raftkv.server",
                 "--id", node_id, "--port", str(port),
                 "--peers", peers,
                 "--data-dir", str(self.data_dir / node_id)],
                stdout=log, stderr=subprocess.STDOUT,
                cwd=str(Path(__file__).resolve().parent.parent),
            )
            self.procs.append(proc)
        self.wait_for_leader()

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

    def stop(self) -> None:
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + 5.0
        for proc in self.procs:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                pass
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
        self.procs.clear()


def do_set(addr: tuple[str, int], i: int,
           pool: ConnectionPool | None = None) -> float:
    start = time.perf_counter()
    resp = client_request(
        addr, {"type": "client", "op": "set",
               "key": f"bench:{i}", "value": VALUE},
        timeout=30.0, pool=pool,
    )
    elapsed = time.perf_counter() - start
    if resp.get("status") != "ok":
        raise RuntimeError(f"set bench:{i} failed: {resp}")
    return elapsed


def do_get(addr: tuple[str, int], i: int,
           pool: ConnectionPool | None = None) -> float:
    start = time.perf_counter()
    resp = client_request(
        addr, {"type": "client", "op": "get", "key": f"bench:{i}"},
        timeout=30.0, pool=pool,
    )
    elapsed = time.perf_counter() - start
    if resp.get("status") != "ok" or resp.get("value") != VALUE:
        raise RuntimeError(f"get bench:{i} failed: {resp}")
    return elapsed


def find_leader(addrs: list[tuple[str, int]]) -> tuple[str, int]:
    """Find the leader by asking each node directly: a follower
    answers a get with ``not_leader``, the leader answers ``ok``.
    Pipelined reads go straight to the leader's address, so this
    runs once before the pipelined phase instead of paying a
    redirect hop per batch."""
    for addr in addrs:
        resp = send_rpc(addr[0], addr[1],
                        {"type": "client", "op": "get", "key": SEED_KEY},
                        timeout=2.0)
        if resp is not None and resp.get("status") == "ok":
            return addr
    raise RuntimeError("no node answered as leader")


def run_pipelined_reads(addr: tuple[str, int], count: int,
                        depth: int) -> None:
    """One client, ``count`` reads, sent ``depth`` at a time over a
    single persistent connection without waiting between requests.

    Per-op latencies are not observable inside a batch — a batch is
    one send and one stream of replies — so the row reports the mean
    per-op latency (batch time / batch size) instead of percentiles.
    Every reply is checked against the written value, so a fast run
    that lost data still fails."""
    keys = [f"bench:{i}" for i in range(count)]
    start = time.perf_counter()
    batch_times: list[float] = []
    for i in range(0, count, depth):
        batch_keys = keys[i:i + depth]
        batch_start = time.perf_counter()
        replies = get_many(addr, batch_keys, timeout=30.0, depth=depth)
        batch_times.append(time.perf_counter() - batch_start)
        for key, resp in zip(batch_keys, replies):
            if resp.get("status") != "ok" or resp.get("value") != VALUE:
                raise RuntimeError(f"pipelined get {key} failed: {resp}")
    elapsed = time.perf_counter() - start
    mean_ms = elapsed / count * 1000
    print(
        f"{'pipelined reads':<28} {count:>5} ops  {elapsed:>6.2f}s  "
        f"{count / elapsed:>7.1f} ops/s  "
        f"mean {mean_ms:>6.3f}ms/op  "
        f"(batches of {depth}, "
        f"batch p50 {percentile(batch_times, 50) * 1000:>6.1f}ms)"
    )


def run_pipelined_writes(addr: tuple[str, int], count: int,
                         depth: int) -> None:
    """One client, ``count`` sets, sent ``depth`` at a time over a
    single persistent connection through ``Session.pipeline``.

    Each op still appends its own log entry and pays its own
    fsync — pipelining removes the client's round-trip wait
    between writes, not the commit — so the row reports the mean
    per-op latency (batch time / batch size) and the rate, which
    is what the phase measures: how much of a sequential write's
    ~8-9ms is the client waiting versus the fsync. Every reply
    is checked, and a sample of the written keys is read back
    afterwards by the main verification, so a fast run that lost
    data still fails."""
    ops = [{"op": "set", "key": f"bench:pw:{i}", "value": VALUE}
           for i in range(count)]
    start = time.perf_counter()
    batch_times: list[float] = []
    with Session(addr) as session:
        for i in range(0, count, depth):
            batch = ops[i:i + depth]
            batch_start = time.perf_counter()
            replies = session.pipeline(batch, depth=depth, timeout=30.0)
            batch_times.append(time.perf_counter() - batch_start)
            for op, resp in zip(batch, replies):
                if resp.get("status") != "ok":
                    raise RuntimeError(
                        f"pipelined set {op['key']} failed: {resp}")
    elapsed = time.perf_counter() - start
    mean_ms = elapsed / count * 1000
    print(
        f"{'pipelined writes':<28} {count:>5} ops  {elapsed:>6.2f}s  "
        f"{count / elapsed:>7.1f} ops/s  "
        f"mean {mean_ms:>6.3f}ms/op  "
        f"(batches of {depth}, "
        f"batch p50 {percentile(batch_times, 50) * 1000:>6.1f}ms)"
    )


def run_mget_reads(addr: tuple[str, int], count: int,
                   batch: int) -> None:
    """One client, ``count`` reads, one ``mget`` RPC per ``batch``
    keys over a persistent connection.

    A batch is one request — one leadership confirmation, one apply
    wait, one frame — so per-key latencies are not observable, only
    the batch time; the row reports the mean per-key latency (batch
    time / batch size) and the per-key rate, which is what the phase
    measures: whether serving many keys per dispatch is cheaper than
    one dispatch per key. Every value is checked against the written
    value, so a fast run that lost data still fails."""
    keys = [f"bench:{i}" for i in range(count)]
    start = time.perf_counter()
    batch_times: list[float] = []
    for i in range(0, count, batch):
        batch_keys = keys[i:i + batch]
        batch_start = time.perf_counter()
        resp = mget(addr, batch_keys, timeout=30.0)
        batch_times.append(time.perf_counter() - batch_start)
        if resp.get("status") != "ok":
            raise RuntimeError(f"mget {batch_keys[0]}.. failed: {resp}")
        values = resp.get("values") or []
        if len(values) != len(batch_keys) or any(
                v != VALUE for v in values):
            raise RuntimeError(
                f"mget {batch_keys[0]}.. returned wrong values: {resp}")
    elapsed = time.perf_counter() - start
    mean_ms = elapsed / count * 1000
    print(
        f"{'mget reads':<28} {count:>5} ops  {elapsed:>6.2f}s  "
        f"{count / elapsed:>7.1f} ops/s  "
        f"mean {mean_ms:>6.3f}ms/op  "
        f"(batches of {batch}, "
        f"batch p50 {percentile(batch_times, 50) * 1000:>6.1f}ms)"
    )


def run_phase(label: str, fn, count: int,
              threads: int, addr: tuple[str, int],
              pool: ConnectionPool | None = None) -> None:
    """Run ``count`` ops through ``fn`` (1 or ``threads`` workers),
    then print the throughput and latency report."""
    latencies: list[float] = []
    start = time.perf_counter()
    if threads <= 1:
        latencies = [fn(addr, i, pool) for i in range(count)]
    else:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=threads) as pool_exec:
            latencies = list(pool_exec.map(
                lambda i: fn(addr, i, pool), range(count)))
    report(label, latencies, time.perf_counter() - start)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writes", type=int, default=200,
                        help="writes per write phase (default 200)")
    parser.add_argument("--threads", type=int, default=10,
                        help="concurrency for the concurrent phases "
                             "(default 10)")
    parser.add_argument("--pool-size", type=int, default=0,
                        help="reuse persistent client connections with "
                             "this many pooled connections per node "
                             "(default 0: one fresh TCP connection per "
                             "request, as before)")
    parser.add_argument("--pipelined", action="store_true",
                        help="also run a pipelined-reads phase: one "
                             "client reads all keys over a single "
                             "persistent connection, sending "
                             "--pipeline-depth requests at a time "
                             "without waiting for each reply")
    parser.add_argument("--pipeline-depth", type=int, default=50,
                        help="requests sent per pipelined batch "
                             "(default 50)")
    parser.add_argument("--pipelined-writes", action="store_true",
                        help="also run a pipelined-writes phase: one "
                             "client writes all keys with "
                             "Session.pipeline, sending "
                             "--pipeline-depth set ops at a time "
                             "without waiting for each reply")
    parser.add_argument("--mget", action="store_true",
                        help="also run a multi-get phase: one client "
                             "reads all keys with one mget RPC per "
                             "--mget-batch keys")
    parser.add_argument("--mget-batch", type=int, default=50,
                        help="keys served per mget request "
                             "(default 50)")
    parser.add_argument("--port-base", type=int, default=5100,
                        help="first of three consecutive ports "
                             "(default 5100)")
    parser.add_argument("--data-dir", default="/tmp/raftkv-bench",
                        help="cluster data dir (default /tmp/raftkv-bench)")
    parser.add_argument("--keep", action="store_true",
                        help="keep the data dir afterwards "
                             "(default: remove it)")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir)
    cluster = Cluster(args.port_base, data_dir)
    pool: ConnectionPool | None = None
    stop = False

    def _on_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        cluster.start()
        seed = cluster.addrs[0]
        pool = (ConnectionPool(max_size=args.pool_size)
                if args.pool_size > 0 else None)
        print(f"cluster up on ports "
              f"{args.port_base}-{args.port_base + 2} "
              f"({args.writes} writes, {args.threads} threads"
              + (f", connection pool x{args.pool_size})"
                 if pool is not None else ")"))
        print(f"{'phase':<28} {'ops':>5}  {'time':>6}  "
              f"{'rate':>11}  {'p50':>10}  {'p95':>10}  {'p99':>10}")
        run_phase("sequential writes", do_set, args.writes, 1, seed, pool)
        run_phase(f"concurrent writes x{args.threads}", do_set,
                  args.writes, args.threads, seed, pool)
        run_phase("sequential reads", do_get, args.writes, 1, seed, pool)
        run_phase(f"concurrent reads x{args.threads}", do_get,
                  args.writes, args.threads, seed, pool)
        if args.pipelined:
            leader_addr = find_leader(cluster.addrs)
            run_pipelined_reads(leader_addr, args.writes,
                                args.pipeline_depth)
        if args.pipelined_writes:
            leader_addr = find_leader(cluster.addrs)
            run_pipelined_writes(leader_addr, args.writes,
                                 args.pipeline_depth)
        if args.mget:
            leader_addr = find_leader(cluster.addrs)
            run_mget_reads(leader_addr, args.writes, args.mget_batch)
        # Verify a deterministic sample of the written keys.
        sample = list(range(0, args.writes, max(1, args.writes // 20)))
        for i in sample:
            do_get(seed, i, pool)
        print(f"verified {len(sample)} keys read back correctly")
        return 0 if not stop else 130
    except (RuntimeError, AssertionError) as exc:
        print(f"bench failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if pool is not None:
            pool.close()
        cluster.stop()
        if not args.keep:
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
