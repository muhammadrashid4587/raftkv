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

Each phase reports ops/sec and per-op latency percentiles (p50/p95/p99).
A sample of the written keys is read back afterwards and checked, so a
fast run that lost data still fails. Exits non-zero if any operation
fails, a verification read disagrees, or the cluster never elects a
leader.

Usage:

    python3 scripts/bench.py [--writes 200] [--threads 10]
        [--pool-size 10] [--port-base 5100]
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

from raftkv.client import request as client_request  # noqa: E402
from raftkv.transport import ConnectionPool  # noqa: E402

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
