"""raftkv node: start one Raft node in a cluster.

Example, three nodes on localhost (run each in its own terminal):

    python -m raftkv.server --id node1 --port 5001 \
        --peers node2=127.0.0.1:5002,node3=127.0.0.1:5003 \
        --data-dir /tmp/raftkv-demo/node1
    python -m raftkv.server --id node2 --port 5002 \
        --peers node1=127.0.0.1:5001,node3=127.0.0.1:5003 \
        --data-dir /tmp/raftkv-demo/node2
    python -m raftkv.server --id node3 --port 5003 \
        --peers node1=127.0.0.1:5001,node2=127.0.0.1:5002 \
        --data-dir /tmp/raftkv-demo/node3

See README.md or `make demo` for a one-command version.
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading

from .raft import RaftNode
from .store import KVStore


def parse_peers(spec: str) -> dict[str, tuple[str, int]]:
    """Parse "node2=127.0.0.1:5002,node3=127.0.0.1:5003" into a dict."""
    peers: dict[str, tuple[str, int]] = {}
    if not spec:
        return peers
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        node_id, _, addr = item.partition("=")
        host, _, port = addr.rpartition(":")
        if not node_id or not host or not port:
            raise ValueError(f"bad peer spec: {item!r} (want id=host:port)")
        peers[node_id] = (host, int(port))
    return peers


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="raftkv Raft node")
    parser.add_argument("--id", required=True, help="this node's id")
    parser.add_argument("--port", type=int, required=True, help="port to listen on")
    parser.add_argument("--host", default="127.0.0.1", help="host to listen on")
    parser.add_argument(
        "--peers", default="",
        help='other nodes as "id=host:port,id=host:port"',
    )
    parser.add_argument("--data-dir", required=True,
                        help="directory for meta.json + log.jsonl")
    args = parser.parse_args(argv)

    peers = parse_peers(args.peers)
    if args.id in peers:
        parser.error("--peers must list only the *other* nodes, not this node")

    node = RaftNode(args.id, peers, args.data_dir, KVStore())
    node.start(args.host, args.port)
    print(
        f"[{args.id}] listening on {args.host}:{args.port} "
        f"peers={sorted(peers)} data-dir={args.data_dir}",
        flush=True,
    )

    stop = threading.Event()

    def _on_signal(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    stop.wait()
    print(f"[{args.id}] shutting down", flush=True)
    node.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
