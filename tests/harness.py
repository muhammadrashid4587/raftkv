"""Shared helpers for raftkv tests: spin up real nodes on localhost."""

from __future__ import annotations

import socket
import tempfile
import time
from pathlib import Path

from raftkv.raft import RaftNode
from raftkv.store import KVStore
from raftkv.transport import send_rpc


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Cluster:
    """A throwaway N-node cluster running in-process on localhost."""

    def __init__(self, n: int = 3, base_dir: str | None = None):
        self.n = n
        self.tmp = tempfile.TemporaryDirectory(prefix="raftkv-test-")
        self.base = Path(base_dir) if base_dir else Path(self.tmp.name)
        self.nodes: dict[str, RaftNode] = {}
        self.addrs: dict[str, tuple[str, int]] = {}
        self.live: set[str] = set()
        ids = [f"node{i + 1}" for i in range(n)]
        ports = {i: free_port() for i in ids}
        for node_id in ids:
            peers = {p: ("127.0.0.1", ports[p]) for p in ids if p != node_id}
            data_dir = self.base / node_id
            node = RaftNode(node_id, peers, data_dir, KVStore())
            self.nodes[node_id] = node
            self.addrs[node_id] = ("127.0.0.1", ports[node_id])

    def start_all(self) -> None:
        for node_id, node in self.nodes.items():
            host, port = self.addrs[node_id]
            node.start(host, port)
            self.live.add(node_id)

    def stop_all(self) -> None:
        for node in self.nodes.values():
            node.stop()
        self.live.clear()
        self.tmp.cleanup()

    def stop_node(self, node_id: str) -> None:
        self.nodes[node_id].stop()
        self.live.discard(node_id)

    def restart_node(self, node_id: str) -> RaftNode:
        """Restart a stopped node with the same id, peers and data dir."""
        peers = {p: self.addrs[p] for p in self.nodes if p != node_id}
        node = RaftNode(node_id, peers, self.base / node_id, KVStore())
        self.nodes[node_id] = node
        host, port = self.addrs[node_id]
        node.start(host, port)
        self.live.add(node_id)
        return node

    def wait_for_leader(self, timeout: float = 10.0) -> RaftNode:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = [self.nodes[i] for i in self.live
                       if self.nodes[i].is_leader]
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.05)
        raise AssertionError(
            f"no single leader after {timeout}s: "
            + ", ".join(f"{i}={n.role}" for i, n in self.nodes.items())
        )

    def leader(self) -> RaftNode | None:
        leaders = [self.nodes[i] for i in self.live if self.nodes[i].is_leader]
        return leaders[0] if len(leaders) == 1 else None


def client_request(addr: tuple[str, int], msg: dict, timeout: float = 12.0) -> dict:
    """Send a client request, following redirects like the real client."""
    host, port = addr
    for _ in range(10):
        resp = send_rpc(host, port, msg, timeout=timeout)
        if resp is None:
            return {"status": "error", "error": "node did not answer"}
        if resp.get("status") == "not_leader" and resp.get("leader_addr"):
            host, port = resp["leader_addr"][0], int(resp["leader_addr"][1])
            continue
        return resp
    return {"status": "error", "error": "too many redirects"}


def wait_for_key(node: RaftNode, key: str, value: str | None,
                 timeout: float = 5.0) -> bool:
    """Poll a node's local state machine until it holds key=value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.sm.get(key) == value:
            return True
        time.sleep(0.02)
    return False
