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

    def __init__(self, n: int = 3, base_dir: str | None = None,
                 snapshot_threshold: int = 1000,
                 snapshot_chunk_size: int = 65536,
                 pipeline_window: int = 10,
                 membership_catchup_timeout: float = 5.0,
                 group_commit_window_ms: float = 2.0,
                 group_commit_max_batch: int = 0):
        self.n = n
        self.snapshot_threshold = snapshot_threshold
        self.snapshot_chunk_size = snapshot_chunk_size
        self.pipeline_window = pipeline_window
        self.membership_catchup_timeout = membership_catchup_timeout
        self.group_commit_window_ms = group_commit_window_ms
        self.group_commit_max_batch = group_commit_max_batch
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
            node = RaftNode(node_id, peers, data_dir, KVStore(),
                            snapshot_threshold=snapshot_threshold,
                            snapshot_chunk_size=snapshot_chunk_size,
                            pipeline_window=pipeline_window,
                            membership_catchup_timeout=membership_catchup_timeout,
                            group_commit_window_ms=group_commit_window_ms,
                            group_commit_max_batch=group_commit_max_batch)
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
        node = RaftNode(node_id, peers, self.base / node_id, KVStore(),
                        snapshot_threshold=self.snapshot_threshold,
                        snapshot_chunk_size=self.snapshot_chunk_size,
                        pipeline_window=self.pipeline_window,
                        membership_catchup_timeout=self.membership_catchup_timeout,
                        group_commit_window_ms=self.group_commit_window_ms,
                        group_commit_max_batch=self.group_commit_max_batch)
        self.nodes[node_id] = node
        host, port = self.addrs[node_id]
        node.start(host, port)
        self.live.add(node_id)
        return node

    def add_node(self, node_id: str,
                 membership_catchup_timeout: float | None = None) -> RaftNode:
        """Start a brand-new node that is not yet a member of the
        cluster, with the current nodes as its peers. It sits idle
        (pre-vote refuses it while the cluster has a live leader)
        until the leader adds it with an add_server client request."""
        port = free_port()
        peers = {p: self.addrs[p] for p in self.nodes}
        kwargs: dict = {"snapshot_threshold": self.snapshot_threshold,
                        "pipeline_window": self.pipeline_window}
        if membership_catchup_timeout is not None:
            kwargs["membership_catchup_timeout"] = membership_catchup_timeout
        node = RaftNode(node_id, peers, self.base / node_id, KVStore(), **kwargs)
        self.nodes[node_id] = node
        self.addrs[node_id] = ("127.0.0.1", port)
        node.start("127.0.0.1", port)
        self.live.add(node_id)
        return node

    def partition(self, a: str, b: str) -> None:
        """Split the link between nodes ``a`` and ``b`` in both
        directions: neither sends peer RPCs to the other, and each
        drops the other's RPCs on arrival. Client and status traffic
        is unaffected, so a partitioned node still answers status
        and still claims leadership to a client that asks it."""
        self.nodes[a].partition(b)
        self.nodes[b].partition(a)

    def heal_partition(self, a: str, b: str) -> None:
        """Restore the link between nodes ``a`` and ``b``."""
        self.nodes[a].heal(b)
        self.nodes[b].heal(a)

    def isolate(self, node_id: str) -> None:
        """Partition ``node_id`` from every other node in the
        cluster — the node is on its own side of the split."""
        for other in self.nodes:
            if other != node_id:
                self.partition(node_id, other)

    def heal_all(self) -> None:
        """Heal every partition on every node."""
        for node in self.nodes.values():
            node.heal_all()

    def status(self, node_id: str) -> dict:
        """Ask one node for its status RPC (role, term, config, ...)."""
        host, port = self.addrs[node_id]
        return send_rpc(host, port, {"type": "status"},
                        timeout=2.0) or {}

    def wait_for_config(self, node_id: str, members: list[str],
                        timeout: float = 5.0) -> bool:
        """Poll a node's status until its voting config is ``members``."""
        deadline = time.monotonic() + timeout
        want = sorted(members)
        while time.monotonic() < deadline:
            if sorted(self.status(node_id).get("config", [])) == want:
                return True
            time.sleep(0.05)
        return False

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

    def wait_for_stable_leader(self, stable_for: float = 0.5,
                               timeout: float = 10.0) -> RaftNode:
        """Like wait_for_leader, but only returns once the same node has
        been the single leader for ``stable_for`` seconds. After rapid
        restarts the cluster churns through elections (a candidate that
        already bumped its term can still depose the leader
        wait_for_leader just saw), so a single sighting is not enough
        to know the cluster has settled."""
        deadline = time.monotonic() + timeout
        seen_id: str | None = None
        seen_since = time.monotonic()
        while time.monotonic() < deadline:
            leaders = [self.nodes[i] for i in self.live
                       if self.nodes[i].is_leader]
            if len(leaders) == 1 and leaders[0].id == seen_id:
                if time.monotonic() - seen_since >= stable_for:
                    return leaders[0]
            elif len(leaders) == 1:
                seen_id = leaders[0].id
                seen_since = time.monotonic()
            else:
                seen_id = None
            time.sleep(0.05)
        raise AssertionError(
            f"no stable leader after {timeout}s: "
            + ", ".join(f"{i}={n.role}" for i, n in self.nodes.items())
        )

    def wait_for_removed(self, node_id: str, timeout: float = 5.0) -> bool:
        """Poll a node's status until it reports removed=True. A
        removed follower only learns of its removal on the next
        heartbeat carrying the new commit index, so the flag lags the
        leader's ok by a heartbeat or two."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.status(node_id).get("removed"):
                return True
            time.sleep(0.05)
        return False

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
