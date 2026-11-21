"""Raft consensus (Ongaro & Ousterhout, 2014), one class, stdlib only.

Threading model
---------------
- A TCP server thread accepts RPCs; each connection gets a handler thread.
- A timer thread drives elections and heartbeats.
- Client requests run on connection threads.
All mutable Raft state is guarded by a single re-entrant lock, and network
I/O is *always* done outside the lock. That last rule is load-bearing:
two nodes RPC-ing each other at the same time must never hold each
other's lock while waiting on the network, or they deadlock.

Log indexing
-------------
Log entries are 1-based, as in the Raft paper (entry 1 is log[0]).
``prev_log_index == 0`` means "no previous entry" (empty prefix).

Persistence
-----------
Per node, in ``data_dir``:
- ``meta.json`` — current_term, voted_for, commit_index (rewritten
  atomically via tmp file + rename whenever it changes).
- ``log.jsonl`` — one JSON object per line, {"term": n, "command": {...}}.
  Appended + fsynced on every new entry; rewritten whole when the log is
  truncated by a conflicting AppendEntries.

On restart the node reloads term/vote/log and re-applies the committed
prefix to the state machine, so it rejoins with the same state it had.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from pathlib import Path

from .transport import send_rpc, serve_in_background

# Randomized election timeout. Short values keep the test suite and the
# demo snappy; 150-300 ms is comfortably above localhost RTT.
ELECTION_TIMEOUT_MIN = 0.15
ELECTION_TIMEOUT_MAX = 0.30
HEARTBEAT_INTERVAL = 0.05
RPC_TIMEOUT = 0.5
CLIENT_COMMIT_TIMEOUT = 10.0
LEADER_LEASE_TIMEOUT = 1.0


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: dict[str, tuple[str, int]],
        data_dir: str | os.PathLike,
        state_machine,
    ) -> None:
        """
        node_id: this node's id, e.g. "node1".
        peers:   {peer_id: (host, port)} for every *other* node in the cluster.
        data_dir: directory holding meta.json + log.jsonl for this node.
        state_machine: object with an ``apply(command: dict)`` method.
        """
        self.id = node_id
        self.peers = dict(peers)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sm = state_machine

        self.lock = threading.RLock()
        self.commit_cond = threading.Condition(self.lock)

        # Persistent state (must survive restarts).
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[dict] = []  # {"term": int, "command": dict}

        # Volatile state.
        self.commit_index = 0
        self.last_applied = 0
        self.role = "follower"  # follower | candidate | leader
        self.leader_id: str | None = None
        self.votes: set[str] = set()

        # Leader-only volatile state, rebuilt on every election win.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()
        self._last_heartbeat_sent = 0.0
        self._running = False
        self._server = None
        self._timer_thread: threading.Thread | None = None

        self._load_persistent_state()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    @property
    def _meta_path(self) -> Path:
        return self.data_dir / "meta.json"

    @property
    def _log_path(self) -> Path:
        return self.data_dir / "log.jsonl"

    def _load_persistent_state(self) -> None:
        if self._meta_path.exists():
            with open(self._meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            self.current_term = int(meta.get("current_term", 0))
            self.voted_for = meta.get("voted_for")
            self.commit_index = int(meta.get("commit_index", 0))
        if self._log_path.exists():
            with open(self._log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.log.append(json.loads(line))
        # Re-apply the committed prefix so the state machine matches what
        # this node had before the restart. SET/DEL are deterministic, so
        # replaying the committed prefix is always safe.
        self.last_applied = 0
        self._apply_committed()

    def _save_meta(self) -> None:
        """Persist term/vote/commit atomically. Call with the lock held."""
        meta = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
        }
        tmp = self._meta_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path)

    def _append_log_entry(self, entry: dict) -> None:
        """Append one entry to log.jsonl and fsync. Call with lock held."""
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _rewrite_log(self) -> None:
        """Rewrite the whole log file (used after truncation). Lock held."""
        tmp = self._log_path.with_suffix(".jsonl.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for entry in self.log:
                f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._log_path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self, host: str, port: int) -> None:
        with self.lock:
            if self._running:
                return
            self._running = True
        self._server = serve_in_background(host, port, self.handle_rpc)
        self._timer_thread = threading.Thread(
            target=self._timer_loop, name=f"raft-timer-{self.id}", daemon=True
        )
        self._timer_thread.start()

    def stop(self) -> None:
        with self.lock:
            self._running = False
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._timer_thread is not None:
            self._timer_thread.join(timeout=2.0)
            self._timer_thread = None

    @property
    def is_leader(self) -> bool:
        with self.lock:
            return self.role == "leader"

    def leader_info(self) -> tuple[str | None, tuple[str, int] | None]:
        """(leader_id, (host, port) or None). Lock-free read via the lock."""
        with self.lock:
            lid = self.leader_id
        addr = self.peers.get(lid) if lid and lid != self.id else None
        return lid, addr

    # ------------------------------------------------------------------
    # Small helpers (call with the lock held unless noted)
    # ------------------------------------------------------------------
    def _random_timeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _reset_election_timer(self) -> None:
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()

    def _last_log_index(self) -> int:
        return len(self.log)

    def _last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _step_down(self, term: int) -> None:
        """Move to follower at a newer term. Lock held."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._save_meta()
        self.role = "follower"
        self.leader_id = None
        self._reset_election_timer()

    def _apply_committed(self) -> None:
        """Apply every committed-but-unapplied entry, in order. Lock held."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            self.sm.apply(entry["command"])
        self._save_meta()

    def _advance_commit(self) -> None:
        """Leader only: advance commit_index when a majority has an entry
        from the *current* term, then apply it. Lock held.

        The current-term restriction matters: a leader must never conclude
        an old-term entry is committed just by counting replicas (Raft
        section 5.4.2). Committing a current-term entry transitively
        commits everything before it.
        """
        if self.role != "leader":
            return
        for n in range(self._last_log_index(), self.commit_index, -1):
            if self.log[n - 1]["term"] != self.current_term:
                continue
            count = 1  # the leader itself has it
            count += sum(1 for m in self.match_index.values() if m >= n)
            if count >= self._majority():
                self.commit_index = n
                self._apply_committed()
                self.commit_cond.notify_all()
                break

    # ------------------------------------------------------------------
    # RPC dispatch
    # ------------------------------------------------------------------
    def handle_rpc(self, msg: dict) -> dict:
        msg_type = msg.get("type")
        if msg_type == "request_vote":
            return self._on_request_vote(msg)
        if msg_type == "append_entries":
            return self._on_append_entries(msg)
        if msg_type == "client":
            return self._on_client(msg)
        if msg_type == "status":
            with self.lock:
                return {
                    "type": "status_response",
                    "node_id": self.id,
                    "role": self.role,
                    "term": self.current_term,
                    "leader_id": self.leader_id,
                    "commit_index": self.commit_index,
                    "last_applied": self.last_applied,
                    "log_length": len(self.log),
                }
        return {"type": "error", "error": f"unknown message type: {msg_type!r}"}

    # ------------------------------------------------------------------
    # RequestVote
    # ------------------------------------------------------------------
    def _on_request_vote(self, msg: dict) -> dict:
        term = int(msg["term"])
        candidate = msg["candidate_id"]
        with self.lock:
            if term < self.current_term:
                return {"type": "request_vote_response",
                        "term": self.current_term, "vote_granted": False}
            if term > self.current_term:
                self._step_down(term)
            # A candidate's log is up to date if its last term is newer,
            # or the terms tie and its log is at least as long.
            cand_term = int(msg["last_log_term"])
            cand_index = int(msg["last_log_index"])
            up_to_date = (cand_term > self._last_log_term()) or (
                cand_term == self._last_log_term()
                and cand_index >= self._last_log_index()
            )
            granted = False
            if (self.voted_for is None or self.voted_for == candidate) and up_to_date:
                self.voted_for = candidate
                self._save_meta()
                self._reset_election_timer()
                granted = True
            return {"type": "request_vote_response",
                    "term": self.current_term, "vote_granted": granted}

    # ------------------------------------------------------------------
    # AppendEntries
    # ------------------------------------------------------------------
    def _on_append_entries(self, msg: dict) -> dict:
        term = int(msg["term"])
        leader_id = msg["leader_id"]
        prev_index = int(msg["prev_log_index"])
        prev_term = int(msg["prev_log_term"])
        entries = msg.get("entries") or []
        leader_commit = int(msg["leader_commit"])
        with self.lock:
            if term < self.current_term:
                return {"type": "append_entries_response",
                        "term": self.current_term, "success": False,
                        "match_index": self._last_log_index()}
            if term > self.current_term:
                self._step_down(term)
            # A valid AppendEntries from the current term's leader means
            # this node is not a leader any more, even if it was a
            # candidate in the same term.
            self.role = "follower"
            self.leader_id = leader_id
            self._reset_election_timer()

            # Log matching property: the log must contain an entry at
            # prev_log_index with term prev_log_term, or the RPC fails
            # and the leader backs up and retries.
            if prev_index > 0:
                if prev_index > self._last_log_index():
                    return {"type": "append_entries_response",
                            "term": self.current_term, "success": False,
                            "match_index": self._last_log_index()}
                if self.log[prev_index - 1]["term"] != prev_term:
                    return {"type": "append_entries_response",
                            "term": self.current_term, "success": False,
                            "match_index": prev_index - 1}

            # Append new entries, deleting anything that conflicts.
            idx = prev_index
            for entry in entries:
                idx += 1
                if idx <= self._last_log_index():
                    if self.log[idx - 1]["term"] != entry["term"]:
                        del self.log[idx - 1:]
                        self._rewrite_log()
                        self.log.append(entry)
                        self._append_log_entry(entry)
                else:
                    self.log.append(entry)
                    self._append_log_entry(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_committed()

            return {"type": "append_entries_response",
                    "term": self.current_term, "success": True,
                    "match_index": self._last_log_index()}

    # ------------------------------------------------------------------
    # Client protocol
    # ------------------------------------------------------------------
    def _on_client(self, msg: dict) -> dict:
        op = msg.get("op")
        key = msg.get("key")
        if op not in ("set", "get", "del") or key is None:
            return {"type": "client_response", "status": "error",
                    "error": "op must be set/get/del with a key"}
        if op == "get":
            return self._serve_read(key)
        command = {"op": op, "key": key}
        if op == "set":
            command["value"] = msg.get("value")
        return self._submit_write(command)

    def _redirect(self) -> dict:
        leader_id, addr = self.leader_info()
        return {"type": "client_response", "status": "not_leader",
                "leader": leader_id,
                "leader_addr": list(addr) if addr else None}

    def _submit_write(self, command: dict) -> dict:
        """Append a command to the log, replicate it, wait for commit."""
        with self.lock:
            if self.role != "leader":
                return self._redirect()
            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            self._append_log_entry(entry)
            index = self._last_log_index()
            peers = list(self.peers)

        # Replicate in parallel; each thread updates match/next and may
        # advance the commit index.
        threads = [
            threading.Thread(target=self._replicate_to_peer, args=(p,), daemon=True)
            for p in peers
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=RPC_TIMEOUT * 4)

        deadline = time.monotonic() + CLIENT_COMMIT_TIMEOUT
        with self.commit_cond:
            while self.commit_index < index:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {"type": "client_response", "status": "timeout",
                            "error": "write did not commit in time"}
                self.commit_cond.wait(timeout=remaining)
            if self.role != "leader":
                return {"type": "client_response", "status": "error",
                        "error": "lost leadership while committing"}
        return {"type": "client_response", "status": "ok"}

    def _serve_read(self, key: str) -> dict:
        """Leader-only read with a leadership check.

        A leader that is partitioned from the cluster must not serve stale
        reads, so before reading it sends a heartbeat round and waits for
        a majority to acknowledge it in the current term. Only then is
        the local (fully applied) state safe to read.
        """
        with self.lock:
            if self.role != "leader":
                return self._redirect()
            term = self.current_term
        if not self._confirm_leadership(term):
            with self.lock:
                if self.role != "leader":
                    return self._redirect()
            return {"type": "client_response", "status": "error",
                    "error": "could not confirm leadership"}
        with self.lock:
            value = self.sm.get(key)
        return {"type": "client_response", "status": "ok", "value": value}

    def _confirm_leadership(self, term: int) -> bool:
        """Heartbeat round: succeed only if a majority acks in this term."""
        peers = list(self.peers)
        acks: list[bool] = []
        ack_lock = threading.Lock()

        def _ping(peer: str) -> None:
            ok = self._replicate_to_peer(peer)
            with ack_lock:
                acks.append(ok)

        threads = [threading.Thread(target=_ping, args=(p,), daemon=True)
                   for p in peers]
        for t in threads:
            t.start()
        deadline = time.monotonic() + LEADER_LEASE_TIMEOUT
        for t in threads:
            t.join(timeout=max(0.0, deadline - time.monotonic()))
        with self.lock:
            still_leader = self.role == "leader" and self.current_term == term
        return still_leader and (1 + sum(acks)) >= self._majority()

    # ------------------------------------------------------------------
    # Leader replication
    # ------------------------------------------------------------------
    def _build_append_entries(self, peer: str) -> dict | None:
        """Build the next AppendEntries for a peer. Lock held. Returns
        None if this node is no longer the leader."""
        if self.role != "leader":
            return None
        next_idx = self.next_index.get(peer, self._last_log_index() + 1)
        prev_index = next_idx - 1
        prev_term = self.log[prev_index - 1]["term"] if prev_index > 0 else 0
        entries = self.log[next_idx - 1:]
        return {
            "type": "append_entries",
            "term": self.current_term,
            "leader_id": self.id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

    def _replicate_to_peer(self, peer: str) -> bool:
        """Send AppendEntries until the peer catches up or we give up.
        Returns True if the peer acked an AppendEntries in our term.
        Network I/O happens outside the lock; responses are applied
        under it. Retries back the peer's next_index down on mismatch,
        which is how a new leader brings a stale follower up to date."""
        host, port = self.peers[peer]
        for _attempt in range(5):
            with self.lock:
                if self.role != "leader":
                    return False
                msg = self._build_append_entries(peer)
                if msg is None:
                    return False
                sent_entries = len(msg["entries"])
                sent_prev = msg["prev_log_index"]
            resp = send_rpc(host, port, msg, timeout=RPC_TIMEOUT)
            with self.lock:
                if resp is None:
                    return False
                resp_term = int(resp.get("term", 0))
                if resp_term > self.current_term:
                    self._step_down(resp_term)
                    return False
                if self.role != "leader" or resp_term != self.current_term:
                    return False
                if resp.get("success"):
                    self.match_index[peer] = sent_prev + sent_entries
                    self.next_index[peer] = self.match_index[peer] + 1
                    self._advance_commit()
                    return True
                # Log mismatch: back up and retry with an earlier prefix.
                hint = int(resp.get("match_index", sent_prev - 1))
                self.next_index[peer] = max(1, min(self.next_index.get(peer, 1), hint + 1))
        return False

    def _become_leader(self) -> None:
        with self.lock:
            self.role = "leader"
            self.leader_id = self.id
            last = self._last_log_index()
            self.next_index = {p: last + 1 for p in self.peers}
            self.match_index = {p: 0 for p in self.peers}
        # Assert leadership immediately so followers learn about it and
        # the commit index starts flowing.
        self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        peers = list(self.peers)
        for peer in peers:
            threading.Thread(target=self._replicate_to_peer,
                             args=(peer,), daemon=True).start()

    # ------------------------------------------------------------------
    # Elections
    # ------------------------------------------------------------------
    def _run_election(self) -> None:
        """Start an election in this term. Runs on the timer thread."""
        with self.lock:
            self.current_term += 1
            self.voted_for = self.id
            self.role = "candidate"
            self.leader_id = None
            self.votes = {self.id}
            term = self.current_term
            last_index = self._last_log_index()
            last_term = self._last_log_term()
            self._save_meta()
            self._reset_election_timer()
        msg = {
            "type": "request_vote",
            "term": term,
            "candidate_id": self.id,
            "last_log_index": last_index,
            "last_log_term": last_term,
        }

        def _ask(peer: str) -> None:
            host, port = self.peers[peer]
            resp = send_rpc(host, port, msg, timeout=RPC_TIMEOUT)
            if resp and resp.get("vote_granted") and int(resp.get("term", 0)) == term:
                with self.lock:
                    self.votes.add(peer)

        threads = [threading.Thread(target=_ask, args=(p,), daemon=True)
                   for p in self.peers]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=RPC_TIMEOUT + 0.2)
        with self.lock:
            if (
                self.role == "candidate"
                and self.current_term == term
                and len(self.votes) >= self._majority()
            ):
                won = True
            else:
                won = False
        if won:
            self._become_leader()

    # ------------------------------------------------------------------
    # Timer loop
    # ------------------------------------------------------------------
    def _timer_loop(self) -> None:
        while True:
            with self.lock:
                if not self._running:
                    return
                role = self.role
                now = time.monotonic()
                timed_out = now - self._last_heartbeat > self._election_timeout
                heartbeat_due = (
                    role == "leader"
                    and now - self._last_heartbeat_sent >= HEARTBEAT_INTERVAL
                )
                if heartbeat_due:
                    self._last_heartbeat_sent = now
            if role == "leader" and heartbeat_due:
                self._send_heartbeats()
            elif role != "leader" and timed_out:
                self._run_election()
            time.sleep(0.005)
