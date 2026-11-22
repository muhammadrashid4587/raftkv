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
- ``log.jsonl`` — one JSON object per line,
  {"term": n, "index": g, "command": {...}}. Appended + fsynced on
  every new entry; rewritten whole when the log is truncated by a
  conflicting AppendEntries or compacted by a snapshot. Each entry
  carries its global log index so a restart can tell a pre-snapshot
  entry (index <= snapshot_index) from a live one.
- ``snapshot.json`` — {"last_included_index": n, "last_included_term": t,
  "state": {...}}, the compacted state machine as of index n, written
  atomically. The in-memory log holds only entries *after* the
  snapshot; global index of log[k] is snapshot_index + k + 1.

On restart the node loads the snapshot into the state machine, then
replays the remaining committed log entries, so it rejoins with the
same state it had — without replaying the whole history.
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
# How long a ReadIndex read waits for the state machine to apply up to
# its read index before falling back to a plain heartbeat-confirmed read.
READ_INDEX_TIMEOUT = 2.0
# Compact the log once it holds more than this many post-snapshot
# entries. 1000 is large enough that compaction is rare in normal use
# but still bounds log.jsonl on a long-lived cluster. Tests pass a
# much smaller value to force snapshots quickly.
SNAPSHOT_THRESHOLD = 1000
# Pipelined replication: at most this many AppendEntries batches in
# flight per follower, each carrying at most PIPELINE_BATCH_SIZE
# entries. 10 x 32 = 320 entries can be outstanding, which is plenty
# on a LAN and still bounds memory and threads per peer.
PIPELINE_WINDOW = 10
PIPELINE_BATCH_SIZE = 32
# Backoff before re-dispatching to a peer whose batches timed out
# (unknown outcome) or were rejected for a log mismatch. Without it a
# dead peer would spin the window as fast as connections fail.
PIPELINE_RETRY_BACKOFF = 0.1
PIPELINE_MISMATCH_BACKOFF = 0.05


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: dict[str, tuple[str, int]],
        data_dir: str | os.PathLike,
        state_machine,
        snapshot_threshold: int = SNAPSHOT_THRESHOLD,
        pipeline_window: int = PIPELINE_WINDOW,
        pipeline_batch_size: int = PIPELINE_BATCH_SIZE,
    ) -> None:
        """
        node_id: this node's id, e.g. "node1".
        peers:   {peer_id: (host, port)} for every *other* node in the cluster.
        data_dir: directory holding meta.json + log.jsonl for this node.
        state_machine: object with an ``apply(command: dict)`` method.
        snapshot_threshold: compact the log once it holds more than this
            many post-snapshot entries (see SNAPSHOT_THRESHOLD).
        pipeline_window: at most this many AppendEntries batches in
            flight per follower (see PIPELINE_WINDOW). 1 means strictly
            sequential replication; tests use it to compare against.
        pipeline_batch_size: at most this many entries per
            AppendEntries batch (see PIPELINE_BATCH_SIZE).
        """
        self.id = node_id
        self.peers = dict(peers)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sm = state_machine
        self.snapshot_threshold = snapshot_threshold
        self.pipeline_window = pipeline_window
        self.pipeline_batch_size = pipeline_batch_size

        self.lock = threading.RLock()
        self.commit_cond = threading.Condition(self.lock)

        # Persistent state (must survive restarts).
        self.current_term = 0
        self.voted_for: str | None = None
        # self.log holds only entries AFTER snapshot_index; the global
        # index of log[k] is snapshot_index + k + 1.
        self.log: list[dict] = []  # {"term": int, "index": int, "command": dict}
        self.snapshot_index = 0
        self.snapshot_term = 0

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
        # Last time this node heard from a leader it accepts (a valid
        # AppendEntries / InstallSnapshot in the current term). Kept
        # separate from _last_heartbeat on purpose: _last_heartbeat is
        # reset by vote grants and election attempts too, but the
        # pre-vote voter rule is only about leader contact — a node
        # that just ran its own (failed) pre-vote must still be able
        # to grant one to a peer, or a fresh cluster could never elect
        # a leader at all. None means "never heard from a leader".
        self._last_leader_contact: float | None = None
        self._last_heartbeat_sent = 0.0
        # ReadIndex reads waiting for the state machine to apply up to
        # their read index. Each entry is {"read_index": int,
        # "term": int, "event": threading.Event}; the event is set by
        # _resolve_pending_reads once last_applied passes the index, or
        # by _fail_pending_reads when the node loses leadership.
        self._pending_reads: list[dict] = []
        # Pipelined replication state (leader only, rebuilt on every
        # election win by _become_leader). Per peer:
        # - _in_flight: {batch_start_index: {"prev", "end", "epoch"}}
        #   for AppendEntries batches sent but not yet answered.
        # - _next_to_send: next log index not yet dispatched.
        # - _pipeline_epoch: bumped on every log-mismatch backoff, so
        #   replies from superseded batches are ignored.
        # - _pipeline_acks: successful AppendEntries acks this term.
        # - _max_in_flight: high-water mark of concurrent batches.
        # - _peer_retry_after: monotonic time before which no new batch
        #   is dispatched (backoff after timeouts / mismatches).
        # - _pipeline_failures: consecutive AppendEntries timeouts /
        #   refusals per peer since the last ack. A driver gives up
        #   after 5 fresh failures in one call (the old code retried 5
        #   times) instead of spinning until its deadline — otherwise
        #   every write while a follower is down would cost the full
        #   2s driver deadline.
        # - _replicating: peers with an active replication driver; a
        #   second caller for the same peer waits for an ack instead of
        #   driving a second window, so a dead peer cannot spin up a
        #   driver per heartbeat.
        self._in_flight: dict[str, dict[int, dict]] = {}
        self._next_to_send: dict[str, int] = {}
        self._pipeline_epoch: dict[str, int] = {}
        self._pipeline_acks: dict[str, int] = {}
        self._max_in_flight: dict[str, int] = {}
        self._peer_retry_after: dict[str, float] = {}
        self._pipeline_failures: dict[str, int] = {}
        self._replicating: set[str] = set()
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

    @property
    def _snapshot_path(self) -> Path:
        return self.data_dir / "snapshot.json"

    def _load_persistent_state(self) -> None:
        if self._meta_path.exists():
            with open(self._meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            self.current_term = int(meta.get("current_term", 0))
            self.voted_for = meta.get("voted_for")
            self.commit_index = int(meta.get("commit_index", 0))
        # The snapshot comes first: it defines the state machine's
        # starting point and which log indices are already covered.
        if self._snapshot_path.exists():
            with open(self._snapshot_path, "r", encoding="utf-8") as f:
                snap = json.load(f)
            self.snapshot_index = int(snap.get("last_included_index", 0))
            self.snapshot_term = int(snap.get("last_included_term", 0))
            self.sm.load(snap.get("state") or {})
        if self._log_path.exists():
            expected = 1
            with open(self._log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    # Log files written before snapshots existed have no
                    # "index" field; their entries are 1..N in order.
                    idx = entry.get("index")
                    if idx is None:
                        idx = expected
                        entry["index"] = idx
                    expected = idx + 1
                    # A crash between writing snapshot.json and rewriting
                    # log.jsonl leaves covered entries in the file; the
                    # snapshot already holds their effect, so drop them.
                    if idx <= self.snapshot_index:
                        continue
                    self.log.append(entry)
        # The snapshot covers committed state, so the commit index can
        # never be behind it, even if meta.json was saved before the
        # snapshot landed.
        self.commit_index = max(self.commit_index, self.snapshot_index)
        # Re-apply the committed entries after the snapshot so the state
        # machine matches what this node had before the restart. SET/DEL
        # are deterministic, so replaying the committed suffix is safe.
        self.last_applied = self.snapshot_index
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

    def _append_entry(self, entry: dict) -> int:
        """Append one entry to the in-memory log and log.jsonl, stamping
        it with its global log index. Call with the lock held. Returns
        the entry's global index."""
        index = self.snapshot_index + len(self.log) + 1
        entry["index"] = index
        self.log.append(entry)
        self._append_log_entry(entry)
        return index

    def _write_snapshot_file(self) -> None:
        """Write snapshot.json atomically from the current snapshot
        index/term and state machine. Call with the lock held."""
        snap = {
            "last_included_index": self.snapshot_index,
            "last_included_term": self.snapshot_term,
            "state": self.sm.items(),
        }
        tmp = self._snapshot_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snapshot_path)

    def _take_snapshot(self, index: int) -> None:
        """Compact the log up to ``index``. Only correct when ``index``
        equals ``last_applied``: the state machine then holds exactly
        the state as of that index, so the covered prefix can be
        discarded. Writes snapshot.json *before* truncating the log,
        so a crash mid-compaction is recoverable on reload (covered
        entries left in log.jsonl are dropped by their index). Lock held.
        """
        if index <= self.snapshot_index or index != self.last_applied:
            return
        term = self._term_at(index)
        cut = index - self.snapshot_index
        self.snapshot_index = index
        self.snapshot_term = term
        self._write_snapshot_file()
        del self.log[:cut]
        self._rewrite_log()

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
        return self.snapshot_index + len(self.log)

    def _last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else self.snapshot_term

    def _term_at(self, index: int) -> int:
        """Term of the entry at global log ``index``. Index 0 has term 0;
        anything at or below the snapshot is covered by the snapshot, so
        it reports the snapshot term. Call with the lock held."""
        if index <= 0:
            return 0
        if index <= self.snapshot_index:
            return self.snapshot_term
        return self.log[index - self.snapshot_index - 1]["term"]

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
        self._fail_pending_reads()
        self._reset_election_timer()

    def _apply_committed(self) -> None:
        """Apply every committed-but-unapplied entry, in order. Lock held.

        Entries are addressed by global index; the in-memory log starts
        just after the snapshot, so the position of index ``i`` is
        ``i - snapshot_index - 1``. Once the applied prefix grows past
        the threshold, compact it into a snapshot so the log (and the
        replay on the next restart) stays bounded.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - self.snapshot_index - 1]
            self.sm.apply(entry["command"])
        self._save_meta()
        if (self.last_applied > self.snapshot_index
                and len(self.log) > self.snapshot_threshold):
            self._take_snapshot(self.last_applied)
        # Applying advanced: any ReadIndex read waiting on an index now
        # covered by the state machine can be served.
        self._resolve_pending_reads()

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
            if self._term_at(n) != self.current_term:
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
        if msg_type == "request_pre_vote":
            return self._on_request_pre_vote(msg)
        if msg_type == "append_entries":
            return self._on_append_entries(msg)
        if msg_type == "install_snapshot":
            return self._on_install_snapshot(msg)
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
                    "snapshot_index": self.snapshot_index,
                    "snapshot_term": self.snapshot_term,
                    "pending_reads": len(self._pending_reads),
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
    # RequestPreVote
    # ------------------------------------------------------------------
    def _on_request_pre_vote(self, msg: dict) -> dict:
        """Pre-vote round (Raft dissertation §4.2.3): a candidate asks
        whether it *would* win an election, without changing any state.

        A pre-vote is granted only if all of these hold:
        - the proposed term is newer than ours (a stale candidate is
          not even a contender);
        - we are not the leader (a live leader never votes itself out);
        - we have not heard from a leader recently — a follower with a
          live leader must not help start an election that would
          disrupt it;
        - the candidate's log is at least as up to date as ours (same
          check as RequestVote).

        Granting or denying a pre-vote changes nothing: no term bump,
        no voted_for, no timer reset. That is the whole point — a
        partitioned node that cannot reach a majority can never inflate
        its term, so when it rejoins it cannot force the cluster to
        step down or re-elect.
        """
        term = int(msg["term"])
        candidate = msg["candidate_id"]
        with self.lock:
            if term <= self.current_term:
                return {"type": "request_pre_vote_response",
                        "term": self.current_term, "pre_vote_granted": False}
            if self.role == "leader":
                return {"type": "request_pre_vote_response",
                        "term": self.current_term, "pre_vote_granted": False}
            if (self._last_leader_contact is not None
                    and time.monotonic() - self._last_leader_contact
                    < self._election_timeout):
                return {"type": "request_pre_vote_response",
                        "term": self.current_term, "pre_vote_granted": False}
            cand_term = int(msg["last_log_term"])
            cand_index = int(msg["last_log_index"])
            up_to_date = (cand_term > self._last_log_term()) or (
                cand_term == self._last_log_term()
                and cand_index >= self._last_log_index()
            )
            return {"type": "request_pre_vote_response",
                    "term": self.current_term, "pre_vote_granted": up_to_date}

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
            self._fail_pending_reads()
            self._reset_election_timer()
            self._last_leader_contact = time.monotonic()

            # Log matching property: the log must contain an entry at
            # prev_log_index with term prev_log_term, or the RPC fails
            # and the leader backs up and retries. Entries at or below
            # our snapshot are already compacted: if the snapshot covers
            # prev_log_index, the prefix matches by construction — a
            # snapshot only ever covers committed entries, and committed
            # entries cannot differ between nodes.
            if prev_index > 0:
                if prev_index > self._last_log_index():
                    return {"type": "append_entries_response",
                            "term": self.current_term, "success": False,
                            "match_index": self._last_log_index()}
                if prev_index > self.snapshot_index:
                    if self.log[prev_index - self.snapshot_index - 1]["term"] != prev_term:
                        return {"type": "append_entries_response",
                                "term": self.current_term, "success": False,
                                "match_index": prev_index - 1}
                elif prev_index == self.snapshot_index:
                    if prev_term != self.snapshot_term:
                        return {"type": "append_entries_response",
                                "term": self.current_term, "success": False,
                                "match_index": prev_index - 1}

            # Append new entries, deleting anything that conflicts.
            # Entries at or below the snapshot are already applied here,
            # so they are skipped, not re-appended.
            idx = prev_index
            for entry in entries:
                idx += 1
                if idx <= self.snapshot_index:
                    continue
                pos = idx - self.snapshot_index - 1
                if pos < len(self.log):
                    if self.log[pos]["term"] != entry["term"]:
                        del self.log[pos:]
                        self._rewrite_log()
                        self._append_entry(entry)
                else:
                    self._append_entry(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_committed()

            return {"type": "append_entries_response",
                    "term": self.current_term, "success": True,
                    "match_index": self._last_log_index()}

    # ------------------------------------------------------------------
    # InstallSnapshot
    # ------------------------------------------------------------------
    def _on_install_snapshot(self, msg: dict) -> dict:
        """Install a snapshot from the leader (Raft §7).

        Sent when a follower is so far behind that the leader has
        already compacted away the entries it is missing — AppendEntries
        can no longer bring it up to date. The follower replaces its
        state machine with the snapshot's state, discards its log up to
        the snapshot's last included index, and keeps any log entries
        after that index whose prefix still matches.
        """
        term = int(msg["term"])
        leader_id = msg["leader_id"]
        last_included_index = int(msg["last_included_index"])
        last_included_term = int(msg["last_included_term"])
        data = msg.get("data") or {}
        with self.lock:
            if term < self.current_term:
                return {"type": "install_snapshot_response",
                        "term": self.current_term, "success": False,
                        "match_index": self._last_log_index()}
            if term > self.current_term:
                self._step_down(term)
            self.role = "follower"
            self.leader_id = leader_id
            self._fail_pending_reads()
            self._reset_election_timer()
            self._last_leader_contact = time.monotonic()

            if last_included_index <= self.snapshot_index:
                # We already have this snapshot (or a newer one).
                return {"type": "install_snapshot_response",
                        "term": self.current_term, "success": True,
                        "match_index": self._last_log_index()}

            # Keep log entries after last_included_index only if the
            # entry at that index still matches the snapshot's term;
            # otherwise the whole log is stale and goes.
            if (last_included_index <= self._last_log_index()
                    and self._term_at(last_included_index) == last_included_term):
                self.log = self.log[last_included_index - self.snapshot_index:]
            else:
                self.log = []
            self.snapshot_index = last_included_index
            self.snapshot_term = last_included_term
            self.sm.load(data)
            self._write_snapshot_file()
            self._rewrite_log()
            self.commit_index = max(self.commit_index, last_included_index)
            self.last_applied = max(self.last_applied, last_included_index)
            self._save_meta()
            return {"type": "install_snapshot_response",
                    "term": self.current_term, "success": True,
                    "match_index": last_included_index}

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
            index = self._append_entry(entry)
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

    # ------------------------------------------------------------------
    # ReadIndex pending reads
    # ------------------------------------------------------------------
    def _register_pending_read(self, read_index: int, term: int) -> threading.Event:
        """Register a read waiting for the state machine to apply up to
        ``read_index``. Lock held. Returns an event that is set once the
        read can be served (apply caught up) or the node loses
        leadership — the waiter re-checks both itself after waking."""
        event = threading.Event()
        self._pending_reads.append(
            {"read_index": read_index, "term": term, "event": event})
        return event

    def _unregister_pending_read(self, event: threading.Event) -> None:
        """Drop a pending read (timed out, or served after its event was
        set). Lock held. A no-op if the read was already resolved or
        failed, since those paths remove it from the list themselves."""
        self._pending_reads = [p for p in self._pending_reads
                               if p["event"] is not event]

    def _resolve_pending_reads(self) -> None:
        """Wake every pending read whose index the state machine has now
        applied. Lock held; called from _apply_committed after
        last_applied advances. Reads still ahead of the applied prefix
        stay registered and keep waiting."""
        ready, waiting = [], []
        for p in self._pending_reads:
            (ready if p["read_index"] <= self.last_applied else waiting).append(p)
        self._pending_reads = waiting
        for p in ready:
            p["event"].set()

    def _fail_pending_reads(self) -> None:
        """Wake all pending reads because leadership changed. Lock held.
        Waiters re-check role/term after waking and return a redirect
        instead of serving from a deposed leader."""
        for p in self._pending_reads:
            p["event"].set()
        self._pending_reads = []

    def _serve_read(self, key: str) -> dict:
        """ReadIndex read (Raft dissertation §6.4).

        1. Record read_index = commit_index while leader in this term.
        2. Confirm leadership with a heartbeat round to a majority —
           a partitioned ex-leader cannot get those acks.
        3. Wait until the state machine has applied up to read_index;
           only then is the local state guaranteed to include every
           write committed before the read started. The wait registers
           a pending read, resolved by _apply_committed once the apply
           index catches up — no log entry is written for the read.
        4. If leadership is lost mid-read, redirect (the client follows
           it to the new leader) — never serve from a deposed leader.
           If apply stalls past READ_INDEX_TIMEOUT while we are still
           leader in this term, fall back to the old heartbeat-confirmed
           read of whatever is applied: on this leader apply is
           synchronous with commit advancement under the same lock, so
           last_applied == commit_index whenever we hold leadership,
           and the fallback is exactly the previous behaviour.
        """
        with self.lock:
            if self.role != "leader":
                return self._redirect()
            term = self.current_term
            read_index = self.commit_index
        if not self._confirm_leadership(term):
            with self.lock:
                if self.role != "leader":
                    return self._redirect()
            return {"type": "client_response", "status": "error",
                    "error": "could not confirm leadership"}
        # Wait for the state machine to catch up to the read index.
        deadline = time.monotonic() + READ_INDEX_TIMEOUT
        while True:
            with self.lock:
                if self.role != "leader" or self.current_term != term:
                    return self._redirect()
                if self.last_applied >= read_index:
                    value = self.sm.get(key)
                    return {"type": "client_response", "status": "ok",
                            "value": value}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                event = self._register_pending_read(read_index, term)
            # Wait outside the lock; _apply_committed resolves the event
            # once the state machine catches up, _step_down (and the
            # follower transitions) fail it on leadership loss.
            event.wait(timeout=remaining)
            with self.lock:
                self._unregister_pending_read(event)
        # Fallback: apply stalled past the timeout, but we are still
        # leader in this term — re-confirm leadership and read whatever
        # is applied, exactly like the old heartbeat-confirmed read.
        if not self._confirm_leadership(term):
            with self.lock:
                if self.role != "leader":
                    return self._redirect()
            return {"type": "client_response", "status": "error",
                    "error": "could not confirm leadership"}
        with self.lock:
            if self.role != "leader" or self.current_term != term:
                return self._redirect()
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
    def _build_append_entries(self, peer: str, start_index: int | None = None,
                              max_entries: int | None = None) -> dict | None:
        """Build an AppendEntries for a peer. Lock held. Returns None if
        this node is no longer the leader, or if the start index is at
        or below the snapshot (the caller must send InstallSnapshot
        instead — those entries no longer exist in the log).

        start_index defaults to next_index[peer] (the oldest entry the
        follower is not known to have); max_entries caps the batch so
        the pipelined driver can send several batches at once instead
        of one message carrying the whole suffix.
        """
        if self.role != "leader":
            return None
        next_idx = (start_index if start_index is not None
                    else self.next_index.get(peer, self._last_log_index() + 1))
        if next_idx <= self.snapshot_index:
            return None
        prev_index = next_idx - 1
        prev_term = self._term_at(prev_index)
        entries = self.log[next_idx - self.snapshot_index - 1:]
        if max_entries is not None:
            entries = entries[:max_entries]
        return {
            "type": "append_entries",
            "term": self.current_term,
            "leader_id": self.id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

    def pipeline_stats(self) -> dict[str, dict]:
        """Per-peer pipelining state, for tests and debugging: the
        current in-flight window, its high-water mark, acks this term,
        and where the leader thinks the peer is."""
        with self.lock:
            return {
                p: {
                    "in_flight": len(self._in_flight.get(p, {})),
                    "max_in_flight": self._max_in_flight.get(p, 0),
                    "acks": self._pipeline_acks.get(p, 0),
                    "next_to_send": self._next_to_send.get(p, 0),
                    "next_index": self.next_index.get(p, 0),
                    "match_index": self.match_index.get(p, 0),
                }
                for p in self.peers
            }

    def _apply_append_reply(self, peer: str, resp: dict | None,
                            prev: int, end: int, start: int | None,
                            epoch: int) -> None:
        """Apply one AppendEntries reply. Lock held.

        ``start`` is the batch's start index, or None for a heartbeat
        (a zero-entry batch, not tracked in the window). A reply is
        only applied if its batch is still tracked at the same epoch:
        a log-mismatch backoff bumps the epoch and clears the window,
        so replies from superseded batches are ignored instead of
        clobbering the new next_index. The commit rule is unchanged —
        _advance_commit only counts current-term entries.
        """
        if start is not None:
            tracked = self._in_flight.get(peer, {}).get(start)
            if tracked is None or tracked["epoch"] != epoch:
                return
            del self._in_flight[peer][start]
        if resp is None:
            # Outcome unknown — the batch may or may not have landed.
            # Count it against this peer's failure budget either way:
            # five fresh timeouts in one call means the peer is not
            # answering, and the driver should give up instead of
            # spinning until its deadline.
            self._pipeline_failures[peer] = (
                self._pipeline_failures.get(peer, 0) + 1)
            if start is not None:
                # Requeue it from its start index; AppendEntries is
                # idempotent, so a re-send is safe either way. Back
                # off briefly so a dead peer does not spin the window.
                self._next_to_send[peer] = min(
                    self._next_to_send.get(peer, start), start)
                self._peer_retry_after[peer] = (
                    time.monotonic() + PIPELINE_RETRY_BACKOFF)
            return
        resp_term = int(resp.get("term", 0))
        if resp_term > self.current_term:
            self._step_down(resp_term)
            return
        if self.role != "leader" or resp_term != self.current_term:
            return
        if resp.get("success"):
            # A successful batch proves the follower holds the whole
            # prefix up to its end: the follower rejects a batch whose
            # prev_log_index it does not have, so an ack for a later
            # batch implies the earlier ones landed too — which is
            # what lets out-of-order acks advance match_index safely.
            self.match_index[peer] = max(
                self.match_index.get(peer, 0), end)
            self.next_index[peer] = self.match_index[peer] + 1
            self._pipeline_acks[peer] = self._pipeline_acks.get(peer, 0) + 1
            self._pipeline_failures[peer] = 0
            self._advance_commit()
            return
        # Log mismatch: back up and retry from the follower's hint.
        # Bump the epoch and clear the window — every other batch in
        # flight was built on the old prefix, so their replies are
        # stale; they are re-dispatched from the backed-off index.
        hint = int(resp.get("match_index", prev - 1))
        self._pipeline_epoch[peer] = self._pipeline_epoch.get(peer, 0) + 1
        self._in_flight.get(peer, {}).clear()
        self.next_index[peer] = max(
            1, min(self.next_index.get(peer, 1), hint + 1))
        self._next_to_send[peer] = self.next_index[peer]
        self._peer_retry_after[peer] = (
            time.monotonic() + PIPELINE_MISMATCH_BACKOFF)

    def _send_pipeline_batch(self, peer: str, msg: dict, start: int,
                             prev: int, end: int, epoch: int) -> None:
        """Send one pipelined AppendEntries batch and apply its reply.
        Runs on its own thread; network I/O outside the lock, the
        reply applied under it by _apply_append_reply."""
        host, port = self.peers[peer]
        resp = send_rpc(host, port, msg, timeout=RPC_TIMEOUT)
        with self.lock:
            self._apply_append_reply(peer, resp, prev, end, start, epoch)

    def _wait_for_ack(self, peer: str, acks_at_entry: int) -> bool:
        """Another driver is already driving this peer; wait for it to
        get an ack in our term instead of opening a second window.
        Bail out early if the peer has five consecutive timeouts and
        no ack since — it is not answering, and waiting out the full
        deadline would stall the caller for nothing. (Bailing never
        stops future attempts: the next heartbeat drives a fresh
        call.)"""
        deadline = time.monotonic() + RPC_TIMEOUT * 4
        while time.monotonic() < deadline:
            with self.lock:
                if self.role != "leader":
                    return False
                if self._pipeline_acks.get(peer, 0) > acks_at_entry:
                    return True
                if self._pipeline_failures.get(peer, 0) >= 5:
                    return False
            time.sleep(0.005)
        return False

    def _replicate_to_peer(self, peer: str) -> bool:
        """Replicate to one peer; returns True if the peer acked an
        AppendEntries in our term during this call.

        At most one driver per peer is active: if another driver is
        already driving this peer, this call just waits for an ack.
        Otherwise this call drives the window itself (see
        _drive_replication)."""
        with self.lock:
            acks_at_entry = self._pipeline_acks.get(peer, 0)
            if peer in self._replicating:
                already_active = True
            else:
                self._replicating.add(peer)
                already_active = False
        if already_active:
            return self._wait_for_ack(peer, acks_at_entry)
        try:
            return self._drive_replication(peer, acks_at_entry)
        finally:
            with self.lock:
                self._replicating.discard(peer)

    def _drive_replication(self, peer: str, acks_at_entry: int) -> bool:
        """Drive pipelined replication to one peer: keep up to
        pipeline_window AppendEntries batches in flight, each carrying
        at most pipeline_batch_size entries, and return True once the
        peer acks one in our term.

        The old code sent one batch, waited for its reply, then sent
        the next — a lagging follower stalled the pipeline for a round
        trip per batch. Here batches are dispatched concurrently and
        acks are applied as they arrive; a rejected batch backs
        next_index down (via _apply_append_reply) and the window is
        re-dispatched from there. When the peer is caught up and the
        window is empty, one empty AppendEntries goes out as the
        heartbeat, sent synchronously like the old path.

        Give up after the peer has five consecutive timeouts and this
        call has tried at least once — the old code retried five
        times, and spinning until the deadline would make every write
        while a follower is down cost the full 2s. The budget is
        persistent across calls (it resets on the next ack), so a
        peer that has been dead for a while fails fast on the first
        fresh attempt instead of paying five more rounds.
        """
        host, port = self.peers[peer]
        deadline = time.monotonic() + RPC_TIMEOUT * 4
        tried_this_call = False
        while time.monotonic() < deadline:
            send_snapshot = False
            heartbeat_msg = None
            heartbeat_meta = None
            with self.lock:
                if self.role != "leader":
                    return False
                if self._pipeline_acks.get(peer, 0) > acks_at_entry:
                    return True
                last = self._last_log_index()
                in_flight = self._in_flight.setdefault(peer, {})
                # If the peer is behind our snapshot, AppendEntries
                # cannot help it: the entries it needs no longer exist
                # in our log. Send the snapshot instead — but only
                # once the window is empty, so no batch is in flight
                # when the snapshot lands.
                if (self.next_index.get(peer, 1) <= self.snapshot_index
                        and not in_flight):
                    send_snapshot = True
                else:
                    # next_index is the oldest index not known to be
                    # replicated; anything below it is already on the
                    # follower (a later batch's ack proves the whole
                    # prefix), so never re-dispatch below it — a
                    # timed-out batch requeued behind a newer ack is
                    # skipped, not re-sent.
                    next_to_send = max(
                        self._next_to_send.get(peer, last + 1),
                        self.next_index.get(peer, 1))
                    if ((time.monotonic()
                            >= self._peer_retry_after.get(peer, 0.0)
                            # A peer already known dead (five
                            # consecutive timeouts) fails fast: bypass
                            # the backoff so the call can try once and
                            # give up, instead of paying a backoff
                            # period on every write while it is down.
                            or self._pipeline_failures.get(peer, 0) >= 5)
                            and next_to_send > self.snapshot_index):
                        while (len(in_flight) < self.pipeline_window
                                and next_to_send <= last):
                            if next_to_send in in_flight:
                                # Already dispatched (a timed-out batch
                                # was requeued behind a batch still in
                                # flight): skip past it instead of
                                # double-sending.
                                next_to_send = (
                                    in_flight[next_to_send]["end"] + 1)
                                continue
                            count = min(self.pipeline_batch_size,
                                        last - next_to_send + 1)
                            msg = self._build_append_entries(
                                peer, start_index=next_to_send,
                                max_entries=self.pipeline_batch_size)
                            if msg is None:
                                break
                            epoch = self._pipeline_epoch.get(peer, 0)
                            in_flight[next_to_send] = {
                                "prev": msg["prev_log_index"],
                                "end": next_to_send + count - 1,
                                "epoch": epoch,
                            }
                            tried_this_call = True
                            self._max_in_flight[peer] = max(
                                self._max_in_flight.get(peer, 0),
                                len(in_flight))
                            threading.Thread(
                                target=self._send_pipeline_batch,
                                args=(peer, msg, next_to_send,
                                      msg["prev_log_index"],
                                      next_to_send + count - 1, epoch),
                                daemon=True).start()
                            next_to_send += count
                        self._next_to_send[peer] = next_to_send
                    # Caught up and the window is empty: one empty
                    # AppendEntries as the heartbeat.
                    if (not in_flight and next_to_send > last
                            and self.match_index.get(peer, 0) >= last):
                        msg = self._build_append_entries(
                            peer, start_index=last + 1, max_entries=0)
                        if msg is not None:
                            heartbeat_msg = msg
                            heartbeat_meta = (
                                msg["prev_log_index"], last,
                                self._pipeline_epoch.get(peer, 0))
                    # Five consecutive timeouts, this call has tried
                    # at least once, the window is empty, and work is
                    # still pending: the peer is not answering. Give
                    # up now instead of spinning until the deadline;
                    # the next heartbeat retries from a clean call.
                    if (not in_flight and next_to_send <= last
                            and tried_this_call
                            and self._pipeline_failures.get(peer, 0) >= 5):
                        return False
            if send_snapshot:
                return self._send_install_snapshot(peer)
            if heartbeat_msg is not None:
                resp = send_rpc(host, port, heartbeat_msg,
                                timeout=RPC_TIMEOUT)
                with self.lock:
                    prev, end, epoch = heartbeat_meta
                    self._apply_append_reply(peer, resp, prev, end,
                                             None, epoch)
                continue
            time.sleep(0.005)
        with self.lock:
            return self._pipeline_acks.get(peer, 0) > acks_at_entry

    def _send_install_snapshot(self, peer: str) -> bool:
        """Send this node's snapshot to a lagging peer. Network I/O
        happens outside the lock; the reply is applied under it.
        Returns True if the peer installed the snapshot in our term."""
        host, port = self.peers[peer]
        with self.lock:
            if self.role != "leader":
                return False
            msg = {
                "type": "install_snapshot",
                "term": self.current_term,
                "leader_id": self.id,
                "last_included_index": self.snapshot_index,
                "last_included_term": self.snapshot_term,
                "data": self.sm.items(),
            }
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
                self.match_index[peer] = int(
                    resp.get("match_index", self.snapshot_index))
                self.next_index[peer] = self.match_index[peer] + 1
                self._next_to_send[peer] = self.next_index[peer]
                self._pipeline_acks[peer] = self._pipeline_acks.get(peer, 0) + 1
                self._pipeline_failures[peer] = 0
                self._advance_commit()
                return True
        return False

    def _become_leader(self) -> None:
        with self.lock:
            self.role = "leader"
            self.leader_id = self.id
            last = self._last_log_index()
            self.next_index = {p: last + 1 for p in self.peers}
            self.match_index = {p: 0 for p in self.peers}
            # Fresh pipeline state for this term: no batches in flight,
            # nothing dispatched yet, no acks counted, no backoff.
            self._in_flight = {p: {} for p in self.peers}
            self._next_to_send = {p: last + 1 for p in self.peers}
            self._pipeline_epoch = {p: 0 for p in self.peers}
            self._pipeline_acks = {p: 0 for p in self.peers}
            self._max_in_flight = {p: 0 for p in self.peers}
            self._peer_retry_after = {p: 0.0 for p in self.peers}
            self._pipeline_failures = {p: 0 for p in self.peers}
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
    def _run_pre_vote(self) -> tuple[bool, int, float]:
        """One pre-vote round for term current_term + 1.

        Sends RequestPreVote to every peer without changing any local
        state. Returns (majority_granted, term_before, heartbeat_mark):
        the gate values _run_election re-checks under the lock before
        starting the real election, so a leader that contacted us
        mid-round still wins the race. If any voter reports a term
        newer than ours, step down to it first — we learned of a newer
        term, so an election in our old term + 1 is moot.
        """
        with self.lock:
            term_before = self.current_term
            heartbeat_mark = self._last_heartbeat
            proposed_term = term_before + 1
            last_index = self._last_log_index()
            last_term = self._last_log_term()
        msg = {
            "type": "request_pre_vote",
            "term": proposed_term,
            "candidate_id": self.id,
            "last_log_index": last_index,
            "last_log_term": last_term,
        }
        grants: set[str] = {self.id}
        grant_lock = threading.Lock()
        max_term = [term_before]

        def _ask(peer: str) -> None:
            host, port = self.peers[peer]
            resp = send_rpc(host, port, msg, timeout=RPC_TIMEOUT)
            if resp is None:
                return
            with grant_lock:
                max_term[0] = max(max_term[0], int(resp.get("term", 0)))
                if resp.get("pre_vote_granted"):
                    grants.add(peer)

        threads = [threading.Thread(target=_ask, args=(p,), daemon=True)
                   for p in self.peers]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=RPC_TIMEOUT + 0.2)
        with self.lock:
            if max_term[0] > self.current_term:
                self._step_down(max_term[0])
        return len(grants) >= self._majority(), term_before, heartbeat_mark

    def _run_election(self) -> None:
        """Start an election in this term. Runs on the timer thread.

        Pre-vote first: only start a real election if a majority would
        vote for us. A candidate that cannot reach a majority (or whose
        log is stale, or whose cluster already has a live leader) never
        bumps its term, so it cannot disrupt the leader when it
        rejoins. The gate is re-checked under the lock right before the
        term increment, so a heartbeat that lands mid-round still wins.
        """
        granted, term_before, heartbeat_mark = self._run_pre_vote()
        with self.lock:
            if (
                not granted
                or self.role == "leader"
                or self.current_term != term_before
                or self._last_heartbeat != heartbeat_mark
            ):
                # No mandate, or something changed while we asked: a
                # leader contacted us, we stepped down to a newer term,
                # or we granted a vote. Back off and try again after a
                # fresh randomized timeout instead of spinning.
                self._reset_election_timer()
                return
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
