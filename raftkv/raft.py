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
  {"term": n, "index": g, "command": {...}}. Appended in batches with
  one fsync per batch (group commit); rewritten whole when the log is
  truncated by a conflicting AppendEntries or compacted by a snapshot.
  Each entry carries its global log index so a restart can tell a
  pre-snapshot entry (index <= snapshot_index) from a live one.
- ``snapshot.json`` — {"last_included_index": n, "last_included_term": t,
  "state": {...}}, the compacted state machine as of index n, written
  atomically. The in-memory log holds only entries *after* the
  snapshot; global index of log[k] is snapshot_index + k + 1. The
  snapshot also carries the voting config and the client-dedup keys
  covered by the compacted prefix, so neither is lost to compaction.

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
from .watch import WatchManager

# Randomized election timeout. Short values keep the test suite and the
# demo snappy; 150-300 ms is comfortably above localhost RTT.
ELECTION_TIMEOUT_MIN = 0.15
ELECTION_TIMEOUT_MAX = 0.30
HEARTBEAT_INTERVAL = 0.05
RPC_TIMEOUT = 0.5
CLIENT_COMMIT_TIMEOUT = 10.0
LEADER_LEASE_TIMEOUT = 1.0
# A watch RPC waits at most this long for an event. The transport
# handler thread's socket timeout is 30s, so a watch must answer
# before that or its reply can never be sent; 25s leaves headroom.
WATCH_MAX_TIMEOUT = 25.0
# Leader lease for reads (Raft dissertation §6.4): how long a majority
# ack stays fresh. Must be shorter than ELECTION_TIMEOUT_MIN (150ms):
# a follower that acked at time T heard from the leader no later than
# T, so it cannot start an election before T + 150ms, and any new
# election majority overlaps the acking majority in at least one node —
# so no new leader can be elected while this lease holds. 100ms leaves
# 50ms of slack. Heartbeats go out every 50ms, so a healthy leader's
# lease is almost always fresh and reads skip the heartbeat round.
LEADER_LEASE_DURATION = 0.1
# How long a ReadIndex read waits for the state machine to apply up to
# its read index before falling back to a plain heartbeat-confirmed read.
READ_INDEX_TIMEOUT = 2.0
# Follower lease for reads: how long a follower may serve a plain read
# from its applied state after hearing from the leader. Same length as
# the leader lease (100ms, shorter than ELECTION_TIMEOUT_MIN): the last
# heartbeat proves the leader was alive and leading at that moment, and
# no new leader can be elected before the follower's election timeout
# (150ms) expires, so within the window every entry the follower has
# applied still came from that same leader's committed log. The read is
# NOT linearizable — a write committed after the last heartbeat may be
# missing — but its staleness is bounded: it reflects the leader's
# committed state as of the last heartbeat, at most ~one heartbeat
# interval (50ms) plus apply lag behind the leader, and a partitioned
# follower stops serving within 100ms of losing contact. Session reads
# never take this path: they carry a watermark the follower may not
# have applied, so they still redirect to the leader.
FOLLOWER_LEASE_DURATION = LEADER_LEASE_DURATION
# Compact the log once it holds more than this many post-snapshot
# entries. 1000 is large enough that compaction is rare in normal use
# but still bounds log.jsonl on a long-lived cluster. Tests pass a
# much smaller value to force snapshots quickly.
SNAPSHOT_THRESHOLD = 1000
# InstallSnapshot streams the snapshot in chunks of at most this many
# characters of serialized JSON (64 KiB by default), instead of one
# RPC carrying the whole state machine. A large state (megabytes of
# keys, plus the config and dedup table) would otherwise ride in a
# single message — blocking the leader's replication driver and the
# follower's handler thread for the whole transfer, and a failure
# mid-transfer would discard all of it. Chunked, each chunk is acked
# before the next is sent, so a failed chunk aborts cleanly and the
# follower keeps only the buffered prefix; the transfer resumes from
# chunk 0 on the next replication pass. Tests pass a tiny value to
# force many chunks through a small snapshot.
SNAPSHOT_CHUNK_SIZE = 65536
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
# Membership changes: how long the leader waits for a server being
# added to catch up with the log before appending the config entry
# that makes it a voting member. 5s is generous on localhost (catch-up
# is normally well under a second); tests pass a shorter value to make
# a down server fail fast.
MEMBERSHIP_CATCHUP_TIMEOUT = 5.0
# Leader-side group commit: how long the first writer in a batch waits
# for concurrent client writes to join it before flushing. 2ms is
# long enough for threads woken together to pile in, and short enough
# that a lone sequential write barely notices it — it pays one window
# instead of one fsync per write either way.
GROUP_COMMIT_WINDOW = 0.002
# Client session deduplication (at-most-once writes): how many applied
# (client_id, request_id) pairs the dedup table keeps. 10000 is far
# above what a retry window needs, and evicting the oldest is
# deterministic — every node applies the same log in the same order,
# so every node's table stays identical.
DEDUP_MAX_ENTRIES = 10000


def _dedup_key(command: dict) -> tuple[str, str] | None:
    """The dedup key for a client write command, or None when the
    client sent no session id (old clients, and internal entries like
    no-ops and membership changes, are never deduplicated)."""
    client_id = command.get("client_id")
    request_id = command.get("request_id")
    if client_id is None or request_id is None:
        return None
    return (str(client_id), str(request_id))


def _config_list_of(config: dict[str, tuple[str, int] | None]) -> list:
    """A config dict as JSON: [[id, [host, port] | null], ...] in
    sorted id order, so meta.json and snapshot.json agree."""
    return [[mid, (list(addr) if addr else None)]
            for mid, addr in sorted(config.items())]


def _config_dict_of(entries: list | None) -> dict[str, tuple[str, int] | None] | None:
    """The inverse of _config_list_of: a JSON config list back into a
    dict, or None when there is no config to adopt."""
    if entries is None:
        return None
    return {str(mid): (tuple(addr) if addr else None)
            for mid, addr in entries}


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: dict[str, tuple[str, int]],
        data_dir: str | os.PathLike,
        state_machine,
        snapshot_threshold: int = SNAPSHOT_THRESHOLD,
        snapshot_chunk_size: int = SNAPSHOT_CHUNK_SIZE,
        pipeline_window: int = PIPELINE_WINDOW,
        pipeline_batch_size: int = PIPELINE_BATCH_SIZE,
        membership_catchup_timeout: float = MEMBERSHIP_CATCHUP_TIMEOUT,
    ) -> None:
        """
        node_id: this node's id, e.g. "node1".
        peers:   {peer_id: (host, port)} for every *other* node in the cluster.
        data_dir: directory holding meta.json + log.jsonl for this node.
        state_machine: object with an ``apply(command: dict)`` method.
        snapshot_threshold: compact the log once it holds more than this
            many post-snapshot entries (see SNAPSHOT_THRESHOLD).
        snapshot_chunk_size: at most this many characters of serialized
            snapshot JSON per InstallSnapshot chunk (see
            SNAPSHOT_CHUNK_SIZE). Tests pass a tiny value to force a
            snapshot through many chunks.
        pipeline_window: at most this many AppendEntries batches in
            flight per follower (see PIPELINE_WINDOW). 1 means strictly
            sequential replication; tests use it to compare against.
        pipeline_batch_size: at most this many entries per
            AppendEntries batch (see PIPELINE_BATCH_SIZE).
        membership_catchup_timeout: how long an add waits for the new
            server to catch up before giving up (see
            MEMBERSHIP_CATCHUP_TIMEOUT).
        """
        self.id = node_id
        self.peers = dict(peers)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sm = state_machine
        # Watch/subscribe registry (v0.11.0): callbacks registered here
        # fire from _apply_committed whenever a committed entry
        # changes a watched key. Local to this node, in-memory only —
        # not replicated, not persisted, not rebuilt on restart.
        self.watch = WatchManager()
        self.snapshot_threshold = snapshot_threshold
        self.snapshot_chunk_size = max(1, snapshot_chunk_size)
        # Partial InstallSnapshot transfer in progress (see
        # _on_snapshot_chunk): {"key": (term, leader_id,
        # last_included_index, chunk_total), "chunks": {index: str},
        # "last_included_term": int} — or None. In-memory only: a
        # restart drops it and the leader re-sends from chunk 0.
        self._snapshot_transfer: dict | None = None
        self.pipeline_window = pipeline_window
        self.pipeline_batch_size = pipeline_batch_size
        self.membership_catchup_timeout = membership_catchup_timeout

        # Membership (Raft §6). ``config`` is the voting configuration:
        # every server that counts toward a majority, mapped to its
        # address (this node's own address is None until start() records
        # it). ``peers`` is the wider address book — every node this one
        # sends RPCs to, which is the config minus itself plus any
        # non-voting server currently catching up for an add. The two
        # are kept in sync by _apply_config_entry and _load; the
        # distinction matters because a server being added must receive
        # log entries before it may vote, and a vote or a commit must
        # never count a non-voting server.
        self.config: dict[str, tuple[str, int] | None] = {
            p: addr for p, addr in self.peers.items()
        }
        self.config[self.id] = None
        # Peers being added: in self.peers (so they get replicated to)
        # but not yet in self.config (so they cannot vote or count
        # toward commit). Cleared when the add's config entry commits.
        self._non_voting: set[str] = set()
        # The membership change currently being driven by this leader,
        # {"op", "server_id", "index" (or None until appended)} — or
        # None. Only one change at a time: a second request is rejected
        # while one is in flight (see _membership_change_in_progress).
        self._membership_change: dict | None = None
        # Removal in progress: peer -> index of the remove_server
        # entry. The leader keeps replicating to a removed peer until
        # that peer's commit index reaches the entry — otherwise the
        # peer never learns it was removed (the leader would stop
        # sending heartbeats the moment it applied the entry, and the
        # peer would sit there election-timing-out forever). The peer
        # is already out of self.config, so it cannot vote or count
        # toward a majority while it catches up on its own removal.
        self._removal_pending: dict[str, int] = {}
        # Joint consensus (Raft dissertation §6). While a membership
        # change is mid-flight, ``_joint_new`` holds C-new and
        # ``config`` still holds C-old: the joint config C-old,new is
        # in effect, and every quorum check (commit, election, lease)
        # needs a majority of BOTH configs. The final config entry
        # adopts C-new and clears this. Persisted in meta.json and
        # snapshot.json, so a restart mid-transition resumes in joint
        # phase instead of forgetting the change halfway.
        self._joint_new: dict[str, tuple[str, int] | None] | None = None
        # Removal is an event, not a config derivation (see
        # _is_removed): True once this node has been removed from the
        # cluster, and False for a server that was never a member —
        # a server being added adopts the old config from a snapshot
        # before it ever joins, so "not in the config" must not by
        # itself mean "removed". Persisted in meta.json, so a restart
        # of a removed node stays removed.
        self._removed = False
        # True once this node has been a member of a cluster config —
        # via a config entry, a snapshot, or applying entries while
        # in the config. A snapshot whose config excludes a server
        # that has been a member is a removal; the same snapshot
        # landing on a server that never was one is just catch-up.
        self._was_member = False
        # True while a thread is driving the final config entry that
        # completes a joint transition (see _complete_joint_transition):
        # exactly one driver, so a leader that applies a joint entry
        # and the client thread that submitted it cannot each append
        # their own final entry.
        self._joint_completion_running = False
        # This node's own address once start() runs; recorded into the
        # config so other members learn it from the log/snapshot.
        self._self_addr: tuple[str, int] | None = None
        # Simulated network partition (see partition()/heal()): peer
        # ids this node refuses to talk to — no peer RPC is sent to
        # them, and their peer RPCs are dropped on arrival. Client and
        # status traffic is unaffected, so a partitioned node still
        # answers status and still claims leadership to a client that
        # asks it directly — exactly like a real partition. In-memory
        # only, never persisted: a restart heals every partition.
        self._partitioned: set[str] = set()

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
        # The term the last leader contact arrived in. The follower
        # lease (see _follower_lease_valid) only holds while the term
        # is unchanged: a higher term means a new election happened
        # since the contact, so the old leader may be deposed even
        # though the contact itself is still recent.
        self._last_leader_contact_term: int | None = None
        self._last_heartbeat_sent = 0.0
        # ReadIndex reads waiting for the state machine to apply up to
        # their read index. Each entry is {"read_index": int,
        # "term": int, "event": threading.Event}; the event is set by
        # _resolve_pending_reads once last_applied passes the index, or
        # by _fail_pending_reads when the node loses leadership.
        self._pending_reads: list[dict] = []
        # Leader lease (see _lease_valid): last time each peer acked an
        # AppendEntries (batch or heartbeat) or InstallSnapshot in our
        # term. A deposed leader holds no lease, so this is cleared on
        # step-down and rebuilt on every election win.
        self._last_ack: dict[str, float] = {}
        # How many explicit leadership-confirmation rounds _serve_read
        # has run (lease-served reads do not count). Exposed in
        # status() so tests can tell the two read paths apart.
        self._confirm_rounds = 0
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
        # Leader-side group commit (see _submit_write): client writes
        # arriving while a batch is open join it instead of appending
        # on their own, so one fsync covers several client requests.
        # A batch is a list of {"command", "index", "event", "result"};
        # None means no batch is open. The first writer in a batch is
        # its flusher: it waits GROUP_COMMIT_WINDOW for joiners, then
        # appends the whole batch with a single fsync.
        self._write_batch: list[dict] | None = None
        # Client session deduplication (at-most-once writes): applied
        # (client_id, request_id) -> cached {"status": "ok"} result.
        # A retry of a write whose response was lost returns the cached
        # result instead of appending a second log entry. The table is
        # derived from the applied log (see _apply_committed), so a new
        # leader rebuilds it by replay and a snapshot carries it (see
        # _write_snapshot_file). Insertion-ordered and bounded (see
        # DEDUP_MAX_ENTRIES). _dedup_inflight maps a key to the waiter
        # of a write that is appended but not yet committed, so a retry
        # arriving mid-commit joins the same waiter instead of
        # appending a duplicate.
        self._dedup: dict[tuple[str, str], dict] = {}
        self._dedup_inflight: dict[tuple[str, str], dict] = {}
        # Apply-time results by log index, for CAS entries only (see
        # _apply_committed): a CAS outcome is decided when its entry
        # applies, so the leader records it here and _flush_write_batch
        # hands each waiter its own entry's result instead of a shared
        # "ok". Entries are pruned once their batch's waiters are woken,
        # so this stays tiny; followers never read it.
        self._apply_results: dict[int, dict] = {}
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
        meta_config = False
        meta_joint = False
        removed_from_meta: bool | None = None
        was_member_from_meta: bool | None = None
        meta_exists = self._meta_path.exists()
        if meta_exists:
            with open(self._meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            self.current_term = int(meta.get("current_term", 0))
            self.voted_for = meta.get("voted_for")
            self.commit_index = int(meta.get("commit_index", 0))
            removed_from_meta = meta.get("removed")
            was_member_from_meta = meta.get("was_member")
            # The config as of the last applied prefix (see _save_meta):
            # newest first, so it wins over the snapshot's older one.
            # meta.json written before membership changes existed has no
            # "config" key; then the snapshot's config (or the initial
            # constructor config) stands.
            if meta.get("config") is not None:
                meta_config = True
                self.config = {
                    str(mid): (tuple(addr) if addr else None)
                    for mid, addr in meta["config"]
                }
            # The joint phase as of the last applied prefix, same
            # newest-first rule: meta.json written before joint
            # consensus existed has no "joint_new" key, so the
            # snapshot's value (or none) stands.
            if "joint_new" in meta:
                meta_joint = True
                self._joint_new = _config_dict_of(meta["joint_new"])
        # The snapshot comes first: it defines the state machine's
        # starting point and which log indices are already covered.
        if self._snapshot_path.exists():
            with open(self._snapshot_path, "r", encoding="utf-8") as f:
                snap = json.load(f)
            self.snapshot_index = int(snap.get("last_included_index", 0))
            self.snapshot_term = int(snap.get("last_included_term", 0))
            self.sm.load(snap.get("state") or {})
            # The dedup table for entries the snapshot covers: without
            # it a retry of a compacted-away write would append a
            # duplicate. The result is always ok once committed for
            # set/del, so only the keys need storing for those — but a
            # CAS result (success, mismatch, or missing key) is part of
            # the reply a retry must get back, so CAS entries store
            # [client_id, request_id, result] triples. Snapshots
            # written before CAS existed store pairs and still load as
            # ok, exactly as before.
            self._dedup = {}
            for item in (snap.get("dedup") or []):
                if len(item) == 3:
                    c, r, res = item
                    self._dedup[(str(c), str(r))] = dict(res)
                else:
                    c, r = item
                    self._dedup[(str(c), str(r))] = {
                        "type": "client_response", "status": "ok"}
            # A snapshot only ever covers committed entries, so a config
            # entry compacted into it is committed state too: adopt the
            # config it was written with, unless meta.json (newer)
            # already supplied one above. Snapshot files written before
            # membership changes existed have no "config" key.
            if snap.get("config") is not None and not meta_config:
                self.config = {
                    str(mid): (tuple(addr) if addr else None)
                    for mid, addr in snap["config"]
                }
            # The joint phase as of the snapshot, unless meta.json
            # (newer) already supplied one above.
            if snap.get("joint_new") is not None and not meta_joint:
                self._joint_new = _config_dict_of(snap["joint_new"])
        # The address book follows the config: every voting member
        # except this node. A stale --peers list on the command line
        # cannot resurrect a removed server, and a server added since
        # this node last ran is learned from the persisted config.
        # Removal and membership survive restarts too. Metas written
        # before the flags existed have neither key: derive removal
        # from the config, as _is_removed used to — a node whose
        # persisted config leaves it out was removed. A node with no
        # meta file at all is fresh: it keeps the __init__ defaults
        # (not removed, never a member), so the derivation must not
        # run for it — its constructor config still names itself.
        if removed_from_meta is not None:
            self._removed = bool(removed_from_meta)
            self._was_member = bool(was_member_from_meta)
        elif meta_exists:
            self._removed = (self.id not in self.config
                             and (self._joint_new is None
                                  or self.id not in self._joint_new))
            self._was_member = self.id in self.config
        self._rebuild_peers_from_config()
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
        """Persist term/vote/commit/config atomically. Call with the lock
        held."""
        meta = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "config": self._config_list(),
            # C-new while a joint transition is in flight (None
            # otherwise): a restart mid-transition resumes in joint
            # phase instead of forgetting the change halfway.
            "joint_new": self._joint_list(),
            # Removal and membership (see _is_removed): a restart of
            # a removed node stays removed, and a joining server
            # that restarts mid-add comes back not-removed.
            "removed": self._removed,
            "was_member": self._was_member,
        }
        tmp = self._meta_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path)

    def _append_log_entry(self, entry: dict) -> None:
        """Append one entry to log.jsonl and fsync. Call with lock held."""
        self._append_log_entries([entry])

    def _append_log_entries(self, entries: list) -> None:
        """Append entries to log.jsonl with a single fsync (group commit).

        Every entry in the batch is written and flushed before the one
        fsync, so a crash can never leave a later entry durable while
        an earlier one is lost — the batch is durable as a unit. Call
        with the lock held.
        """
        with open(self._log_path, "a", encoding="utf-8") as f:
            for entry in entries:
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
        return self._append_entries([entry])

    def _append_entries(self, entries: list) -> int:
        """Append entries to the in-memory log and log.jsonl, stamping
        each with its global log index, with a single fsync for the
        whole batch (group commit). Call with the lock held. Returns
        the last entry's global index."""
        index = self.snapshot_index + len(self.log)
        for entry in entries:
            index += 1
            entry["index"] = index
            self.log.append(entry)
        self._append_log_entries(entries)
        return index

    def _write_snapshot_file(self) -> None:
        """Write snapshot.json atomically from the current snapshot
        index/term and state machine. Call with the lock held."""
        snap = {
            "last_included_index": self.snapshot_index,
            "last_included_term": self.snapshot_term,
            "state": self.sm.items(),
            "config": self._config_list(),
            # C-new while a joint transition is in flight: a snapshot
            # taken mid-transition carries it, so a follower that
            # installs the snapshot resumes in joint phase.
            "joint_new": self._joint_list(),
            # The dedup table as [client_id, request_id, result]
            # triples: a CAS result is part of the reply a retry must
            # get back, so it rides in the snapshot with the key.
            "dedup": [[c, r, res] for (c, r), res in self._dedup.items()],
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
    # Membership helpers (Raft §6; call with the lock held unless noted)
    # ------------------------------------------------------------------
    def _config_list(self) -> list:
        """The voting config as JSON: [[id, [host, port] | null], ...]
        in sorted id order, so meta.json and snapshot.json agree."""
        return _config_list_of(self.config)

    def _joint_list(self) -> list | None:
        """C-new as JSON while a joint transition is in flight, else
        None. Persisted alongside the config so a restart resumes in
        the joint phase."""
        if self._joint_new is None:
            return None
        return _config_list_of(self._joint_new)

    def _has_quorum(self, holders: set[str]) -> bool:
        """True if ``holders`` (servers holding an entry, granting a
        vote, or acking recently — always including this node itself)
        form a quorum. Outside a transition that is a majority of the
        voting config; inside a joint transition (Raft dissertation
        §6) it is a majority of BOTH C-old and C-new. That is the
        whole safety point of joint consensus: a majority of the old
        config alone cannot commit anything while the transition is
        open, so an old leader and a new leader can never both win in
        the same term, and a new leader that inherits the joint config
        still needs the new servers' agreement."""
        if self._joint_new is None:
            return (len(holders & set(self.config))
                    >= self._majority())
        old = set(self.config)
        new = set(self._joint_new)
        return (len(holders & old) >= len(old) // 2 + 1
                and len(holders & new) >= len(new) // 2 + 1)

    def _is_removed(self) -> bool:
        """True once this node has been removed from the cluster.

        Removal is an explicit event, not a reading of the config: a
        node is removed when it applies a config entry that takes it
        out (see _apply_removal), or when it installs a snapshot
        whose config leaves it out after it has been a member (see
        _install_snapshot_payload). It used to be derived from the
        config — "not in the voting config means removed" — but a
        server being added is not in the config yet either, and it
        adopts the old config from the snapshot it installs to catch
        up: the derivation flipped a joining server to removed the
        moment the transfer landed, it then refused every
        AppendEntries after it, and the add could never complete.
        The flag is persisted in meta.json, so a restart of a removed
        node stays removed; a joining server that restarts mid-add
        comes back not-removed and the add can finish."""
        return self._removed

    def _rebuild_peers_from_config(self) -> None:
        """Reset the address book from the voting config (every member
        except this node). Non-voting catch-up peers are re-added by
        the leader when it drives an add; a plain restart never
        resurrects a removed server from a stale --peers list. During
        a joint transition the address book is the union of both
        configs: the leader must replicate to every server in either
        one, including a server being added (in C-new only) and a
        server being removed (in C-old only)."""
        members = dict(self._joint_new or {})
        members.update(self.config)
        self.peers = {mid: addr for mid, addr in members.items()
                      if mid != self.id and addr is not None}

    def _apply_config_entry(self, command: dict) -> None:
        """Apply one committed membership command to the config. Lock
        held. Called from _apply_committed instead of the KV state
        machine: membership is Raft state, not key-value data.

        Two-phase changes (joint consensus, Raft dissertation §6):
        a ``joint_config`` entry opens the transition — C-old,new is
        in effect from here, so every quorum needs a majority of both
        configs — and a ``final_config`` entry closes it by adopting
        C-new. The old single-entry ``add_server`` / ``remove_server``
        commands are still applied on replay (logs written before
        joint consensus exist), but no new change uses them.
        """
        op = command.get("op")
        server_id = command.get("server_id")
        if op == "joint_config":
            # Enter the joint phase. The config itself stays C-old
            # until the final entry applies; _joint_new holds C-new,
            # and _has_quorum requires a majority of both from now
            # on. The address book becomes the union, so the leader
            # replicates to every server in either config.
            self._joint_new = _config_dict_of(command.get("new"))
            for mid, addr in (self._joint_new or {}).items():
                if mid != self.id and addr is not None:
                    self.peers.setdefault(mid, tuple(addr))
                    if (self.role == "leader"
                            and mid not in self.next_index):
                        # A server the leader first meets through this
                        # entry (no catch-up ran for it — e.g. a new
                        # leader inherited the joint config) has no
                        # leader state yet: replicate the whole log to
                        # it from index 1. The driver would heal this
                        # on its own via the reject/backoff path, but
                        # starting at 1 skips the wasted round trip.
                        # A catch-up peer that already has state keeps
                        # it: the guard never clobbers match_index.
                        self._init_peer_state(mid, 1)
            # A leader that just applied a joint entry must drive the
            # transition to its end: if it does not, the cluster sits
            # in joint phase forever and every later membership change
            # is rejected. This fires for the leader that submitted
            # the entry (its own client thread joins the same driver)
            # and for a leader that inherited an uncommitted joint
            # entry from a dead predecessor and only just committed
            # it with its no-op. Followers do nothing — they cannot
            # drive it.
            if self.role == "leader":
                threading.Thread(
                    target=self._complete_joint_transition,
                    daemon=True).start()
        elif op == "final_config":
            # Close the transition: adopt C-new, leave the joint
            # phase. The one server that differs between the configs
            # is the change itself: an added server stops being
            # non-voting, a removed server gets the removal treatment
            # (a leader that removed itself steps down here).
            new_config = _config_dict_of(command.get("new"))
            if new_config is None:
                return
            old_config = self.config
            self.config = new_config
            self._joint_new = None
            self._rebuild_peers_from_config()
            for mid in new_config:
                if mid not in old_config:
                    self._non_voting.discard(mid)
            for mid in old_config:
                if mid not in new_config:
                    self._non_voting.discard(mid)
                    self._apply_removal(mid, old_config)
        elif op == "add_server":
            addr = command.get("addr")
            self.config[server_id] = tuple(addr) if addr else None
            if server_id != self.id and addr is not None:
                self.peers[server_id] = tuple(addr)
            self._non_voting.discard(server_id)
        elif op == "remove_server":
            self.config.pop(server_id, None)
            self._non_voting.discard(server_id)
            self._apply_removal(server_id)
        # Membership is learned here too: a server the change adds
        # (in C-new, or the subject of a legacy add) is a member from
        # the joint entry on, and one the change keeps is still one —
        # so a snapshot that later leaves it out registers as its
        # removal (see _install_snapshot_payload), while a server the
        # change removes is not one any more.
        if (self.id in self.config
                or (self._joint_new is not None
                    and self.id in self._joint_new)):
            self._was_member = True

    def _apply_removal(self, server_id: str,
                       old_config: dict | None = None) -> None:
        """Bookkeeping for a server leaving the config. Lock held.

        A removed server stops participating at once: if it is this
        node, step down (it will not vote, run for election, or serve
        client requests). A leader that removed someone else keeps
        replicating to it until its commit index reaches this entry
        (see _apply_append_reply), so it actually learns it was
        removed instead of timing out forever — the peer is already
        out of the config, so it cannot vote or count toward a
        majority in the meantime. Dropping it from self.peers here
        would cut off the heartbeats that carry the commit index.
        A follower (or a node that just stepped down) drops the peer
        from the address book and the leader-state tables at once.
        ``old_config`` is the pre-change config, so a leader closing
        a joint removal can put the removed server's address back in
        its address book: _rebuild_peers_from_config dropped it when
        the config changed, and the removal-pending replication needs
        an address to send to."""
        if server_id == self.id:
            # This node was removed: stop participating at once. It
            # keeps its server socket (a status query still answers)
            # but it will not vote, run for election, or serve
            # client requests, and a leader that removed itself
            # steps down here — the removal entry is committed, so
            # the cluster has already agreed on it. This is the
            # removal event _is_removed is built on: a config entry
            # taking this node out marks it removed here, not a
            # reading of the config afterwards.
            self._removed = True
            self.role = "follower"
            self.leader_id = None
            self._fail_pending_reads()
            self._reset_election_timer()
        if self.role == "leader":
            addr = (old_config or {}).get(server_id)
            if addr is not None and server_id != self.id:
                self.peers[server_id] = tuple(addr)
            # self.last_applied is this entry's index: _apply_committed
            # increments it before calling here.
            self._removal_pending[server_id] = self.last_applied
        else:
            # Follower (or a node that just stepped down): it does
            # not replicate, so the peer can leave the address book
            # and the leader-state tables at once.
            self.peers.pop(server_id, None)
            for table in (self.next_index, self.match_index):
                table.pop(server_id, None)
            self._in_flight.pop(server_id, None)
            self._next_to_send.pop(server_id, None)
            self._pipeline_epoch.pop(server_id, None)
            self._pipeline_acks.pop(server_id, None)
            self._max_in_flight.pop(server_id, None)
            self._peer_retry_after.pop(server_id, None)
            self._pipeline_failures.pop(server_id, None)
            self._replicating.discard(server_id)

    def _has_uncommitted_config_entry(self) -> bool:
        """True if the log holds a membership entry past the commit
        index. Lock held. Used alongside the in-flight flag so a new
        leader that inherited an uncommitted config entry still
        enforces one change at a time."""
        return any(
            entry["index"] > self.commit_index
            and entry["command"].get("op") in (
                "add_server", "remove_server",
                "joint_config", "final_config")
            for entry in self.log
        )

    def _membership_change_in_progress(self) -> bool:
        """True while a membership change is being driven or sits
        uncommitted in the log. Lock held. A flag whose entry has
        since committed is stale — the change is done, so clear it
        and let the next change through. A joint phase with no
        uncommitted entry left (the joint entry committed, the final
        one not yet appended — e.g. the driving leader just died) is
        still a change in progress: the next change must wait until
        the new leader completes this one."""
        if self._joint_new is not None:
            return True
        flag = self._membership_change
        if flag is not None:
            idx = flag.get("index")
            if idx is None or idx > self.commit_index:
                return True
            self._membership_change = None
        return self._has_uncommitted_config_entry()

    def _membership_status(self) -> dict:
        """The cluster's membership as this node sees it, for the
        status RPC and the ``membership-status`` CLI subcommand. Lock
        held.

        A client driving an add/remove only hears back once the
        change has fully committed, so while a change is in flight
        there is otherwise no way to see where it stands: this
        reports the current voting config, the joint config while a
        transition is open, which phase the transition is in, and
        the change itself (op, server, and the log indexes of its
        entries).

        Phases:
        - "stable": no transition open, nothing in flight.
        - "catching_up": the leader is still replicating the log to
          a server being added as a non-voting peer; no config entry
          has been appended yet.
        - "joint_pending": the joint_config entry is in the log but
          has not committed yet.
        - "joint_committed": the joint entry committed, so a majority
          of BOTH configs is now needed for every quorum; the final
          entry has not been appended yet.
        - "final_pending": the final_config entry is in the log but
          has not committed yet.

        ``pending_change`` is None when the cluster is stable. Its
        op/server come from the joint entry itself while the entry
        is still in the log; when a snapshot taken mid-transition
        has compacted it away, they are derived by diffing C-old
        against C-new (exactly one server differs); when the change
        has not reached the log yet, they come from the flag the
        driving leader holds. ``joint_index`` / ``final_index`` are
        None when the entry is not in this node's log.
        """
        joint_entry = None
        final_entry = None
        legacy_entry = None
        for entry in self.log:
            op = entry["command"].get("op")
            if op == "joint_config":
                joint_entry = entry
                final_entry = None
            elif op == "final_config":
                final_entry = entry
            elif op in ("add_server", "remove_server"):
                legacy_entry = entry
        if (final_entry is not None
                and final_entry["index"] > self.commit_index):
            phase = "final_pending"
        elif self._joint_new is not None:
            phase = "joint_committed"
        elif (joint_entry is not None
                and joint_entry["index"] > self.commit_index):
            phase = "joint_pending"
        elif (legacy_entry is not None
                and legacy_entry["index"] > self.commit_index):
            phase = "joint_pending"
        elif self._membership_change is not None:
            phase = "catching_up"
        else:
            phase = "stable"
        pending: dict | None = None
        if (joint_entry is not None
                and (final_entry is None
                     or final_entry["index"] > self.commit_index)):
            cmd = joint_entry["command"]
            pending = {
                "op": cmd.get("change_op"),
                "server_id": cmd.get("server_id"),
                "joint_index": joint_entry["index"],
                "final_index": (final_entry["index"]
                                if final_entry is not None else None),
            }
        elif self._joint_new is not None:
            old, new = set(self.config), set(self._joint_new)
            added, removed = new - old, old - new
            if len(added) == 1 and not removed:
                pending = {"op": "add_server",
                           "server_id": sorted(added)[0],
                           "joint_index": None, "final_index": None}
            elif len(removed) == 1 and not added:
                pending = {"op": "remove_server",
                           "server_id": sorted(removed)[0],
                           "joint_index": None, "final_index": None}
        elif (legacy_entry is not None
                and legacy_entry["index"] > self.commit_index):
            cmd = legacy_entry["command"]
            pending = {"op": cmd.get("op"),
                       "server_id": cmd.get("server_id"),
                       "joint_index": None, "final_index": None}
        elif self._membership_change is not None:
            flag = self._membership_change
            pending = {"op": flag.get("op"),
                       "server_id": flag.get("server_id"),
                       "joint_index": flag.get("index"),
                       "final_index": None}
        return {
            "config": sorted(self.config),
            "joint_new": (sorted(self._joint_new)
                          if self._joint_new is not None else None),
            "phase": phase,
            "change_in_progress": self._membership_change_in_progress(),
            "pending_change": pending,
            "joint_committed": self._joint_new is not None,
            "non_voting": sorted(self._non_voting),
            "removal_pending": dict(self._removal_pending),
        }

    def _init_peer_state(self, peer: str, next_index: int) -> None:
        """(Re)initialize all per-peer leader state for ``peer``. Lock
        held. Used by _become_leader for the whole cluster and when a
        server is added mid-term: the new peer starts with next_index 1
        so the leader replicates the whole log to it (or sends it a
        snapshot if the prefix is already compacted), instead of
        assuming — as for an established follower — that it is caught
        up to the log end."""
        self.next_index[peer] = next_index
        self.match_index[peer] = 0
        self._in_flight[peer] = {}
        self._next_to_send[peer] = next_index
        self._pipeline_epoch[peer] = 0
        self._pipeline_acks[peer] = 0
        self._max_in_flight[peer] = 0
        self._peer_retry_after[peer] = 0.0
        self._pipeline_failures[peer] = 0
        self._replicating.discard(peer)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self, host: str, port: int) -> None:
        with self.lock:
            if self._running:
                return
            self._running = True
            # Record our own address in the config so the other members
            # learn it from the log/snapshot, and so a restart on the
            # same port keeps a config that names us correctly.
            self._self_addr = (host, port)
            if self.config.get(self.id) != (host, port):
                self.config[self.id] = (host, port)
                self._save_meta()
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

    # ------------------------------------------------------------------
    # Simulated network partitions (test tooling, see also handle_rpc)
    # ------------------------------------------------------------------
    def partition(self, peer_id: str) -> None:
        """Drop all peer RPC traffic with ``peer_id``, both ways: this
        node sends nothing to the peer, and the peer's RPCs are
        dropped on arrival. Partition both directions (each node
        partitions the other) for a clean split; a one-sided partition
        still isolates the peer because its RPCs never get a reply
        here and its own sends to us are dropped at our dispatch.
        A partitioned leader keeps its role — it simply cannot commit
        anything, because no majority can ack it — and a partitioned
        follower times out and (with pre-vote) cannot depose the live
        leader on the other side."""
        with self.lock:
            self._partitioned.add(peer_id)

    def heal(self, peer_id: str) -> None:
        """Restore peer RPC traffic with ``peer_id``."""
        with self.lock:
            self._partitioned.discard(peer_id)

    def heal_all(self) -> None:
        """Restore peer RPC traffic with every peer."""
        with self.lock:
            self._partitioned.clear()

    def partitioned_peers(self) -> set[str]:
        """Peer ids currently partitioned from this node."""
        with self.lock:
            return set(self._partitioned)

    def _is_partitioned(self, peer_id: str | None) -> bool:
        if peer_id is None:
            return False
        with self.lock:
            return peer_id in self._partitioned

    def _send_peer_rpc(self, peer: str, msg: dict,
                       timeout: float = RPC_TIMEOUT) -> dict | None:
        """One-shot RPC to a peer, or None without touching the
        network when the peer is partitioned (callers already treat
        None as "peer did not answer")."""
        if self._is_partitioned(peer):
            return None
        addr = self.peers.get(peer)
        if addr is None:
            return None
        host, port = addr
        return send_rpc(host, port, msg, timeout=timeout)

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
        """Majority of the *voting* config. A server being added does
        not count until its config entry commits; a removed server
        stops counting as soon as its removal commits."""
        return len(self.config) // 2 + 1

    def _step_down(self, term: int) -> None:
        """Move to follower at a newer term. Lock held."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._save_meta()
        self.role = "follower"
        self.leader_id = None
        self._last_ack = {}
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
        applied_from = self.last_applied
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - self.snapshot_index - 1]
            # Membership commands are Raft state, not key-value data:
            # they update the config (and the address book) instead of
            # the state machine. A "noop" entry (appended by a new
            # leader to commit earlier terms) changes no state at all.
            # Everything else applies to the KV store exactly as before.
            op = entry["command"].get("op")
            if op in ("add_server", "remove_server",
                      "joint_config", "final_config"):
                self._apply_config_entry(entry["command"])
            elif op == "noop":
                pass
            else:
                # The apply result matters for CAS: its outcome is
                # decided here, at this exact point in the log, so the
                # result is recorded in the dedup table (a retry gets
                # the first attempt's outcome, not a re-execution) and,
                # while this node is the leader, by log index for the
                # waiter that submitted the entry (see
                # _flush_write_batch). Set/del return None: their
                # outcome is always ok.
                result = self.sm.apply(entry["command"])
                self._record_dedup(entry["command"], result,
                                   self.last_applied, entry["term"])
                # Watches fire on committed changes only, in log
                # order, from the apply path — so a watcher on any
                # node sees exactly the changes that committed, and a
                # failed CAS (no state change) fires nothing.
                if op == "set":
                    self.watch.notify(entry["command"]["key"],
                                      entry["command"]["value"], "set",
                                      index=self.last_applied)
                elif op == "del":
                    self.watch.notify(entry["command"]["key"], None, "del",
                                      index=self.last_applied)
                elif (op == "cas" and result is not None
                        and result.get("status") == "ok"):
                    self.watch.notify(entry["command"]["key"],
                                      entry["command"]["value"], "set",
                                      index=self.last_applied)
                if (op == "cas" and result is not None
                        and self.role == "leader"):
                    # Record the outcome only when a local waiter is
                    # waiting on it: the waiter is the only consumer,
                    # and a result recorded for an entry this node did
                    # not submit — a new leader applying the old
                    # leader's batch — would never be claimed, so it
                    # would sit in _apply_results forever. A write
                    # without a session id has no dedup key, so it
                    # always records: its waiter is identified by
                    # index alone.
                    key = _dedup_key(entry["command"])
                    if key is None or key in self._dedup_inflight:
                        self._apply_results[self.last_applied] = result
        # Applying entries while in the voting config is membership
        # too: the initial members never apply a config entry that
        # names them, so without this they would never count as
        # members — and a snapshot that later left one out would not
        # register as its removal. Only a call that actually applied
        # something counts: the replay on load runs this check too,
        # and a fresh server's constructor config still names itself,
        # so an empty apply must not mark a joiner a member before
        # it ever held a membership. A joining server applies its
        # catch-up entries while the config still leaves it out, so
        # this never marks one a member early either.
        if (self.last_applied > applied_from
                and (self.id in self.config
                     or (self._joint_new is not None
                         and self.id in self._joint_new))):
            self._was_member = True
        self._save_meta()
        if (self.last_applied > self.snapshot_index
                and len(self.log) > self.snapshot_threshold):
            self._take_snapshot(self.last_applied)
        # Applying advanced: any ReadIndex read waiting on an index now
        # covered by the state machine can be served.
        self._resolve_pending_reads()

    def _record_dedup(self, command: dict, result: dict | None = None,
                      index: int | None = None,
                      term: int | None = None) -> None:
        """Record an applied client write in the dedup table. Lock held.
        The cached result is what a retry with the same session id gets
        back instead of a second log entry. For set/del the result is
        always ok once committed; for CAS it is the apply-time outcome
        (success, mismatch, or missing key), so a retry returns the
        first attempt's exact result. The result also carries the log
        index and term of the entry that produced it: a retried write
        reports the same index as the first attempt, and a client
        session uses that index as its read-your-writes watermark —
        the index of its last acknowledged write. Evicting the oldest
        entry when the table is full is deterministic — every node
        applies the same log in the same order, so every node's table
        stays identical."""
        key = _dedup_key(command)
        if key is None:
            return
        recorded = (dict(result) if result is not None
                    else {"type": "client_response", "status": "ok"})
        if index is not None:
            recorded["index"] = index
        if term is not None:
            recorded["term"] = term
        self._dedup[key] = recorded
        while len(self._dedup) > DEDUP_MAX_ENTRIES:
            self._dedup.pop(next(iter(self._dedup)))

    def _clear_inflight(self, waiter: dict) -> None:
        """Drop a waiter's dedup-inflight claim. Lock held. Only clears
        it when it still points at this waiter — a retry that arrived
        after a timeout may have replaced it with a fresh waiter."""
        key = _dedup_key(waiter["command"])
        if key is not None and self._dedup_inflight.get(key) is waiter:
            del self._dedup_inflight[key]

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
            # Who holds entry n: the leader itself, plus every peer
            # whose match_index reaches it. A server being added may
            # hold it too, but it is not a voter until its config
            # entry commits — _has_quorum intersects with the voting
            # config(s), so its match_index must not advance the
            # commit index on its own. During a joint transition the
            # same holders must form a majority of BOTH configs.
            holders = {self.id}
            holders.update(pid for pid, m in self.match_index.items()
                           if m >= n)
            if self._has_quorum(holders):
                self.commit_index = n
                self._apply_committed()
                self.commit_cond.notify_all()
                break

    # ------------------------------------------------------------------
    # RPC dispatch
    # ------------------------------------------------------------------
    def handle_rpc(self, msg: dict) -> dict:
        msg_type = msg.get("type")
        if msg_type in ("request_vote", "request_pre_vote"):
            # Peer RPCs from a partitioned sender are dropped on
            # arrival, so a one-sided partition isolates the peer
            # even though its sends to us still land on the socket.
            if self._is_partitioned(msg.get("candidate_id")):
                return {"type": "error", "error": "partitioned"}
        if msg_type in ("append_entries", "install_snapshot"):
            if self._is_partitioned(msg.get("leader_id")):
                return {"type": "error", "error": "partitioned"}
        if msg_type == "partition":
            # Control message for the chaos harness (subprocess
            # clusters have no in-process handle on the node):
            # {"type": "partition", "peer": "node2",
            #  "action": "drop" | "heal"}. Replies ok, or an error
            # for an unknown peer/action. Never persisted.
            peer = msg.get("peer")
            action = msg.get("action")
            if peer not in self.peers and peer != self.id:
                return {"type": "error",
                        "error": f"unknown peer: {peer!r}"}
            if action == "drop":
                self.partition(peer)
            elif action == "heal":
                self.heal(peer)
            else:
                return {"type": "error",
                        "error": f"unknown action: {action!r}"}
            return {"type": "partition_response", "status": "ok",
                    "peer": peer, "action": action,
                    "partitioned": sorted(self.partitioned_peers())}
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
        if msg_type == "watch":
            return self._on_watch(msg)
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
                    "dedup_entries": len(self._dedup),
                    "lease_valid": self._lease_valid(),
                    "follower_lease_valid": self._follower_lease_valid(),
                    "confirm_rounds": self._confirm_rounds,
                    "config": sorted(self.config),
                    "joint_new": (sorted(self._joint_new)
                                  if self._joint_new is not None else None),
                    "membership": self._membership_status(),
                    "peers": sorted(self.peers),
                    "removed": self._is_removed(),
                }
        return {"type": "error", "error": f"unknown message type: {msg_type!r}"}

    # ------------------------------------------------------------------
    # RequestVote
    # ------------------------------------------------------------------
    def _on_request_vote(self, msg: dict) -> dict:
        term = int(msg["term"])
        candidate = msg["candidate_id"]
        with self.lock:
            if self._is_removed():
                # No longer a member: our vote must not count anywhere.
                return {"type": "request_vote_response",
                        "term": self.current_term, "vote_granted": False}
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
            if self._is_removed():
                return {"type": "request_pre_vote_response",
                        "term": self.current_term, "pre_vote_granted": False}
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
            if self._is_removed():
                # No longer a member: do not accept log entries from a
                # leader that has not learned of our removal yet.
                return {"type": "append_entries_response",
                        "term": self.current_term, "success": False,
                        "match_index": self._last_log_index()}
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
            self._last_leader_contact_term = term

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
            # so they are skipped, not re-appended. The entries that are
            # actually new go on in one batch with a single fsync
            # (group commit): once the first divergent entry is found,
            # everything from there on is new, so the rest of the batch
            # is appended together.
            new_entries: list = []
            idx = prev_index
            for i, entry in enumerate(entries):
                idx += 1
                if idx <= self.snapshot_index:
                    continue
                pos = idx - self.snapshot_index - 1
                if pos < len(self.log) and self.log[pos]["term"] == entry["term"]:
                    continue
                if pos < len(self.log):
                    del self.log[pos:]
                    self._rewrite_log()
                new_entries = entries[i:]
                break
            if new_entries:
                self._append_entries(new_entries)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_committed()

            return {"type": "append_entries_response",
                    "term": self.current_term, "success": True,
                    # The follower's commit index, so the leader can
                    # tell when a removed peer has applied its own
                    # removal entry (see _apply_append_reply).
                    "commit_index": self.commit_index,
                    "match_index": self._last_log_index()}

    # ------------------------------------------------------------------
    # InstallSnapshot
    # ------------------------------------------------------------------
    def _on_install_snapshot(self, msg: dict) -> dict:
        """Install a snapshot from the leader (Raft §7).

        Sent when a follower is so far behind that the leader has
        already compacted away the entries it is missing — AppendEntries
        can no longer bring it up to date. The snapshot arrives
        streamed in chunks (see _send_install_snapshot): each chunk is
        acked, buffered, and installed only once every chunk has
        arrived. A message with no ``chunk_index`` is the old
        whole-snapshot form and installs immediately, so an older
        leader can still catch a follower up.
        """
        term = int(msg["term"])
        leader_id = msg["leader_id"]
        last_included_index = int(msg["last_included_index"])
        last_included_term = int(msg["last_included_term"])
        with self.lock:
            if self._is_removed():
                return {"type": "install_snapshot_response",
                        "term": self.current_term, "success": False,
                        "match_index": self._last_log_index()}
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
            self._last_leader_contact_term = term

            if "chunk_index" in msg:
                return self._on_snapshot_chunk(msg, last_included_index,
                                               last_included_term)
            return self._install_snapshot_payload(
                last_included_index, last_included_term,
                msg.get("data") or {}, msg.get("config"),
                msg.get("dedup"), msg.get("joint_new"))

    def _on_snapshot_chunk(self, msg: dict, last_included_index: int,
                           last_included_term: int) -> dict:
        """Buffer one chunk of a streamed InstallSnapshot (lock held).

        The transfer is keyed by (term, leader, last included index,
        chunk total): a chunk for a different snapshot — a newer
        snapshot, a new leader, a new term — drops whatever partial
        transfer was buffered and starts a fresh one, so a superseded
        transfer can never complete from mixed pieces. A duplicate
        chunk (the leader re-sending from chunk 0 after a failed
        chunk) is acked again without storing it twice. The snapshot
        installs only once every chunk 0..total-1 is buffered, in
        whatever order they arrived, so a chunk that arrives out of
        order just waits for the missing ones.
        """
        try:
            chunk_index = int(msg["chunk_index"])
            chunk_total = int(msg["chunk_total"])
            chunk = msg["chunk"]
        except (KeyError, TypeError, ValueError):
            return {"type": "install_snapshot_response",
                    "term": self.current_term, "success": False,
                    "match_index": self._last_log_index()}
        if (chunk_total < 1 or chunk_index < 0
                or chunk_index >= chunk_total
                or not isinstance(chunk, str)):
            return {"type": "install_snapshot_response",
                    "term": self.current_term, "success": False,
                    "match_index": self._last_log_index()}
        if last_included_index <= self.snapshot_index:
            # We already have this snapshot (or a newer one): ack so
            # the leader moves on, and buffer nothing.
            return {"type": "install_snapshot_response",
                    "term": self.current_term, "success": True,
                    "match_index": self._last_log_index()}
        key = (self.current_term, msg["leader_id"],
               last_included_index, chunk_total)
        transfer = self._snapshot_transfer
        if transfer is None or transfer["key"] != key:
            transfer = {"key": key, "chunks": {},
                        "last_included_term": last_included_term}
            self._snapshot_transfer = transfer
        transfer["chunks"].setdefault(chunk_index, chunk)
        if len(transfer["chunks"]) != chunk_total:
            # Still waiting on the rest: ack the chunk, install
            # nothing yet.
            return {"type": "install_snapshot_response",
                    "term": self.current_term, "success": True,
                    "match_index": self._last_log_index()}
        try:
            payload = json.loads("".join(
                transfer["chunks"][i] for i in range(chunk_total)))
        except ValueError:
            self._snapshot_transfer = None
            return {"type": "install_snapshot_response",
                    "term": self.current_term, "success": False,
                    "match_index": self._last_log_index()}
        self._snapshot_transfer = None
        return self._install_snapshot_payload(
            last_included_index, last_included_term,
            payload.get("data") or {}, payload.get("config"),
            payload.get("dedup"), payload.get("joint_new"))

    def _install_snapshot_payload(self, last_included_index: int,
                                  last_included_term: int, data: dict,
                                  config, dedup,
                                  joint_new=None) -> dict:
        """Install a fully received snapshot (lock held).

        The follower replaces its state machine with the snapshot's
        state, discards its log up to the snapshot's last included
        index, and keeps any log entries after that index whose
        prefix still matches.
        """
        with self.lock:
            if last_included_index <= self.snapshot_index:
                # We already have this snapshot (or a newer one).
                return {"type": "install_snapshot_response",
                        "term": self.current_term, "success": True,
                        "match_index": self._last_log_index()}

            # Whether this node had been a member before this
            # install: the snapshot's config decides removal below,
            # and only a server that had been a member can be removed
            # by one (see _is_removed). Captured before the install
            # mutates anything.
            was_member_before = self._was_member

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
            # The snapshot covers committed entries, so it covers
            # committed config changes too: adopt the config it was
            # written with (snapshots from before membership changes
            # carry no "config" key and leave ours alone).
            if config is not None:
                self.config = {
                    str(mid): (tuple(addr) if addr else None)
                    for mid, addr in config
                }
                self._rebuild_peers_from_config()
            # The joint phase as of the snapshot: adopt it when the
            # snapshot carries one. When it carries none, clear ours
            # — but only if our applied frontier is inside the
            # snapshot: the snapshot then covers every entry that
            # could have put us in joint phase, so its silence is
            # authoritative. If we applied past the snapshot (our
            # joint entry sits in the log after it), keep our state:
            # the entry is still in our log and still in effect.
            if joint_new is not None:
                self._joint_new = _config_dict_of(joint_new)
                self._rebuild_peers_from_config()
            elif self.last_applied <= last_included_index:
                self._joint_new = None
            # Membership from the snapshot's config: a config that
            # includes this node makes it a member; one that leaves
            # it out removes it — but only if it had been a member
            # before. A server being added installs exactly this
            # snapshot to catch up, and the old config it adopts is
            # not a membership it ever held, so the same exclusion
            # must not remove it: it just adopts the config and keeps
            # accepting replication until its own add completes.
            if (self.id in self.config
                    or (self._joint_new is not None
                        and self.id in self._joint_new)):
                self._was_member = True
            elif was_member_before:
                self._removed = True
            # The snapshot carries the dedup table for the entries it
            # covers (see _write_snapshot_file): adopt it, merged with
            # any keys this node already holds for entries past the
            # snapshot index, so a retry of a compacted-away write
            # still dedups after installing a snapshot. Snapshots from
            # before CAS carry pairs and load as ok.
            if dedup is not None:
                for item in dedup:
                    if len(item) == 3:
                        c, r, res = item
                        self._dedup[(str(c), str(r))] = dict(res)
                    else:
                        c, r = item
                        self._dedup[(str(c), str(r))] = {
                            "type": "client_response", "status": "ok"}
                while len(self._dedup) > DEDUP_MAX_ENTRIES:
                    self._dedup.pop(next(iter(self._dedup)))
            # A whole-snapshot install supersedes any partial chunked
            # transfer buffered for an older snapshot.
            self._snapshot_transfer = None
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
        if op in ("add_server", "remove_server"):
            return self._on_membership_request(msg)
        key = msg.get("key")
        if op not in ("set", "get", "del", "cas") or key is None:
            return {"type": "client_response", "status": "error",
                    "error": "op must be set/get/del/cas with a key"}
        if self._is_removed():
            return {"type": "client_response", "status": "error",
                    "error": "this node was removed from the cluster"}
        if op == "get":
            # A read-your-writes session read carries the index (and
            # term) of the client's last acknowledged write; the
            # serving node waits until it has applied that far, so the
            # client never sees its own write missing — including
            # after a failover to a new leader. Plain reads send
            # neither field and behave exactly as before.
            try:
                min_index = int(msg.get("min_commit_index") or 0)
                session_term = msg.get("session_term")
                session_term = (int(session_term)
                                if session_term is not None else None)
            except (TypeError, ValueError):
                return {"type": "client_response", "status": "error",
                        "error": "min_commit_index and session_term "
                                 "must be integers"}
            if min_index < 0:
                return {"type": "client_response", "status": "error",
                        "error": "min_commit_index must be >= 0"}
            # An explicit stale read asks to be served from any node
            # holding a fresh leader lease (see _serve_read): bounded
            # staleness, no redirect hop. Off by default — a plain
            # read stays linearizable through the leader.
            return self._serve_read(key, min_index, session_term,
                                    stale_ok=bool(msg.get("stale_ok")))
        command = {"op": op, "key": key}
        if op == "set":
            command["value"] = msg.get("value")
        if op == "cas":
            # A conditional write: set key to value only if its current
            # value equals expected. The comparison happens at apply
            # time (see KVStore.apply), so the outcome is decided by
            # the entry's position in the log, like any other write —
            # and a missing key fails distinctly from a mismatch.
            if msg.get("expected") is None:
                return {"type": "client_response", "status": "error",
                        "error": "cas requires expected"}
            if msg.get("value") is None:
                return {"type": "client_response", "status": "error",
                        "error": "cas requires value"}
            command["expected"] = msg["expected"]
            command["value"] = msg["value"]
        # Client session id, when the client sent one: it rides in the
        # log entry so every node records the same dedup key on apply.
        for field in ("client_id", "request_id"):
            if msg.get(field):
                command[field] = msg[field]
        return self._submit_write(command)

    def _on_watch(self, msg: dict) -> dict:
        """Serve a watch RPC by long-polling the local WatchManager.

        ``{"type": "watch", "key": ..., "prefix": ..., "timeout": ...}``
        registers a callback, waits up to ``timeout`` seconds for the
        first committed change to the key (or prefix), and replies
        with every event that arrived in that window. Any node can
        serve a watch — the apply path fires on leader and followers
        alike — so there is no leader redirect and no read-index
        round: a watch on a lagging follower simply fires later, once
        that node applies the entry. The wait happens on this handler
        thread with no lock held (the callback only appends to a list
        under its own condition), so it cannot stall the apply path
        or the node's lock; a raising or slow watcher is impossible
        here because the callback does no work beyond recording the
        event. The watch is cancelled before replying either way, so
        a timed-out watch leaves nothing registered behind.
        """
        key = msg.get("key")
        if not isinstance(key, str) or not key:
            return {"type": "watch_response", "status": "error",
                    "error": "watch needs a non-empty key"}
        try:
            timeout = float(msg.get("timeout") or 0.0)
        except (TypeError, ValueError):
            return {"type": "watch_response", "status": "error",
                    "error": "timeout must be a number of seconds"}
        timeout = max(0.0, min(timeout, WATCH_MAX_TIMEOUT))
        prefix = bool(msg.get("prefix", False))
        events: list[dict] = []
        cond = threading.Condition()

        def _record(event: dict) -> None:
            with cond:
                events.append(event)
                cond.notify_all()

        watch_id = self.watch.watch(key, _record, prefix=prefix)
        try:
            deadline = time.monotonic() + timeout
            with cond:
                while not events:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    cond.wait(remaining)
                # Any further events that landed while we were waking
                # ride along too; the client re-issues to keep
                # watching either way.
                seen = list(events)
        finally:
            self.watch.cancel(watch_id)
        return {"type": "watch_response",
                "status": "ok" if seen else "timeout",
                "events": seen}

    # ------------------------------------------------------------------
    # Membership changes (Raft §6, single-server)
    # ------------------------------------------------------------------
    def _on_membership_request(self, msg: dict) -> dict:
        """Handle a client add_server / remove_server request.

        Only the leader drives a change, and only one change at a time
        (see _membership_change_in_progress). An add first replicates
        the log to the new server as a non-voting peer and waits for it
        to catch up; only then is the change submitted, in two phases
        (see _submit_joint_change): a joint_config entry (C-old,new),
        then a final_config entry (C-new). A leader that removes
        itself steps down as soon as the final entry commits (in
        _apply_config_entry). Runs on a connection thread; the
        catch-up and commit waits happen outside the lock.
        """
        op = msg.get("op")
        server_id = msg.get("server_id")
        if not server_id:
            return {"type": "client_response", "status": "error",
                    "error": "server_id is required"}
        with self.lock:
            if self._is_removed():
                return {"type": "client_response", "status": "error",
                        "error": "this node was removed from the cluster"}
            if self.role != "leader":
                return self._redirect()
            if self._membership_change_in_progress():
                return {"type": "client_response", "status": "error",
                        "error": "a membership change is already in progress"}
            if op == "add_server":
                addr = msg.get("addr")
                if not addr or len(addr) != 2:
                    return {"type": "client_response", "status": "error",
                            "error": "addr [host, port] is required"}
                if server_id in self.config:
                    return {"type": "client_response", "status": "error",
                            "error": f"{server_id} is already a member"}
                if any(a == (addr[0], int(addr[1]))
                       for a in self.peers.values()) or (
                        self._self_addr is not None
                        and self._self_addr == (addr[0], int(addr[1]))):
                    return {"type": "client_response", "status": "error",
                            "error": f"address {addr} is already in use"}
                self._membership_change = {
                    "op": op, "server_id": server_id, "index": None}
            else:  # remove_server
                if server_id not in self.config:
                    return {"type": "client_response", "status": "error",
                            "error": f"{server_id} is not a member"}
                if len(self.config) <= 1:
                    return {"type": "client_response", "status": "error",
                            "error": "cannot remove the last member"}
                self._membership_change = {
                    "op": op, "server_id": server_id, "index": None}
        try:
            if op == "add_server":
                return self._add_server(server_id, (addr[0], int(addr[1])))
            return self._remove_server(server_id)
        finally:
            with self.lock:
                # The change is over (committed, timed out, or failed):
                # clear the flag so the next change can start. The
                # in-progress check also clears a flag whose entry has
                # committed, so a stale flag can never wedge the
                # cluster if this finally block is somehow skipped.
                if (self._membership_change is not None
                        and self._membership_change.get("server_id") == server_id
                        and self._membership_change.get("op") == op):
                    self._membership_change = None

    def _add_server(self, server_id: str, addr: tuple[str, int]) -> dict:
        """Add one server: catch it up as a non-voting peer, then commit
        the config entry that makes it a voter. Caller holds no lock
        (the flag is already set)."""
        with self.lock:
            target = self._last_log_index()
            self.peers[server_id] = addr
            self._non_voting.add(server_id)
            self._init_peer_state(server_id, 1)
        # Drive replication to the new peer right away instead of
        # waiting for the next heartbeat; heartbeats keep driving it
        # afterwards through _send_heartbeats.
        threading.Thread(target=self._replicate_to_peer, args=(server_id,),
                         daemon=True).start()
        deadline = time.monotonic() + self.membership_catchup_timeout
        while time.monotonic() < deadline:
            with self.lock:
                if self.role != "leader":
                    self._drop_catchup_peer(server_id)
                    return {"type": "client_response", "status": "error",
                            "error": "lost leadership while adding server"}
                if self.match_index.get(server_id, 0) >= target:
                    break
            time.sleep(0.02)
        else:
            with self.lock:
                self._drop_catchup_peer(server_id)
            return {"type": "client_response", "status": "error",
                    "error": f"{server_id} did not catch up in time"}
        # Caught up: the two-phase change (joint entry, then final).
        return self._submit_joint_change("add_server", server_id, addr)

    def _drop_catchup_peer(self, server_id: str) -> None:
        """Forget a server whose add failed before its config entry was
        appended: it never became a member, so remove it from the
        address book and the leader state. Lock held."""
        self.peers.pop(server_id, None)
        self._non_voting.discard(server_id)
        for table in (self.next_index, self.match_index):
            table.pop(server_id, None)
        self._in_flight.pop(server_id, None)
        self._next_to_send.pop(server_id, None)
        self._pipeline_epoch.pop(server_id, None)
        self._pipeline_acks.pop(server_id, None)
        self._max_in_flight.pop(server_id, None)
        self._peer_retry_after.pop(server_id, None)
        self._pipeline_failures.pop(server_id, None)
        self._replicating.discard(server_id)

    def _remove_server(self, server_id: str) -> dict:
        """Remove one server: the two-phase change (joint entry, then
        final). The entry is replicated to the removed server too (it
        is still a peer until the entry is applied), so it learns of
        its own removal and stops participating. Caller holds no lock."""
        return self._submit_joint_change("remove_server", server_id, None)

    def _submit_config_entry(self, command: dict) -> dict:
        """Append a membership command to the log, replicate it, wait
        for commit — like _submit_write, except success is judged by
        the commit alone: a leader that removes itself steps down as
        soon as the entry commits, and that must still report success
        to the client (the removal did happen)."""
        with self.lock:
            if self.role != "leader":
                return self._redirect()
            entry = {"term": self.current_term, "command": command}
            index = self._append_entry(entry)
            if self._membership_change is not None:
                self._membership_change["index"] = index
            peers = list(self.peers)

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
                            "error": "membership change did not commit in time"}
                self.commit_cond.wait(timeout=remaining)
        return {"type": "client_response", "status": "ok"}

    def _final_config_command(self) -> dict | None:
        """The final_config entry that closes the joint transition in
        progress: adopt C-new. Lock held; None when no transition is
        open."""
        if self._joint_new is None:
            return None
        return {"op": "final_config",
                "new": _config_list_of(self._joint_new)}

    def _complete_joint_transition(self) -> dict:
        """Drive the open joint transition to its end: append the
        final_config entry and wait for it to commit. Caller holds no
        lock.

        Exactly one thread drives: if another thread is already
        driving, wait for it and report its outcome instead of
        appending a second final entry. The driver can be the client
        thread that submitted the joint entry, the thread
        _apply_config_entry spawned when this leader applied it, or
        the thread _become_leader spawned when a new leader inherited
        it — all three funnel here. A driver that loses leadership
        reports the failure; the next leader picks the transition up
        again from _become_leader.
        """
        with self.lock:
            if self._joint_new is None:
                return {"type": "client_response", "status": "ok"}
            if self._joint_completion_running:
                driver = False
            else:
                self._joint_completion_running = True
                driver = True
        if not driver:
            deadline = time.monotonic() + CLIENT_COMMIT_TIMEOUT
            while time.monotonic() < deadline:
                with self.lock:
                    if self._joint_new is None:
                        return {"type": "client_response", "status": "ok"}
                    if not self._joint_completion_running:
                        break
                time.sleep(0.02)
            with self.lock:
                if self._joint_new is None:
                    return {"type": "client_response", "status": "ok"}
            return {"type": "client_response", "status": "timeout",
                    "error": "joint transition did not complete in time"}
        try:
            with self.lock:
                command = self._final_config_command()
            if command is None:
                return {"type": "client_response", "status": "ok"}
            return self._submit_config_entry(command)
        finally:
            with self.lock:
                self._joint_completion_running = False

    def _submit_joint_change(self, change_op: str, server_id: str,
                             addr: tuple[str, int] | None) -> dict:
        """Two-phase membership change (Raft dissertation §6).

        Phase one appends a joint_config entry (C-old,new): from the
        moment it commits, every quorum — commit, election, lease —
        needs a majority of BOTH configs, so an old-majority-only
        quorum cannot commit anything and two leaders in one term
        stay impossible while the configs differ. Phase two appends
        the final_config entry (C-new) and waits for it to commit;
        only then is the change done. If the leader dies between the
        phases, the next leader completes the transition from
        _become_leader, and a client that sees an error can retry:
        the change is either already done (the final entry committed)
        or still open (a new leader finishes it). Caller holds no lock.
        """
        with self.lock:
            if self.role != "leader":
                return self._redirect()
            new_config = dict(self.config)
            if change_op == "add_server":
                new_config[server_id] = tuple(addr) if addr else None
            else:  # remove_server
                new_config.pop(server_id, None)
            joint = {"op": "joint_config", "change_op": change_op,
                     "server_id": server_id,
                     "addr": [addr[0], addr[1]] if addr else None,
                     "old": _config_list_of(self.config),
                     "new": _config_list_of(new_config)}
        result = self._submit_config_entry(joint)
        if result.get("status") != "ok":
            return result
        return self._complete_joint_transition()

    def _redirect(self) -> dict:
        leader_id, addr = self.leader_info()
        return {"type": "client_response", "status": "not_leader",
                "leader": leader_id,
                "leader_addr": list(addr) if addr else None}

    def _submit_write(self, command: dict) -> dict:
        """Append a command to the log, replicate it, wait for commit.

        Leader-side group commit: a write does not append on its own.
        It joins the currently open batch (or opens one, becoming its
        flusher), and the flusher appends the whole batch with a single
        fsync once the batch window closes. Concurrent client writes
        in the same window therefore share one fsync instead of paying
        one each; a lone sequential write just pays the 2ms window.
        Membership entries bypass this — they are rare, and their
        caller tracks the entry's index directly.

        Deduplication: a write carrying a (client_id, request_id) that
        is already applied returns the cached result without appending
        anything, and one whose first attempt is still in flight joins
        that attempt's waiter instead of appending a duplicate — so a
        client that retries after a lost response gets at-most-once
        semantics.
        """
        join_waiter: dict | None = None
        key: tuple[str, str] | None = None
        flush_batch = False
        with self.lock:
            if self.role != "leader":
                return self._redirect()
            key = _dedup_key(command)
            if key is not None:
                if key in self._dedup:
                    # Already applied: the retry is a no-op.
                    return dict(self._dedup[key])
                join_waiter = self._dedup_inflight.get(key)
            if join_waiter is None:
                waiter = {"command": command, "index": None,
                          "event": threading.Event(), "result": None}
                if key is not None:
                    self._dedup_inflight[key] = waiter
                if self._write_batch is None:
                    self._write_batch = [waiter]
                    flush_batch = True
                else:
                    self._write_batch.append(waiter)
            else:
                waiter = join_waiter
        if flush_batch:
            # Wait outside the lock so concurrent writes can join the
            # batch, then flush it: one append (one fsync) for all of
            # them, and one commit wait for the batch's last index.
            time.sleep(GROUP_COMMIT_WINDOW)
            self._flush_write_batch()
        waiter["event"].wait(timeout=CLIENT_COMMIT_TIMEOUT)
        result = waiter["result"]
        if result is None:
            # The flusher never woke us (it crashed or lost leadership
            # without failing the batch): drop our inflight claim so a
            # retry with the same id can proceed, then report a timeout
            # rather than hanging the client past its deadline.
            with self.lock:
                if (key is not None
                        and self._dedup_inflight.get(key) is waiter):
                    del self._dedup_inflight[key]
            return {"type": "client_response", "status": "timeout",
                    "error": "write did not commit in time"}
        return result

    def _flush_write_batch(self) -> None:
        """Flush the open client-write batch: append every entry with
        one fsync, replicate, wait for the batch's last index to
        commit, then wake every waiter with the shared result. Runs
        on the thread of the first writer in the batch."""
        with self.lock:
            batch = self._write_batch
            self._write_batch = None
            if not batch:
                return
            if self.role != "leader":
                # Leadership was lost while the batch was forming: fail
                # every waiter so it redirects instead of hanging.
                for w in batch:
                    self._clear_inflight(w)
                    w["result"] = {"type": "client_response",
                                   "status": "error",
                                   "error": "lost leadership while committing"}
                    w["event"].set()
                return
            entries = [{"term": self.current_term, "command": w["command"]}
                       for w in batch]
            last_index = self._append_entries(entries)
            for i, w in enumerate(batch):
                w["index"] = last_index - len(batch) + 1 + i
                # The term of the entry itself, not the term when the
                # waiter is woken: leadership may have changed hands
                # in between, and the session token must name the
                # entry's own term.
                w["term"] = self.current_term
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
            while self.commit_index < last_index:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.commit_cond.wait(timeout=remaining)
            if self.commit_index >= last_index:
                if self.role != "leader":
                    result: dict = {"type": "client_response",
                                    "status": "error",
                                    "error": "lost leadership while committing"}
                else:
                    result = {"type": "client_response", "status": "ok"}
            else:
                result = {"type": "client_response", "status": "timeout",
                          "error": "write did not commit in time"}
        # Wake every waiter — and only now drop their inflight claims:
        # by the time a waiter is woken the entry is committed (so the
        # dedup table already holds it via _apply_committed) or the
        # batch failed (so nothing was appended), and a retry arriving
        # after this point takes the _dedup path or appends fresh.
        # A waiter whose entry has an apply-time result (a CAS) gets
        # that result, not the shared ok: the outcome was decided when
        # the entry applied, and the waiter must learn it, not just
        # that the entry committed. Apply results for this batch are
        # pruned here, once every waiter has its own — and only this
        # batch's: batches flush concurrently, so pruning every
        # result up to last_index could delete another batch's CAS
        # outcome before its flusher read it, and that waiter would
        # then get the shared "ok" for a CAS that actually failed.
        with self.lock:
            for w in batch:
                own = (self._apply_results.get(w["index"])
                       if result.get("status") == "ok" else None)
                w["result"] = dict(own) if own is not None else dict(result)
                # Every committed write reports the log index (and term)
                # of its own entry: the client session uses it as its
                # read-your-writes watermark, and a retried write
                # reports the same index as the first attempt.
                if w["index"] is not None:
                    w["result"]["index"] = w["index"]
                    w["result"]["term"] = w.get("term")
                w["event"].set()
                self._clear_inflight(w)
            for w in batch:
                if w["index"] is not None:
                    self._apply_results.pop(w["index"], None)

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

    def _serve_read(self, key: str, min_index: int = 0,
                    session_term: int | None = None,
                    stale_ok: bool = False) -> dict:
        """ReadIndex read (Raft dissertation §6.4).

        A stale read (``stale_ok``) is the one exception to the
        leader-only rule: a follower holding a fresh leader lease
        serves it from its applied state instead of redirecting.
        Bounded staleness, not linearizability — it reflects the
        leader's committed state as of the follower's last heartbeat
        — and it is opt-in only: a plain read without the flag still
        redirects to the leader and is served linearizably, and a
        stale read that also carries a session watermark redirects
        too, since the follower may not have applied that far.

        1. Record read_index = commit_index while leader in this term —
           or the session's last acknowledged write index, whichever is
           higher: a read-your-writes session must not be served from
           state that predates its own write, so the read waits until
           the state machine has applied at least that far. A new
           leader after a failover holds the committed write in its
           log (Raft's election restriction), so the wait ends as soon
           as it commits its no-op entry and applies the prefix.
        2. Confirm leadership — with a heartbeat round to a majority,
           unless the leader lease is still valid: a majority acked
           within LEADER_LEASE_DURATION, which is shorter than any
           follower's election timeout, so no new leader can have been
           elected since. A partitioned ex-leader holds no lease (its
           acks stopped arriving), so it still falls through to the
           round and is caught there.
        3. Wait until the state machine has applied up to read_index;
           only then is the local state guaranteed to include every
           write committed before the read started, plus the session's
           own writes. The wait registers a pending read, resolved by
           _apply_committed once the apply index catches up — no log
           entry is written for the read. The reply carries the
           applied index (and its term) the returned value reflects,
           so a client can remember how far it has read: its next
           read waits until the serving node has applied at least
           that far, and reads are monotonic — they never go
           backwards to an older state.
        4. If leadership is lost mid-read, redirect (the client follows
           it to the new leader) — never serve from a deposed leader.
           If apply stalls past READ_INDEX_TIMEOUT while we are still
           leader in this term, a plain read falls back to the old
           heartbeat-confirmed read of whatever is applied: on this
           leader apply is synchronous with commit advancement under
           the same lock, so last_applied == commit_index whenever we
           hold leadership, and the fallback is exactly the previous
           behaviour. A session read never takes that fallback — it
           would serve state that predates the session's write — so
           it times out instead and the client retries.
        A session index past the end of this node's log, or a term
        that does not match the entry at that index (while the entry
        is still in the log — past the snapshot it cannot be checked,
        but then it is already applied), is an error, not a wait: the
        token names a write this node cannot have.
        """
        with self.lock:
            if self.role != "leader":
                # A follower with a fresh leader lease serves an
                # explicit stale read from its applied state instead
                # of redirecting: it heard from the current term's
                # leader within FOLLOWER_LEASE_DURATION, so no new
                # leader can exist yet and its applied state is a
                # committed prefix of that leader's log. Bounded
                # staleness, not linearizability — a write committed
                # after the last heartbeat may be missing — so the
                # flag is required: a plain read without it still
                # redirects to the leader and stays linearizable,
                # and a session read (which carries a watermark the
                # follower may not have applied) redirects too.
                if (stale_ok and min_index == 0 and session_term is None
                        and self._follower_lease_valid()):
                    value = self.sm.get(key)
                    return {"type": "client_response", "status": "ok",
                            "value": value,
                            "read_index": self.last_applied,
                            "read_term": self._term_at(self.last_applied),
                            "lease": "follower"}
                return self._redirect()
            term = self.current_term
            if min_index > self._last_log_index():
                return {"type": "client_response", "status": "error",
                        "error": f"session write at index {min_index} "
                                 "is not in this node's log"}
            if (session_term is not None
                    and min_index > self.snapshot_index
                    and self._term_at(min_index) != session_term):
                return {"type": "client_response", "status": "error",
                        "error": f"session term mismatch at index "
                                 f"{min_index}: expected term "
                                 f"{session_term}"}
            read_index = max(self.commit_index, min_index)
            lease_ok = self._lease_valid()
        if not lease_ok and not self._confirm_leadership(term):
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
                            "value": value,
                            "read_index": self.last_applied,
                            "read_term": self._term_at(self.last_applied)}
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
        # leader in this term. A session read never takes it: serving
        # whatever happens to be applied could return state that
        # predates the session's own write, which is exactly the stale
        # read the session exists to prevent — so it times out and the
        # client retries against the leader once it has caught up.
        if min_index > 0:
            return {"type": "client_response", "status": "timeout",
                    "error": "session write not yet applied; "
                             "retry the read"}
        # A plain read re-confirms leadership and reads whatever is
        # applied, exactly like the old heartbeat-confirmed read.
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
            read_index = self.last_applied
            read_term = self._term_at(read_index)
        return {"type": "client_response", "status": "ok", "value": value,
                "read_index": read_index, "read_term": read_term}

    def _follower_lease_valid(self) -> bool:
        """True if this follower heard from the current term's leader
        within FOLLOWER_LEASE_DURATION. Lock held.

        The contact must be in the current term: a higher term means
        an election happened since, so the leader we heard from may
        be deposed even though the contact is still recent. Safe for
        the same reason as the leader lease — the window is shorter
        than the minimum election timeout, so no new leader can have
        been elected since the contact, and every entry this node has
        applied came from that leader's committed log. A candidate
        holds no lease (role check), and a removed node never reaches
        here — _on_client rejects it first.
        """
        if self.role != "follower":
            return False
        if self._last_leader_contact is None:
            return False
        if self._last_leader_contact_term != self.current_term:
            return False
        return (time.monotonic() - self._last_leader_contact
                <= FOLLOWER_LEASE_DURATION)

    def _lease_valid(self) -> bool:
        """True if a majority of voters acked us within
        LEADER_LEASE_DURATION. Lock held.

        The leader itself counts as one. A peer counts only if it is a
        voting member — a server being added may ack, but it is not a
        voter yet. Safe because the lease is shorter than the minimum
        election timeout: a follower that acked at time T cannot start
        an election before T + ELECTION_TIMEOUT_MIN, and any majority
        that could elect a new leader overlaps the acking majority in
        at least one node, so no new leader can exist while the lease
        holds. A single-node cluster is trivially leased (majority 1).
        """
        if self.role != "leader":
            return False
        now = time.monotonic()
        # The leader itself counts as one; a peer counts if it acked
        # recently. _has_quorum intersects with the voting config(s),
        # so a server being added never extends the lease — and during
        # a joint transition the acking set must be a majority of BOTH
        # configs.
        holders = {self.id}
        holders.update(
            p for p, t in self._last_ack.items()
            if now - t <= LEADER_LEASE_DURATION
        )
        return self._has_quorum(holders)

    def _confirm_leadership(self, term: int) -> bool:
        """Heartbeat round: succeed only if a majority acks in this term.
        Only voting members count — a server being added may ack too,
        but it is not a voter until its config entry commits."""
        peers = list(self.peers)
        acks: dict[str, bool] = {}
        ack_lock = threading.Lock()

        def _ping(peer: str) -> None:
            ok = self._replicate_to_peer(peer)
            with ack_lock:
                acks[peer] = ok

        threads = [threading.Thread(target=_ping, args=(p,), daemon=True)
                   for p in peers]
        for t in threads:
            t.start()
        deadline = time.monotonic() + LEADER_LEASE_TIMEOUT
        for t in threads:
            t.join(timeout=max(0.0, deadline - time.monotonic()))
        with self.lock:
            self._confirm_rounds += 1
            still_leader = self.role == "leader" and self.current_term == term
            holders = {self.id}
            holders.update(p for p, ok in acks.items() if ok)
        return still_leader and self._has_quorum(holders)

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
            # It also refreshes this peer's share of the leader lease:
            # the follower heard from us just before it acked, so it
            # cannot time out and elect a new leader before the lease
            # (shorter than its election timeout) runs out.
            self._last_ack[peer] = time.monotonic()
            self.match_index[peer] = max(
                self.match_index.get(peer, 0), end)
            self.next_index[peer] = self.match_index[peer] + 1
            self._pipeline_acks[peer] = self._pipeline_acks.get(peer, 0) + 1
            self._pipeline_failures[peer] = 0
            self._advance_commit()
            # A removed peer just reported a commit index at or past
            # its removal entry: it has applied the removal, so it
            # knows it is out. Stop replicating to it now — keeping it
            # in self.peers past this point would mean heartbeating a
            # server that refuses every AppendEntries.
            pending = self._removal_pending.get(peer)
            if (pending is not None
                    and int(resp.get("commit_index", 0)) >= pending):
                del self._removal_pending[peer]
                self.peers.pop(peer, None)
                for table in (self.next_index, self.match_index):
                    table.pop(peer, None)
                self._in_flight.pop(peer, None)
                self._next_to_send.pop(peer, None)
                self._pipeline_epoch.pop(peer, None)
                self._pipeline_acks.pop(peer, None)
                self._max_in_flight.pop(peer, None)
                self._peer_retry_after.pop(peer, None)
                self._pipeline_failures.pop(peer, None)
                self._replicating.discard(peer)
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
        # _send_peer_rpc returns None for a partitioned (or removed)
        # peer, which _apply_append_reply treats as a timeout.
        resp = self._send_peer_rpc(peer, msg)
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
        addr = self.peers.get(peer)
        if addr is None:
            return False  # Peer was removed while this call was queued.
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
                    # AppendEntries as the heartbeat. There is nothing
                    # left to send (next_to_send is past the log end),
                    # so the peer is caught up as far as this leader
                    # knows — send the heartbeat even if match_index is
                    # still 0. A leader elected with a non-empty log
                    # starts with match_index 0 for every peer, and
                    # requiring match_index >= last here meant it never
                    # sent anything at all until a write arrived: the
                    # followers heard nothing, timed out, and elected a
                    # new leader with the same problem, forever. The
                    # heartbeat is what keeps followers from timing
                    # out, so it must not depend on an ack that can
                    # only arrive after a heartbeat.
                    if not in_flight and next_to_send > last:
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
                resp = self._send_peer_rpc(peer, heartbeat_msg)
                with self.lock:
                    prev, end, epoch = heartbeat_meta
                    self._apply_append_reply(peer, resp, prev, end,
                                             None, epoch)
                continue
            time.sleep(0.005)
        with self.lock:
            return self._pipeline_acks.get(peer, 0) > acks_at_entry

    def _send_install_snapshot(self, peer: str) -> bool:
        """Stream this node's snapshot to a lagging peer, in chunks.
        Network I/O happens outside the lock; each chunk's reply is
        applied under it. The snapshot (state + config + dedup table)
        is serialized once, split into ``snapshot_chunk_size``
        pieces, and sent one RPC at a time — each chunk acked before
        the next goes out, so a failed chunk aborts the transfer
        instead of leaving a half-installed snapshot behind, and the
        follower never has to hold a multi-megabyte snapshot in one
        message. Returns True only if the peer installed the final
        chunk in our term."""
        addr = self.peers.get(peer)
        if addr is None:
            return False  # Peer was removed while this call was queued.
        if self._is_partitioned(peer):
            return False  # Partitioned: the snapshot never arrives.
        with self.lock:
            if self.role != "leader":
                return False
            term = self.current_term
            last_included_index = self.snapshot_index
            last_included_term = self.snapshot_term
            payload = json.dumps({
                "data": self.sm.items(),
                "config": self._config_list(),
                # C-new while a joint transition is in flight, so a
                # follower that installs this snapshot resumes in
                # joint phase instead of forgetting the change.
                "joint_new": self._joint_list(),
                # The dedup table for the covered prefix, so the
                # follower adopts it (see _on_install_snapshot) and a
                # retry of a compacted-away write still dedups there.
                "dedup": [[c, r, res]
                          for (c, r), res in self._dedup.items()],
            })
        size = self.snapshot_chunk_size
        chunks = [payload[i:i + size]
                  for i in range(0, len(payload), size)] or [""]
        for i, chunk in enumerate(chunks):
            msg = {
                "type": "install_snapshot",
                "term": term,
                "leader_id": self.id,
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "chunk_index": i,
                "chunk_total": len(chunks),
                "chunk": chunk,
            }
            resp = self._send_peer_rpc(peer, msg)
            with self.lock:
                if resp is None:
                    return False
                resp_term = int(resp.get("term", 0))
                if resp_term > self.current_term:
                    self._step_down(resp_term)
                    return False
                if self.role != "leader" or resp_term != term:
                    return False
                if not resp.get("success"):
                    return False
                # Every chunk ack is a fresh ack from this follower,
                # so it extends the leader lease like any other ack.
                self._last_ack[peer] = time.monotonic()
                if i != len(chunks) - 1:
                    continue
                self.match_index[peer] = int(
                    resp.get("match_index", last_included_index))
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
            self.next_index = {}
            self.match_index = {}
            # Fresh pipeline state for this term: no batches in flight,
            # nothing dispatched yet, no acks counted, no backoff.
            # Established followers start at last+1 (assumed caught up);
            # a non-voting server still catching up keeps the state it
            # already has instead of being reset.
            self._in_flight = {}
            self._next_to_send = {}
            self._pipeline_epoch = {}
            self._pipeline_acks = {}
            self._max_in_flight = {}
            self._peer_retry_after = {}
            self._pipeline_failures = {}
            self._last_ack = {}
            for p in self.peers:
                if p in self._non_voting:
                    # A server still catching up for an add starts from
                    # index 1, so the whole log is replicated to it
                    # (or a snapshot, if the prefix is compacted) —
                    # unlike an established follower, it is not assumed
                    # caught up to the log end.
                    self._init_peer_state(p, 1)
                elif (self._joint_new is not None
                        and p in self._joint_new and p not in self.config):
                    # A server in C-new only (being added) may not
                    # have the log yet. _non_voting is not persisted,
                    # so a new leader that inherited the joint config
                    # cannot tell a caught-up new server from a fresh
                    # one — start it from 1 and let the log flow.
                    self._init_peer_state(p, 1)
                else:
                    self._init_peer_state(p, last + 1)
            # Append a no-op entry in this term. The commit rule only
            # counts current-term entries (Raft section 5.4.2), so
            # without one, committed entries from earlier terms would
            # sit unapplied — and invisible to reads — until the next
            # client write arrived. The no-op commits as soon as a
            # majority acks it, and committing it transitively commits
            # everything before it. It changes no state: _apply_committed
            # skips it. (next_index was set from the pre-append last, so
            # the no-op is exactly the one entry left to dispatch.)
            self._append_entry({"term": self.current_term,
                                "command": {"op": "noop"}})
            # A leader that inherits a joint config must complete the
            # transition: the change's driver may have died after the
            # joint entry committed, and until the final entry lands
            # the cluster refuses every new membership change.
            complete_joint = self._joint_new is not None
        if complete_joint:
            threading.Thread(target=self._complete_joint_transition,
                             daemon=True).start()
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
        with self.lock:
            # Voters are the union of both configs during a joint
            # transition: a server in C-new only still votes, and
            # _has_quorum needs its grant for the new majority.
            voters = set(self.config) | set(self._joint_new or {})

        def _ask(peer: str) -> None:
            resp = self._send_peer_rpc(peer, msg)
            if resp is None:
                return
            with grant_lock:
                max_term[0] = max(max_term[0], int(resp.get("term", 0)))
                # Only a voting member's grant counts: a server being
                # added may answer, but it is not a voter yet.
                if resp.get("pre_vote_granted") and peer in voters:
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
            # A pre-vote win needs a quorum: a majority of the config,
            # or of BOTH configs during a joint transition.
            return self._has_quorum(grants), term_before, heartbeat_mark

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
                or self._is_removed()
                or self.current_term != term_before
                or self._last_heartbeat != heartbeat_mark
            ):
                # No mandate, or something changed while we asked: a
                # leader contacted us, we stepped down to a newer term,
                # we were removed from the cluster, or we granted a
                # vote. Back off and try again after a fresh randomized
                # timeout instead of spinning.
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
            resp = self._send_peer_rpc(peer, msg)
            if resp and resp.get("vote_granted") and int(resp.get("term", 0)) == term:
                with self.lock:
                    # Only a voting member's vote counts. During a
                    # joint transition the voters are the union of
                    # both configs, so a server in C-new only (being
                    # added) still votes — its vote is what makes the
                    # new majority.
                    if (peer in self.config
                            or (self._joint_new is not None
                                and peer in self._joint_new)):
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
                and self._has_quorum(self.votes)
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
                if self._is_removed():
                    # Removed from the cluster: never run for election
                    # or send heartbeats again. The loop keeps polling
                    # so a (hypothetical) re-add could resume it, but
                    # nothing in this node drives that — rejoining goes
                    # through a fresh add_server.
                    role = "follower"
                    timed_out = False
                    heartbeat_due = False
                else:
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
