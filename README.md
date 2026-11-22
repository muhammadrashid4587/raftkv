# raftkv

A distributed key-value store in **pure Python 3 (standard library only)**,
built on the **Raft consensus protocol**. Three nodes form a cluster; one
is elected leader, writes are committed to a majority before they are
acknowledged, and if the leader dies the survivors elect a new one and
keep serving — with no lost or divergent data.

Built as a portfolio project: the interesting parts are the consensus
logic and the failure handling, not the key-value API on top.

## Features

- **Raft consensus** — leader election with randomized timeouts,
  RequestVote / AppendEntries RPCs, the log matching property, and
  commit-index advancement restricted to current-term entries
  (Raft §5.4.2).
- **Durable log** — `currentTerm`, `votedFor`, and the log are persisted
  per node (`meta.json` + `log.jsonl`, fsynced). A restarted node replays
  its committed log and rejoins without losing data.
- **ReadIndex reads** — a read records the leader's commit index,
  confirms leadership with a heartbeat round to a majority, and only
  returns once the state machine has applied up to that index, so the
  read is linearizable without writing a log entry. Pending reads
  register in a list resolved by the apply path; losing leadership
  mid-read fails them to a redirect, and a stalled apply falls back
  to a heartbeat-confirmed read after a timeout.
- **Log compaction via snapshotting** — once the log grows past a
  threshold (default 1000 entries), a node snapshots its state
  machine (`snapshot.json`: state + last included index/term) and
  discards the covered log prefix. A follower too far behind to catch
  up by AppendEntries receives the snapshot via InstallSnapshot
  (Raft §7) instead. A restarted node loads its snapshot and replays
  only the remaining log suffix.
- **Pre-vote** — before starting an election a candidate runs a
  pre-vote round (RequestPreVote, Raft dissertation §4.2.3) and only
  stands if a majority would vote for it. A voter refuses a pre-vote
  when it is the leader, when it heard from a live leader recently,
  or when the candidate's log is stale — and granting one changes no
  state. A partitioned node can therefore never inflate its term, so
  rejoining it cannot disrupt the leader or force a re-election.
- **Leader redirects** — a write or read sent to a follower returns the
  current leader's address; the client follows it automatically.
- **Real failure handling** — tested end to end: kill the leader, the
  cluster re-elects, writes continue, the old leader rejoins and
  catches up instead of forking.

## Quick start

```bash
# Start a 3-node cluster on localhost (one command)
make demo

# In another terminal:
python3 -m raftkv.client --node 127.0.0.1:5001 set hello world
python3 -m raftkv.client --node 127.0.0.1:5002 get hello   # -> world

# Watch failover: kill the leader's process, then keep writing.
# The remaining two nodes elect a new leader within ~a second.
```

Or start nodes manually (each in its own terminal):

```bash
python3 -m raftkv.server --id node1 --port 5001 \
  --peers node2=127.0.0.1:5002,node3=127.0.0.1:5003 \
  --data-dir /tmp/raftkv-demo/node1
# ... node2 on 5002, node3 on 5003, with the other two as peers
```

## Tests

```bash
make test
# or: python3 -m unittest discover -s tests -v
```

25 tests, all passing:

| Test | What it proves |
|---|---|
| `test_exactly_one_leader_is_elected` | Election converges to a single leader |
| `test_leader_stays_stable_with_no_failures` | No spurious re-elections when healthy |
| `test_write_replicates_to_all_nodes` | Committed writes reach every node |
| `test_write_to_follower_redirects_to_leader` | Followers redirect, client follows |
| `test_delete_replicates` | Deletes replicate like writes |
| `test_leader_serves_reads` | Leader reads return committed values |
| `test_follower_restart_replays_committed_log` | Restarted node replays disk log + catches up |
| `test_kill_leader_reelect_write_and_rejoin` | Full failover: kill leader, re-elect, write, rejoin, verify identical state on all nodes |
| `test_snapshot_compacts_log_and_survives_restart` | Log compacts past the threshold; restarted node resumes from snapshot + suffix |
| `test_lagging_follower_catches_up_via_install_snapshot` | Follower behind the leader's snapshot catches up via InstallSnapshot |
| `test_restart_loads_snapshot_without_a_cluster` | Node restarted alone reloads snapshotted state from disk |
| `test_stale_candidate_prevote_rejected` | A candidate with a stale log gets no pre-vote, and the voter is unchanged |
| `test_prevote_granted_when_log_current_and_no_leader` | An up-to-date candidate with no live leader wins a pre-vote |
| `test_prevote_has_no_side_effects` | Granting a pre-vote changes no state and does not consume the real vote |
| `test_prevote_rejected_when_leader_recently_heard` | A follower with a live leader refuses a pre-vote |
| `test_partitioned_node_rejoin_does_not_disrupt_leader` | A rejoined node cannot force a re-election or a term jump |
| `test_election_still_works_after_leader_failure` | Pre-vote does not stop a genuine failover election |
| `test_readindex_returns_latest_committed_value` | ReadIndex read returns the latest committed value and writes no log entry |
| `test_read_after_write_sees_the_write` | A read after a committed write sees it, including after overwrite and delete |
| `test_read_across_leader_change_never_returns_stale_data` | Reads across a leader change never return the pre-change value |
| `test_deposed_leader_does_not_serve_stale_reads` | A deposed ex-leader redirects instead of serving its stale state |
| `test_concurrent_reads_are_all_correct` | Concurrent reads during a write only ever see committed values |
| `test_read_served_with_one_follower_down` | Reads keep working with a majority of two |
| `test_pending_read_resolves_when_apply_catches_up` | Pending reads resolve once the apply index passes them, fail on leadership loss |
| `test_step_down_fails_pending_reads` | Stepping down wakes pending reads so they redirect instead of hanging |

Tests use real nodes (real TCP, real threads, real disk) on localhost —
no mocks of the consensus path.

## Benchmark

Measured on this dev VM, 3-node localhost cluster, single client doing
sequential committed writes (each write waits for majority replication
and an fsync of the log append):

```
200 sequential committed writes in 0.54s -> ~370 writes/sec
```

This is latency-bound, not throughput-bound: every write pays a full
round trip to a majority plus a disk fsync. Batching / pipelining and
group commit would raise it substantially; see the roadmap.

## Architecture

```
raftkv/
  transport.py   Length-prefixed JSON framing, one-shot RPC, threaded TCP server
  raft.py        RaftNode: elections, log replication, commit, persistence
  store.py       KVStore: the deterministic state machine (SET/GET/DEL)
  server.py      Node CLI: --id --port --peers --data-dir
  client.py      Client CLI: set/get/del with leader-redirect following
tests/
  harness.py     In-process 3-node cluster fixture
  test_raft.py       Election, replication, redirects, persistence
  test_integration.py  Leader-kill failover scenario
  test_readindex.py    ReadIndex reads, pending-read registry, deposed leader
```

**Threading.** One TCP server thread (a thread per connection), one
timer thread driving elections/heartbeats, client requests on connection
threads. All Raft state is guarded by a single lock, and network I/O is
always done *outside* the lock — two nodes RPC-ing each other at the
same time must never hold each other's lock while waiting on the
network.

**Persistence format.** Per node, in `--data-dir`:

- `meta.json` — `current_term`, `voted_for`, `commit_index`, rewritten
  atomically (tmp file + rename) on every change.
- `log.jsonl` — one JSON object per line,
  `{"term": n, "index": g, "command": {"op": "set", "key": "k", "value": "v"}}`,
  appended + fsynced per entry, rewritten whole after a truncation or
  a snapshot. Each entry carries its global log index, so a restart can
  tell a pre-snapshot entry from a live one.
- `snapshot.json` — `{"last_included_index": n, "last_included_term": t,
  "state": {...}}`, the state machine as of index `n`, written
  atomically (tmp file + rename) when the log is compacted or a
  snapshot is installed. The in-memory log holds only entries after
  the snapshot.

On restart the node loads its snapshot into the state machine, then
re-applies the committed entries after the snapshot. SET/DEL are
deterministic, so replay is safe. A snapshot is written *before* the
log is truncated, so a crash mid-compaction is recoverable: covered
entries left in `log.jsonl` are dropped by their index on reload.

## Design decisions

- **Why Raft, not a primary-backup hack.** A single leader with a
  replicated log is the only design here that survives a leader crash
  without losing acknowledged writes or splitting the data. Paxos is
  harder to reason about; Raft's election + log matching gives the
  same safety with code a reader can actually follow.
- **Read path.** Reads go to the leader as ReadIndex reads
  (dissertation §6.4): record the commit index, confirm leadership
  with a heartbeat round to a majority, then wait until the state
  machine has applied up to that index. That closes the stale-read
  hole where a partitioned ex-leader keeps answering from old state,
  without writing a log entry per read. Pending reads register in a
  list that the apply path resolves and leadership loss fails; if
  apply stalls past a timeout while leadership still holds, the read
  falls back to a plain heartbeat-confirmed read of the applied state
  (on this leader apply is synchronous with commit advancement under
  the same lock, so the fallback is exactly the previous behaviour).
- **Commit rule.** A leader only advances `commit_index` for entries
  from its *own* term, counting replicas — never for old-term entries
  alone. Committing a current-term entry transitively commits
  everything before it. This is the subtle rule that keeps Raft safe
  across elections.
- **Log conflict handling.** On a mismatched `prev_log_index/term` the
  follower rejects and the leader backs `next_index` down and retries,
  so a new leader brings stale followers up to date instead of
  diverging.
- **Pre-vote.** Every election starts with a pre-vote round, and the
  voter rule is the whole point: refuse if you are the leader, if you
  heard from a leader recently, or if the candidate's log is stale —
  and change nothing either way. One subtlety: "heard from a leader
  recently" is tracked with its own timestamp, updated only by a valid
  AppendEntries / InstallSnapshot. The election timer is reset by vote
  grants and failed attempts too, so reusing it for the voter rule
  would make nodes that just attempted an election keep rejecting
  each other, and a fresh cluster could never elect a leader at all.
- **No dependencies.** Pure stdlib: `socket`, `threading`, `json`,
  `socketserver`. Nothing to install, nothing to break.

## Roadmap

- ~~Log compaction via snapshotting~~ — done in v0.2.0: snapshot the
  KV state + last included index/term, truncate the log, InstallSnapshot
  RPC for lagging followers, automatic compaction past 1000 entries,
  snapshot loading on restart.
- Membership changes (joint consensus) for adding/removing nodes.
- Pre-vote to avoid disruptive re-elections when a partitioned node
  rejoins with a stale-but-high term — done in v0.3.0: RequestPreVote
  round before every election, voters refuse when they are the leader
  or heard from a live leader recently, granting changes no state.
- ~~ReadIndex / lease-based reads to serve reads without a heartbeat
  round on the hot path~~ — done in v0.4.0: ReadIndex reads record the
  commit index, confirm leadership with a heartbeat round, and wait
  for the state machine to apply up to that index, with a pending-read
  registry resolved by the apply path, redirect on leadership loss,
  and a heartbeat-confirmed fallback after a timeout.
- Pipelined replication and group commit for higher write throughput.
- A benchmark script (`scripts/bench.py`) with concurrent clients.
