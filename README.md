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
- **Group commit** — concurrent client writes in the same 2ms window
  share a single fsync on the leader: the first writer opens a batch,
  joiners pile in, and the batch is appended and replicated as one
  unit. Follower-side batching does the same per AppendEntries batch.
  Concurrent write throughput more than doubled (see Benchmark); a
  lone sequential write just pays the 2ms window.
- **Leader lease reads** — a leader that heard acks from a majority
  within the last 100ms (shorter than any follower's 150ms minimum
  election timeout, so no new leader can have been elected since)
  serves reads straight from its applied state, with no heartbeat
  round at all. Heartbeats go out every 50ms, so a healthy leader's
  lease is almost always fresh; sequential read throughput went from
  ~250-730 to ~1300-3500 ops/s (see Benchmark). If the lease is stale
  the read falls back to the ReadIndex heartbeat round, and a deposed
  leader holds no lease — stepping down clears it.
- **Follower lease reads** — since v0.20.0 a follower that heard
  from the current term's leader within the last 100ms (shorter
  than any follower's 150ms minimum election timeout, so no new
  leader can have been elected since) serves an explicit stale
  read (`get --stale`, `"stale_ok": true`) straight from its
  applied state instead of redirecting to the leader — no
  heartbeat round, no redirect hop, and the reply carries the
  applied index and term like any other read. The read is
  bounded-stale, not linearizable: it reflects the leader's
  committed state as of the follower's last heartbeat, so a write
  committed after that heartbeat may be missing — at most ~one
  heartbeat interval (50ms) plus apply lag behind the leader in a
  healthy cluster, and a partitioned follower stops serving within
  100ms of losing contact and redirects instead. Its applied state
  is always a committed prefix of that leader's log, never
  divergent state. The flag is opt-in on purpose: a plain read
  without it still redirects to the leader and is served
  linearizably, so the chaos harness's linearizability checks
  keep passing. Session reads never take this path either: they
  carry a watermark the follower may not have applied, so they
  still redirect to the leader, which waits for it.
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
- **Pipelined replication** — the leader keeps up to 10 AppendEntries
  batches in flight per follower (at most 32 entries each) instead of
  waiting for one reply before sending the next, so a lagging follower
  catches up in a few concurrent batches. Replies are applied as they
  arrive; a rejected batch backs `next_index` down from the follower's
  hint, bumps an epoch that invalidates stale in-flight replies, and
  re-dispatches from there. Commit advancement is unchanged — only
  current-term entries advance the commit index. A five-timeout
  failure budget keeps a dead peer from stalling writes, and at most
  one replication driver runs per peer.
- **Membership changes** — a client can add or remove a server while
  the cluster is live (`python3 -m raftkv.client add-server node4
  127.0.0.1:5004`, `remove-server node4`). Single-server changes, one
  at a time (Raft dissertation §6): the change is a log entry, the
  config is persisted with it, a new server replicates the whole log
  via AppendEntries/InstallSnapshot before it starts voting, and a
  removed server stops participating. A leader that removes itself
  steps down. The leader keeps replicating to a removed server until
  that server's commit index reaches the removal entry, so the removed
  server actually learns it was removed instead of timing out forever.
- **Leader redirects** — a write or read sent to a follower returns the
  current leader's address; the client follows it automatically.
- **Client session deduplication** — a write may carry a
  `(client_id, request_id)` session id (`--client-id` / `--request-id`
  on the CLI). The leader records the pair when the entry applies, and
  a retry with the same pair — after a lost response, after a leader
  change, or while the first attempt is still in flight — returns the
  first attempt's result instead of appending a second log entry, so
  clients get at-most-once writes. The table is derived from the
  applied log, so a new leader rebuilds it by replay and a snapshot
  carries it; it is bounded (10,000 entries, oldest evicted first)
  and deterministic, so every node's table stays identical. Writes
  without a session id behave exactly as before.
- **Linearizable compare-and-swap** — `cas key expected value`
  (`python3 -m raftkv.client cas counter 41 42`) sets the key only if
  its current value equals `expected`, and fails distinctly when the
  key is missing (`missing_key`) versus when the value differs
  (`mismatch`, with the current value in the reply). The CAS rides
  the Raft log like any other write and its outcome is decided when
  the entry applies, at its exact position in the log, so it is
  linearizable: a read after a committed CAS sees the new value (or
  the old one, if it failed). With a session id a retry returns the
  first attempt's exact result — success or failure — instead of
  re-executing, and the result survives snapshots and leader changes
  with the dedup table.
- **Read-your-writes sessions** — `raftkv.client.Session` is one
  client under one `client_id`. Every write it acks advances its
  watermark to the log index and term of that write's entry (write
  responses now carry both, and a dedup retry reports the first
  attempt's index), and every read it issues carries the watermark:
  the serving node waits until it has applied up to that index
  before answering, so a client that wrote through the leader and
  then reads from any node — or from a new leader after a failover,
  which holds the committed write in its log and applies it once it
  commits its no-op entry — never sees its own write missing. A
  session read never falls back to serving whatever happens to be
  applied (that fallback exists only for plain reads); if the
  watermark is not applied in time it times out and the client
  retries. A watermark past the end of the node's log, or a term
  that does not match the entry at that index, is an error, not a
  wait. The CLI exposes the same read as `get --min-index N
  --session-term T`.
- **Monotonic-reads sessions** — since v0.18.0 the session also
  remembers the highest log index it has *read*: every read reply
  carries the applied index (and term) the returned value
  reflects, and every later read carries the higher of the write
  watermark and the read watermark, so the serving node waits
  until it has applied at least that far. A client that read a
  newer state never goes backwards to an older one — not when it
  moves between nodes, not after a failover to a leader that has
  not applied that far yet. The CLI exposes it as `get
  --min-read-index N --read-term T`, which wins over
  `--min-index` when it names the newer position.
- **Chunked snapshot streaming** — since v0.19.0 InstallSnapshot
  streams the snapshot in bounded chunks (64 KiB of serialized
  JSON each by default, `RaftNode(snapshot_chunk_size=...)` to
  tune) instead of one RPC carrying the whole state machine. Each
  chunk is acked before the next is sent, so a failed chunk aborts
  the transfer instead of leaving a half-installed snapshot, and
  a transfer resumes from chunk 0 on the next replication pass.
  The follower buffers chunks keyed by (term, leader, last
  included index, chunk total) and installs only once every chunk
  has arrived, in whatever order; a chunk for a newer snapshot
  drops the buffered prefix of the older one, so a superseded
  transfer can never complete from mixed pieces, and a duplicate
  chunk (a leader retry) is acked without storing it twice. A
  message with no chunk fields is still the old whole-snapshot
  form and installs immediately. Every chunk ack extends the
  leader lease like any other follower ack.
- **Watch/subscribe** — `node.watch.watch(key, callback)` (or
  `prefix=True` for a key prefix) fires the callback with
  `{"key", "value", "op"}` whenever a *committed* log entry changes
  that key: a set fires with the new value, a delete with
  `value=None`, a successful CAS like a set, and a failed CAS fires
  nothing. Notification happens in the apply path, so it fires
  exactly once per committed change, in log order, on every node
  that applies the entry — leader and followers alike. Watches are
  local to one node and in-memory only: not replicated, not
  persisted, not rebuilt on restart; a watcher re-registers after
  reconnecting. Callbacks run synchronously on the applying thread,
  so keep them short and non-blocking. Since v0.17.0 the same
  primitive is reachable over the wire: a client sends a `watch`
  RPC (`python3 -m raftkv.client --node 127.0.0.1:5001 watch k`,
  or `Session.watch("k")`) and the node long-polls — it waits up to
  the timeout for the first committed change and replies with every
  event that arrived, each carrying the log index of the entry that
  caused it. Any node can serve a watch, leader or follower, and a
  timed-out watch leaves nothing registered behind.
- **Chaos/fault-injection harness** — `scripts/chaos.py` starts a real
  3-node cluster and, on a seeded random schedule, SIGKILLs a node
  (a real crash, no clean shutdown) and restarts it while writer
  threads keep issuing committed writes with session ids. At the end
  it verifies every acked write reads back and every node reports the
  same commit/applied index, so no acked write is lost and no node
  diverges. The same scenario runs in-process as `tests/test_chaos.py`
  (seeded, 4 kill/restart cycles, 2 writers). Same seed, same faults,
  so a failure is reproducible: `python3 scripts/chaos.py --seed 7`.
- **Linearizability checker** — `raftkv/linearizability.py` records
  the full history of concurrent operations (invocation/response
  times, client/session ids) during a chaos run — writer threads
  record their acked writes, reader threads record every read they
  observe — and verifies the history is linearizable: every read must
  return the latest write that completed before it was invoked, or
  the value of a write concurrent with it. A stale read after an
  overwrite, a read of a value from the future, or a read after a
  delete returning the old value all fail the check with the exact
  write the read should have seen. Failed CAS counts as a read of
  its reported current value; a key written by several clients falls
  back to a weaker rule (the value must have been written before the
  read finished) and is reported as such. The checker runs at the
  end of `scripts/chaos.py` and of `tests/test_chaos.py`, and
  `tests/test_linearizability.py` covers it with synthetic histories
  plus a live-cluster test.
- **CAS-heavy chaos workload** — `scripts/chaos.py` (`--cas-threads`,
  `--cas-writes`) and `tests/test_chaos.py` run CAS threads alongside
  the set writers: each owns a counter key it increments with CAS, and
  all of them contend on one shared counter they increment the same
  way (read, then CAS expected = the value just read). A lost race
  fails the CAS and the thread re-reads and retries, so the shared
  counter only ever moves forward by exactly one — the end-of-run
  check verifies it equals the number of successful shared CAS ops.
  The linearizability checker verifies the precondition of every
  successful CAS, not just what reads returned. Building this caught
  a real bug: two group-commit batches flushing concurrently could
  prune each other's CAS apply results, so a failed CAS came back
  "ok" to the client (the counter did not move, but the client was
  told it did). Fixed by pruning only each batch's own results, and
  by recording an apply result only when a local waiter is waiting
  on it. Verified with seeds 7 and 42: 276 and 610 successful CAS
  respectively, 49 and 107 failed races, zero lost, all nodes
  converged, linearizability ok.
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
python3 -m raftkv.client --node 127.0.0.1:5001 cas hello world there  # -> OK

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

133 tests, plus a smoke test for the benchmark script, all passing:

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
| `test_many_entries_replicate_with_pipelining` | A follower down for 60 writes catches up: every entry lands, in order, no duplicates |
| `test_rejected_batch_backs_off_and_retries_without_duplicates` | A new leader's first batch to a lagging follower is rejected, backs off, retries; log ends up identical, commit index at the end |
| `test_pipeline_window_bounds_in_flight_batches` | Catching up 200 entries has >1 batch in flight at once and never more than the window (10) |
| `test_pipeline_window_one_is_sequential` | With window=1 at most one batch is ever in flight and replication is still correct |
| `test_lagging_follower_catches_up_quickly` | 200 entries to a down follower replicate and apply well inside 5s |
| `test_concurrent_writes_share_one_leader_fsync` | 5 concurrent client writes cost the leader's log exactly 1 fsync, and all 5 replicate to every node |
| `test_sequential_writes_each_fsync_once` | Sequential writes each pay their own fsync — batching only merges real concurrency |
| `test_add_fourth_node_catches_up_and_serves_reads` | A 4th node added to a live cluster catches up and serves reads |
| `test_add_server_already_a_member_is_rejected` | Adding a server that is already a member is rejected |
| `test_remove_node_cluster_keeps_working` | Removing a node leaves a working 2-node cluster; the removed node stops participating |
| `test_remove_leader_triggers_new_election` | Removing the leader triggers a new election and writes continue |
| `test_add_then_remove_keeps_three_nodes_and_survives_leader_loss` | Add then remove keeps 3 nodes and survives losing the leader afterwards |
| `test_remove_unknown_server_is_rejected` | Removing a server that is not a member is rejected |
| `test_concurrent_membership_changes_rejected` | Two concurrent membership changes are rejected/serialized, and the cluster is not wedged afterwards |
| `test_membership_survives_restart` | Membership persists across restarts of every node; the 4-node cluster keeps serving |
| `test_lease_read_skips_heartbeat_round` | Five reads on a leased leader run zero confirmation rounds and return the right values |
| `test_lease_expires_without_majority_acks` | With both followers down the lease goes stale, the read falls back to a round and errors instead of serving unconfirmed |
| `test_step_down_clears_lease` | Stepping down clears the ack record, so a deposed leader holds no lease |
| `test_lease_counts_only_voting_members` | Only voting members' acks extend the lease; a non-voter's fresh ack does not help |
| `test_follower_serves_plain_read_from_lease` | A follower with a fresh leader lease serves an explicit stale read itself, no confirmation round, no log entry |
| `test_plain_read_still_redirects_from_follower` | A plain read without the stale flag still redirects to the leader, even with a fresh lease |
| `test_follower_lease_read_sees_latest_committed_value` | A follower lease read returns the latest value it has applied |
| `test_follower_lease_read_carries_applied_index` | A follower lease read names the applied index and term its value reflects |
| `test_follower_lease_expires_after_isolation` | An isolated follower stops serving within 100ms of losing contact and redirects |
| `test_session_read_still_redirects_from_follower` | A session read never takes the follower lease path; it redirects to the leader |
| `test_follower_without_leader_contact_redirects` | A node that never heard from a leader holds no lease and redirects |
| `test_retry_with_same_request_id_appends_once` | A retried write returns ok and appends no second log entry |
| `test_concurrent_duplicate_writes_append_once` | Five identical concurrent writes produce exactly one log entry |
| `test_different_request_ids_are_distinct_writes` | Different request ids are distinct writes |
| `test_retry_after_leader_change_is_deduped` | A retry after killing the leader is still deduped by the new leader |
| `test_dedup_survives_snapshot_and_restart` | Dedup survives log compaction and a leader restart via the snapshot |
| `test_delete_with_request_id_is_deduped` | Deletes dedup like sets |
| `test_cas_succeeds_on_match` | CAS with a matching expected value writes and replicates |
| `test_cas_fails_on_mismatch_value_unchanged` | CAS with a wrong expected value fails and writes nothing |
| `test_cas_fails_on_missing_key` | CAS on an absent key fails distinctly as missing_key |
| `test_cas_requires_expected_and_value` | CAS without expected or value is rejected |
| `test_cas_survives_leader_change` | CAS succeeds against the new leader after the old one dies |
| `test_cas_retry_with_same_session_id_returns_same_result` | Retried CAS returns the first attempt's result, one log entry |
| `test_cas_failed_result_survives_snapshot_and_restart` | A failed CAS result survives compaction and a leader restart |
| `test_cas_cli` | The `cas` CLI subcommand works end to end |
| `test_random_kill_restart_mid_write_loses_nothing` | Seeded chaos: 4 kill/restart cycles mid-write, every acked write survives on every node, all nodes converge to identical state, and the recorded read history is linearizable — now with CAS threads incrementing owned and shared counters through the same faults |
| `test_stale_read_after_overwrite_is_flagged` | A read invoked after an overwrite completed but returning the old value fails the linearizability check |
| `test_concurrent_read_may_see_old_or_new_value` | A read overlapping an overwrite may return either value |
| `test_read_returning_a_future_value_is_flagged` | A read returning a value written only after the read finished fails the check |
| `test_delete_then_read_returns_none` | A read after a delete returns None; returning the old value fails |
| `test_failed_cas_counts_as_a_read_of_current_value` | A failed CAS is checked like a read of its reported current value |
| `test_successful_cas_with_stale_expected_is_flagged` | A CAS that "succeeded" against a value superseded before it was invoked fails the check |
| `test_successful_cas_with_concurrent_expected_is_ok` | A CAS expecting the value of a concurrent write may have linearized either side of it |
| `test_successful_cas_with_future_expected_is_flagged` | A CAS that finished before its expected value was written fails the check |
| `test_successful_cas_on_missing_key` | A CAS on a missing key may only expect the key absent |
| `test_successful_cas_after_delete_may_expect_none` | After a delete, a CAS may expect None but not the deleted value |
| `test_failed_cas_carries_no_precondition` | A failed CAS is checked only as a read; its expected is never checked |
| `test_multi_writer_cas_expected_uses_weak_check` | A contended CAS expecting a value another writer wrote is legal; expecting a value nobody wrote is impossible |
| `test_multi_writer_key_uses_weak_check` | A key written by several clients uses the weaker rule and is reported as such |
| `test_concurrent_reads_and_writes_on_a_live_cluster` | Two writers and two readers on a real 3-node cluster: every observed read is linearizable |
| `test_write_response_carries_log_index_and_term` | A write response names its log index and term; the session watermark tracks them |
| `test_session_read_after_write_sees_write_from_any_node` | A session read from any node sees the session's own write, overwrite, and delete |
| `test_read_your_writes_across_leader_failover` | A session read right after killing the leader still sees its write, via the new leader |
| `test_session_read_with_unknown_index_errors_not_stale` | A read naming an index past the log errors instead of serving stale state |
| `test_session_read_with_wrong_term_errors` | A read naming the right index with the wrong term errors; the right term serves |
| `test_dedup_retry_returns_same_index` | A retried write reports the first attempt's index and appends no second entry |
| `test_session_get_without_writes_reads_normally` | A fresh session (watermark 0) reads like a plain ReadIndex read; CAS advances the watermark |
| `test_read_after_write_returns_the_write` | A monotonic-reads session read after its own write returns the write |
| `test_read_response_carries_applied_index_and_term` | A read reply names the applied index and term its value reflects |
| `test_session_read_advances_read_watermark` | A served read raises the session's read watermark at least as far as the write it returned |
| `test_read_never_goes_backwards_after_higher_index_read` | A read after a higher-index read returns the newer value, never the older one |
| `test_advanced_read_watermark_is_enforced_not_ignored` | An out-of-range or wrong-term read watermark errors instead of serving stale state |
| `test_session_read_on_follower_waits_for_watermark` | A session read via a follower waits for the read watermark across a leader failover |
| `test_cli_min_read_index_flags` | The `get --min-read-index/--read-term` CLI flags serve the newer value and reject an out-of-range index |
| `test_lagging_follower_catches_up_via_chunked_snapshot` | A lagging follower catches up via InstallSnapshot streamed in 64-byte chunks |
| `test_partial_transfer_installs_nothing` | A partial chunked transfer installs nothing; out-of-order and duplicate chunks are safe; a stale-term chunk is rejected; re-sending an installed snapshot is a no-op |
| `test_superseded_transfer_never_mixes_pieces` | Chunks for a newer snapshot drop the older transfer's buffered prefix; completing the old transfer afterwards cannot overwrite the new snapshot |
| `test_watch_rpc_returns_event_on_set` | A watch RPC long-polls and returns the set event with its log index |
| `test_watch_rpc_times_out_with_no_change` | A watch with no change times out with no events and leaves nothing registered |
| `test_watch_rpc_on_follower_sees_committed_write` | A watch on a follower fires when the leader's write commits and applies there |
| `test_watch_rpc_prefix_matches_only_matching_keys` | A prefix watch fires only for keys under the prefix |
| `test_watch_rpc_delete_event_has_none_value` | A delete over RPC fires with value None and op del |
| `test_watch_rpc_rejects_empty_key` | A watch with an empty or missing key is an error, not a wait |
| `test_watch_rpc_event_index_matches_log` | A watch event's index is the index of the write's own log entry |
| `test_session_watch_returns_events` | `Session.watch` returns the committed change over the wire |
| `test_watch_cli_prints_event_as_json` | The `watch` CLI subcommand prints the event as one JSON line |

Tests use real nodes (real TCP, real threads, real disk) on localhost —
no mocks of the consensus path.

## Benchmark

Measured with `scripts/bench.py` (real 3-node cluster on localhost,
200 writes, 10 threads, 32-byte values), Nov 22, 2026, on this dev VM
(three runs; the first two ran while ten sibling agents were pushing
on the same box, so concurrent-write throughput dipped — the third
run, on a quieter box, is back near the old number):

```
$ python3 scripts/bench.py
sequential writes          200 ops   ~104-111 ops/s   p50 ~8.6-9.1ms
concurrent writes x10      200 ops   ~714 ops/s       p50 ~12.4ms
sequential reads           200 ops   ~1300-3500 ops/s p50 ~0.3-0.6ms
concurrent reads x10       200 ops   ~184-190 ops/s   p50 ~2.2-4.0ms
```

This is latency-bound, not throughput-bound: every committed write
pays a full round trip to a majority plus a disk fsync, ~8-9ms each
on this VM, so a single sequential client manages ~105 writes/sec.
Group commit (v0.7.0) is what raised the concurrent number: ten
clients writing at once now share one fsync per batch instead of
one per write, so concurrent throughput went from ~360 to ~810
writes/sec. Pipelining (v0.5.0) does not raise the sequential number
— its win is elsewhere: a lagging follower catches up in a few
concurrent batches instead of one round trip per batch, and on a
real network (where RTT is not ~0.2ms) the overlapped round trips
matter. The leader lease (v0.8.0) is what raised the sequential-read
number: a read no longer pays a heartbeat round to a majority, it is
served straight from the leader's applied state in ~0.3-0.6ms, so
sequential reads went from ~250-730 to ~1300-3500 ops/s. Concurrent
reads did not move (~185-190 ops/s either way): with ten clients
hammering the leader the bottleneck is the per-connection TCP
handling, not the heartbeat round — a probe of 500 concurrent reads
showed zero confirmation rounds served from the lease, yet one read
still took ~1s in the transport layer.

## Architecture

```
raftkv/
  transport.py   Length-prefixed JSON framing, one-shot RPC, threaded TCP server
  raft.py        RaftNode: elections, log replication, commit, persistence
  store.py       KVStore: the deterministic state machine (SET/GET/DEL)
  linearizability.py  History/Op: record concurrent ops, verify linearizability
  server.py      Node CLI: --id --port --peers --data-dir
  client.py      Client CLI: set/get/del with leader-redirect following;
                 Session: read-your-writes client sessions
tests/
  harness.py     In-process 3-node cluster fixture
  test_raft.py       Election, replication, redirects, persistence
  test_integration.py  Leader-kill failover scenario
  test_readindex.py    ReadIndex reads, pending-read registry, deposed leader
  test_membership.py   Add/remove server, serialization, persistence
  test_dedup.py        Client session deduplication
  test_cas.py          Linearizable compare-and-swap
  test_linearizability.py  Linearizability checker: synthetic histories + live cluster
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
  appended in batches with one fsync per batch (group commit),
  rewritten whole after a truncation or a snapshot. Each entry
  carries its global log index, so a restart can tell a
  pre-snapshot entry from a live one.
- `snapshot.json` — `{"last_included_index": n, "last_included_term": t,
  "state": {...}}`, the state machine as of index `n`, written
  atomically (tmp file + rename) when the log is compacted or a
  snapshot is installed. It also carries the voting config and the
  client-dedup keys (with their cached results, so a retried CAS
  gets its first outcome back) covered by the compacted prefix, so
  neither is lost to compaction. The in-memory log holds only entries after
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
  Since v0.8.0 the heartbeat round itself is usually skipped: the
  leader holds a *lease* — a majority acked within the last 100ms,
  shorter than any follower's 150ms minimum election timeout, so no
  new leader can have been elected since those acks, and the read is
  served straight from applied state. A stale lease falls back to
  the round, and stepping down clears the lease, so a deposed leader
  can never serve from an old one. Since v0.16.0 a read may also
  carry a session watermark (the index of the client's last
  acknowledged write): the read index is then the max of the commit
  index and the watermark, so the read waits until the session's own
  writes are applied — across a failover as well as on the same
  leader — and a session read times out instead of falling back to
  stale applied state.
- **Commit rule.** A leader only advances `commit_index` for entries
  from its *own* term, counting replicas — never for old-term entries
  alone. Committing a current-term entry transitively commits
  everything before it. This is the subtle rule that keeps Raft safe
  across elections. A new leader therefore appends a **no-op entry in
  its own term** on election: it commits as soon as a majority acks
  it, which commits the whole prefix — without it, committed entries
  from earlier terms would sit unapplied (and invisible to reads)
  until the next client write arrived.
- **Heartbeat liveness.** The replication driver sends an empty
  AppendEntries as the heartbeat whenever the window is empty and
  there is nothing left to send — it must not depend on the peer's
  `match_index`, which starts at 0 for a fresh leader. Requiring an
  ack before the first heartbeat meant a leader elected with a
  non-empty log never sent anything at all until a write arrived, and
  the followers timed out and elected a new leader with the same
  problem, forever.
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
- **Membership changes.** Single-server changes, one at a time, no
  joint consensus — old and new majorities overlap by one server, so
  two leaders in the same term are impossible. The config lives in
  the log (and in `meta.json` / `snapshot.json`), so a restart never
  resurrects a removed server or forgets an added one. A new server
  is added to the address book as non-voting first and only joins
  the config once its config entry commits, after it has caught up.
  A removed server is dropped from the config at once (so it cannot
  vote or count toward a majority), but the leader keeps replicating
  to it until its commit index reaches the removal entry — the
  AppendEntries response carries the follower's commit index for
  exactly that check. Removing the last member and removing a server
  that is not a member are both rejected.
- **Client session deduplication.** The dedup table is derived from
  the applied log, not from leader memory: the `(client_id,
  request_id)` pair rides in the log entry and is recorded when the
  entry applies, so a new leader rebuilds the table by replay and a
  snapshot carries it — a retry after a leader change dedups exactly
  like one on the same leader. The in-flight half is the subtle part:
  a write that is appended but not yet committed registers its pair
  in `_dedup_inflight`, so a retry arriving before the first attempt
  commits joins the same waiter instead of appending a second entry.
  The inflight claim is dropped only when the waiter is woken, and by
  then the entry is committed (so the dedup table already holds it —
  apply runs before the commit notification) or the batch failed (so
  nothing was appended). A retry that arrives after a client-side
  timeout but before commit therefore still dedups, and one that
  arrives after a failed attempt appends fresh, which is correct: the
  first write never took effect.
- **Compare-and-swap.** A CAS is just a log entry whose outcome
  depends on the state at its position in the log, so it is decided
  by `KVStore.apply` — the same deterministic function every node
  runs — and it is linearizable for exactly the same reason a set
  is: the entry commits to a majority before the client hears back,
  and a read after it sees the new value (or the old one, if it
  failed). The only new machinery is returning the outcome to the
  client: the leader records each CAS entry's apply-time result by
  log index (`_apply_results`, pruned once the batch's waiters are
  woken), and the dedup table caches the full result, not just "ok",
  so a retry with the same session id gets the first attempt's exact
  outcome. Batches flush concurrently, so each batch prunes only its
  own results: pruning by index range once deleted another batch's
  unread outcome, and a failed CAS came back "ok" to the client. An
  apply result is recorded only when a local waiter is waiting on
  it, so a new leader applying the old leader's batch leaves nothing
  stranded. Snapshots carry `[client_id, request_id, result]`
  triples (old pair-only snapshots still load as ok), and
  InstallSnapshot carries the table too, so a lagging follower that
  installs a snapshot adopts the dedup results for the covered
  prefix.
- **Linearizability checker.** The checker only trusts what clients
  actually observed, so the timestamps have to mean what the check
  assumes: a retried write is recorded with its *first* attempt's
  invocation time, because the write may have committed any time
  after that — recording the retry's later invocation would make a
  read that legitimately saw the write look stale. Errored ops are
  skipped entirely: a client that heard nothing cannot contradict a
  linearization. The strict per-key rule needs each key written by
  one client in program order (true for the chaos workload, where
  each writer owns its keys); a key with several writers falls back
  to the weaker rule and is flagged in the result, so a run never
  looks stricter than it was. A successful CAS gets the same
  treatment for its precondition: its `expected` must be a value the
  key could have held inside the CAS's own interval — the latest
  completed write's value, a concurrent write's value, or `None` if
  nothing had completed — under the strict rule for single-writer
  keys and the weak rule for contended ones.
- **No dependencies.** Pure stdlib: `socket`, `threading`, `json`,
  `socketserver`. Nothing to install, nothing to break.

## Roadmap

- ~~Log compaction via snapshotting~~ — done in v0.2.0: snapshot the
  KV state + last included index/term, truncate the log, InstallSnapshot
  RPC for lagging followers, automatic compaction past 1000 entries,
  snapshot loading on restart.
- ~~Membership changes~~ — done in v0.6.0: single-server add/remove
  as log entries (one change at a time, no joint consensus), config
  persisted in the log/meta/snapshot, new servers catch up before
  voting, removed servers stop participating, a leader that removes
  itself steps down; plus the two liveness fixes the work exposed —
  a no-op entry on election so prior-term entries commit, and a
  heartbeat that does not wait for an ack first.
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
- ~~Pipelined replication~~ — done in v0.5.0: up to 10 AppendEntries
  batches in flight per follower (32 entries each), in-flight
  tracking with an epoch that invalidates stale replies, retry from
  the follower's hint on rejection, one active driver per peer, and
  a five-timeout failure budget so a dead peer fails fast.
- ~~Group commit for higher write throughput (batch the fsyncs)~~ —
  done in v0.7.0: follower-side batching (one AppendEntries batch is
  written to `log.jsonl` with a single fsync, tested: 5 entries share
  1 fsync, a conflicting batch truncates then appends in 2 fsyncs, a
  duplicate batch costs 0) and leader-side batching (concurrent
  client writes in the same 2ms window share one fsync before
  replicating; tested: 5 concurrent writes cost the leader's log
  exactly 1 fsync, sequential writes still pay one each). Concurrent
  write throughput went from ~360 to ~810 ops/s on the benchmark.
- ~~A benchmark script (`scripts/bench.py`) with concurrent clients~~ — done Nov 22, 2026: `scripts/bench.py` starts a real 3-node cluster, measures sequential/concurrent committed writes and ReadIndex reads with p50/p95/p99 latencies, and verifies a sample of written keys read back correctly.
- ~~Leader lease for reads~~ — done in v0.8.0, Nov 22, 2026: a leader
  that heard acks from a majority within 100ms (shorter than the
  150ms minimum election timeout, so no new leader can exist) serves
  reads straight from its applied state with no heartbeat round;
  stale lease falls back to the ReadIndex round, step-down clears
  the lease, only voting members' acks count. Sequential reads went
  from ~250-730 to ~1300-3500 ops/s.
- ~~Client session deduplication for at-most-once writes~~ — done in
  v0.9.0, Nov 22, 2026: a write carrying a `(client_id, request_id)`
  session id is recorded when it applies, and a retry with the same
  pair returns the cached result instead of appending a second log
  entry — after a lost response, after a leader change (the new
  leader rebuilds the table by replay), or while the first attempt
  is still in flight (the retry joins its waiter). The table is
  bounded at 10,000 entries with deterministic oldest-first eviction
  and is carried in snapshots, so compaction and restart do not lose
  it. The CLI takes `--client-id` / `--request-id` on `set`/`del`.
- ~~Linearizable compare-and-swap~~ — done in v0.10.0, Nov 22, 2026:
  `cas key expected value` is a conditional write replicated through
  the log, decided at apply time, with distinct `missing_key` and
  `mismatch` failures; the outcome is returned to the waiting client
  and cached in the dedup table (including in snapshots and across
  InstallSnapshot), so a retry with the same session id returns the
  first attempt's exact result. The CLI takes `cas` with
  `--client-id` / `--request-id` like `set`/`del`.
- ~~Watch/subscribe primitive~~ — done in v0.11.0, Nov 22, 2026:
  `WatchManager` (`raftkv/watch.py`, `node.watch`): register a
  callback on a key or key prefix, fired from the apply path with
  the new value (or `None` on delete) exactly once per committed
  change, in log order, on every node that applies the entry; a
  failed CAS fires nothing, a raising callback cannot break the
  apply path or other watchers. Watches are local and in-memory
  only for now — no replication, persistence, or watch RPC yet.
- ~~Chaos/fault-injection harness~~ — done in v0.12.0, Nov 22, 2026:
  `scripts/chaos.py` runs a real 3-node cluster through a seeded
  random SIGKILL/restart schedule while writer threads keep writing
  with session ids, then verifies every acked write reads back and
  all three nodes report the same commit/applied index; the same
  scenario runs in-process as `tests/test_chaos.py`. Verified with
  seeds 7 and 42: 283 and 199 acked writes respectively, zero lost,
  all nodes converged, including a leader kill and re-election.
- ~~Linearizability checker on the chaos harness~~ — done in v0.13.0,
  Nov 22, 2026: `raftkv/linearizability.py` records the full history
  of concurrent operations during a chaos run (writers record acked
  writes with first-attempt invocation times, readers record every
  read they observe) and verifies it is linearizable — each read must
  return the latest write completed before it was invoked, or a
  concurrent write's value; failed CAS counts as a read, multi-writer
  keys fall back to a weaker rule and are reported. Runs at the end
  of `scripts/chaos.py` and `tests/test_chaos.py`, with synthetic
  histories plus a live-cluster test in
  `tests/test_linearizability.py`.
- ~~Jepsen-style partition test on the chaos harness~~ — done in
  v0.14.0, Nov 22, 2026: `RaftNode.partition()` / `heal()` drops peer
  RPCs in both directions (outgoing sends return nothing, incoming
  peer RPCs are dropped on arrival) while client and status traffic
  still flow, so a partitioned node still answers status and still
  claims leadership — a real split, not a crash — plus a `partition`
  control RPC so the subprocess chaos harness can split a live
  cluster too. `tests/test_partition.py` proves both shapes: isolating
  the leader (the two followers elect a new leader, writes keep
  acking through it, the isolated leader's commit index does not
  move and a write sent to it directly never acks) and a 2-vs-1
  follower split (the same leader stays leader, the minority falls
  behind). After the heal every acked write reads back on every node,
  all three state machines hold exactly the acked writes, and the
  recorded history is linearizable. `scripts/chaos.py --partition
  SECONDS` runs the leader-isolation split mid-chaos-run, covered by
  the same end-of-run verification. Verified with seeds 42 and 7
  plus a 3s partition: 242 and 191 acked writes respectively, zero
  lost, all nodes converged, linearizability ok.
- ~~Linearizability checking on the CAS-heavy chaos workload~~ — done
  in v0.15.0, Nov 22, 2026: the checker now verifies the
  precondition of every successful CAS — the key must have held
  `expected` at some instant inside the CAS's own interval (the
  latest completed write's value, a concurrent write's value, or
  None if nothing had completed) — under the strict rule for
  single-writer keys and the weak rule for contended ones, with
  failed CAS ops still checked only as reads. `scripts/chaos.py`
  (`--cas-threads`, `--cas-writes`) and `tests/test_chaos.py` run
  CAS threads through the fault schedule: each owns a counter key
  it increments with CAS, and all contend on one shared counter
  (read, then CAS expected = the value just read), verified at the
  end to equal the number of successful shared CAS ops. Building
  this caught a real bug — concurrent group-commit batches pruning
  each other's CAS apply results, so a failed CAS came back "ok" —
  fixed by pruning only each batch's own results and recording an
  apply result only when a local waiter is waiting on it. Verified
  with seeds 7 and 42: 276 and 610 successful CAS respectively, 49
  and 107 failed races, zero lost, all nodes converged,
  linearizability ok.
- ~~Read-your-writes client sessions~~ — done in v0.16.0, Nov 22,
  2026: `raftkv.client.Session` carries a watermark — the log index
  and term of the client's last acknowledged write — and every read
  it issues waits until the serving node has applied that far, so a
  client never sees its own write missing, including across a
  leader failover; write responses now carry their entry's index
  and term, a dedup retry reports the first attempt's index, and a
  session read times out rather than falling back to stale applied
  state. `tests/test_session.py` covers the watermark, reads from
  any node, failover, unknown-index and wrong-term errors, and
  dedup retries reporting the same index.
- ~~Watch/subscribe over RPC~~ — done in v0.17.0, Nov 22, 2026: the
  local watch primitive is now reachable over the wire as a
  long-poll RPC — a client sends `{"type": "watch", "key": ...,
  "prefix": ..., "timeout": ...}` and the node waits up to the
  timeout for the first committed change and replies with every
  event that arrived, each carrying the log index of the entry that
  caused it, so a client re-registering after a disconnect can
  order events and spot a gap. Any node serves a watch, leader or
  follower, since the apply path fires on every node; a timed-out
  watch leaves nothing registered, and the wait holds no lock, so
  it cannot stall the apply path. `Session.watch(key, prefix,
  timeout)` and the `watch` CLI subcommand (one JSON line per
  event, `--prefix` for a key prefix) expose it;
  `tests/test_watch_rpc.py` covers set/delete/prefix watches,
  follower watches, timeouts, empty-key rejection, the event index
  matching the write's log index, and the CLI.
- ~~Monotonic-reads client sessions~~ — done in v0.18.0, Nov 23,
  2026: the session now tracks a read watermark alongside the
  write watermark — every read reply carries the applied index and
  term the returned value reflects, and every later read carries
  the higher of the two watermarks, so the serving node waits
  until it has applied at least that far and a read never goes
  backwards to older state, including across a leader failover
  or when the client moves between followers. The CLI takes `get
  --min-read-index N --read-term T`, which wins over
  `--min-index` when it names the newer position.
  `tests/test_monotonic_reads.py` covers the read watermark
  advancing, reads never going backwards after a higher-index
  read, out-of-range and wrong-term watermarks erroring instead
  of serving stale state, follower reads waiting across a
  failover, and the CLI flags.
- ~~Chunked snapshot streaming~~ — done in v0.19.0, Nov 23,
  2026: InstallSnapshot now streams the snapshot in bounded
  chunks (64 KiB of serialized JSON each by default) instead of
  one RPC carrying the whole state machine — each chunk acked
  before the next is sent, the follower buffering keyed by
  (term, leader, last included index, chunk total) and installing
  only once every chunk has arrived, so a failed or superseded
  transfer can never leave a half-installed or mixed snapshot
  behind. `tests/test_snapshot_stream.py` covers end-to-end
  catch-up through 64-byte chunks, partial transfers installing
  nothing, out-of-order and duplicate chunks, stale-term
  rejection, and a superseded transfer never mixing pieces.
- ~~Follower lease reads~~ — done in v0.20.0, Nov 23, 2026: a
  follower that heard from the current term's leader within 100ms
  serves an explicit stale read (`get --stale`, `"stale_ok":
  true`) from its applied state instead of redirecting — no
  heartbeat round, no redirect hop — with the reply carrying the
  applied index and term and a `"lease": "follower"` marker.
  Bounded staleness, not linearizability: the read reflects the
  leader's committed state as of the last heartbeat, at most ~one
  heartbeat interval plus apply lag behind in a healthy cluster,
  and a partitioned follower stops serving within 100ms and
  redirects. The flag is opt-in on purpose, so a plain read
  without it still redirects to the leader and stays
  linearizable; session reads still redirect too.
  `tests/test_follower_lease.py` covers serving a stale read from
  the lease, the latest committed value, the applied index in the
  reply, a plain read still redirecting, lease expiry after
  isolation, the session-read redirect, and a node with no leader
  contact redirecting.