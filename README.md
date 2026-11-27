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
  lone sequential write just pays the 2ms window. Since v0.26.0 the
  window and the per-batch cap are configurable
  (`RaftNode(group_commit_window_ms=..., group_commit_max_batch=...)`,
  `--group-commit-window-ms` / `--group-commit-max-batch` on the
  server CLI): a full batch flushes as soon as it fills instead of
  waiting out the window, and window 0 disables batching entirely —
  every write is its own batch and pays its own fsync, exactly as
  before group commit. The status RPC reports the config and the
  batching stats (`group_commit`: batches, entries, fsyncs,
  largest batch) so the batching ratio is visible live.
- **Thread-pool server** — since v0.28.0 one reactor thread owns
  every socket (accept, read, write) via a selector and dispatch
  runs on a bounded pool of 32 daemon workers, instead of one
  thread per connection. Idle connections hold no thread at all,
  so 50 pooled client connections cost a reactor plus a few workers
  instead of 50 handler threads contending on the GIL — that
  contention was the read ceiling (see Benchmark). Frames from one
  connection still dispatch in order, a bad frame still closes its
  connection, and idle connections are still closed after 30s; a
  long dispatch (a watch long-poll, a write waiting on
  replication) occupies one worker exactly as it occupied one
  handler thread before, so a handful of watches can never starve
  reads.
- **Persistent client connections** — since v0.27.0 the client
  reuses TCP connections instead of opening one per request: a
  `ConnectionPool` in `transport.py`, one connection checked out
  per in-flight request, configurable per session
  (`Session(pool_size=...)`, default 8; 0 means one-shot as
  before) and per benchmark run (`scripts/bench.py --pool-size`).
  Concurrent reads went from ~172 to ~3000-5200 ops/s and
  sequential reads from ~1030 to ~9400-16600 ops/s (see Benchmark);
  writes are unchanged — they are fsync-bound. A failed call is
  retried once on a fresh connection, so a stale pooled socket is
  invisible, and watches stay one-shot so a long-poll never pins
  a pooled connection.
- **Pipelined client reads** — since v0.29.0 a client can send many
  reads over one persistent connection without waiting for each
  reply (`PipelinedConnection` in `transport.py`,
  `client.get_many(keys, depth=50)`): the whole batch goes out in
  one send and the replies come back in order, so a single client
  stops paying a round-trip wait per read. The server still
  dispatches frames from one connection in order, so pipelining
  removes the client's wait, not the server's serial dispatch —
  the ceiling becomes the server's per-request processing time.
  Reads are idempotent, so a batch that hits a follower is re-sent
  once to the leader it names; a batch that fails mid-way reports
  an error in that position instead of raising. Measured on this
  box (see Benchmark): a single pipelined client reads at
  ~13800-18300 ops/s against ~3900-6000 sequential in the same
  runs — roughly 2-5x — while the sustained concurrent ceiling
  (~7000 ops/s) is unchanged, since that one is CPU-bound on the
  server, not on the client's wait. `scripts/bench.py --pipelined`
  runs the phase (`--pipeline-depth` sets the batch size).
- **Multi-get reads** — since v0.30.0 one `mget` RPC serves many
  keys in a single request (`client.mget(keys)`,
  `Session.mget(keys)`, `mget k1 k2 ...` on the CLI): one
  leadership confirmation and one apply wait for the whole batch,
  through the same ReadIndex/lease path as a single `get`, so it is
  exactly as linearizable — and a stale `mget` is served from a
  follower lease exactly like a stale `get`. The reply carries
  `values` aligned with `keys` (None for a missing key) plus the
  same `read_index` / `read_term`, so a session advances its read
  watermark from it like any other read. At most 1000 keys per
  request, so one frame can never hold an unbounded batch; a
  client that needs more splits it, like `get_many` already does.
  Measured on this box (see Benchmark): a batch of 50 keys costs
  ~0.2ms total — ~0.004-0.005ms per key, ~12x cheaper per key
  than 50 pipelined gets — because the per-request cost (dispatch,
  frame, lease check) is paid once, and each key past the first is
  one dictionary lookup. That is a per-key rate inside one batch,
  not a rate at which independent clients can read: the sustained
  concurrent ceiling is unchanged, and a batch of 10 is barely
  faster per key than pipelining, since the dispatch still
  dominates. `scripts/bench.py --mget` runs the phase
  (`--mget-batch` sets the batch size).
- **Multi-set writes** — since v0.34.0 one `mset` RPC writes many
  keys in a single request (`client.mset(pairs)`,
  `Session.mset(pairs)`, `mset k1 v1 k2 v2 ...` on the CLI): the
  whole batch appends as one log entry and applies atomically at
  that entry's position, so a read after it commits sees every
  pair, never a prefix — one dispatch, one entry, one fsync for
  the batch instead of one per key. It rides the same write path
  as a single `set`: group-commit batching, dedup by session id
  (a retry with the same id returns the first attempt's result
  instead of appending a second entry), the response's log index
  and term advancing a session's watermark, and one watch event
  per pair, in batch order, all at the batch entry's index. At
  most 1000 pairs per request, the same cap rationale as mget,
  so one frame — and one log entry — stays bounded; a duplicate
  key inside a batch keeps its last value, like successive sets.
  The honest limit is the same one group commit already has: a
  batch that shares an fsync with other concurrent writes is no
  faster to commit than a lone write, and a 1000-pair entry is a
  bigger single fsync and a bigger replication frame than a
  single set — the win is one round trip and one commit wait for
  the batch, not a cheaper write per key.
- **Pipelined batch writes** — since v0.35.0 a client can send
  many write ops over one persistent connection without waiting
  for each reply (`Session.pipeline(ops, depth=50)`,
  `client.pipeline(addr, ops)`, `pipeline` on the CLI reading
  JSON ops one per line from stdin): the ops go out in batches
  of `depth` and the replies come back aligned by position, a
  failed op an `error` response in its place instead of an
  exception. Every op carries its own session request id, so a
  batch re-sent after a redirect, or a reply lost to a dead
  connection and retried once with the same ids, returns the
  first attempt's result instead of applying twice — the dedup
  table is what makes pipelining writes safe where pipelining
  bare writes would not be. A malformed op never goes on the
  wire: its position comes back an error and the ops around it
  still run. The write watermark advances to the highest index
  any reply carries, so a later read through the same session
  is still read-your-writes. This is not `mset`: each op
  appends its own log entry and pays its own fsync, so the
  batch is not atomic — a failure part-way leaves the earlier
  ops applied — and a batch of pure sets is cheaper as one
  `mset` (one entry, one fsync, atomic). Measured on this box
  (see Benchmark): pipelined writes run at ~85-96 ops/s
  against ~84-99 sequential in the same runs — no measurable
  gain on localhost, as expected: the ~0.2ms round-trip wait
  pipelining removes is noise next to an ~8-9ms commit, and
  the server dispatches frames from one connection in order,
  so pipelined writes never overlap server-side and cannot
  share an fsync the way concurrent clients do under group
  commit. The win is one connection and no per-op round trip,
  which matters over a real network RTT, not on loopback.
  `scripts/bench.py --pipelined-writes` runs the phase
  (`--pipeline-depth` sets the batch size).
- **TTL / key expiry** — since v0.36.0 a `set` may carry
  `ttl_entries` (`Session.set(k, v, ttl_entries=N)`, `set k v
  --ttl-entries N` on the CLI, or `"ttl_entries": N` in a
  pipelined set op): the key's deadline is the log index of
  its own entry + N, and every node purges the key before
  applying the entry at that index — so the key vanishes on
  every replica at exactly the same log position, with no wall
  clock anywhere. The unit is deliberately log entries, not
  seconds: replicas can never disagree about whether a key is
  alive, which a wall-clock TTL (clocks skew, apply times
  differ) could never promise. The honest limit is the unit
  itself — on a quiet cluster a key can live arbitrarily long
  in wall-clock time, and on a busy cluster it can expire
  quickly; the TTL is a distance in the log, not a duration.
  A plain `set` without a TTL clears the key's deadline (like
  Redis, a fresh SET drops the old TTL), as do an `mset`
  without a TTL and `del`; a successful CAS keeps it, since it
  is a conditional update of the value, not a fresh set. Expiry fires a `del`
  watch event at the index of the entry whose apply purged
  the key, so a watcher sees the key vanish exactly when it
  vanishes, and the deadlines ride in snapshots, so a node
  that installs one keeps expiring keys on schedule instead
  of holding them forever. A TTL must be an integer from 1 to
  1,000,000 (`TTL_MAX_ENTRIES`); a bad value is a client
  error and appends no log entry. Since v0.37.0 a key's
  remaining TTL is readable back: `ttl key`
  (`Session.ttl(k)`, `client.ttl(addr, k)`, `ttl k` on the
  CLI) returns the entries left until the key expires, in
  Redis's convention — -2 the key does not exist, -1 it
  exists with no deadline, otherwise the entries left —
  plus `deadline`, the absolute log index at which the key
  is purged (None without one). It rides the same
  ReadIndex/lease path as a `get`, so the number reflects
  the committed state as of the read index it names — a
  session query sees its own write's deadline, a stale
  query is served from a follower lease like any stale
  read. Since v0.39.0 an `mset` may carry a TTL too
  (`Session.mset(pairs, ttl_entries=N)`,
  `client.mset(addr, pairs, ttl_entries=N)`,
  `mset k1 v1 k2 v2 ... --ttl-entries N` on the CLI): one
  deadline — this entry's index + N — for every pair in the
  batch, so the whole batch expires together at one log
  position, purged by the same path with the same `del` watch
  event per key, and the deadlines ride snapshots like any
  other. A plain `mset` without a TTL still clears every
  pair's deadline, like a plain set. Since v0.38.0 a deadline can
  also be set on a key that already exists: `expire key N`
  (`Session.expire(k, N)`, `client.expire(addr, k, N)`,
  `expire key N` on the CLI) sets the key's deadline to this
  entry's log index + N — the same log-index units, the same
  purge path, the same `del` watch event when it goes, and
  the deadline rides snapshots like any other. Unlike a
  `set` it does not touch the value; re-expiring replaces
  the deadline. A missing key is a no-op, like Redis: the
  reply answers `expire` 0 (1 when the key exists, plus the
  `deadline` itself), and the entry still commits — the
  outcome is decided at apply time, like a CAS, so a retry
  with the same session id returns the first attempt's
  result instead of appending a second entry. An `expire`
  with no count, or a count outside 1 to 1,000,000, is a
  client error and appends nothing.
- **Atomic counters** — since v0.40.0 `incr key [by]` (`Session.incr(k,
  by=1)`, `client.incr(addr, k, by)`, `incr key [by]` on the CLI)
  adds `by` (default 1, may be negative) to the key's integer value
  and returns the new value as a string, Redis INCR/INCRBY: a
  missing key counts as 0, so the first incr creates it. The outcome
  is decided at apply time, at the entry's exact position in the log,
  like a CAS — a non-integer value fails distinctly (`incr_failed`,
  reason `not_an_integer`, with the current value in the reply) from
  a result outside the signed 64-bit range (`overflow`), and a failed
  incr writes nothing but still committed its entry, so a retry with
  the same session id returns the first attempt's exact result
  instead of incrementing twice. A successful incr keeps the key's
  TTL deadline, like a CAS, and fires a `set` watch event with the
  new value; a failed one fires nothing. It rides the same write path
  as a single `set` — group-commit batching, dedup, the response's
  log index advancing a session's watermark — and `{"op": "incr",
  "key": k, "by": n}` is a pipeline op too. The honest limit is the
  range itself: values are decimal strings, so a counter past
  9,223,372,036,854,775,807 is an error, not a wrap, and there is no
  `decr` spelling — a negative `by` is the decrement.
- **Float counters** — since v0.54.0 `incrbyfloat key [by]`
  (`Session.incrbyfloat(k, by=1.0)`, `client.incrbyfloat(addr, k,
  by)`, `incrbyfloat key [by]` on the CLI) adds `by` (default
  1.0, may be negative) to the key's float value and returns the
  new value as a string, Redis INCRBYFLOAT: a missing key counts
  as 0, so the first incrbyfloat creates it. The outcome is
  decided at apply time, at the entry's exact position in the log,
  like an incr — a non-float value fails distinctly
  (`incrbyfloat_failed`, reason `not_a_float`, with the current
  value in the reply) from a result that would be NaN or Infinity
  (`overflow`), and a failed incrbyfloat writes nothing but still
  committed its entry, so a retry with the same session id returns
  the first attempt's exact result instead of adding twice. A
  successful incrbyfloat keeps the key's TTL deadline, like an
  incr, and fires a `set` watch event with the new value; a failed
  one fires nothing. It rides the same write path as a single
  `set` — group-commit batching, dedup, the response's log index
  advancing a session's watermark — and `{"op": "incrbyfloat",
  "key": k, "by": x}` is a pipeline op too. An integer string is
  a valid float, so incrbyfloat on "41" works — and the result
  ("41.5") is no longer an integer string, so a later incr on it
  fails. The honest limit is the float itself: values are decimal
  strings formatted like Redis ("2", not "2.0", for an integral
  result, full repr precision otherwise), so 0.1 + 0.2 reads
  "0.30000000000000004", exactly as it does in Redis — binary
  floating point, not decimal arithmetic.
- **Set-if-not-exists** — since v0.41.0 `setnx key value`
  (`Session.setnx(k, v)`, `client.setnx(addr, k, v)`, `setnx key
  value` on the CLI) sets the key only when it is absent and
  answers 1 when it set it, 0 when the key already existed — and
  the existing value is left untouched, Redis SETNX. The outcome
  is decided at apply time, at the entry's exact position in the
  log, like a CAS: two concurrent setnx on the same key race in
  log order and exactly one wins, so it is a usable lock-acquire
  primitive, and a retry with the same session id returns the
  first attempt's result instead of setting twice. A successful
  setnx fires a `set` watch event with the value it wrote; a
  losing one fires nothing, like a failed CAS. It rides the same
  write path as a single `set` — group-commit batching, dedup,
  the response's log index advancing a session's watermark — and
  `{"op": "setnx", "key": k, "value": v}` is a pipeline op too.
  A setnx with no value is a client error and appends nothing.
  The honest limit is the same one every single-key write has:
  it is not a lock with an expiry — a key set by setnx lives
  until it is deleted or overwritten, so a holder that crashes
  without releasing leaves the key held; pair it with `expire`
  for a lease, or use it only where a permanent claim is what
  you want.
- **Get-and-set** — since v0.42.0 `getset key value`
  (`Session.getset(k, v)`, `client.getset(addr, k, v)`, `getset key
  value` on the CLI) atomically sets the key and answers with the
  value it held before — `old_value`, None when the key did not
  exist, which the getset creates — Redis GETSET. The old value is
  decided at apply time, at the entry's exact position in the log,
  like a CAS: a retry with the same session id returns the first
  attempt's old value instead of setting twice, and a second getset
  sees the first one's value as the old value — the atomic swap
  Redis promises. A getset always writes, so it always fires a
  `set` watch event with the new value, and it keeps the key's TTL
  deadline, like a CAS. It rides the same write path as a single
  `set` — group-commit batching, dedup, the response's log index
  advancing a session's watermark — and `{"op": "getset", "key": k,
  "value": v}` is a pipeline op too. A getset with no value is a
  client error and appends nothing. The honest limit is the same
  one every single-key read-modify-write has: the old value it
  returns is the value at its log position, so a concurrent write
  that commits first is the old value it sees — there is no way to
  ask for the value "as of now" without going through the log.
- **Append** — since v0.43.0 `append key value` (`Session.append(k, v)`,
  `client.append(addr, k, v)`, `append key value` on the CLI) appends to
  the string at the key and answers with the new length — Redis APPEND.
  A missing key is created holding just the value, and the new length is
  decided at apply time, at the entry's exact position in the log, like
  a CAS: a retry with the same session id returns the first attempt's
  length instead of appending twice. A successful append fires a `set`
  watch event with the new value and keeps the key's TTL deadline, like
  a CAS. A non-string value fails with `append_failed` and reason
  `wrongtype` instead of appending — every value this store holds is a
  string, so that branch is defensive, reached only by a hand-built
  state. It rides the same write path as a single `set` — group-commit
  batching, dedup, the response's log index advancing a session's
  watermark — and `{"op": "append", "key": k, "value": v}` is a pipeline
  op too. An append with no value is a client error and appends nothing.
- **String length** — since v0.43.0 `strlen key` (`Session.strlen(k)`,
  `client.strlen(addr, k)`, `strlen key` on the CLI) prints the length of
  the string at the key, 0 when the key does not exist — Redis STRLEN.
  It rides the same ReadIndex path as a `get`, so it carries the same
  session watermark and is exactly as linearizable. A non-string value
  is a client error, not a length — defensive, like append's wrongtype,
  since every value this store holds is a string.
- **Get-and-delete** — since v0.43.0 `getdel key` (`Session.getdel(k)`,
  `client.getdel(addr, k)`, `getdel key` on the CLI) atomically answers
  with the value the key held and deletes it — Redis GETDEL — answering
  `value` None and `getdel` 0 when the key did not exist, which is a
  no-op, not an error. The value is decided at apply time, at the
  entry's exact position in the log, like a CAS: a retry with the same
  session id returns the first attempt's value instead of deleting a key
  a later write recreated. A getdel that found its key fires a `del`
  watch event, and the key takes its TTL deadline with it when it goes.
  It rides the same write path as a single `set` — group-commit
  batching, dedup, the response's log index advancing a session's
  watermark — and `{"op": "getdel", "key": k}` is a pipeline op too.
- **Substring read** — since v0.44.0 `getrange key start end`
  (`Session.getrange(k, start, end)`, `client.getrange(addr, k,
  start, end)`, `getrange key start end` on the CLI) answers the
  substring of the string at the key from `start` to `end`
  inclusive — Redis GETRANGE. A negative index counts back from
  the end of the string (-1 is the last character), an index past
  either end is clamped to the string, and a range with start past
  end is empty rather than an error; a missing key holds the empty
  string, so any range of it is "". It rides the same ReadIndex
  path as a `get`, so it carries the same session watermark and is
  exactly as linearizable. A non-string value is a client error,
  not a substring — defensive, like strlen's wrongtype, since
  every value this store holds is a string.
- **Substring overwrite** — since v0.44.0 `setrange key offset
  value` (`Session.setrange(k, offset, v)`, `client.setrange(addr,
  k, offset, v)`, `setrange key offset value` on the CLI) overwrites
  the string at the key starting at `offset` and answers with the
  new value and its length — Redis SETRANGE. A missing key counts
  as an empty string, and when the offset lies past the end the gap
  is padded with zero bytes (`\x00`), exactly as Redis pads with
  zero bytes. The outcome is decided at apply time, at the entry's
  exact position in the log, like a CAS: a retry with the same
  session id returns the first attempt's value and length instead
  of overwriting twice. A successful setrange fires a `set` watch
  event with the new value and keeps the key's TTL deadline, like
  a CAS. A non-string value fails with `setrange_failed` and reason
  `wrongtype` instead of overwriting. It rides the same write path
  as a single `set` — group-commit batching, dedup, the response's
  log index advancing a session's watermark — and `{"op":
  "setrange", "key": k, "offset": n, "value": v}` is a pipeline op
  too. An offset must be an integer from 0 to 1,000,000
  (`SETRANGE_MAX_OFFSET`): a negative offset is a client error and
  appends nothing, and the cap keeps one entry from growing a value
  without limit — the same scale as the TTL cap, for the same
  reason. The honest limit is the padding itself: a setrange past
  the end grows the value by the whole gap in one entry, so a
  large offset is a large value on every replica, not just on the
  leader — the cap is what keeps that bounded.
- **Get-and-expire** — since v0.45.0 `getex key [EX n | PX n |
  EXAT t | PXAT t | PERSIST]` (`Session.getex(k, ex=n, ...)`,
  `client.getex(addr, k, ...)`, `getex key --ex n` on the CLI)
  answers with the value the key held and optionally sets,
  refreshes, or clears its expiry in the same atomic operation —
  Redis GETEX. A missing key answers None with `getex` 0, a
  no-op, not an error. EX / PX name a relative deadline (this
  entry's index + N, in the store's log-entry units, like
  `ttl_entries` — this store has no wall clock, so the Redis
  seconds / milliseconds are read as log entries); EXAT / PXAT
  name the absolute log index of the deadline; PERSIST clears
  it; no option leaves the value and the deadline both alone.
  The outcome is decided at apply time, at the entry's exact
  position in the log, like a CAS: a retry with the same session
  id returns the first attempt's value and deadline instead of
  re-expiring, and the deadline rides snapshots like any other.
  A GETEX never changes the value, so a pure expiry change fires
  no watch event — only the purge fires a `del` event, when the
  deadline entry lands, like an expire. The one exception is an
  absolute deadline already past at apply time: Redis deletes the
  key there, so this does too — the reply is None with `getex` 0
  and the apply path fires the `del` event for it. At most one
  option: two options, an unknown option, or a bad value is a
  client error and appends nothing. It rides the same write path
  as a single `set` — group-commit batching, dedup, the
  response's log index advancing a session's watermark — and
  `{"op": "getex", "key": k, "ex": n}` is a pipeline op too.
  The honest limit is the unit itself: the options spell Redis
  times, but the deadlines they set are distances and positions
  in the log, not seconds — a key expired with EX 10 lives for
  10 further log entries, however long those take in wall-clock
  time.
- **Wall-clock TTL** — since v0.48.0 `setex key value seconds`
  (`Session.setex(k, v, seconds)`, `client.setex(addr, k, v,
  seconds)`, `setex key value seconds` on the CLI) writes the key
  with a wall-clock TTL — Redis SETEX. The leader fixes the
  absolute deadline (its wall clock at log time + seconds) in the
  log entry itself, so every replica applies the same deadline,
  and the key dies when the wall clock reaches it — enforced by
  a background sweep every 100ms (so a key dies on a quiet
  cluster too, with no log advance at all), by a purge before
  each log entry applies, and by a purge before serving a read,
  so a read never returns a key whose deadline passed between
  sweeps. A plain `set` clears a wall deadline and a `setex`
  clears a log deadline, so a key holds at most one kind of TTL;
  a successful CAS or incr keeps the wall deadline, like it keeps
  the log one. The deadline rides snapshots like any other, so a
  node that installs one keeps expiring the key on schedule, and
  expiry fires a `del` watch event like a log-entry expiry. The
  `ttl` query reports the wall side too — `wall_ttl` the whole
  seconds left (-2 the key does not exist, -1 it exists with no
  wall deadline) and `wall_deadline` the absolute Unix time, or
  None without one. It rides the same write path as a single
  `set` — group-commit batching, dedup, the response's log index
  advancing a session's watermark — and `{"op": "setex", "key":
  k, "value": v, "seconds": n}` is a pipeline op too. Seconds
  must be an integer from 1 to 1,000,000
  (`WALL_TTL_MAX_SECONDS`); a bad value is a client error and
  appends no log entry. The honest limit is the clock itself:
  the deadline is identical on every node, but each node
  enforces it against its own clock, so two nodes may disagree
  briefly about whether a key is alive — which is why this is
  opt-in, and the log-entry TTL stays the default.
- **Absolute wall-clock expiry** — since v0.49.0 `expireat key
  timestamp` (`Session.expireat(k, ts)`,
  `client.expireat(addr, k, ts)`, `expireat key timestamp` on the
  CLI) and `pexpireat key timestamp_ms` (`Session.pexpireat(k,
  ts_ms)`, `client.pexpireat(addr, k, ts_ms)`, `pexpireat key
  timestamp_ms` on the CLI) set an absolute wall-clock deadline
  on a key that already exists — Redis EXPIREAT / PEXPIREAT, the
  wall-clock analogue of `expire`: the value is untouched, only
  the deadline changes, and a log-entry deadline the key had is
  dropped, so a key holds at most one kind of TTL. They answer 1
  when the key exists (the reply also carries `wall_deadline`,
  the absolute Unix time in seconds) and 0 when it does not — a
  missing key is a no-op, not an error — and a deadline already
  past deletes the key outright, still answering 1 with
  `deleted` set, and firing a `del` watch event like a `getex`
  whose absolute deadline was already past. The deadline is
  fixed by the leader at log time, so every replica applies the
  same deadline and it rides snapshots like a `setex` deadline;
  the key then dies by the same three enforcements — the
  background sweep every 100ms, the purge before each log entry
  applies, and the purge before serving a read. The timestamp
  must be a positive integer — Unix seconds for `expireat`,
  Unix milliseconds for `pexpireat` (the leader divides by 1000
  before the entry is logged) — and a bad value is a client
  error and appends no log entry. It rides the same write path
  as an `expire` — group-commit batching, dedup, the response's
  log index advancing a session's watermark — and `{"op":
  "expireat", "key": k, "timestamp": t}` /
  `{"op": "pexpireat", "key": k, "timestamp_ms": t}` are
  pipeline ops too. The `ttl` query reports the deadline they
  set, in `wall_ttl` / `wall_deadline`, exactly like a `setex`
  deadline. The honest limit is the same clock as `setex`: the
  deadline is identical on every node, but each node enforces
  it against its own clock.
- **SET NX / XX / KEEPTTL** — since v0.50.0 `set key value
  [--nx | --xx] [--keepttl]` (`Session.set(k, v, nx=True, ...)`,
  `set key value --nx` on the CLI) carries Redis's SET options:
  NX sets only when the key does not exist, XX only when it
  does, and KEEPTTL preserves the deadline a plain set would
  clear — log-entry or wall-clock — instead of dropping it. The
  reply carries `set` 1 when the key was written, 0 when the
  condition failed and nothing changed, so a plain set answers
  `set` 1 too. The outcome is decided at apply time, at the
  entry's exact position in the log, like a CAS: two concurrent
  conditional sets race in log order and exactly one wins, and
  a retry with the same session id returns the first attempt's
  result instead of setting twice. A losing set writes nothing
  at all — the value and any deadline stay exactly as they
  were — and fires no watch event; a winning one fires a `set`
  event with the new value, like a plain set. NX and XX
  together, or KEEPTTL with a fresh TTL, is a client error and
  appends no log entry. It rides the same write path as a
  single `set` — group-commit batching, dedup, the response's
  log index advancing a session's watermark — and `{"op":
  "set", "key": k, "value": v, "nx": true}` is a pipeline op
  too. The honest limit is the same one every single-key
  conditional write has: NX is a claim, not a lease — a key
  claimed by NX lives until it is deleted or overwritten, so
  pair it with a TTL for a lock with an expiry.
- **SET with GET** — since v0.51.0 `set key value [--get]`
  (`Session.set(k, v, get=True)`, `set key value --get` on the
  CLI) carries Redis's SET GET option: one round trip both
  swaps the value and reports what it replaced — the reply
  carries `old_value`, the value the key held at the entry's
  exact position in the log, or None when the key was absent.
  A losing conditional set answers `old_value` None, like
  Redis answering nil when the condition fails, and writes
  nothing at all — value and deadline untouched, no watch
  event — while a winning one fires a `set` event with the new
  value, like a plain set. GET changes nothing about the
  write itself: a winning GET set clears the deadline like a
  plain set, or keeps it with KEEPTTL, and it composes with
  NX / XX and with a fresh TTL exactly like the other flags.
  The old value rides the same write path as a single `set` —
  group-commit batching, dedup (a retry with the same session
  id returns the first attempt's old value instead of setting
  twice), the response's log index advancing a session's
  watermark — and `{"op": "set", "key": k, "value": v, "get":
  true}` is a pipeline op too. The honest limit is the same
  one getset already has: the old value is whatever the key
  held when the entry applied, so a concurrent write that
  lands first in the log is the old value you get back.
- **SET with EX / PX** — since v0.52.0 `set key value [--ex
  seconds | --px milliseconds]` (`Session.set(k, v, ex=n,
  px=n)`, `set key value --ex n` / `--px n` on the CLI)
  carries Redis's SET EX / PX options: an inline wall-clock
  TTL on SET, instead of the separate `setex` op — the leader
  fixes the absolute deadline at log time (now + seconds, now
  + milliseconds / 1000), so every replica applies the same
  deadline and the key dies when the wall clock reaches it,
  enforced by the same three paths as a `setex` deadline: the
  background sweep every 100ms, the purge before each log
  entry applies, and the purge before serving a read. A
  winning EX/PX set is a fresh life for the key: any
  log-entry deadline it had is dropped, so a key still holds
  at most one kind of TTL, and a plain `set` clears an EX/PX
  deadline like any other wall deadline. EX and PX are
  mutually exclusive, and neither rides with a log-entry TTL
  or with KEEPTTL — Redis rejects all three combinations, and
  the server rejects them as client errors that append no log
  entry. It composes with NX / XX and with GET exactly like
  the other flags: a losing conditional set writes nothing,
  deadline included, and answers `old_value` None with GET.
  The `ttl` query reports the deadline it sets, in `wall_ttl`
  / `wall_deadline`, exactly like a `setex` deadline. It rides
  the same write path as a single `set` — group-commit
  batching, dedup (a retry with the same session id returns
  the first attempt's result instead of setting twice, and the
  deadline stays the first attempt's), the response's log index
  advancing a session's watermark — and `{"op": "set", "key":
  k, "value": v, "ex": n}` is a pipeline op too. The honest
  limit is the same clock as `setex`: the deadline is
  identical on every node, but each node enforces it against
  its own clock.
- **SET with EXAT / PXAT** — since v0.53.0 `set key value
  [--exat timestamp | --pxat timestamp_ms]` (`Session.set(k,
  v, exat=t, pxat=t)`, `set key value --exat t` / `--pxat t`
  on the CLI) carries Redis's SET EXAT / PXAT options: an
  absolute wall-clock deadline inline on SET, the absolute
  analogue of EX / PX — EXAT names the deadline as Unix
  seconds, PXAT as Unix milliseconds, and the timestamp
  itself is the deadline, recorded as-is by the leader, so
  every replica applies the same one and the key dies when
  the wall clock reaches it, enforced by the same three paths
  as a `setex` deadline: the background sweep every 100ms,
  the purge before each log entry applies, and the purge
  before serving a read. A winning EXAT/PXAT set is a fresh
  life for the key: any log-entry deadline it had is dropped,
  so a key still holds at most one kind of TTL, and a plain
  `set` clears an EXAT/PXAT deadline like any other wall
  deadline. EXAT and PXAT are mutually exclusive, and neither
  rides with EX or PX, with a log-entry TTL, or with KEEPTTL
  — Redis rejects all of those combinations, and the server
  rejects them as client errors that append no log entry. A
  timestamp must be a positive integer — the same validation
  `expireat` / `pexpireat` use for the same absolute
  deadline. A deadline already past at log time deletes the
  key outright at apply time instead of recording a deadline
  nobody could honour, like an `expireat` whose deadline was
  already past: the set still answers `set` 1, with
  `deleted` set when a key actually went, and the apply path
  fires a `del` watch event for it — a past deadline on a
  missing key writes nothing and fires nothing, and a losing
  conditional set deletes nothing either. It composes with
  NX / XX and with GET exactly like the other flags: a losing
  conditional set writes nothing, deadline included, and
  answers `old_value` None with GET, while a winning GET set
  with a past deadline reports the old value it replaced.
  The `ttl` query reports the deadline it sets, in `wall_ttl`
  / `wall_deadline`, exactly like a `setex` deadline. It rides
  the same write path as a single `set` — group-commit
  batching, dedup (a retry with the same session id returns
  the first attempt's result instead of setting twice, and the
  deadline stays the first attempt's), the response's log index
  advancing a session's watermark — and `{"op": "set", "key":
  k, "value": v, "exat": t}` is a pipeline op too. The honest
  limit is the same clock as `setex`: the deadline is
  identical on every node, but each node enforces it against
  its own clock.
- **Bit operations** — since v0.46.0 `setbit key offset bit`
  (`Session.setbit(k, offset, bit)`, `client.setbit(addr, k,
  offset, bit)`, `setbit key offset bit` on the CLI) sets one
  bit in the string at the key to 0 or 1 and answers with the
  old bit value (`old_bit`) and the new value, and `getbit key
  offset` (`Session.getbit(k, offset)`, `client.getbit(addr, k,
  offset)`, `getbit key offset` on the CLI) answers the bit at
  the offset — Redis SETBIT / GETBIT. Bit 0 is the most
  significant bit of the first byte, bit 7 the least
  significant, bit 8 the most significant bit of the second
  byte, and so on. A missing key counts as an empty string, so
  a setbit creates it and a getbit on it answers 0; when the
  offset lies past the end the gap is padded with zero bytes,
  and a getbit past the end answers 0. The outcome is decided
  at apply time, at the entry's exact position in the log, like
  a CAS: a retry with the same session id returns the first
  attempt's old bit instead of setting twice. A successful
  setbit fires a `set` watch event with the new value and keeps
  the key's TTL deadline, like a CAS. A value that is not a byte
  string fails with `setbit_failed` and reason `wrongtype`
  instead of writing — a bit op reads the value as bytes, one
  byte per character (latin-1), so a value holding a character
  above U+00FF has no byte form to set a bit in. A getbit on
  such a value is a client error, not a bit. The offset must be
  an integer from 0 to 1,000,000 (`SETBIT_MAX_OFFSET`): a
  negative offset or a bit other than 0 or 1 is a client error
  and appends nothing, and the cap keeps one entry from growing
  a value without limit — the same scale as the setrange cap,
  for the same reason. A getbit rides the same ReadIndex path as
  a `get`, so it carries the same session watermark and is
  exactly as linearizable. `{"op": "setbit", "key": k,
  "offset": n, "bit": b}` is a pipeline op too.
  The honest limit is the encoding itself: the store's values
  are strings, so a bit op treats each character as one byte —
  a value written as multi-byte UTF-8 text is read as its
  latin-1 characters, not its UTF-8 bytes, and a character
  above U+00FF is not a byte string at all.
- **Bitwise combination** — since v0.60.0 `bitop operation
  destkey key [key ...]` (`Session.bitop(dest, operation, keys)`,
  `client.bitop(addr, dest, operation, keys)`, `bitop AND dest
  k1 k2 ...` on the CLI) combines the string values at the source
  keys bitwise — AND, OR, XOR, or NOT (NOT takes exactly one
  source key) — stores the result at the destination, and answers
  with the result length (`bitop`) and the result value itself —
  Redis BITOP. The result is as long as the longest input, missing
  keys count as empty (all-zero) strings, and a shorter input is
  padded with zero bytes, so AND zeroes a missing tail while OR /
  XOR keep the long key's tail whole. When every input is missing
  or empty the result is the empty string: the destination is
  deleted instead of storing an empty value, answering `bitop` 0
  with `deleted` set when a key actually went. A bitop is a fresh
  set of its destination, so it clears the destination's TTL
  deadline, like a plain set — unlike a setbit, which keeps it —
  and it fires a `set` watch event with the result, or a `del`
  event when the empty result deletes the destination. The outcome
  is decided at apply time, at the entry's exact position in the
  log, like a CAS: a retry with the same session id returns the
  first attempt's length instead of combining twice. A source value
  that is not a byte string fails with `bitop_failed` and reason
  `wrongtype` instead of writing, and a bad operation, an empty
  key list, or more than 1000 keys (`BITOP_MAX_KEYS`) is a client
  error and appends nothing — NOT with two keys is one too. The
  operation is matched case-insensitively and normalized to upper
  case before it is logged, so every node applies the same
  spelling. `{"op": "bitop", "key": d, "operation": "AND", "keys":
  [k1, k2, ...]}` is a pipeline op too. The result rides in the
  store as a plain string, so it survives snapshots and replay
  exactly like any other value. The honest limit is the same
  encoding as setbit: a value holding a character above U+00FF
  has no byte form to combine.
- **Bit counting** — since v0.61.0 `bitcount key [start end
  [BYTE|BIT]]` (`Session.bitcount(k, start, end, mode)`,
  `client.bitcount(addr, k, start, end, mode)`, `bitcount key
  [start end] [--mode BYTE|BIT]` on the CLI) answers how many
  bits are set to 1 in the string at the key, 0 when the key
  does not exist — Redis BITCOUNT. With no range the whole value
  is counted; with `start` and `end` only that slice is — bytes
  in BYTE mode (the default), bits in BIT mode, bit 0 the most
  significant bit of the first byte, exactly like getbit's
  numbering. A negative bound counts back from the end, an
  out-of-range bound is clamped, like a getrange, so a range
  with start past end counts 0 rather than raising. It is a pure
  read — no log entry, no state change, no watch event — and it
  rides the same ReadIndex path as a `get`, so it carries the
  same session watermark and is exactly as linearizable, and a
  stale bitcount is served from a follower lease exactly like a
  stale `get`. A value that is not a byte string is a client
  error, not a count, like a getbit on it; `start` and `end`
  come as a pair, and a mode with no range is a client error,
  and the mode is matched case-insensitively and normalized to
  upper case before it is served, so every node counts the same
  range. Like getbit and strlen it is a read, so it is not a
  pipeline op. The honest limit is the same encoding as setbit:
  a value holding a character above U+00FF has no byte form to
  count.
- **Geo** — since v0.62.0 `geoadd key lon lat member [lon lat
  member ...]` (`Session.geoadd(k, members)`,
  `client.geoadd(addr, k, members)`, `geoadd key lon lat member
  ...` on the CLI) adds members with positions to the geo set at
  the key and answers with the number of members that did not
  exist before — Redis GEOADD — and `geodist key member1 member2
  [unit]` (`Session.geodist(k, m1, m2, unit)`,
  `client.geodist(addr, k, m1, m2, unit)`, `geodist key m1 m2
  [--unit m|km|mi|ft]` on the CLI) answers the distance between
  two members — Redis GEODIST — in metres by default, or km, mi,
  or ft. Longitude must be in -180..180 and latitude in
  -85.05112878..85.05112878, Redis's ranges; a member whose
  position is updated is not counted, and a member repeated in
  one call counts once, at its last position. A missing key is
  created as an empty geo set, and a geodist on a missing key or
  between a member and one that is not in the set answers None,
  like a get on a missing key. The members ride in the store as
  a JSON blob with a marker prefix, like a hash — the JSON maps
  member names to [lon, lat] pairs, dumped with sorted keys and
  no spaces, so the encoding is a pure function of the members
  and every node applying the same log computes the same bytes —
  and the distance is the haversine over the stored coordinates
  divided by the unit's metres, so every unit agrees on the same
  underlying distance. The outcome is decided at apply time, at
  the entry's exact position in the log, like a CAS: a retry with
  the same session id returns the first attempt's count instead
  of adding twice. A key holding a plain string fails a geoadd
  with `geoadd_failed` and reason `wrongtype` instead of writing,
  and a geodist on one is a client error, not a distance. At most
  1000 members per geoadd, like a pfadd batch, so one entry never
  carries an unbounded list. A successful geoadd keeps the key's
  TTL deadline, like an hset, and fires a `set` watch event with
  the new value; a failed one fires nothing. A geodist rides the
  same ReadIndex path as a `get`, so it carries the same session
  watermark and is exactly as linearizable, and a stale geodist
  is served from a follower lease exactly like a stale `get`.
  `{"op": "geoadd", "key": k, "members": [[lon, lat, m], ...]}`
  is a pipeline op too; like hget and hgetall a geodist is a
  read, so it is not one. The honest limit is the encoding
  itself: positions are stored as plain longitude/latitude pairs,
  not as Redis's 52-bit geohash scores in a sorted set, so there
  is no radius search yet — GEORADIUS / GEOSEARCH would need the
  geohash ordering — and a distance computed here is the exact
  haversine, which can differ from Redis's geohash-quantized
  answer by a fraction of a metre (Palermo to Catania: 166274.26
  m here, 166274.15 m in Redis's documentation).
- **HyperLogLog** — since v0.47.0 `pfadd key element [element ...]`
  (`Session.pfadd(k, *elements)`, `client.pfadd(addr, k, *elements)`,
  `pfadd key element ...` on the CLI) adds elements to the
  HyperLogLog at the key and answers `pfadd` 1 when at least one
  register rose — the HLL was modified — and 0 when every element
  was already counted, plus `estimate`, the cardinality estimate
  after the add; and `pfcount key` (`Session.pfcount(k)`,
  `client.pfcount(addr, k)`, `pfcount key` on the CLI) answers the
  estimate, 0 when the key does not exist — Redis PFADD / PFCOUNT.
  A missing key is created as an empty HLL. The registers are
  16384 one-byte registers (precision 14, Redis's default), hashed
  with SHA-256 of the element's UTF-8 bytes — deterministic across
  processes and nodes, so every node applying the same log computes
  the same bytes — and the estimate is the standard HyperLogLog one
  with the small-range (linear counting) and large-range
  corrections, good to under 1% once the cardinality is past a few
  thousand. The outcome is decided at apply time, at the entry's
  exact position in the log, like a CAS: a retry with the same
  session id returns the first attempt's pfadd flag and estimate
  instead of adding twice. A successful pfadd fires a `set` watch
  event with the new value — but only when a register rose: a
  pfadd that changed nothing fires nothing, like a failed setnx —
  and keeps the key's TTL deadline, like a CAS. A plain string
  value fails a pfadd with `pfadd_failed` and reason `wrongtype`
  instead of writing, and a pfcount on one is a client error, not
  an estimate. At most 1000 elements per pfadd, like an mget batch,
  so one entry never carries an unbounded list. A pfcount rides
  the same ReadIndex path as a `get`, so it carries the same
  session watermark and is exactly as linearizable.
  `{"op": "pfadd", "key": k, "elements": [...]}` is a pipeline op
  too. The honest limit is the precision itself: the estimate is
  probabilistic — it can be off by a few percent at small counts
  and by ~0.8% (one standard error) at large ones — and each HLL
  key costs ~16 KiB of registers however few elements it holds, so
  an HLL is the wrong shape for counting ten things exactly.
- **Hashes** — since v0.55.0 `hset key field value [field value ...]`
  (`Session.hset(k, fields)`, `client.hset(addr, k, fields)`,
  `hset key f1 v1 f2 v2 ...` on the CLI) sets fields in the hash
  at the key and answers with the number of fields that did not
  exist before — Redis HSET — and `hget key field`
  (`Session.hget(k, field)`, `client.hget(addr, k, field)`,
  `hget key field` on the CLI) answers the value of one field,
  None when the key does not exist or the field is not set —
  Redis HGET. A missing key is created as an empty hash, and a
  field overwritten with a new value is not counted, so the count
  is the number of fields the call added; a field repeated in one
  call counts once, at its last value. The fields ride in the
  store as a JSON object with a marker prefix, like the HLL
  blob — the JSON is dumped with sorted keys and no spaces, so
  the encoding is a pure function of the fields and every node
  applying the same log computes the same bytes — and the hash
  survives snapshots and replay exactly like any other value. The
  outcome is decided at apply time, at the entry's exact position
  in the log, like a CAS: a retry with the same session id returns
  the first attempt's count instead of setting twice. A key
  holding a plain string fails an hset with `hset_failed` and
  reason `wrongtype` instead of writing, and an hget on one is a
  client error, not a field. A successful hset keeps the key's
  TTL deadline, like a CAS, and fires a `set` watch event with the
  new value; a failed one fires nothing. An hget rides the same
  ReadIndex path as a `get`, so it carries the same session
  watermark and is exactly as linearizable. At most 1000 pairs
  per hset, like a pfadd batch, so one entry never carries an
  unbounded list. `{"op": "hset", "key": k, "pairs": [[f, v],
  ...]}` is a pipeline op too. The honest limit is the encoding
  itself: a plain `get` on a hash key returns the encoded blob,
  not a field, and the string ops see the blob as a plain string,
  exactly as they see an HLL blob.
- **Hash field removal and listing** — since v0.56.0 `hdel key
  field [field ...]` (`Session.hdel(k, *fields)`,
  `client.hdel(addr, k, fields)`, `hdel key f1 f2 ...` on the
  CLI) deletes fields from the hash at the key and answers with
  the number of fields actually removed — Redis HDEL — and
  `hgetall key` (`Session.hgetall(k)`, `client.hgetall(addr, k)`,
  `hgetall key` on the CLI) answers the whole field map as one
  JSON object, an empty dict when the key does not exist — Redis
  HGETALL. A missing key or a missing field counts 0 on an hdel,
  not an error, and a field repeated in one call counts once;
  removing the last field deletes the key outright, like Redis —
  the key leaves the sorted index and takes its deadline with
  it. The outcome is decided at apply time, at the entry's exact
  position in the log, like a CAS: a retry with the same session
  id returns the first attempt's count instead of deleting
  twice. A key holding a plain string fails an hdel with
  `hdel_failed` and reason `wrongtype` instead of writing, and
  an hgetall on one is a client error, not a field map. An hdel
  that leaves fields behind keeps the key's TTL deadline, like
  an hset, and fires a `set` watch event with the new value;
  an hdel that takes the last field fires a `del` event instead,
  like a getdel; an hdel that removes nothing fires nothing.
  An hgetall rides the same ReadIndex path as a `get`, so it
  carries the same session watermark and is exactly as
  linearizable. At most 1000 fields per hdel, like an hset
  batch, so one entry never carries an unbounded list.
  `{"op": "hdel", "key": k, "fields": [f, ...]}` is a pipeline
  op too. The honest limit is the same encoding as hset: a plain
  `get` on a hash key still returns the encoded blob.
- **Hash length and field presence** — since v0.57.0 `hlen key`
  (`Session.hlen(k)`, `client.hlen(addr, k)`, `hlen key` on the
  CLI) answers the number of fields in the hash at the key, 0
  when the key does not exist — Redis HLEN — and `hexists key
  field` (`Session.hexists(k, field)`, `client.hexists(addr, k,
  field)`, `hexists key field` on the CLI) answers 1 when the
  field is set, 0 when the key does not exist or the field is
  not set — Redis HEXISTS. Both are pure reads: no log entry,
  no state change, no watch event, and the TTL deadline is
  untouched — a hash's size and one field's presence are read
  off the applied state machine exactly like an hget. A key
  holding a plain string makes both a client error, not a count
  or a 0, like an hgetall on a plain string. Both ride the same
  ReadIndex path as a `get`, so they carry the same session
  watermark and are exactly as linearizable, and they survive
  snapshots and replay exactly like any other hash read — the
  blob is a plain string in the store, so nothing about the
  encoding changes. The honest limit is the same encoding as
  hset: a plain `get` on a hash key still returns the encoded
  blob, and there is still no way to list field names without
  their values — that is what hgetall is for.
- **Hash field and value listings** — since v0.58.0 `hkeys key`
  (`Session.hkeys(k)`, `client.hkeys(addr, k)`, `hkeys key` on
  the CLI) answers every field name in the hash at the key,
  sorted, an empty list when the key does not exist — Redis
  HKEYS — `hvals key` (`Session.hvals(k)`,
  `client.hvals(addr, k)`, `hvals key` on the CLI) answers
  every field value, in sorted field order, an empty list when
  the key does not exist — Redis HVALS — and `hmget key field
  [field ...]` (`Session.hmget(k, fields)`,
  `client.hmget(addr, k, fields)`, `hmget key f1 f2 ...` on the
  CLI) answers the values of the requested fields, aligned
  with the request, None per missing field and all None when
  the key does not exist — Redis HMGET. All three are pure
  reads: no log entry, no state change, no watch event, and
  the TTL deadline is untouched — the listings are read off
  the applied state machine exactly like an hget. A key
  holding a plain string makes all three a client error, not
  a list, like an hgetall on a plain string. All three ride
  the same ReadIndex path as a `get`, so they carry the same
  session watermark and are exactly as linearizable, and they
  survive snapshots and replay exactly like any other hash
  read — the blob is a plain string in the store, so nothing
  about the encoding changes. The values `hvals` returns are
  aligned with `hkeys` — the value at position i is the value
  of the field at position i in the sorted field list — so the
  two calls together rebuild the map; `hmget` answers in the
  order the caller asked, not sorted order, and a field
  repeated in one call is answered once per occurrence, so the
  reply never shifts after a miss or a repeat, like an mget.
  At most 1000 fields per hmget, like an hdel batch. Like
  hget and hgetall they are reads, so they are not pipeline
  ops. The honest limit is the same encoding as hset: a plain
  `get` on a hash key still returns the encoded blob.
- **Hash field scan** — since v0.59.0 `hscan key cursor [MATCH
  pattern] [COUNT n]` (`Session.hscan(k, cursor, match, count)`,
  `client.hscan(addr, k, cursor, match, count)`, `hscan key
  cursor [--match pattern] [--count n]` on the CLI) answers one
  page of the hash at the key — the [field, value] pairs from
  the cursor on, in sorted field order, and the next cursor to
  pass back, 0 when the scan is done — Redis HSCAN. The cursor
  is an offset into the sorted field list: start at 0 and keep
  going until a reply answers 0. COUNT is how many fields one
  call examines (a hint, like Redis's COUNT — default 10, at
  most 1000), MATCH a glob pattern — `*`, `?`, `[abc]` —
  applied to field names, so a selective MATCH can answer a
  page with no pairs at all and a cursor still to go. A missing
  key answers cursor 0 with an empty page. It is a pure read:
  no log entry, no state change, no watch event, and the TTL
  deadline is untouched — the page is read off the applied
  state machine exactly like an hget. A key holding a plain
  string makes it a client error, not a page. It rides the same
  ReadIndex path as a `get`, so it carries the same session
  watermark and is exactly as linearizable, and it survives
  snapshots and replay exactly like any other hash read — the
  blob is a plain string in the store, so nothing about the
  encoding changes. `Session.hscan_all` / `client.hscan_all`
  walk the pages in one call and answer every pair in sorted
  field order, plus `pages`, how many RPCs the walk took. Like
  hget and hgetall it is a read, so it is not a pipeline op.
  The honest limit is the same encoding as hset: a plain `get`
  on a hash key still returns the encoded blob — and the scan
  as a whole is not a snapshot: each page is linearizable on
  its own, but a field written or deleted between pages can be
  missed, or appear in a later page if it sorts ahead of the
  cursor, like the keys scan.
- **Key enumeration** — since v0.31.0 one `keys` RPC lists the
  store's keys (`client.keys(prefix, limit, start_after)`,
  `Session.keys(prefix, limit, start_after)`, `keys --prefix p
  --limit 100 --start-after k` on the CLI): every matching key in
  sorted order, at most 1000 per reply, with `truncated` saying
  whether more matched and `next_start_after` naming the last key
  returned. Since v0.32.0 a scan takes a `start_after` cursor — an
  exclusive lower bound on the sorted key order — so a client
  pages through a large keyspace by passing back the previous
  page's last key (or `next_start_after`), with no repeats and no
  gaps while the state is unchanged; `keys_all` does the walk in
  one call and returns every key plus the page count. The scan
  rides the same ReadIndex/lease path as a single `get` — one
  leadership confirmation, one apply wait — so each page reflects
  the committed state as of one read index, exactly as
  linearizable, and a stale scan is served from a follower lease
  exactly like a stale `get`. The reply carries the same
  `read_index` / `read_term`, so a session advances its read
  watermark from a scan like any other read. Honest limit: the
  pages are individually linearizable, but the enumeration as a
  whole is not a snapshot — a key written or deleted between pages
  can be missed, or appear in a later page if it sorts ahead of
  the cursor. Since v0.33.0 the state machine keeps a sorted
  index of its keys (`KVStore._keys`, maintained with `bisect` on
  write: a `set` of a new key inserts it in sorted order, a `del`
  removes it), so a page seeks to its cursor with bisect and
  walks at most one page past it — O(page + log n) instead of
  sorting the whole matching keyspace per page. The index is
  derived state, exactly like the dict: replaying the log
  rebuilds it entry by entry and loading a snapshot rebuilds it
  from the snapshot state, so every node holding the same log
  computes the same index. Honest limit: the index is a plain
  Python list, so the bisect is O(log n) but inserting or
  removing a key shifts the tail of the list — a write of a new
  key or a delete of an existing one pays an O(n) memmove — and
  the index holds one pointer per key on top of the dict. A
  balanced tree would remove the memmove, but it would need a
  dependency or a hand-rolled tree for a store whose writes are
  fsync-bound anyway, so the list is the right trade for now.
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
  at a time (Raft dissertation §6), now in two phases — a
  `joint_config` entry (C-old,new) then a `final_config` entry
  (C-new) — instead of one entry: while the transition is open, a
  commit, an election, or a lease needs a majority of BOTH configs,
  so an old-majority-only quorum can never commit anything and two
  leaders in one term stay impossible. The config is persisted with
  it, a new server replicates the whole log via
  AppendEntries/InstallSnapshot before it starts voting, and a
  removed server stops participating. A leader that removes itself
  steps down. The leader keeps replicating to a removed server until
  that server's commit index reaches the removal entry, so the removed
  server actually learns it was removed instead of timing out forever.
  If the driving leader dies mid-transition, the next leader finishes
  it on election; a snapshot taken mid-transition carries the joint
  config, and a node restarted mid-transition resumes in it.
- **Membership-change status** — since v0.23.0 the status RPC
  carries a `membership` object, and the client exposes it as
  `membership-status` (`--json` for the raw object): the current
  voting config, the joint config while a transition is open, which
  phase it is in (`stable`, `catching_up`, `joint_pending`,
  `joint_committed`, `final_pending`), and the change itself — the
  op (`add_server` / `remove_server`), the server, and the log
  indexes of its joint and final entries — plus the servers still
  catching up as non-voters and any removal the leader is still
  replicating. A client driving an add/remove only hears back once
  the change has fully committed, so this is the only way to see
  where an in-flight change stands; each node reports its own view,
  so a follower a heartbeat behind the leader may still show the
  previous phase. The op and server come from the joint entry
  itself, or — when a snapshot taken mid-transition has compacted
  it away — from diffing C-old against C-new.
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
  Since v0.22.0 `--membership` also drives a membership change
  mid-run: a fourth node is started and added while the kill/restart
  faults (and the partition, if enabled) are active, then the leader
  is killed and restarted, and the run verifies the 4-node membership
  is durable and every acked write still reads back on the enlarged
  cluster. While the add is in flight a poller thread asks every
  node for its membership-status every 20ms and records the phases
  it sees, so the run output shows the transition the cluster
  actually went through (e.g. ``catching_up -> joint_pending ->
  joint_committed -> final_pending -> stable``), not just the final
  config. `tests/test_membership_chaos.py` covers the same
  combination in-process: add under a partition, remove while a
  follower is killed and restarted, and membership surviving a leader
  restart mid-chaos.
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

727 tests, plus a smoke test for the benchmark script, all passing:

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
| `test_concurrent_writes_share_one_fsync_in_stats` | 5 concurrent writes cost 1 fsync and the group-commit stats (and status RPC) say so |
| `test_acked_writes_survive_crash_restart` | 10 acked batched writes all survive a leader stop/restart from the same data dir |
| `test_window_zero_disables_batching` | Window 0: 5 concurrent writes cost 5 fsyncs, sequential writes 1 each — exactly as before group commit |
| `test_max_batch_caps_batch_and_flushes_early` | Max batch 3 turns 6 concurrent writes into exactly 2 batches of 3, flushed on fill, not on the window |
| `test_add_fourth_node_catches_up_and_serves_reads` | A 4th node added to a live cluster catches up and serves reads |
| `test_add_server_already_a_member_is_rejected` | Adding a server that is already a member is rejected |
| `test_remove_node_cluster_keeps_working` | Removing a node leaves a working 2-node cluster; the removed node stops participating |
| `test_remove_leader_triggers_new_election` | Removing the leader triggers a new election and writes continue |
| `test_add_then_remove_keeps_three_nodes_and_survives_leader_loss` | Add then remove keeps 3 nodes and survives losing the leader afterwards |
| `test_remove_unknown_server_is_rejected` | Removing a server that is not a member is rejected |
| `test_concurrent_membership_changes_rejected` | Two concurrent membership changes are rejected/serialized, and the cluster is not wedged afterwards |
| `test_membership_survives_restart` | Membership persists across restarts of every node; the 4-node cluster keeps serving |
| `test_old_majority_alone_cannot_commit_during_joint` | During a joint transition, holders that are a majority of C-old only do not commit a current-term entry; adding the new member's ack does |
| `test_add_server_completes_joint_then_final` | A client add runs both phases: the cluster ends in the 4-node config with no joint phase left, and the new node serves reads |
| `test_leader_change_mid_transition_completes` | Frozen mid-transition by partitioning two members, a snapshot carries the joint config, a restarted follower resumes in it, and the new leader after the old one dies finishes the change |
| `test_add_node_under_partition` | Adding a node while one follower is partitioned: the majority side plus the catching-up node4 commit the joint and final entries, and after the heal every node reports the 4-node config |
| `test_remove_node_while_follower_killed_and_restarted` | Removing a node while another follower is down: the joint entry waits out the restart, then commits, and writes continue on the 2-node cluster |
| `test_membership_survives_leader_restart_mid_chaos` | Writers and kill/restart faults running throughout, a node added mid-run, then the leader killed and restarted: every acked write present on all four nodes, 4-node config everywhere, no joint phase left |
| `test_chaos_harness_polls_phases_during_add` | The chaos harness's membership poller observes a real add in-process: the timeline holds only valid phases, includes an intermediate phase, and ends stable |
| `test_summarize_phases_dedupes_and_keeps_order` | The phase-timeline summarizer counts each phase once in first-seen order and still ends with the settled phase, and empty input summarizes to nothing |
| `test_add_node_catches_up_via_snapshot_during_membership_change` | Adding a node whose catch-up needs a chunked InstallSnapshot (leader compacted past it): the transfer streams in chunks while the add is in flight, writers keep acking through it, and the cluster ends in the 4-node config with every acked write on every node and a linearizable history |
| `test_lagging_follower_needs_snapshot_during_add` | A lagging follower that needs a snapshot and a new node that needs one transfer concurrently while the add runs: both install chunked snapshots in flight, the add completes, and every acked write is on every node |
| `test_status_reports_stable_membership` | A healthy cluster reports phase stable, the 3-node config, no joint config, nothing in flight |
| `test_status_reports_uncommitted_joint_entry` | An isolated leader with a joint entry in its log reports joint_pending with the op, server, and joint index; a follower that never saw the entry still reports stable |
| `test_status_reports_joint_phase_mid_transition` | A cluster frozen in the joint phase reports joint_committed with the change and the non-voting server from both leader and follower, then stable once the change completes |
| `test_membership_status_cli` | The `membership-status` CLI prints the summary and `--json`, and the `add-server` / `remove-server` CLI paths drive the change end to end |
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
| `test_incr_missing_key_creates_at_by` | Incr on a missing key creates it at `by` (default 1) |
| `test_incr_adds_and_subtracts` | Incr adds, a negative `by` subtracts, `by` 0 is a no-op |
| `test_incr_rejects_non_integer_value` | Incr on a non-integer value fails as not_an_integer and writes nothing |
| `test_incr_rejects_loose_integer_spellings` | Whitespace, underscores, and decimals are not integers to incr |
| `test_incr_rejects_out_of_range_value` | A value outside the signed 64-bit range is not an integer to incr |
| `test_incr_rejects_overflow` | A result past the signed 64-bit bounds fails as overflow |
| `test_incr_keeps_ttl_deadline` | A successful incr keeps the key's TTL deadline and the key still expires on schedule |
| `test_incr_new_key_joins_sorted_index` | A key created by incr joins the sorted scan index |
| `test_incr_succeeds_and_replicates` | Incr writes and replicates to every node, and reads back the new value |
| `test_incr_on_non_integer_fails_and_writes_nothing` | A failed incr leaves the value unchanged on every node |
| `test_incr_rejects_non_integer_by` | A non-integer `by` is a client error and appends nothing |
| `test_incr_from_follower_redirects` | An incr sent to a follower is redirected to the leader and still applies |
| `test_incr_retry_appends_once_and_returns_same_value` | A retried incr returns the first attempt's value and appends no second entry |
| `test_failed_incr_retry_returns_same_failure` | A retried failed incr returns the same failure, one log entry |
| `test_incr_continues_after_leader_change` | The counter keeps counting through a leader change |
| `test_incr_fires_watch_event_with_new_value` | A successful incr fires a set watch event with the new value on leader and follower; a failed one fires nothing |
| `test_incr_survives_snapshot_and_restart` | The counter survives log compaction and a leader restart |
| `test_session_incr_is_read_your_writes` | A session incr advances the write watermark and the next read sees the new value |
| `test_pipeline_incr_ops` | Pipelined incr ops reply in order, a malformed `by` is an error in position |
| `test_incr_cli` | The `incr` CLI subcommand prints the new value and exits 1 on a failed incr |
| `test_setnx_sets_absent_key` | Setnx on a missing key sets it and answers 1 |
| `test_setnx_leaves_existing_key_untouched` | Setnx on an existing key answers 0 and writes nothing |
| `test_setnx_after_delete_sets_again` | A deleted key is absent again, so a later setnx wins |
| `test_setnx_new_key_joins_sorted_index` | A key created by setnx joins the sorted scan index |
| `test_setnx_succeeds_and_replicates` | Setnx writes and replicates to every node, and reads back the value |
| `test_setnx_on_existing_key_is_noop_everywhere` | A losing setnx leaves the value unchanged on every node |
| `test_setnx_requires_value` | A setnx with no value is a client error and appends nothing |
| `test_setnx_from_follower_redirects` | A setnx sent to a follower is redirected to the leader and still applies |
| `test_setnx_retry_appends_once_and_returns_same_result` | A retried setnx returns the first attempt's 1 and appends no second entry |
| `test_setnx_noop_retry_returns_same_zero` | A retried losing setnx returns the same 0, one log entry |
| `test_setnx_continues_after_leader_change` | Setnx keeps working through a leader change and the new leader sees the existing key |
| `test_setnx_fires_watch_event_only_on_success` | A successful setnx fires a set watch event on leader and follower; a losing one fires nothing |
| `test_setnx_survives_snapshot_and_restart` | The setnx claim survives log compaction and a leader restart |
| `test_session_setnx_is_read_your_writes` | A session setnx advances the write watermark and the next read sees the value |
| `test_pipeline_setnx_ops` | Pipelined setnx ops reply in order, a missing value is an error in position |
| `test_setnx_cli` | The `setnx` CLI subcommand prints 1 on a set, 0 when the key exists |
| `test_getset_on_missing_key_returns_none_and_sets` | Getset on a missing key answers None and creates it |
| `test_getset_returns_old_value_and_replaces` | Getset returns the old value and replaces it |
| `test_getset_after_delete_returns_none` | After a delete a getset answers None — the old value is live state, not history |
| `test_getset_keeps_ttl_deadline` | A getset keeps the key's TTL deadline and the key still expires on schedule |
| `test_getset_new_key_joins_sorted_index` | A key created by getset joins the sorted scan index |
| `test_getset_succeeds_and_replicates` | Getset writes and replicates to every node, and reads back the new value |
| `test_getset_returns_old_value_everywhere` | A getset on an existing key answers the old value on every node |
| `test_getset_requires_value` | A getset with no value is a client error and appends nothing |
| `test_getset_from_follower_redirects` | A getset sent to a follower is redirected to the leader and still applies |
| `test_getset_retry_appends_once_and_returns_first_old_value` | A retried getset returns the first attempt's old value and appends no second entry, even after another getset moved the value |
| `test_getset_continues_after_leader_change` | Getset keeps working through a leader change and the new leader sees the first getset's value as the old one |
| `test_getset_fires_watch_event_with_new_value` | A getset fires a set watch event with the new value on leader and follower, on a missing key and an existing one |
| `test_getset_survives_snapshot_and_restart` | The getset swap survives log compaction and a leader restart |
| `test_session_getset_is_read_your_writes` | A session getset advances the write watermark and the next read sees the new value |
| `test_pipeline_getset_ops` | Pipelined getset ops reply in order, a missing value is an error in position |
| `test_getset_cli` | The `getset` CLI subcommand prints (nil) for a missing key, the old value otherwise |
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
| `test_pool_reuses_one_connection` | Five RPCs through a pool ride one server connection |
| `test_pool_recovers_after_server_restart` | A pooled socket to a restarted server is retried on a fresh connection |
| `test_idle_connection_expires` | An idle socket past its max age is dropped, not reused |
| `test_pool_serves_concurrent_calls_past_the_cap` | Five concurrent slow calls past a cap of 1 all succeed without queueing |
| `test_one_shot_send_rpc_unchanged` | `send_rpc` with no pool still opens a fresh connection per call |
| `test_pool_rejects_bad_size` | A pool with `max_size` below 1 is rejected |
| `test_session_uses_pool_and_close_releases_it` | A session reads and writes over pooled connections; close releases them and the session stays usable |
| `test_session_pool_size_zero_means_one_shot` | A session with `pool_size=0` works with no pool at all |
| `test_pooled_concurrent_reads_are_correct` | Ten threads reading through one shared pool all get the right values |
| `test_idle_connections_use_bounded_threads` | Thirty idle connections cost no extra threads: one reactor owns them all |
| `test_concurrent_requests_all_correct` | Twenty threads x ten requests through the pool server all get the right replies |
| `test_split_frame_is_reassembled` | A frame split across two TCP reads is reassembled and answered |
| `test_pipelined_frames_reply_in_order` | Two frames sent back-to-back reply in order, even when the first is slow |
| `test_slow_dispatch_does_not_starve_pool` | Eight 1s dispatches run in parallel on the pool instead of serially |
| `test_idle_connection_is_closed` | A connection idle past the timeout is closed by the server |
| `test_watch_does_not_starve_reads` | Reads stay fast while a 5s watch long-polls on the leader |
| `test_mget_returns_values_in_order` | 50 keys in one mget come back in key order with the right values |
| `test_mget_includes_missing_keys` | A missing key mid-batch answers None in that position and does not shift the values after it |
| `test_mget_from_follower_redirects` | An mget sent to a follower is redirected to the leader and still answers correctly |
| `test_mget_stale_served_from_follower_lease` | An mget with stale_ok is served by a follower holding a fresh leader lease, carrying the follower marker |
| `test_mget_rejects_bad_key_lists` | An empty list, a non-list, a non-string key, or more than 1000 keys is an error, not a read |
| `test_mget_rejects_bad_read_params` | A non-integer or negative min_commit_index is an error, exactly as for a single get |
| `test_session_mget_is_read_your_writes` | A session mget carries the write watermark, sees its own write, and advances the read watermark |
| `test_mget_matches_pipelined_get_many` | The same 50 keys read by one mget and by pipelined get_many agree exactly |
| `test_mset_writes_many_keys_in_one_log_entry` | 50 pairs in one mset, values right, leader log grew by exactly one entry |
| `test_mset_overwrites_and_last_duplicate_wins` | An mset overwrites old values; a duplicate key in one batch keeps its last value |
| `test_mset_replicates_to_followers` | An mset lands on every follower as one entry — no follower holds half the batch |
| `test_mset_from_follower_redirects` | An mset sent to a follower is redirected to the leader and still writes |
| `test_mset_rejects_bad_pair_lists` | Empty, non-list, malformed, non-string, or >1000 pairs is an error, not a write |
| `test_session_mset_is_read_your_writes` | A session mset advances the watermark; a later read (even from a follower) sees it |
| `test_mset_retry_with_same_request_id_appends_once` | A retried mset with the same session id appends no second log entry |
| `test_mset_fires_one_watch_event_per_pair` | A prefix watch fires once per pair, in order, all at the batch entry's index |
| `test_mset_keys_appear_in_sorted_scan` | Keys written by mset show up in a paged scan in sorted order |
| `test_mset_cli` | The `mset` CLI writes the batch and prints OK; an odd argument count is a usage error |
| `test_keys_returns_all_keys_sorted` | A scan with no prefix returns every key in sorted order, with the read index/term |
| `test_keys_start_after_pages` | Paging limit=2 through five keys returns each page in order, no repeats or gaps, last page untruncated |
| `test_keys_start_after_with_prefix` | A cursor composes with a prefix: paging under one prefix never leaks another's keys |
| `test_keys_start_after_beyond_last_key` | A cursor past every key returns [], and a cursor equal to a key excludes that key (exclusive bound) |
| `test_keys_start_after_pages_whole_keyspace` | Paging one key at a time walks the whole keyspace in sorted order |
| `test_keys_prefix_filters` | A prefix scan returns only keys under that prefix; a non-matching prefix returns [] |
| `test_keys_limit_truncates` | A limit below the match count returns the first keys in order with truncated True |
| `test_keys_empty_store` | A scan of an empty store returns an empty list, not an error |
| `test_keys_reflects_deletes` | A deleted key is gone from the next scan |
| `test_keys_from_follower_redirects` | A scan sent to a follower is redirected to the leader and still answers |
| `test_keys_stale_served_from_follower_lease` | A scan with stale_ok is served by a follower holding a fresh leader lease |
| `test_keys_rejects_bad_params` | A non-string prefix or start_after, or a limit outside 1-1000, is an error, not a scan |
| `test_keys_rejects_bad_read_params` | A non-integer or negative min_commit_index is an error, exactly as for a single get |
| `test_session_keys_is_read_your_writes` | A session scan sees its own write and advances the session's read watermark |
| `test_session_keys_start_after_and_keys_all` | A session scan takes a cursor, and `keys_all` walks the whole keyspace in pages and returns every key plus the page count |
| `test_keys_cli` | The `keys` CLI subcommand prints the keys as one JSON list, honoring --prefix and --limit |
| `test_keys_cli_start_after` | The `keys` CLI subcommand honors --start-after, resuming past the given key, exclusive |
| `test_keys_pages_large_keyspace` | Paging 120 keys ten at a time returns every key in sorted order, no repeats or gaps, and `keys_all` walks it in 12 pages |
| `test_keys_index_survives_snapshot_and_restart` | A follower restarted after a snapshot serves a paged stale scan from its rebuilt sorted index |
| `test_index_tracks_sets_deletes_and_rewrites` | The sorted index tracks sets, overwrites, deletes, and re-sets — always exactly the store's keys, sorted |
| `test_index_page_returns_only_the_page` | A page returns at most `limit` keys plus the truncated flag, never the whole matching keyspace |
| `test_index_prefix_cursor_and_limit_compose` | Prefix, cursor, and limit compose on the index: no leaks across prefixes, cursor is an exclusive bound |
| `test_index_cas_does_not_disturb_it` | A CAS — successful, failed, or on a missing key — never adds or removes an index entry |
| `test_index_survives_load` | Loading snapshot state rebuilds the index, and writes after a load keep maintaining it |
| `test_store_records_deadline_at_apply_index` | A set with ttl_entries at index 10 is alive at 14 and gone at 15 — deadline is index + ttl, purged before the deadline entry applies |
| `test_store_plain_set_clears_deadline` | A set without a TTL drops the key's deadline, so the key outlives the position it would have expired at |
| `test_store_cas_keeps_deadline` | A successful CAS updates the value but keeps the deadline |
| `test_store_mset_and_del_clear_deadline` | An mset pair and a delete both drop the key's deadline, like a plain set |
| `test_store_ttl_remaining` | `KVStore.ttl` answers -2 for a missing key, -1 with no deadline, otherwise the entries left plus the deadline itself |
| `test_store_ttl_without_index_raises` | A TTL with no log position to anchor it raises instead of recording a deadline nobody could agree on |
| `test_store_load_restores_expiry` | A snapshot load restores the deadlines with the data, and a load with no expiry (an old snapshot) expires nothing |
| `test_ttl_query_counts_down` | A TTL query counts down 3, 2, 1 as the log advances, then answers -2 once the deadline entry lands |
| `test_ttl_query_missing_and_no_deadline` | A missing key answers -2 and a key with no deadline answers -1 with no deadline |
| `test_ttl_query_from_follower_redirects` | A TTL query sent to a follower is redirected to the leader and still answers correctly |
| `test_session_ttl_is_read_your_writes` | A session TTL query sees its own write's deadline and counts down as the session writes fillers |
| `test_ttl_query_cli` | The `ttl` CLI prints the remaining entries, and -2 for a missing key |
| `test_set_with_ttl_expires_at_deadline` | A key with ttl 3 reads for two more entries and is gone once the third lands, on the leader and every follower |
| `test_plain_set_clears_deadline_on_cluster` | Re-setting a key without a TTL clears its deadline while a sibling key with the same TTL still expires |
| `test_cas_on_expired_key_is_missing` | A CAS after the deadline fails as missing_key — the purge runs before the CAS entry applies |
| `test_expired_key_leaves_sorted_scan` | An expired key disappears from the keys scan — the purge removes it from the sorted index too |
| `test_expiry_fires_del_watch_event` | A watch on a TTL key fires a del event with value None at the index of the entry whose apply purged it |
| `test_ttl_survives_leader_change` | A key's deadline survives killing the leader: the new leader expires it at the same log position |
| `test_ttl_survives_snapshot_and_restart` | A deadline survives log compaction and a restart — the snapshot carries the expiry map |
| `test_ttl_rejects_bad_values` | A TTL of 0, negative, non-integer, boolean, or past the cap is a client error and appends no log entry |
| `test_session_set_with_ttl` | Session.set takes ttl_entries, and read-your-writes still works before and after expiry |
| `test_pipeline_set_with_ttl` | A pipelined set may carry ttl_entries; a malformed TTL is an error in its position and the ops around it still run |
| `test_ttl_cli` | The `set --ttl-entries N` CLI writes the key with a deadline, and a bad TTL is rejected instead of writing |
| `test_store_setex_records_wall_deadline` | A setex records its absolute wall deadline, alive before it and gone at it |
| `test_store_setex_without_deadline_raises` | A setex with no wall deadline raises instead of recording one nobody could agree on |
| `test_store_plain_set_mset_del_clear_wall_deadline` | A plain set, an mset pair, and a delete all drop the key's wall deadline |
| `test_store_cas_and_incr_keep_wall_deadline` | A successful CAS and incr update the value but keep the wall deadline |
| `test_store_setex_clears_log_deadline` | A setex drops the key's log-entry deadline — a key holds at most one kind of TTL |
| `test_store_wall_ttl_remaining` | `KVStore.wall_ttl` answers in Redis's convention, in seconds, plus the absolute deadline |
| `test_store_load_restores_wall_deadline` | A snapshot load restores the wall deadlines alongside the data |
| `test_setex_readable_then_expires` | A key with a 1-second wall TTL is readable at once and gone past the deadline, on every node |
| `test_ttl_query_reports_wall_deadline` | A TTL query reports `wall_ttl` and `wall_deadline` while the log-entry TTL stays -1 |
| `test_setex_rejects_bad_seconds` | Seconds of 0, negative, non-integer, or past the cap is a client error and appends no log entry |
| `test_plain_set_clears_wall_deadline_on_cluster` | Re-setting a key without a TTL clears its wall deadline on the cluster |
| `test_wall_expiry_fires_del_watch_event` | A watch fires a del event once the wall deadline passes, with no log entry landing |
| `test_wall_deadline_survives_snapshot_and_restart` | A wall deadline survives log compaction and a restart, and the key still expires at its original deadline |
| `test_expired_key_leaves_sorted_scan` | A key that expires by the wall clock disappears from the keys scan too |
| `test_session_setex` | Session.setex writes with a wall TTL, and read-your-writes still works before and after expiry |
| `test_pipeline_setex` | A pipelined setex may carry seconds; a malformed value is an error in its position |
| `test_setex_cli` | The `setex` CLI writes the key with a wall deadline, and a bad seconds value is rejected instead of writing |
| `test_store_expireat_sets_wall_deadline_and_clears_log` | An expireat records its absolute wall deadline and drops the key's log-entry deadline |
| `test_store_expireat_missing_key_returns_zero` | Expiring a missing key answers 0, records no deadline, and touches no other key |
| `test_store_pexpireat_sets_wall_deadline` | A pexpireat records the same absolute wall deadline in seconds and answers 1 |
| `test_store_expireat_replaces_wall_deadline` | Re-expiring a key replaces its wall deadline, whether it came from a setex or an earlier expireat |
| `test_store_expireat_past_deadline_deletes_key` | An expireat whose deadline was already past deletes the key outright and answers 1 with `deleted` set |
| `test_store_expireat_past_deadline_missing_key_returns_zero` | A past-deadline expireat on a missing key still answers 0 |
| `test_store_expireat_without_deadline_raises` | An expireat / pexpireat with no wall deadline raises instead of recording one nobody could agree on |
| `test_expireat_readable_then_expires` | A key given a wall deadline 2 seconds out is readable at once and gone past the deadline, on every node |
| `test_expireat_missing_key_returns_zero` | An expireat on a missing key answers 0 and changes nothing |
| `test_expireat_past_deadline_deletes_immediately` | A past-deadline expireat deletes the key at once, answers 1 with `deleted`, and fires a del watch event |
| `test_pexpireat_milliseconds` | A pexpireat with a millisecond timestamp 1.5 seconds out is readable at once and gone past the deadline |
| `test_expireat_rejects_bad_timestamps` | A timestamp of 0, negative, non-integer, or boolean is a client error and appends no log entry |
| `test_expireat_clears_log_deadline_on_cluster` | An expireat drops the log deadline: the key survives the log advancing past it and still dies at its wall deadline |
| `test_session_expireat` | Session.expireat / pexpireat set wall deadlines, and read-your-writes still works before and after expiry |
| `test_pipeline_expireat` | Pipelined expireat / pexpireat ops set wall deadlines; a malformed timestamp is an error in its position |
| `test_expireat_cli` | The `expireat` / `pexpireat` CLIs set wall deadlines, print 1 / 0, and a bad timestamp is rejected instead of writing |
| `test_store_expire_sets_deadline_at_apply_index` | An expire at index 11 with 5 entries sets the deadline to 16 — alive at 15, gone at 16 |
| `test_store_expire_missing_key_returns_zero` | Expiring a missing key answers 0, records no deadline, and touches no other key |
| `test_store_expire_replaces_deadline` | Re-expiring replaces the deadline, whether it came from a set TTL or an earlier expire |
| `test_store_expire_without_index_raises` | An expire with no log position to anchor it raises instead of recording a deadline |
| `test_store_expire_after_deadline_is_missing` | A key whose deadline already passed is already gone when the expire entry applies, so it answers 0 |
| `test_expire_sets_deadline_and_key_expires` | An expire of 3 answers 1 with the deadline, the TTL counts 3, 2, 1, and the key is gone at the deadline on every node |
| `test_expire_missing_key_returns_zero` | Expiring a missing key answers 0 and still commits one log entry, like a failed CAS |
| `test_expire_replaces_deadline` | A key set with a long TTL and re-expired short dies at the new deadline, not the old one |
| `test_expire_from_follower_redirects` | An expire sent to a follower is redirected to the leader and still answers 1 with the right deadline |
| `test_expire_fires_del_watch_event` | A watch fires nothing for the expire itself, then a del event at the index of the entry whose apply purged the key |
| `test_expire_survives_snapshot_and_restart` | A deadline set by expire survives compaction and restart — the snapshot carries it |
| `test_expire_rejects_bad_values` | An expire of 0, negative, non-integer, boolean, or past the cap is a client error and appends no log entry |
| `test_expire_retry_with_same_request_id_appends_once` | A retried expire returns the first attempt's result and appends no second entry, so the deadline is not pushed out |
| `test_session_expire_is_read_your_writes` | Session.expire sets the deadline, advances the watermark, and a read after it sees the key until it expires |
| `test_expire_cli` | The `expire` CLI prints 1 for an existing key and 0 for a missing one, and a bad count is rejected |
| `test_store_plain_set_answers_one` | A plain set applies and answers `set` 1, with the value stored |
| `test_store_set_nx_wins_on_missing_key` | An NX set on a missing key answers 1 and writes the value |
| `test_store_set_nx_loses_on_existing_key` | An NX set on an existing key answers 0, and the value and deadline are untouched |
| `test_store_set_xx_wins_on_existing_key` | An XX set on an existing key answers 1 and overwrites the value |
| `test_store_set_xx_loses_on_missing_key` | An XX set on a missing key answers 0, and the key stays absent |
| `test_store_set_nx_after_delete_wins` | An NX set wins again after the key was deleted — existence is live state, not history |
| `test_store_set_keepttl_keeps_log_deadline` | A KEEPTTL overwrite changes the value but keeps the log-entry deadline; a plain set after it clears it |
| `test_store_set_keepttl_keeps_wall_deadline` | A KEEPTTL overwrite keeps the wall-clock deadline; a plain set after it clears it |
| `test_store_set_keepttl_on_missing_key_records_no_deadline` | A KEEPTTL set of a new key answers 1 and records no deadline |
| `test_store_set_xx_with_keepttl_keeps_deadline` | XX with KEEPTTL composes: the write happens and the deadline is kept, and a losing XX writes nothing |
| `test_set_nx_xx_replicate` | A winning NX and a winning XX replicate to every node |
| `test_set_nx_loses_on_existing_key_everywhere` | A losing NX changes the value on no node |
| `test_set_xx_loses_on_missing_key_everywhere` | A losing XX leaves the key absent on every node |
| `test_set_flags_rejected_with_no_log_entry` | NX with XX, KEEPTTL with a TTL, or a non-boolean flag is a client error and appends no log entry |
| `test_set_retry_returns_first_outcome` | A retried winning NX returns 1 and a retried losing NX returns 0, each appending no second entry |
| `test_set_keepttl_keeps_deadline_on_cluster` | A KEEPTTL overwrite keeps the same deadline on the cluster; a plain set clears it |
| `test_set_keepttl_keeps_wall_deadline_on_cluster` | A KEEPTTL overwrite keeps the wall deadline on the cluster |
| `test_set_fires_watch_event_only_on_win` | A watch fires on a winning set on leader and follower, and nothing on a losing NX or XX |
| `test_set_continues_after_leader_change` | A conditional set keeps working after a leader change, deciding against the new leader's state |
| `test_set_flags_survive_snapshot_and_restart` | A conditional set survives compaction and a restart — the key is still there, so NX still loses |
| `test_session_set_flags_is_read_your_writes` | Session.set with NX / XX answers 1 / 0, advances the watermark even on a losing set, and reads see the write |
| `test_pipeline_set_flags` | Pipelined set ops carry NX / XX / KEEPTTL; malformed flag combinations are errors in position |
| `test_set_flags_cli` | The `set` CLI prints 1 / 0 for --nx / --xx, rejects --nx with --xx as a usage error, and keeps the deadline with --keepttl |
| `test_store_get_on_missing_key_answers_none` | A GET set on a missing key answers `set` 1 and `old_value` None |
| `test_store_get_returns_old_value` | A GET set on an existing key answers the old value and replaces it |
| `test_store_plain_set_carries_no_old_value` | A plain set without GET carries no `old_value` in its reply |
| `test_store_losing_nx_with_get_answers_none` | A losing NX with GET answers `old_value` None, and the value and deadline are untouched |
| `test_store_losing_xx_with_get_answers_none` | A losing XX with GET answers `old_value` None, and the key stays absent |
| `test_store_get_clears_log_deadline_like_plain_set` | A winning GET set clears the log-entry deadline like a plain set |
| `test_store_get_with_keepttl_keeps_log_deadline` | A GET set with KEEPTTL keeps the log-entry deadline and reports the old value |
| `test_store_get_clears_wall_deadline_like_plain_set` | A winning GET set clears the wall-clock deadline like a plain set |
| `test_store_get_with_keepttl_keeps_wall_deadline` | A GET set with KEEPTTL keeps the wall-clock deadline and reports the old value |
| `test_store_get_with_fresh_ttl_sets_deadline` | A GET set with a fresh TTL reports the old value and records the new deadline |
| `test_set_get_replicates` | A GET set replicates to every node and answers the old value |
| `test_set_get_rejected_flag_with_no_log_entry` | A non-boolean GET is a client error and appends no log entry |
| `test_set_get_retry_returns_first_old_value` | A retried GET set returns the first attempt's old value and appends no second entry, winning or losing |
| `test_set_get_fires_watch_event_only_on_win` | A watch fires on a winning GET set on leader and follower, and nothing on a losing NX with GET |
| `test_session_set_get` | Session.set with `get=True` answers the old value, advances the watermark even on a losing set, and reads see the write |
| `test_pipeline_set_get` | Pipelined set ops carry GET and answer the old value in position; a malformed GET is an error in position |
| `test_set_get_cli` | The `set` CLI prints (nil) for --get on a missing key and the old value otherwise, and (nil) for a losing NX with --get |

Tests use real nodes (real TCP, real threads, real disk) on localhost —
no mocks of the consensus path.

## Benchmark

Measured with `scripts/bench.py` (real 3-node cluster on localhost,
200 writes, 10 threads, 32-byte values), Nov 23, 2026, on this dev VM
— one one-shot run, then two pooled runs with `--pool-size 10`. The
box is shared with sibling agents pushing on other repos, so the
numbers move run to run; the ranges below are what was actually
measured, not a best case picked out:

```
$ python3 scripts/bench.py                  # one fresh TCP connection per request
sequential writes          200 ops    84.0 ops/s   p50  9.0ms
concurrent writes x10      200 ops   680.2 ops/s   p50 12.3ms
sequential reads           200 ops  1030.5 ops/s   p50  0.7ms
concurrent reads x10       200 ops   172.4 ops/s   p50  3.8ms  p99 1011.7ms

$ python3 scripts/bench.py --pool-size 10  # persistent connections, 10 pooled per node
sequential writes          200 ops    90.9-102.3 ops/s   p50  8.2-8.6ms
concurrent writes x10      200 ops   740.9-757.7 ops/s   p50  9.0-10.3ms
sequential reads           200 ops  9385-16565 ops/s     p50  0.0-0.1ms
concurrent reads x10       200 ops  2983-5206 ops/s      p50  0.8-1.5ms  p99 4.5-36.7ms

$ python3 scripts/bench.py --pool-size 50 --threads 50  # thread-pool server, v0.28.0, six runs
sequential reads           200 ops  3373-9058 ops/s     p50  0.1-0.2ms
concurrent reads x50       200 ops  3496-6628 ops/s     p50  0.8-3.1ms  p99 3.2-7.5ms
```

And a longer, steadier load — 50 threads of pooled reads for 20
seconds against a real cluster, with per-process CPU sampled — before
and after the server change:

```
v0.27.0 (thread per connection):  3459 ops/s sustained (client 0.71 cores, leader 0.17)
v0.28.0 (reactor + worker pool):  6985 ops/s sustained (client 0.71 cores, leader 0.63)
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
still took ~1s in the transport layer. Persistent client connections
(v0.27.0) are what finally moved it: with `--pool-size 10` the same
concurrent reads run at ~3000-5200 ops/s with p99 under 40ms, and
sequential reads at ~9400-16600 ops/s, because a read no longer pays
a fresh TCP connection and a fresh server thread each time. Writes
did not move — they are fsync-bound, and pooling never claimed to
help them. At 50 clients the read ceiling was measured, not assumed,
before v0.28.0 changed anything: reads served straight from the
leader (no TCP at all) run at ~40,000 ops/s with 50 threads, and a
probe thread acquiring the Raft lock while 50 readers run waits ~1us
on average — the lock is not the bottleneck, so no lock-skipping read
path was built. The bottleneck was one thread per connection on the
server: 50 handler threads plus 50 client threads contending on the
GIL of a 1.5-CPU box, with the leader burning only 0.17 cores while
a read waited ~3.4ms. The thread-pool server (v0.28.0) is the fix
for exactly that: one reactor thread owns the sockets and dispatch
runs on a bounded pool of 32 workers, so 50 pooled connections cost
a reactor plus a few workers. Sustained concurrent reads doubled
(~3450 to ~7000 ops/s) and the bench's concurrent reads at 50
clients went from ~3100 to ~3500-6600 ops/s with p99 down from
~10ms to ~3-8ms. The cost is on the other side: a lone sequential
reader now pays one extra thread hop (reactor to worker), so
sequential reads run at ~3400-9100 ops/s instead of ~9400-16600,
p50 ~0.2ms instead of ~0.1ms — the server trades a little
single-client latency for a lot of concurrent throughput. The box
is now near CPU-saturated under the sustained load (1.36 of 1.5
cores, client 0.71 + leader 0.63), so the next ceiling is raw CPU
on this VM, not scheduling — a faster client or a real network
would move it before any more server threading will.

The faster client (v0.29.0) is that next step, and it moves the
single-client number without touching the server at all:

```
$ python3 scripts/bench.py --pool-size 50 --threads 50 --pipelined --mget  # three runs
sequential reads           200 ops  3894-5972 ops/s    p50  0.2ms
concurrent reads x50       200 ops  3440-4682 ops/s    p50  2.1-2.5ms
pipelined reads            200 ops  8144-17038 ops/s  mean 0.059-0.123ms/op  (batches of 50)
mget reads                 200 ops  197928-261647 ops/s  mean 0.004-0.005ms/op  (batches of 50)
```

The multi-get (v0.30.0) is the same measurement one level deeper:
a batch of 50 keys in one `mget` costs ~0.2ms total, so per key it
runs at ~198000-262000 ops/s — roughly 12x cheaper per key than 50
pipelined gets (~2.8-3.2ms per batch in the same runs), because the
per-request cost (one dispatch, one frame, one lease check, one
apply wait) is paid once and every key past the first is one
dictionary lookup. The batch time barely grows with size — a batch
of 10 costs ~0.1ms, a batch of 200 ~0.3ms, so the per-key rate
keeps rising with the batch (10: ~19000 ops/s, 200: ~590000) —
but that is a per-key rate inside one batch, and it says nothing
about independent clients: the sustained concurrent ceiling is
still ~7000 ops/s, CPU-bound on the leader, and a lone client
reading one key at a time still pays the ~0.06ms dispatch. The
honest use is a client that already has many keys to read at once:
it pays one request instead of fifty, and the 1000-key cap keeps
one frame bounded while it does.

A lone client sending reads 50 at a time over one connection runs
at ~13800-18300 ops/s in the v0.29.0 runs, roughly 2-5x the sequential client in the
same runs (~3900-6000 ops/s) — the round-trip wait between requests
was most of a sequential read's cost, and the server's per-request
processing (~0.06ms) is what is left. Depth 200 is no faster than
depth 50 in any meaningful way (18284 vs 17066 in runs that also
differ by load), so 50 stays the default: past it the batch just
holds more replies in flight on a server that still dispatches
them one at a time. The sustained concurrent ceiling does not move
— 50 threads of pooled reads still sit at ~7000 ops/s, CPU-bound
on the leader — which is the honest limit of this box: pipelining
lifts a single client to the server's per-request cost, it does not
make the server cheaper.

Pipelined writes (v0.35.0) are the same trick applied where it
cannot pay, and the bench says so plainly:

```
$ python3 scripts/bench.py --pool-size 10 --pipelined --pipelined-writes  # three runs
sequential writes          200 ops   83.9-99.1 ops/s   p50  8.4-8.8ms
pipelined writes           200 ops   85.3-95.9 ops/s   mean 10.4-11.7ms/op  (batches of 50)
```

A batch of 50 pipelined sets takes ~470-600ms — ~9.4-12ms per op,
the same as a sequential write — because every op still appends
its own log entry, still waits for a majority round, and still
pays its own fsync, and the server dispatches the frames from
one connection in order, so the fifty writes never overlap
server-side and never share an fsync the way ten concurrent
clients do under group commit (~1000 ops/s in the same runs).
The ~0.2ms round-trip wait pipelining removes is noise next to
an ~8-9ms commit on loopback. What the client does get is one
connection instead of fifty round trips and one session's dedup
ids across the batch, so a lost reply is retried safely instead
of applied twice — and over a real network, where the RTT is
milliseconds rather than ~0.2ms, the overlapped waits are the
same win pipelined replication already gets. On localhost the
number to beat for writes stays the concurrent one, not the
pipelined one.

## Architecture

```
raftkv/
  transport.py   Length-prefixed JSON framing, pooled, one-shot or
                 pipelined RPC, reactor + worker-pool TCP server
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

**Threading.** One reactor thread owns every socket (a selector
accepts, reads, and writes), a bounded pool of 32 daemon workers
runs dispatch, and one timer thread drives elections/heartbeats.
Client requests run on pool workers. All Raft state is guarded by a
single lock, and network I/O is always done *outside* the lock —
two nodes RPC-ing each other at the same time must never hold each
other's lock while waiting on the network.

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
- **Membership changes.** Single-server changes, one at a time, in
  two phases — joint consensus (Raft dissertation §6): a
  `joint_config` entry opens the transition (C-old,new) and a
  `final_config` entry closes it (C-new). While the joint phase is
  open, every quorum — commit advancement, pre-vote and election
  wins, the leader lease, leadership confirmation — needs a majority
  of BOTH configs (`_has_quorum`), so an old-majority-only quorum
  cannot commit anything and two leaders in the same term stay
  impossible even while the configs differ. The single-entry scheme
  it replaced relied on old and new majorities overlapping by one
  server; joint consensus drops that assumption, which is what makes
  the change safe when the leader dies mid-transition: the next
  leader inherits the joint config and drives the final entry
  itself (from `_become_leader`, and from `_apply_config_entry`
  when it commits a predecessor's joint entry with its no-op), and
  exactly one thread drives it (`_joint_completion_running`), so the
  cluster can never wedge in joint phase. The config and the joint
  config both live in the log (and in `meta.json` /
  `snapshot.json`, and in InstallSnapshot), so a restart never
  resurrects a removed server, forgets an added one, or forgets a
  transition halfway. A new server is added to the address book as
  non-voting first and only joins the config once its change
  completes, after it has caught up. A removed server leaves the
  config only at the final entry (so it can still vote in C-old
  during the joint phase), but the leader keeps replicating to it
  until its commit index reaches the removal entry — the
  AppendEntries response carries the follower's commit index for
  exactly that check. Removing the last member and removing a server
  that is not a member are both rejected. Removal itself is an
  explicit event, not a reading of the config: a node is removed
  when it applies a config entry that takes it out, or when it
  installs a snapshot whose config leaves it out after it has been a
  member (`_removed` / `_was_member`, persisted in `meta.json`).
  It used to be derived — "not in the voting config means removed"
  — but a server being added is not in the config yet either, and it
  adopts the old config from the snapshot it installs to catch up:
  the derivation flipped a joining server to removed the moment the
  transfer landed, it refused every AppendEntries after it, and the
  add could never complete when catch-up needed a snapshot.
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
  as log entries (one change at a time, no joint consensus — that
  changed in v0.21.0, see below), config persisted in the
  log/meta/snapshot, new servers catch up before voting, removed
  servers stop participating, a leader that removes itself steps
  down; plus the two liveness fixes the work exposed — a no-op entry
  on election so prior-term entries commit, and a heartbeat that
  does not wait for an ack first.
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
- ~~Joint-consensus membership changes~~ — done in v0.21.0, Nov 23,
  2026: a membership change is now two log entries, not one — a
  `joint_config` entry opens the transition (C-old,new) and a
  `final_config` entry closes it (C-new), per the Raft dissertation
  §6. While the joint phase is open, every quorum — commit
  advancement, pre-vote and election wins, the leader lease, and
  leadership confirmation — needs a majority of BOTH configs, so an
  old-majority-only quorum cannot commit anything. The joint config
  is persisted in `meta.json` and `snapshot.json` and rides along in
  InstallSnapshot, so a restart or a lagging follower mid-transition
  resumes in the joint phase; a leader that inherits a joint config
  (or commits one with its no-op) drives the final entry itself, so a
  leader dying mid-transition cannot wedge the cluster. Exactly one
  thread drives the final entry. `tests/test_joint_consensus.py`
  covers the old-majority-only commit refusal, the client add
  completing both phases, and a leader change mid-transition (frozen
  by partitioning two members, snapshot carrying the joint config,
  a restarted follower resuming in it, the new leader finishing
  the change).
- ~~Membership changes under chaos faults~~ — done in v0.22.0, Nov 23,
  2026: the chaos harness (`scripts/chaos.py --membership`) now
  drives a membership change mid-run — a fourth node is started and
  added while kill/restart faults (and the partition, if enabled)
  are active, then the leader is killed and restarted, and the run
  verifies the 4-node membership is durable and every acked write
  still reads back on the enlarged cluster. `tests/test_membership_chaos.py`
  covers the same combination in-process: adding a node while one
  follower is partitioned (the majority side plus the catching-up
  node4 still form a majority of both configs, so the joint and
  final entries commit without the isolated node's ack), removing a
  node while another follower is killed and restarted (the joint
  entry waits out the restart, then commits), and membership
  surviving a leader restart mid-chaos (writers and kill/restart
  faults running throughout, every acked write present on all four
  nodes afterwards, no node left in a joint phase). Verified with
  `scripts/chaos.py --membership` (seeds 7 and 42, the second with
  a 3s partition overlapping the add): 81 and 129 acked writes, zero
  lost, 4 nodes converged, linearizability ok.
- ~~Membership-change status reporting~~ — done in v0.23.0, Nov 23,
  2026: the status RPC now carries a `membership` object and the
  client exposes it as `membership-status` — the current voting
  config, the joint config while a transition is open, the phase
  (`stable`, `catching_up`, `joint_pending`, `joint_committed`,
  `final_pending`), and the change itself (op, server, joint/final
  log indexes), plus non-voting servers catching up and removals
  still being replicated. The op/server come from the joint entry
  in the log, or from diffing C-old against C-new when a snapshot
  taken mid-transition has compacted it away.
  `tests/test_membership_status.py` covers a stable cluster, an
  uncommitted joint entry on an isolated leader (and a follower
  still reporting stable), a frozen joint phase reported identically
  by leader and follower and back to stable once the change
  completes, and the CLI — including the `add-server` /
  `remove-server` CLI paths, which previously never matched the
  underscored op comparisons (the hyphenated subcommand name was
  sent to the server verbatim) and so errored instead of driving
  the change.
- ~~Chaos harness polls membership-status during membership
  changes~~ — done in v0.24.0, Nov 23, 2026: while `--membership`
  drives the add, a poller thread asks every node for its
  membership-status every 20ms and records the phases it sees, so
  the run output shows the transition the cluster actually went
  through (`catching_up -> joint_pending -> joint_committed ->
  final_pending -> stable`) instead of just the final config — the
  add RPC blocks until the change commits, so polling from a side
  thread is the only way to observe the intermediate phases. The
  poller keeps running for 0.5s after the add acks so the post-add
  stable phase is sampled too, and the timeline (each phase once, in
  first-seen order, plus the settled phase at the end — nodes report
  different phases at the same instant, so a merged per-sample
  timeline would oscillate) is printed with the sample count.
  On localhost the joint phases last milliseconds, so a run may
  only observe `catching_up` and `final_pending` between the two
  `stable` endpoints — that is the transition being faster than
  the 20ms poll, not a missed phase. `tests/test_membership_chaos.py` covers the poller
  in-process (a 50-key log so the catching-up phase lasts long
  enough to sample) and the timeline summarizer. Note: the
  pre-existing `test_add_node_under_partition` flakes under load
  on this box (the add times out or the leader does not answer —
  4/8 on the clean tree too), unrelated to this change.
- ~~Chaos coverage for snapshot transfer during membership
  change~~ — done in v0.25.0, Nov 23, 2026:
  `tests/test_membership_chaos.py` now drives an add whose catch-up
  needs InstallSnapshot (the leader compacted past the new node's
  start, 150 keys, 64-byte chunks) with writer/reader threads
  running through it, and a second test where a lagging follower
  that needs a snapshot restarts while the add is in flight, so two
  chunked transfers overlap one membership change. Both assert the
  transfers really streamed in chunks while the add RPC was open
  (chunk timestamps inside the add window), the new node ends in the
  4-node config with the right data, every acked write is present on
  every node, all states are identical, and the recorded history is
  linearizable. Building this caught a real bug: a joining server
  installed the snapshot, adopted the old config (which does not
  contain it), concluded from the config alone that it had been
  removed, and then refused every AppendEntries after the transfer
  — the add could never complete when catch-up needed a snapshot.
  Removal is now an explicit persisted event (`_removed` /
  `_was_member` in `meta.json`), set when a config entry takes a
  node out or a snapshot leaves out a server that had been a
  member — never by a joining server adopting the old config to
  catch up. A test-helper fix rides along: `_add_server_retry`
  takes a per-attempt timeout, since a slow catch-up that outruns
  the 12s client timeout leaves the first attempt running on the
  server and every retry answering "already in progress" instead of
  the change's outcome.
- ~~Configurable group commit~~ — done in v0.26.0, Nov 23, 2026:
  group commit (v0.7.0) had a hardcoded 2ms window and no
  visibility; now the window and the per-batch cap are knobs
  (`group_commit_window_ms`, `group_commit_max_batch`, on the
  `RaftNode` constructor and the server CLI), a full batch flushes
  as soon as it fills instead of waiting out the window, a writer
  arriving at a full batch waits for the flusher to take it and
  opens the next one (so no batch ever exceeds the cap), window 0
  disables batching entirely — every write its own batch and fsync,
  exactly as before group commit — and the status RPC carries a
  `group_commit` object with the config and the stats (batches,
  entries, fsyncs, largest batch). Safety is unchanged: the batch
  fsync still happens before any waiter is woken, so no entry is
  acked or considered committed before it is durable.
  `tests/test_groupcommit_config.py` proves it: 5 concurrent writes
  cost 1 fsync and the stats say so (and the status RPC agrees), 10
  acked batched writes all survive a leader stop/restart from the
  same data dir, window 0 costs 5 fsyncs for 5 concurrent writes
  and 1 per sequential write, and a max batch of 3 turns 6
  concurrent writes into exactly 2 batches of 3.
- ~~Persistent client connections / connection pooling~~ — done in
  v0.27.0, Nov 23, 2026: the concurrent-read bottleneck the
  benchmark measured (~185 ops/s, one read taking ~1s in the
  transport layer) was the fresh TCP connection per request, so
  `transport.py` gains a `ConnectionPool` — one connection checked
  out per in-flight request, so threads never share a socket;
  `max_size` (default 8) caps pooled connections per host, and past
  the cap a request opens an ephemeral connection instead of
  blocking, so the cap bounds idle sockets, never throughput; a
  failed call is retried once on a fresh connection, so a stale
  pooled socket (server closed it after 30s idle, or restarted) is
  invisible to the caller, and idle sockets older than 20s are
  dropped on checkout. `Session` owns a pool (`pool_size`, default
  8; 0 means one-shot as before), `request()` and `send_rpc()` take
  an optional pool, and `scripts/bench.py` takes `--pool-size`.
  Watches stay one-shot on purpose: a long-poll would pin a pooled
  connection for up to 25s. The server needed no change — its
  handler already loops on one connection. Measured honestly on
  this shared box (see Benchmark): concurrent reads 172 → 2983-5206
  ops/s with p99 from 1011.7ms to 4.5-36.7ms, sequential reads 1030
  → 9385-16565 ops/s; writes unchanged, as expected — they are
  fsync-bound. `tests/test_pool.py` proves it: 5 RPCs over 1 server
  connection, recovery after a server restart on the same port,
  idle expiry, 5 concurrent slow calls past a cap of 1 finishing in
  one round, one-shot `send_rpc` unchanged, and a shared pool
  serving concurrent reads correctly against a real cluster. Also
  in this release: `__version__` is bumped to 0.27.0 — v0.26.0 never
  updated it from 0.25.0. The next real thing is the remaining read
  ceiling the Benchmark section now names: the single Raft lock and
  one thread per connection on the server, under heavier concurrency
  than 10 clients — measure first, then decide whether that is a
  read path that skips the lock or a thread-pool server.
- ~~Pipelined client reads~~ — done in v0.29.0, Nov 23, 2026: the
  read ceiling the Benchmark section named was the client's wait,
  not the server — a lone sequential reader paid a round trip per
  read while the leader served each in ~0.06ms. `transport.py`
  gains a `PipelinedConnection`: one persistent socket, the whole
  batch encoded and sent in one `sendall`, replies read back in
  order, one `None` per unread position and a closed socket on a
  mid-batch failure. `client.get_many(keys, depth=50)` builds on
  it (via `request_many`, reads only — a re-sent batch repeats
  every message, so writes stay out), re-sending a batch once to
  the leader a follower names, and `scripts/bench.py --pipelined`
  adds a pipelined-reads phase (`--pipeline-depth` sets the batch
  size) that reports mean per-op latency, since per-op latencies
  are not observable inside a batch. Measured honestly on this
  shared box (see Benchmark): a single pipelined client reads at
  13820-18284 ops/s across three runs, against 3894-5972
  sequential in the same runs — the server's per-request cost is
  now the single-client ceiling, and the sustained concurrent
  ceiling (~7000 ops/s) is unchanged, CPU-bound on the leader.
  `tests/test_pipelined_client.py` proves it: 50 and 200 gets in
  key order with the right values, a missing key mid-batch, a
  redirect from a follower, an empty batch, Nones to a dead port,
  and reconnect after close; `tests/test_bench.py` runs the new
  bench phase end to end.
- ~~Multi-get reads~~ — done in v0.30.0, Nov 23, 2026: the
  per-request cost the v0.29.0 measurement named (~0.06ms a
  dispatch) is now paid once per batch instead of once per key —
  one `mget` RPC serves up to 1000 keys through the same
  ReadIndex/lease path as a single `get`, so it is exactly as
  linearizable, with `values` aligned with `keys` (None for a
  missing key) and the same `read_index` / `read_term` a session
  uses to advance its read watermark. `_serve_read` serves both
  shapes from one code path (a `_values()` payload built under the
  lock at the same three return sites), the read-parameter
  validation is shared too (`_parse_read_params`), and the client
  exposes it as `client.mget(keys)`, `Session.mget(keys)` (same
  watermark, same read-your-writes guarantees), and `mget k1 k2
  ...` on the CLI (one JSON list). `scripts/bench.py --mget`
  measures it (`--mget-batch` sets the batch size). Measured
  honestly on this shared box: a batch of 50 costs ~0.2ms total,
  ~0.004-0.005ms per key — ~12x cheaper per key than 50 pipelined
  gets in the same runs — and the batch time barely grows with
  size (10: ~0.1ms, 200: ~0.3ms), so the per-key rate is a
  property of the batch, not of independent clients: the
  sustained concurrent ceiling is unchanged, and a batch of 10 is
  barely faster per key than pipelining. Building this caught a
  real bug in the first draft: `_serve_read` gained its `keys`
  parameter ahead of `min_index`, so the single-key caller's
  positional `min_index` landed in `keys` and every plain `get`
  failed with "'int' object is not iterable" — the parameter order
  now keeps the old positional call working, and the single-key
  call site passes its arguments by keyword. `tests/test_mget.py`
  proves it: values in order, missing keys, redirect from a
  follower, stale service from a follower lease, bad key lists
  and bad read params rejected, session read-your-writes, and
  agreement with pipelined `get_many` on the same 50 keys.
- ~~Key enumeration~~ — done in v0.31.0, Nov 23, 2026: a `keys`
  RPC lists the store's keys through the same ReadIndex/lease path
  as a single `get`, so a scan is exactly as linearizable — it
  reflects the committed state as of one read index, never a
  half-applied prefix. `KVStore.keys(prefix)` returns the matching
  keys sorted (sorted so the answer is deterministic and a
  limit-based re-scan is well-defined); `_serve_read` serves the
  scan from the same `_values()` payload builder as `get`/`mget`,
  with `scan_prefix` / `scan_limit` parameters alongside `key` /
  `keys`, so the lease fast path, the pending-read wait, the
  fallback, and the redirect on leadership loss are all shared,
  not re-implemented. The reply carries `keys` (at most
  `MAX_KEYS_SCAN` = 1000, the same cap rationale as mget) plus
  `truncated` when more matched, and the same `read_index` /
  `read_term` a session uses to advance its read watermark. The
  client exposes it as `client.keys(prefix, limit)`,
  `Session.keys(prefix, limit)` (same watermark, same
  read-your-writes guarantees), and `keys --prefix p --limit N`
  on the CLI (one JSON list). `tests/test_keys.py` proves it: all
  keys sorted, prefix filtering, limit truncation, empty store,
  deletes reflected, redirect from a follower, stale service from
  a follower lease, bad prefixes/limits and bad read params
  rejected, session read-your-writes, and the CLI end to end.
- ~~Cursor-based paging for key enumeration~~ — done in v0.32.0,
  Nov 23, 2026: a `keys` scan takes a `start_after` cursor, an
  exclusive lower bound on the sorted key order, so a client pages
  through a large keyspace in bounded calls instead of re-scanning
  from the start — pass back the previous page's last key, or the
  reply's `next_start_after` (the last key returned when
  `truncated`, else None). `KVStore.keys(prefix, start_after)`
  filters after the cursor before sorting; `_serve_read` gained a
  `scan_start_after` parameter alongside `scan_prefix` /
  `scan_limit`, so the lease fast path and the pending-read wait
  still serve every page exactly like a single `get`. The client
  exposes it as `client.keys(prefix, limit, start_after)`,
  `Session.keys(prefix, limit, start_after)`, and `keys
  --start-after K` on the CLI, plus `keys_all` (module-level and
  on Session) which walks the pages and returns every key plus
  the page count. Documented honestly: each page is linearizable
  on its own, but the walk as a whole is not a snapshot — a key
  written or deleted between pages can be missed or appear in a
  later page — and the server still sorts the whole matching
  keyspace per page, so paging bounds the reply, not the work.
  `tests/test_keys.py` proves it: paging in order with no repeats
  or gaps, cursor with a prefix, cursor past the last key,
  one-key-at-a-time walk of the whole keyspace, bad cursors
  rejected server- and client-side, session paging and
  `keys_all`, and the CLI `--start-after` end to end.
- ~~Sorted index for key enumeration~~ — done in v0.33.0,
  Nov 23, 2026: the state machine keeps its keys in sorted
  order (`KVStore._keys`, a list maintained with `bisect` on
  write), so a scan page seeks to its cursor and walks at most
  one page past it — O(page + log n) instead of sorting the
  whole matching keyspace per page. `KVStore.keys(prefix,
  start_after, limit)` now returns `(page, truncated)` instead
  of the full sorted list; `_serve_read` serves the page from
  the same `_values()` payload builder as before, so the lease
  fast path, the pending-read wait, the fallback, and the
  redirect on leadership loss are all unchanged, and the reply
  shape (`keys`, `truncated`, `next_start_after`) is identical
  — no client or CLI change. The index is derived state:
  replaying the log rebuilds it entry by entry and `load`
  rebuilds it from snapshot state, so a restarted or
  snapshot-installed node scans exactly like one that replayed.
  Documented honestly: the index is a plain Python list, so a
  write of a new key or a delete of an existing one pays an
  O(n) memmove shifting the tail, and the index holds one
  pointer per key on top of the dict — a balanced tree would
  remove the memmove, but it would need a dependency or a
  hand-rolled tree for a store whose writes are fsync-bound
  anyway. `tests/test_keys.py` proves it: the index tracks
  sets, overwrites, deletes, re-sets, and CAS without drifting
  from the store's keys; prefix, cursor, and limit compose on
  the index; a page returns only the page; the index survives
  a snapshot load; paging 120 keys ten at a time returns every
  key in order; and a follower restarted after a snapshot
  serves a paged stale scan from its rebuilt index. The roadmap
  is exhausted again — every item is done — so the next real
  thing is a new roadmap item grounded in this project's
  purpose, proposed by the next run.
- ~~Batch set (mset)~~ — done in v0.34.0, Nov 23, 2026: one
  `mset` RPC writes many key/value pairs as a single log entry
  (`KVStore.apply` gains an `"mset"` op applying the pairs in
  order, duplicate key keeps its last value, the sorted index
  maintained per pair), so the batch applies atomically at one
  log position — a read after it commits sees every pair, never
  a prefix. It rides the same write path as a single `set`:
  `_on_mset` validates the batch (a list of 1-1000 `[key,
  value]` string pairs, `MAX_MSET_PAIRS`) and submits it
  through `_submit_write`, so group-commit batching, dedup by
  session id, and the response's log index/term all work
  unchanged; the apply path fires one watch event per pair, in
  batch order, all at the entry's index. The client exposes it
  as `client.mset(pairs)`, `Session.mset(pairs)` (same
  watermark, same read-your-writes guarantees), and `mset k1 v1
  k2 v2 ...` on the CLI (an odd argument count is a usage error,
  not a half-written batch). `tests/test_mset.py` proves it: 50
  pairs in one log entry, overwrite and last-duplicate-wins,
  replication to every follower as one entry, redirect from a
  follower, bad pair lists rejected, session read-your-writes,
  a dedup retry appending once, one watch event per pair at the
  batch index, mset keys in the sorted scan, and the CLI end to
  end.
- ~~Pipelined batch writes~~ — done in v0.35.0, Nov 23, 2026: a
  client-side batch-write path that pipelines many set/del/cas
  ops over one connection (`Session.pipeline(ops, depth=50)`,
  `client.pipeline(addr, ops)`, and `pipeline` on the CLI
  reading JSON ops one per line from stdin and printing one
  JSON response per line). Every op carries its own session
  request id, so a batch re-sent after a redirect, or a reply
  lost to a dead connection and retried once with the same ids,
  returns the first attempt's result instead of appending a
  second entry — the dedup table is what makes pipelining
  writes safe. Replies come back aligned by position, a failed
  op an `error` response in its place, a malformed op rejected
  client-side without going on the wire, and the session's write
  watermark advancing to the highest index any reply carries.
  Documented honestly: each op is still its own log entry with
  its own fsync, so the batch is not atomic and a batch of pure
  sets is cheaper as one `mset`; and the server dispatches
  frames from one connection in order, so pipelined writes
  never overlap server-side and cannot share an fsync — measured
  on this box at ~85-96 ops/s against ~84-99 sequential, no
  gain on localhost, the win being one connection and no per-op
  round trip over a real network RTT. `scripts/bench.py
  --pipelined-writes` adds the phase. `tests/
  test_pipeline_writes.py` proves it: mixed set/del in order,
  a failed CAS aligned in the middle with the ops around it
  still applied, malformed ops as errors in position, redirect
  from a follower applying each op exactly once, the session
  watermark advancing, 120 ops split by depth, a fake server
  swallowing a batch and the retry reusing the same request
  ids, and the CLI end to end.
- ~~TTL / key expiry in log-index units~~ — done in v0.36.0,
  Nov 23, 2026: a `set` may carry `ttl_entries`, and the key's
  deadline is its entry's log index + the TTL — every node
  purges the key before applying the entry at that index, so
  the key vanishes on every replica at exactly the same log
  position, with no wall clock anywhere (`KVStore._expiry`,
  `expire_up_to` called from `_apply_committed` before every
  entry type, since the log advancing is what moves TTL time).
  A plain `set` clears the deadline, as do an `mset` without
  a TTL and `del`; a successful CAS keeps it. The deadline rides in the
  snapshot (`snapshot.json` and the InstallSnapshot payload
  both carry an `expiry` map, loaded with the state, so a
  node that installs one keeps expiring on schedule), and
  expiry fires a `del` watch event at the purging entry's
  index. The client exposes it as `Session.set(k, v,
  ttl_entries=N)`, `set k v --ttl-entries N` on the CLI, and
  `"ttl_entries"` in a pipelined set op; the leader validates
  it (an integer 1..1,000,000, `TTL_MAX_ENTRIES`) before the
  command is ever logged. Documented honestly: the unit is log
  entries, not seconds — on a quiet cluster a key can live
  arbitrarily long in wall-clock time, and on a busy cluster
  it can expire quickly — and `mset` takes no TTL yet (that
  came in v0.39.0), and there is no `expire`/`ttl` query to
  read a key's remaining entries back. `tests/test_ttl.py` proves it: store-level deadline
  arithmetic, plain set / mset / del clearing the deadline,
  CAS keeping it, a TTL with no log index raising, snapshot
  load restoring the deadlines, expiry at the exact deadline
  on leader and followers, CAS on an expired key failing as
  missing_key, the expired key leaving the sorted scan, the
  `del` watch event at the purging index, the deadline
  surviving a leader kill and a snapshot + restart, bad TTLs
  rejected with no log entry appended, and the session,
  pipeline, and CLI paths end to end.
- ~~mset TTL~~ — done in v0.39.0, Nov 24, 2026: an `mset` may
  carry `ttl_entries`, and every pair in the batch gets the
  same deadline — the entry's log index + the TTL — so the
  whole batch expires together at one log position
  (`KVStore.apply` records `index + ttl` in `_expiry` for
  every pair; a TTL with no log index raises, like a single
  set's). The deadline rides the same purge path as any other
  TTL: `expire_up_to` drops every pair before the entry at
  the deadline applies, on every node, firing one `del` watch
  event per key at the purging entry's index, and the
  deadlines ride snapshots with the rest of `_expiry`, so a
  node that installs one keeps expiring the batch on schedule.
  A plain `mset` without a TTL still clears every pair's
  deadline, like a plain set. `_on_mset` validates the TTL on
  the leader (an integer 1..1,000,000, `TTL_MAX_ENTRIES`)
  before the command is ever logged, so a bad TTL is a client
  error and the batch is never written. The client exposes it
  as `Session.mset(pairs, ttl_entries=N)`, `client.mset(addr,
  pairs, ttl_entries=N)`, and `mset k1 v1 k2 v2 ...
  --ttl-entries N` on the CLI. `tests/test_mset.py` proves
  it: store-level deadline arithmetic for the whole batch, a
  TTL with no log index raising, a plain mset clearing the
  deadlines, expiry of the whole batch at the deadline on
  leader and followers with a `ttl` query counting it down,
  one `del` watch event per pair at the deadline index, the
  deadlines surviving a snapshot + restart, bad TTLs rejected
  with no log entry appended, and the session and CLI paths
  end to end.
- ~~Atomic counter (incr/incrby)~~ — done in v0.40.0,
  Nov 24, 2026: `incr key [by]` adds `by` (default 1) to the
  key's integer value and returns the new value as a string —
  a missing key counts as 0, a non-integer value fails as
  `incr_failed` / `not_an_integer`, a result outside the
  signed 64-bit range as `overflow`, both decided at apply
  time like a CAS, so a retry with the same session id
  returns the first attempt's exact result instead of
  incrementing twice. A successful incr keeps the key's TTL
  deadline, fires a `set` watch event with the new value, and
  rides the same write path as a `set` (group-commit
  batching, dedup, session watermark); `{"op": "incr"}` is a
  pipeline op too. `Session.incr`, `client.incr`, and `incr
  key [by]` on the CLI expose it. `tests/test_incr.py`
  proves it: store-level creation, negative and zero deltas,
  strict integer parsing (whitespace, underscores, and
  decimals rejected), out-of-range values and overflow,
  deadline kept across an incr and expiry still firing, the
  new key joining the sorted index, replication to every
  node, redirect from a follower, dedup of a successful and
  a failed retry, continuing after a leader change, the
  watch event on leader and follower with nothing fired on
  failure, surviving a snapshot + restart, session
  read-your-writes, pipelined incr ops with a malformed `by`
  in position, and the CLI end to end. The roadmap is
  exhausted again — group commit, the item the watermark
  named as next, was already shipped in v0.7.0 (and made
  configurable in v0.26.0), so the next real thing is a new
  roadmap item grounded in this project's purpose, proposed
  by the next run.
- ~~Set-if-not-exists (setnx)~~ — done in v0.41.0,
  Nov 24, 2026: `setnx key value` sets the key only when it
  is absent and answers 1 when it set it, 0 when the key
  already existed (the existing value untouched) — Redis
  SETNX, decided at apply time like a CAS, so two concurrent
  setnx on the same key race in log order and exactly one
  wins. A retry with the same session id returns the first
  attempt's result, a successful setnx fires a `set` watch
  event and rides the same write path as a `set`
  (group-commit batching, dedup, session watermark), and
  `{"op": "setnx"}` is a pipeline op too.
  `Session.setnx`, `client.setnx`, and `setnx key value` on
  the CLI expose it. `tests/test_setnx.py` proves it:
  store-level set and no-op, setting again after a delete,
  the new key joining the sorted index, replication to every
  node, a losing setnx leaving the value unchanged
  everywhere, a missing value rejected with no log entry,
  redirect from a follower, dedup of a winning and a losing
  retry, continuing after a leader change, the watch event
  on leader and follower with nothing fired on a loss,
  surviving a snapshot + restart, session
  read-your-writes, pipelined setnx ops with a missing value
  in position, and the CLI end to end. The roadmap is
  exhausted again, so the next real thing is a new roadmap
  item grounded in this project's purpose, proposed by the
  next run — candidates: getset-style string ops, or a
  wall-clock TTL variant.
- ~~Get-and-set (getset)~~ — done in v0.42.0,
  Nov 24, 2026: `getset key value` atomically sets the key
  and answers with the value it held before — `old_value`,
  None when the key did not exist, which the getset creates
  — Redis GETSET, decided at apply time like a CAS, so a
  retry with the same session id returns the first attempt's
  old value instead of setting twice, and a second getset
  sees the first one's value as the old value — the atomic
  swap Redis promises. A getset always writes, so it always
  fires a `set` watch event with the new value, keeps the
  key's TTL deadline like a CAS, and rides the same write
  path as a `set` (group-commit batching, dedup, session
  watermark); `{"op": "getset"}` is a pipeline op too.
  `Session.getset`, `client.getset`, and `getset key value`
  on the CLI expose it. `tests/test_getset.py` proves it:
  store-level missing-key None and replace, answering None
  after a delete, the TTL deadline kept across a getset and
  expiry still firing, the new key joining the sorted index,
  replication to every node, a missing value rejected with
  no log entry, redirect from a follower, dedup of a retry
  returning the first attempt's old value even after another
  getset moved the value, continuing after a leader change,
  the watch event on leader and follower with the new value
  on a missing and an existing key, surviving a snapshot +
  restart, session read-your-writes, pipelined getset ops
  with a missing value in position, and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: a wall-clock TTL
  variant, or more Redis string ops (append, strlen, getdel).
- ~~Append / strlen / getdel~~ — done in v0.43.0,
  Nov 24, 2026: `append key value` appends to the string at
  the key and answers with the new length — Redis APPEND,
  decided at apply time like a CAS, so a retry with the same
  session id returns the first attempt's length instead of
  appending twice; a missing key is created, a successful
  append fires a `set` watch event with the new value and
  keeps the key's TTL deadline, and a non-string value fails
  with `append_failed` / `wrongtype`. `strlen key` answers
  the length of the string value, 0 for a missing key —
  Redis STRLEN — through the same ReadIndex path as a `get`.
  `getdel key` atomically answers with the value and deletes
  the key — Redis GETDEL — answering None and `getdel` 0 on
  a missing key, firing a `del` watch event when it found
  one, and taking the key's TTL deadline with it; a retry
  returns the first attempt's value instead of deleting a
  key a later write recreated. `{"op": "append"}` and
  `{"op": "getdel"}` are pipeline ops too.
  `Session.append` / `Session.strlen` / `Session.getdel`,
  `client.append` / `client.strlen` / `client.getdel`, and
  `append` / `strlen` / `getdel` on the CLI expose them.
  `tests/test_string_ops.py` proves them: store-level
  append create and concatenate, the TTL deadline kept
  across an append, the wrongtype failure leaving the value
  untouched, getdel returning the old value and dropping the
  key from the sorted index and its deadline, strlen on a
  missing, empty, and wrongtype value, replication to every
  node, a missing value rejected with no log entry, redirect
  from a follower, dedup of an append and a getdel retry,
  continuing after a leader change, the watch events on
  leader and follower with nothing fired on a miss or a
  failed append, surviving a snapshot + restart, session
  read-your-writes, pipelined append and getdel ops with a
  missing value in position, and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: a wall-clock TTL
  variant, or more Redis string ops (getrange, setrange).
- ~~Getrange / setrange~~ — done in v0.44.0,
  Nov 24, 2026: `getrange key start end` answers the substring
  from start to end inclusive — Redis GETRANGE — negative indexes
  counting back from the end, out-of-range indexes clamped, a
  missing key answering "", through the same ReadIndex path as a
  `get`. `setrange key offset value` overwrites the string at the
  key from offset and answers with the new value and length —
  Redis SETRANGE — a missing key counting as empty, a gap past
  the end padded with zero bytes, decided at apply time like a
  CAS, so a retry with the same session id returns the first
  attempt's value and length instead of overwriting twice; a
  successful setrange fires a `set` watch event with the new
  value and keeps the key's TTL deadline, and a non-string value
  fails with `setrange_failed` / `wrongtype`. An offset must be
  an integer from 0 to 1,000,000 (`SETRANGE_MAX_OFFSET`); a
  negative or over-cap offset is a client error and appends
  nothing. `{"op": "setrange"}` is a pipeline op too.
  `Session.getrange` / `Session.setrange`,
  `client.getrange` / `client.setrange`, and `getrange` /
  `setrange` on the CLI expose them.
  `tests/test_string_ops.py` proves them: store-level getrange
  whole and partial, negative indexes, clamped out-of-range,
  missing key and empty value, wrongtype, setrange create,
  overwrite in the middle and past the end, zero-byte padding
  on an existing and a missing key, the TTL deadline kept
  across a setrange, the new key joining the sorted index,
  wrongtype leaving the value untouched, and a negative offset
  raising; replication to every node, a missing value and an
  invalid offset rejected with no log entry, redirect from a
  follower, dedup of a setrange retry returning the first
  attempt's value even after another write moved it,
  continuing after a leader change, the watch event on leader
  and follower with the new value, surviving a snapshot +
  restart, session read-your-writes, pipelined setrange ops
  with a bad offset and a missing value in position, and the
  CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: a wall-clock TTL
  variant, or GETEX (get-and-expire), or Redis bit ops
  (SETBIT / GETBIT).
- ~~GETEX~~ — done in v0.45.0,
  Nov 24, 2026: `getex key [EX n | PX n | EXAT t | PXAT t |
  PERSIST]` answers with the value the key held and optionally
  sets, refreshes, or clears its expiry in the same atomic
  operation — Redis GETEX — decided at apply time like a CAS,
  so a retry with the same session id returns the first
  attempt's value and deadline instead of re-expiring. EX / PX
  name a relative deadline (this entry's index + N, in the
  store's log-entry units — the Redis seconds / milliseconds
  are read as log entries, since this store has no wall clock);
  EXAT / PXAT name the absolute log index of the deadline;
  PERSIST clears it; no option leaves the value and the
  deadline both alone. A missing key answers None with `getex`
  0, a no-op; an absolute deadline already past deletes the key
  in its own entry, like Redis. A pure expiry change fires no
  watch event — only the purge fires a `del` event, when the
  deadline entry lands — except the past-deadline delete, which
  fires `del` in its own entry. At most one option: two options,
  an unknown option, or a bad value is a client error and
  appends nothing. `{"op": "getex"}` is a pipeline op too.
  `Session.getex` / `client.getex`, and `getex` on the CLI
  expose it.
  `tests/test_getex.py` proves it: store-level EX, PX, EXAT,
  PXAT, PERSIST, no-option, missing key, replacing a deadline,
  the past absolute deadline deleting the key and dropping it
  from the sorted index, and a relative deadline with no index
  raising; replication to every node, all five option forms
  through the TTL query, a missing key still committing one
  entry, PERSIST clearing a deadline, the past EXAT deleting
  the key on every node, no watch event on a pure expiry change
  and a del event at the purge and at a past-deadline delete,
  invalid option combinations rejected with no log entry,
  dedup of a retry returning the first attempt's value and
  deadline, redirect from a follower, surviving a snapshot +
  restart, continuing after a leader change, session
  read-your-writes, pipelined getex ops with a malformed op in
  position, and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: a wall-clock TTL
  variant, or Redis bit ops (SETBIT / GETBIT), or HyperLogLog
  (PFADD / PFCOUNT).
- ~~SETBIT / GETBIT~~ — done in v0.46.0,
  Nov 24, 2026: `setbit key offset bit` sets one bit in the
  string at the key and answers with the old bit value and the
  new value, `getbit key offset` answers the bit at the offset
  — Redis SETBIT / GETBIT — bit 0 the most significant bit of
  the first byte, a missing key counting as empty, a gap past
  the end padded with zero bytes, a getbit past the end
  answering 0. Decided at apply time like a CAS, so a retry
  with the same session id returns the first attempt's old bit
  instead of setting twice; a successful setbit fires a `set`
  watch event with the new value and keeps the key's TTL
  deadline, and a value that is not a byte string fails with
  `setbit_failed` / `wrongtype`. An offset must be an integer
  from 0 to 1,000,000 (`SETBIT_MAX_OFFSET`); a negative offset
  or a bit other than 0 or 1 is a client error and appends
  nothing. A getbit rides the same ReadIndex path as a `get`.
  `{"op": "setbit"}` is a pipeline op too.
  `Session.setbit` / `Session.getbit`,
  `client.setbit` / `client.getbit`, and `setbit` /
  `getbit` on the CLI expose them.
  `tests/test_bit_ops.py` proves them: store-level setbit on a
  missing key, setting and clearing bits across byte
  boundaries, the old bit returned, zero-byte padding past the
  end, getbit on a missing key and past the end, the TTL
  deadline kept across a setbit, the new key joining the sorted
  index, wrongtype leaving the value untouched, and a negative
  offset and a bad bit raising; replication to every node, an
  invalid offset and bit rejected with no log entry, redirect
  from a follower, dedup of a setbit retry returning the first
  attempt's old bit, continuing after a leader change, the
  watch event on leader and follower with the new value,
  surviving a snapshot + restart, session read-your-writes,
  pipelined setbit ops with a bad offset and bit in position,
  and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: a wall-clock TTL
  variant.
- ~~HyperLogLog (PFADD / PFCOUNT)~~ — done in v0.47.0,
  Nov 24, 2026: `pfadd key element [element ...]` adds elements
  to the HyperLogLog at the key and answers `pfadd` 1 when at
  least one register rose and 0 when every element was already
  counted, plus `estimate`, the cardinality estimate after the
  add, and `pfcount key` answers the estimate, 0 when the key
  does not exist — Redis PFADD / PFCOUNT. The registers are
  16384 one-byte registers (precision 14, Redis's default),
  hashed with SHA-256 so every node applying the same log
  computes the same bytes, and the estimate is the standard
  HyperLogLog one with the small- and large-range corrections —
  measured under 1% error at 10,000 and 100,000 distinct
  elements. The registers ride in the keyspace as a latin-1
  string with a marker prefix, so snapshots, the sorted index,
  TTL expiry, and `get` treat an HLL key like any other key,
  and a plain string fails a pfadd with `pfadd_failed` /
  `wrongtype` instead of writing. Decided at apply time like a
  CAS, so a retry with the same session id returns the first
  attempt's pfadd flag and estimate instead of adding twice; a
  successful pfadd fires a `set` watch event with the new value
  only when a register rose, and keeps the key's TTL deadline.
  At most 1000 elements per pfadd. A pfcount rides the same
  ReadIndex path as a `get`. `{"op": "pfadd"}` is a pipeline op
  too. `Session.pfadd` / `Session.pfcount`,
  `client.pfadd` / `client.pfcount`, and `pfadd` / `pfcount`
  on the CLI expose them.
  `tests/test_hyperloglog.py` proves them: store-level pfadd on
  a missing key, re-adding a counted element answering 0 with
  the blob byte-identical, pfcount on a missing key, accuracy
  within 5% at 1000 elements and 3% at 10000, order-independent
  blobs, the TTL deadline kept across a pfadd, the new key
  joining the sorted index, wrongtype leaving the value
  untouched, bad elements raising, and the HLL surviving a
  snapshot load; replication to every node with identical blobs,
  invalid pfadds rejected with no log entry, wrongtype
  replicating with pfcount erroring, dedup of a pfadd retry
  returning the first attempt's flag and estimate, the watch
  event on leader and follower only when a register rose,
  continuing after a leader change, surviving a snapshot +
  restart, session read-your-writes, pipelined pfadd ops with
  malformed ops in position, and the CLI end to end.
- ~~Wall-clock TTL (setex)~~ — done in v0.48.0,
  Nov 24, 2026: `setex key value seconds` writes the key with a
  wall-clock TTL — Redis SETEX — the leader fixing the absolute
  deadline (its wall clock at log time + seconds) in the log
  entry itself, so every replica applies the same deadline, and
  the key dies when the wall clock reaches it: a background sweep
  every 100ms, a purge before each log entry applies, and a purge
  before serving a read, so a read never returns a key whose
  deadline passed between sweeps. A plain `set` clears a wall
  deadline and a `setex` clears a log deadline, so a key holds at
  most one kind of TTL; a successful CAS or incr keeps the wall
  deadline, like it keeps the log one; the deadline rides
  snapshots, and expiry fires a `del` watch event. The `ttl`
  query reports the wall side too — `wall_ttl` the seconds left
  and `wall_deadline` the absolute Unix time. Seconds must be an
  integer from 1 to 1,000,000 (`WALL_TTL_MAX_SECONDS`); a bad
  value is a client error and appends no log entry. It rides the
  same write path as a `set` (group-commit batching, dedup,
  session watermark), and `{"op": "setex"}` is a pipeline op too.
  `Session.setex`, `client.setex`, and `setex key value seconds`
  on the CLI expose it. `tests/test_wall_ttl.py` proves it:
  store-level setex recording its absolute deadline, a missing
  deadline raising, plain set / mset / del clearing the wall
  deadline, CAS and incr keeping it, setex clearing the log
  deadline, `wall_ttl` in Redis's convention, and a snapshot load
  restoring the wall deadlines; replication to every node with
  the key readable then gone past the deadline, the TTL query
  reporting the wall deadline, bad seconds rejected with no log
  entry, a plain set clearing the wall deadline on the cluster,
  the `del` watch event firing from the sweep with no log entry,
  surviving a snapshot + restart with the key still expiring at
  its original deadline, the expired key leaving the sorted
  scan, session read-your-writes, pipelined setex ops with a bad
  seconds value in position, and the CLI end to end.
- ~~Wall-clock `expireat` / `pexpireat`~~ — done in v0.49.0,
  Nov 24, 2026: `expireat key timestamp` / `pexpireat key
  timestamp_ms` set an absolute wall-clock deadline on a key
  that already exists — Redis EXPIREAT / PEXPIREAT, the
  wall-clock analogue of `expire`: the value is untouched, only
  the deadline changes, and a log-entry deadline the key had is
  dropped, so a key holds at most one kind of TTL. They answer 1
  when the key exists (the reply also carries `wall_deadline`,
  the absolute Unix time in seconds) and 0 when it does not, and
  a deadline already past deletes the key outright, still
  answering 1 with `deleted` set, and firing a `del` watch event.
  The leader fixes the deadline at log time (milliseconds divided
  by 1000 for `pexpireat`), so every replica applies the same
  deadline and it rides snapshots; the key then dies by the same
  three enforcements as a `setex` deadline — the background sweep,
  the purge before each log entry applies, and the purge before
  serving a read. A timestamp of 0, negative, non-integer, or
  boolean is a client error and appends no log entry. It rides
  the same write path as an `expire` (group-commit batching,
  dedup, session watermark), and `{"op": "expireat"}` /
  `{"op": "pexpireat"}` are pipeline ops too.
  `Session.expireat` / `Session.pexpireat`,
  `client.expireat` / `client.pexpireat`, and `expireat` /
  `pexpireat` on the CLI expose them.
  `tests/test_wall_ttl.py` proves them: store-level expireat
  recording its absolute deadline and clearing the log deadline,
  a missing key answering 0, a pexpireat recording the deadline
  in seconds, re-expiring replacing the deadline, a past
  deadline deleting the key outright (and still answering 0 on a
  missing key), and a missing deadline raising; replication to
  every node with the key readable then gone past the deadline,
  a missing key answering 0, a past deadline deleting immediately
  with the `del` watch event firing, a pexpireat in milliseconds
  with the TTL query reporting the deadline, bad timestamps
  rejected with no log entry, an expireat clearing the log
  deadline on the cluster, session read-your-writes, pipelined
  expireat / pexpireat ops with a bad timestamp in position, and
  the CLI end to end.
- ~~SET with NX / XX / KEEPTTL flags~~ — done in v0.50.0,
  Nov 24, 2026: `set key value [--nx | --xx] [--keepttl]` carries
  Redis's SET options — NX sets only when the key does not exist,
  XX only when it does, KEEPTTL preserves the deadline a plain set
  would clear (log-entry or wall-clock) — decided at apply time
  like a CAS, so a retry with the same session id returns the
  first attempt's result instead of setting twice. The reply
  carries `set` 1 when the key was written, 0 when the condition
  failed; a losing set writes nothing at all and fires no watch
  event, a winning one fires a `set` event with the new value.
  NX and XX together, or KEEPTTL with a fresh TTL, is a client
  error and appends no log entry. A plain `set` answers `set` 1
  too. `{"op": "set", "key": k, "value": v, "nx": true}` is a
  pipeline op too. `Session.set` and `set` on the CLI expose
  them.
  `tests/test_set_flags.py` proves them: store-level plain set
  answering 1, NX winning on a missing key and losing on an
  existing one with value and deadline untouched, XX winning on
  an existing key and losing on a missing one with the key still
  absent, NX winning again after a delete, KEEPTTL keeping a
  log-entry deadline and a wall-clock deadline across an
  overwrite, KEEPTTL on a missing key recording no deadline, and
  XX with KEEPTTL composing; replication to every node, a losing
  NX and a losing XX changing nothing on any node, invalid flag
  combinations rejected with no log entry, dedup of a winning
  and a losing retry returning the first attempt's result,
  KEEPTTL keeping both kinds of deadline on the cluster, the
  watch event firing only on a win, continuing after a leader
  change, surviving a snapshot + restart, session
  read-your-writes with the watermark advancing on a losing set
  too, pipelined set ops with flags and malformed ops in
  position, and the CLI end to end.
- ~~SET with GET~~ — done in v0.51.0,
  Nov 24, 2026: `set key value [--get]` carries Redis's SET GET
  option — one round trip both swaps the value and reports what
  it replaced: the reply carries `old_value`, the value the key
  held at the entry's exact position in the log, or None when
  the key was absent, and a losing conditional set answers
  `old_value` None, like Redis answering nil when the
  condition fails. GET changes nothing about the write: a
  winning GET set clears the deadline like a plain set, or
  keeps it with KEEPTTL, and composes with NX / XX and with a
  fresh TTL. It rides the same write path as a single `set` —
  group-commit batching, dedup of a retry returning the first
  attempt's old value, the response's log index advancing a
  session's watermark — and `{"op": "set", "key": k, "value":
  v, "get": true}` is a pipeline op too. `Session.set` and
  `set` on the CLI expose it.
  `tests/test_set_get.py` proves it: store-level old value on
  an existing key and None on a missing one, a losing NX and a
  losing XX answering `old_value` None with value and deadline
  untouched, GET clearing a log-entry and a wall-clock deadline
  like a plain set and KEEPTTL with GET keeping them, a winning
  GET set firing the watch event and a losing one firing
  nothing, replication to every node, dedup of a winning and a
  losing retry returning the first attempt's old value,
  pipelined set ops with GET and a malformed GET in position,
  and the CLI end to end.
- ~~SET with EX / PX~~ — done in v0.52.0,
  Nov 24, 2026: `set key value [--ex seconds | --px
  milliseconds]` carries Redis's SET EX / PX options — an
  inline wall-clock TTL on SET, instead of the separate
  `setex` op: the leader fixes the absolute deadline at log
  time (now + seconds, now + milliseconds / 1000) and stores
  it in the entry as `wall_deadline`, so every replica applies
  the same deadline and the key dies by the same three
  enforcements as a `setex` deadline — the background sweep
  every 100ms, the purge before each log entry applies, and
  the purge before serving a read — with a `del` watch event
  from the sweep when it goes. A winning EX/PX set is a fresh
  life for the key: any log-entry deadline it had is dropped,
  so a key still holds at most one kind of TTL, and a plain
  `set` clears an EX/PX deadline like any other wall deadline.
  EX and PX are mutually exclusive, and neither rides with a
  log-entry TTL or with KEEPTTL — all three are client errors
  that append no log entry — and the values are bounded
  (EX 1..1,000,000 seconds, PX 1..1,000,000,000
  milliseconds, the same horizon in both units). It composes
  with NX / XX and with GET exactly like the other flags: a
  losing conditional set writes nothing, deadline included,
  and answers `old_value` None with GET. The `ttl` query
  reports the deadline it sets, in `wall_ttl` /
  `wall_deadline`, exactly like a `setex` deadline. It rides
  the same write path as a single `set` — group-commit
  batching, dedup (a retry returns the first attempt's result
  and the deadline stays the first attempt's), the response's
  log index advancing a session's watermark — and `{"op":
  "set", "key": k, "value": v, "ex": n}` is a pipeline op too.
  `Session.set` (with `ex=` / `px=`) and `set` on the CLI
  (`--ex` / `--px`, mutually exclusive) expose it.
  `tests/test_set_ex_px.py` proves it: store-level EX
  recording a wall deadline and dropping a log deadline, GET
  with EX reporting the old value, a plain set clearing an EX
  deadline, a losing NX and a losing XX with EX writing
  nothing, and the expired key leaving the sorted scan;
  replication to every node with the key readable then gone
  past the deadline for EX and for PX, the TTL query
  reporting the deadline, an EX set clearing a log deadline
  on the cluster, invalid values and invalid combinations
  rejected with no log entry, dedup of a retry returning the
  first attempt's result with the first deadline kept, the
  `del` watch event firing from the sweep, surviving a
  snapshot + restart with the key still expiring at its
  original deadline, session read-your-writes, pipelined set
  ops with EX/PX and malformed ops in position, and the CLI
  end to end.
- ~~SET with EXAT / PXAT~~ — done in v0.53.0,
  Nov 24, 2026: `set key value [--exat timestamp | --pxat
  timestamp_ms]` carries Redis's SET EXAT / PXAT options —
  an absolute wall-clock deadline inline on SET, the absolute
  analogue of EX / PX: EXAT names the deadline as Unix
  seconds, PXAT as Unix milliseconds, and the timestamp
  itself is the deadline, recorded as-is by the leader, so
  every replica applies the same one and the key dies by the
  same three enforcements as a `setex` deadline — the
  background sweep every 100ms, the purge before each log
  entry applies, and the purge before serving a read. A
  winning EXAT/PXAT set is a fresh life for the key: any
  log-entry deadline it had is dropped, so a key still holds
  at most one kind of TTL, and a plain `set` clears an
  EXAT/PXAT deadline like any other wall deadline. EXAT and
  PXAT are mutually exclusive, and neither rides with EX or
  PX, with a log-entry TTL, or with KEEPTTL — all are client
  errors that append no log entry — and a timestamp must be a
  positive integer, the same validation `expireat` /
  `pexpireat` use. A deadline already past at log time
  deletes the key outright at apply time, like an `expireat`
  whose deadline was already past: the set still answers
  `set` 1, with `deleted` set when a key actually went, and
  the apply path fires a `del` watch event for it, while a
  past deadline on a missing key writes nothing and fires
  nothing and a losing conditional set deletes nothing. It
  composes with NX / XX and with GET exactly like the other
  flags, and the `ttl` query reports the deadline in
  `wall_ttl` / `wall_deadline`. It rides the same write path
  as a single `set` — group-commit batching, dedup of a retry
  returning the first attempt's result with the first deadline
  kept, the response's log index advancing a session's
  watermark — and `{"op": "set", "key": k, "value": v,
  "exat": t}` is a pipeline op too. `Session.set` (with
  `exat=` / `pxat=`) and `set` on the CLI (`--exat` /
  `--pxat`, mutually exclusive with `--ex` / `--px`) expose
  it. `tests/test_set_exat_pxat.py` proves it: store-level
  EXAT recording a wall deadline and dropping a log deadline,
  GET with EXAT reporting the old value, a plain set clearing
  an EXAT deadline, a losing NX and a losing XX with EXAT
  writing nothing, a past deadline deleting an existing key
  at apply time with `deleted` set and the old value reported
  with GET, a past deadline on a missing key writing nothing,
  and a losing NX with a past deadline deleting nothing;
  replication to every node with the key readable then gone
  past the deadline for EXAT and for PXAT, the TTL query
  reporting the exact timestamp, an EXAT set clearing a log
  deadline on the cluster, invalid values and invalid
  combinations rejected with no log entry, dedup of a retry
  returning the first attempt's result with the first deadline
  kept, the `del` watch event firing at apply time for a past
  deadline and from the sweep for a future one, session
  read-your-writes, pipelined set ops with EXAT/PXAT and
  malformed ops in position, and the CLI end to end.
- ~~INCRBYFLOAT~~ — done in v0.54.0,
  Nov 24, 2026: `incrbyfloat key [by]` carries Redis's
  INCRBYFLOAT — an atomic float increment, the float analogue
  of `incr`: a missing key counts as 0, so the first
  incrbyfloat creates it at `by`, and the outcome is decided at
  apply time, at the entry's exact position in the log, like an
  incr — a non-float value fails distinctly (`not_a_float`,
  with the current value in the reply) from a result that
  would be NaN or Infinity (`overflow`), and a failed
  incrbyfloat writes nothing but still committed its entry, so
  a retry with the same session id returns the first attempt's
  exact result instead of adding twice. A successful
  incrbyfloat keeps the key's TTL deadline, like an incr, and
  fires a `set` watch event with the new value; a failed one
  fires nothing. An integer string is a valid float, so
  incrbyfloat on "41" works, and the stored value is the
  formatted float — an integral result reads "2", not "2.0",
  like Redis, and 0.1 + 0.2 reads "0.30000000000000004" — so a
  later incr on it fails, since "41.5" is not an integer
  string. It rides the same write path as a single `set` —
  group-commit batching, dedup, the response's log index
  advancing a session's watermark — and `{"op": "incrbyfloat",
  "key": k, "by": x}` is a pipeline op too. `Session.incrbyfloat`
  (with `by=`) and `incrbyfloat` on the CLI (a float `by`,
  default 1.0) expose it. `tests/test_incrbyfloat.py` proves
  it: store-level float adds and subtracts, an integer value
  taking a float increment, Redis-style result formatting,
  loose spellings ("1_0", "inf", "nan", "1e999") rejected,
  exponent spellings ("1e3", ".5") accepted, a non-finite
  result rejected with the value untouched, the TTL deadline
  kept, a new key joining the sorted scan; replication to
  every node, a non-float value failing with nothing written,
  a bad `by` (a string, a bool, infinity) rejected with no log
  entry, a follower redirect, dedup of a retry returning the
  first attempt's result and of a failed retry returning the
  first failure, continuing after a leader change, the `set`
  watch event firing with the new value and nothing firing on
  failure, surviving a snapshot + restart, session
  read-your-writes, pipelined incrbyfloat ops with a malformed
  op in position, and the CLI end to end.
- ~~Hashes (HSET / HGET)~~ — done in v0.55.0,
  Nov 24, 2026: `hset key field value [field value ...]` sets
  fields in the hash at the key and answers with the number of
  fields that did not exist before — Redis HSET — and `hget key
  field` answers the value of one field, None when the key or
  the field is missing — Redis HGET. A missing key is created
  as an empty hash; a field overwritten with a new value is not
  counted, and a field repeated in one call counts once, at its
  last value. The fields ride in the store as a JSON object
  with a marker prefix, like the HLL blob — dumped with sorted
  keys and no spaces, so every node computes the same bytes —
  and the hash survives snapshots and replay like any other
  value. The outcome is decided at apply time, at the entry's
  exact position in the log, like a CAS, so a retry with the
  same session id returns the first attempt's count instead of
  setting twice. A key holding a plain string fails an hset
  with `hset_failed` and reason `wrongtype`, and an hget on
  one is a client error; a successful hset keeps the key's TTL
  deadline, like a CAS, and fires a `set` watch event with the
  new value, while a failed one fires nothing. An hget rides
  the same ReadIndex path as a `get`, so it is exactly as
  linearizable. At most 1000 pairs per hset, like a pfadd
  batch. `Session.hset` / `Session.hget`,
  `client.hset` / `client.hget`, and `hset` / `hget` on the
  CLI expose them, and `{"op": "hset", "key": k, "pairs": [[f,
  v], ...]}` is a pipeline op too.
  `tests/test_hash.py` proves them: store-level creation and
  counting, overwrites not counted, a repeated field counting
  once, missing key and missing field answering None, a plain
  string failing wrongtype with nothing written, an hget on a
  plain string raising, malformed pairs raising, the TTL
  deadline kept (set from expire), a new key joining the sorted
  scan, and a hash surviving a snapshot load; replication to
  every node, a missing key and field answering None on the
  cluster, a plain string failing with nothing written, an
  hget on a plain string a client error, bad pairs rejected
  with no log entry, a follower redirect, dedup of a retry
  returning the first attempt's count and of a failed retry
  returning the first failure, continuing after a leader
  change, the `set` watch event firing with the new value and
  nothing firing on failure, surviving a snapshot + restart
  with the hash still readable, session read-your-writes,
  pipelined hset ops with a malformed op in position, and the
  CLI end to end.
- ~~Hashes (HDEL / HGETALL)~~ — done in v0.56.0,
  Nov 24, 2026: `hdel key field [field ...]` deletes fields
  from the hash at the key and answers with the number of fields
  actually removed — Redis HDEL — and `hgetall key` answers the
  whole field map as one JSON object, an empty dict when the key
  does not exist — Redis HGETALL. A missing key or a missing
  field counts 0, not an error, and a field repeated in one call
  counts once; removing the last field deletes the key outright,
  like Redis — the key leaves the sorted index and takes its
  deadline with it. The outcome is decided at apply time, at the
  entry's exact position in the log, like a CAS, so a retry with
  the same session id returns the first attempt's count instead
  of deleting twice. A key holding a plain string fails an hdel
  with `hdel_failed` and reason `wrongtype`, and an hgetall on
  one is a client error; an hdel that leaves fields behind keeps
  the key's TTL deadline, like an hset, and fires a `set` watch
  event with the new value, an hdel that takes the last field
  fires a `del` event instead, and an hdel that removes nothing
  fires nothing. An hgetall rides the same ReadIndex path as a
  `get`, so it is exactly as linearizable. At most 1000 fields
  per hdel, like an hset batch. `Session.hdel` /
  `Session.hgetall`, `client.hdel` / `client.hgetall`, and
  `hdel` / `hgetall` on the CLI expose them, and `{"op":
  "hdel", "key": k, "fields": [f, ...]}` is a pipeline op too.
  `tests/test_hash.py` proves them: store-level removal and
  counting, a repeated field counting once, missing key and
  missing field answering 0, removing the last field deleting
  the key (sorted index, deadline, and plain `get` all agree it
  is gone), a plain string failing wrongtype with nothing
  written, an hgetall on a plain string raising, malformed
  fields raising, the TTL deadline kept while fields remain,
  and the whole field map surviving a snapshot load;
  replication to every node, a missing key and field answering
  0 on the cluster, a plain string failing with nothing
  written, an hgetall on a plain string a client error, bad
  fields rejected with no log entry, a follower redirect, dedup
  of a retry returning the first attempt's count and of a failed
  retry returning the first failure, continuing after a leader
  change, the `set` watch event firing while fields remain, the
  `del` event firing when the last field goes, and nothing
  firing when nothing is removed, surviving a snapshot +
  restart with the hash still listable, session
  read-your-writes, pipelined hdel ops with a malformed op in
  position, and the CLI end to end.
- ~~Hashes (HLEN / HEXISTS)~~ — done in v0.57.0,
  Nov 24, 2026: `hlen key` answers the number of fields in the
  hash at the key, 0 when the key does not exist — Redis HLEN —
  and `hexists key field` answers 1 when the field is set, 0
  when the key does not exist or the field is not set — Redis
  HEXISTS. Both are pure reads: no log entry, no state change,
  no watch event, and the TTL deadline is untouched — the size
  of a hash and one field's presence are read off the applied
  state machine exactly like an hget. A key holding a plain
  string makes both a client error, not a count or a 0. Both
  ride the same ReadIndex path as a `get`, so they are exactly
  as linearizable, and they survive snapshots and replay exactly
  like any other hash read — the blob is a plain string in the
  store, so nothing about the encoding changes.
  `Session.hlen` / `Session.hexists`, `client.hlen` /
  `client.hexists`, and `hlen` / `hexists` on the CLI expose
  them; like hget and hgetall they are reads, so they are not
  pipeline ops. `tests/test_hash.py` proves them: store-level
  counts of 0, 1, and 3 fields, a count that tracks hset and
  hdel, presence of 1 and 0 for a set and an unset field, a
  missing key answering 0 on both, an hlen and an hexists on a
  plain string raising, and the count surviving a snapshot load;
  replication to every node, a missing key answering 0 on the
  cluster, an hlen and an hexists on a plain string a client
  error, a non-string field rejected with no log entry, a
  follower redirect, continuing after a leader change,
  surviving a snapshot + restart with the count still right,
  session read-your-writes, and the CLI end to end.
- ~~Hashes (HKEYS / HVALS / HMGET)~~ — done in v0.58.0,
  Nov 24, 2026: `hkeys key` answers every field name in the
  hash at the key, sorted, an empty list when the key does not
  exist — Redis HKEYS — `hvals key` answers every field value,
  in sorted field order, an empty list when the key does not
  exist — Redis HVALS — and `hmget key field [field ...]`
  answers the values of the requested fields, aligned with the
  request, None per missing field and all None when the key
  does not exist — Redis HMGET. All three are pure reads: no
  log entry, no state change, no watch event, and the TTL
  deadline is untouched — the listings are read off the applied
  state machine exactly like an hget. A key holding a plain
  string makes all three a client error, not a list. All three
  ride the same ReadIndex path as a `get`, so they are exactly
  as linearizable, and they survive snapshots and replay exactly
  like any other hash read — the blob is a plain string in the
  store, so nothing about the encoding changes. The values
  `hvals` returns are aligned with `hkeys`, so the two calls
  together rebuild the map; `hmget` answers in the order the
  caller asked, and a field repeated in one call is answered
  once per occurrence, so the reply never shifts after a miss
  or a repeat, like an mget. At most 1000 fields per hmget,
  like an hdel batch. `Session.hkeys` / `Session.hvals` /
  `Session.hmget`, `client.hkeys` / `client.hvals` /
  `client.hmget`, and `hkeys` / `hvals` / `hmget` on the CLI
  expose them; like hget and hgetall they are reads, so they
  are not pipeline ops. `tests/test_hash.py` proves them:
  store-level sorted field names tracking hset and hdel,
  values aligned with the sorted names and rebuilding the map,
  hmget answering in requested order with None per missing
  field and per repeated field, a missing key answering empty
  on all three, an hkeys, an hvals, and an hmget on a plain
  string raising, and the listings surviving a snapshot load;
  replication to every node, a missing key answering empty on
  the cluster, an hkeys, an hvals, and an hmget on a plain
  string a client error, malformed hmget fields rejected with
  no log entry, continuing after a leader change, session
  read-your-writes, and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run.
- ~~Hashes (HSCAN)~~ — done in v0.59.0,
  Nov 26, 2026: `hscan key cursor [MATCH pattern] [COUNT n]`
  answers one page of the hash at the key — the [field, value]
  pairs from the cursor on, in sorted field order, and the next
  cursor to pass back, 0 when the scan is done — Redis HSCAN.
  The cursor is an offset into the sorted field list, so 0
  starts the scan and a reply's cursor of 0 ends it; COUNT is
  how many fields one call examines (a hint, like Redis's
  COUNT — default 10, at most 1000), and MATCH is a glob
  pattern — `*`, `?`, `[abc]` — applied to field names, so a
  selective MATCH can answer a page with no pairs at all and a
  cursor still to go. A missing key answers cursor 0 with an
  empty page, and a cursor past the end finishes the scan the
  same way. It is a pure read — no log entry, no state change,
  no watch event — read off the applied state machine through
  the same ReadIndex path as a `get`, so each page is exactly
  as linearizable, and a plain string value is a client error,
  not a page. A bad cursor, match, or count is a client error
  and appends nothing. `Session.hscan` / `Session.hscan_all`,
  `client.hscan` / `client.hscan_all`, and `hscan` on the CLI
  expose it; like hget and hgetall it is a read, so it is not
  a pipeline op. `tests/test_hash.py` proves it: store-level
  paging through the fields with the cursor, a MATCH page that
  answers empty and still has a cursor to go, the `?` and
  `[...]` glob forms, a missing key and a cursor past the end
  answering empty, a plain string raising, bad cursors, matches,
  and counts raising, and the scan paging off a snapshot load;
  replication to every node, a missing key answering empty on
  the cluster, a plain string a client error, malformed args
  rejected with no log entry, continuing after a leader change,
  session read-your-writes with a full `hscan_all` walk, and
  the CLI end to end with `--match` and `--count`.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: BITOP (bitwise ops
  across string keys), or GEO, or streams.
- ~~BITOP (bitwise combination)~~ — done in v0.60.0,
  Nov 26, 2026: `bitop operation destkey key [key ...]`
  combines the string values at the source keys bitwise — AND,
  OR, XOR, or NOT (NOT takes exactly one source key) — stores
  the result at the destination, and answers with the result
  length (`bitop`) and the result value itself — Redis BITOP.
  The result is as long as the longest input, missing keys count
  as empty strings, shorter inputs are padded with zero bytes,
  and when every input is missing or empty the destination is
  deleted instead of storing an empty value, answering `bitop` 0.
  A bitop is a fresh set of its destination, so it clears the
  destination's TTL deadline, like a plain set, and fires a `set`
  watch event with the result — or a `del` event when the empty
  result deletes the destination. The outcome is decided at apply
  time, so a retry with the same session id returns the first
  attempt's length instead of combining twice; a non-byte-string
  source fails with `bitop_failed` / `wrongtype`, and a bad
  operation, an empty key list, or more than 1000 keys is a client
  error that appends nothing. `Session.bitop` /
  `client.bitop`, and `bitop` on the CLI, plus `{"op": "bitop"}`
  as a pipeline op. `tests/test_bit_ops.py` proves it: store-level
  AND / OR / XOR / NOT, missing keys as empty strings, different
  lengths padded with zeros, all-missing deleting the destination
  and its deadline, TTL cleared on the destination, the
  destination as a source, wrongtype leaving the destination
  untouched, the destination joining the sorted index, and bad
  operations and key lists raising at apply time; cluster-level
  replication to every node, invalid bitops rejected with no log
  entry, dedup of a bitop retry returning the first attempt's
  length, the watch events on leader and follower (set, del, and
  nothing on a failed bitop), surviving a snapshot + restart,
  session read-your-writes, pipelined bitop ops with a bad
  operation and a NOT with two keys in position, the module
  function from a follower, and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: BITCOUNT, or GEO, or
  streams.
- ~~BITCOUNT (counting set bits)~~ — done in v0.61.0,
  Nov 26, 2026: `bitcount key [start end [BYTE|BIT]]` counts
  the bits set to 1 in the string at the key — 0 when the key
  does not exist, Redis BITCOUNT. With no range the whole value
  is counted; with `start` and `end` only that slice is — bytes
  in BYTE mode (the default), bits in BIT mode, bit 0 the most
  significant bit of the first byte, like getbit's numbering —
  negative bounds counting back from the end and out-of-range
  bounds clamped, like a getrange, so a range with start past
  end counts 0. It is a pure read through the same ReadIndex
  path as a `get`, so it is exactly as linearizable and a stale
  bitcount is served from a follower lease like a stale `get`;
  a value that is not a byte string is a client error, not a
  count, like a getbit on it. `start` and `end` come as a pair,
  a mode with no range is a client error, and the mode is
  matched case-insensitively and normalized to upper case
  before it is served. `Session.bitcount` / `client.bitcount`,
  and `bitcount` on the CLI (`--mode BYTE|BIT`); like getbit
  and strlen it is a read, so it is not a pipeline op.
  `tests/test_bit_ops.py` proves it: store-level whole value,
  byte ranges with negative and clamped bounds, BIT mode within
  one byte and across the byte boundary, wrongtype, and bad
  modes and bounds raising; cluster-level counting on every
  node, redirect from a follower, ranges and BIT mode with a
  lowercase mode, invalid bitcounts rejected with no log entry,
  wrongtype as a client error, continuing after a leader
  change, session read-your-writes, the module function from a
  follower, and the CLI end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: GEO, or streams.
- ~~GEO (positions and distance)~~ — done in v0.62.0,
  Nov 26, 2026: `geoadd key lon lat member [lon lat member ...]`
  adds members with positions to the geo set at the key and
  answers with the number of members that did not exist before
  — Redis GEOADD — and `geodist key member1 member2 [unit]`
  answers the distance between two members — Redis GEODIST — in
  metres by default, or km, mi, or ft. Longitude must be in
  -180..180 and latitude in -85.05112878..85.05112878, Redis's
  ranges; a member whose position is updated is not counted, and
  a member repeated in one call counts once, at its last position.
  A missing key is created as an empty geo set, and a geodist on
  a missing key or between a member and one that is not in the
  set answers None. The members ride in the store as a JSON blob
  with a marker prefix, like a hash — member names to [lon, lat]
  pairs, dumped with sorted keys and no spaces, so every node
  applying the same log computes the same bytes — and the distance
  is the haversine over the stored coordinates divided by the
  unit's metres, so every unit agrees on the same underlying
  distance. The outcome is decided at apply time, so a retry with
  the same session id returns the first attempt's count instead
  of adding twice; a plain string value fails a geoadd with
  `geoadd_failed` / `wrongtype`, and a geodist on one is a client
  error, not a distance. At most 1000 members per geoadd, and a
  bad coordinate, an empty member list, or a bad unit is a client
  error that appends nothing. A successful geoadd keeps the key's
  TTL deadline, like an hset, and fires a `set` watch event with
  the new value. A geodist is a pure read through the same
  ReadIndex path as a `get`, so it is exactly as linearizable and
  a stale geodist is served from a follower lease like a stale
  `get`. `Session.geoadd` / `Session.geodist`,
  `client.geoadd` / `client.geodist`, and `geoadd` / `geodist`
  on the CLI, plus `{"op": "geoadd"}` as a pipeline op; a
  geodist is a read, so it is not one. `tests/test_geo.py`
  proves it: store-level adding and counting new members only,
  updating a position without counting, a repeated member
  counting once at its last position, TTL kept, the new key
  joining the sorted index, wrongtype leaving the value
  untouched, bad members raising, missing keys and members
  answering None, the four units agreeing with each other and
  the metre value within a metre of Redis's documented
  166274.1516, bad units raising, and the scan surviving a
  snapshot load; cluster-level replication to every node, a
  redirect from a follower, invalid geoadds and geodists
  rejected with no log entry, dedup of a geoadd retry returning
  the first attempt's count, the watch event on leader and
  follower (and nothing on a failed geoadd), surviving a snapshot
  + restart, continuing after a leader change, session
  read-your-writes, pipelined geoadd ops with bad members in
  position, the module functions from a follower, and the CLI
  end to end.
  The roadmap is exhausted again, so the next real thing is
  a new roadmap item grounded in this project's purpose,
  proposed by the next run — candidates: streams.