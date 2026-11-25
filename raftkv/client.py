"""raftkv client: set / get / del / cas against a cluster.

The client talks to any node; writes and reads sent to a follower get a
redirect naming the current leader, which the client follows
automatically (up to 10 hops).

Examples:

    python -m raftkv.client --node 127.0.0.1:5001 set mykey myvalue
    python -m raftkv.client --node 127.0.0.1:5001 get mykey
    python -m raftkv.client --node 127.0.0.1:5001 ttl mykey
    python -m raftkv.client --node 127.0.0.1:5001 mget mykey otherkey
    python -m raftkv.client --node 127.0.0.1:5001 keys --prefix my
    python -m raftkv.client --node 127.0.0.1:5001 pipeline < ops.jsonl
    python -m raftkv.client --node 127.0.0.1:5001 del mykey
    python -m raftkv.client --node 127.0.0.1:5001 cas mykey myvalue newvalue
    python -m raftkv.client --node 127.0.0.1:5001 watch mykey --timeout 10
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import uuid

from .transport import ConnectionPool, PipelinedConnection, send_rpc

MAX_REDIRECTS = 10
# One pooled connection per concurrent in-flight request is enough;
# past the cap, extra requests open an ephemeral connection instead
# of blocking, so this bounds idle sockets, not throughput.
DEFAULT_POOL_SIZE = 8


class Session:
    """A read-your-writes, monotonic-reads client session.

    A session is one client talking to the cluster under one
    ``client_id``. Every write it acks advances its write watermark
    to the log index (and term) of that write's entry, and every
    read it serves advances its read watermark to the applied index
    (and term) the returned value reflects. Every read it issues
    carries the higher of the two watermarks — so a client that
    wrote through the leader and then reads from any node, or from
    a new leader after a failover, never sees its own write missing,
    and a read never goes backwards to state older than a previous
    read: the serving node waits until it has applied at least that
    far before answering.

    The watermark is also the dedup session: each write carries a
    unique request id within the session, so a retried write returns
    the first attempt's result (and the same index) instead of
    appending a second entry. Reads need no request id — they write
    nothing.
    """

    def __init__(self, node_addr: tuple[str, int],
                 client_id: str | None = None,
                 pool_size: int = DEFAULT_POOL_SIZE):
        self.node_addr = node_addr
        self.client_id = client_id or uuid.uuid4().hex[:12]
        # Index/term of the last write this session saw acknowledged
        # (0/0 = no write yet: reads are plain ReadIndex reads).
        self.last_write_index = 0
        self.last_write_term = 0
        # Applied index/term of the newest state a read of this
        # session has returned (0/0 = nothing read yet). A later
        # read waits until the serving node has applied at least
        # this far, so reads never go backwards.
        self.last_read_index = 0
        self.last_read_term = 0
        self._next_request = 0
        # Persistent connections to the cluster: every request reuses
        # a pooled TCP connection instead of opening a fresh one, so
        # a read stops paying a handshake and a fresh server thread.
        # ``pool_size`` caps pooled connections per node, not
        # concurrency — past the cap a request opens an ephemeral
        # connection instead of waiting. Pass 0 for one-shot
        # connections, exactly as before pooling.
        self._pool_size = pool_size
        self._pool: ConnectionPool | None = None

    @property
    def _pooled(self) -> ConnectionPool | None:
        """The session's pool, created lazily so a closed session can
        be used again — close() releases sockets, it does not retire
        the session."""
        if self._pool is None and self._pool_size > 0:
            self._pool = ConnectionPool(max_size=self._pool_size)
        return self._pool

    def close(self) -> None:
        """Release the session's pooled connections. The session stays
        usable afterwards; the next request opens a fresh pool."""
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def token(self) -> dict:
        """The session token: what identifies this client and how far
        its acknowledged writes reach in the log, plus how far its
        reads have seen."""
        return {"client_id": self.client_id,
                "last_write_index": self.last_write_index,
                "last_write_term": self.last_write_term,
                "last_read_index": self.last_read_index,
                "last_read_term": self.last_read_term}

    def _write(self, msg: dict) -> dict:
        self._next_request += 1
        msg["client_id"] = self.client_id
        msg["request_id"] = str(self._next_request)
        resp = request(self.node_addr, msg, pool=self._pooled)
        # Any response that names a committed entry advances the
        # watermark — a failed CAS still committed its entry, so its
        # index is a valid watermark too. Timeouts and errors carry no
        # index and leave the watermark where it was: the write may
        # still have committed, and the next read must not claim a
        # watermark the client never saw acknowledged.
        if (resp.get("status")
                in ("ok", "cas_failed", "incr_failed",
                    "incrbyfloat_failed", "append_failed",
                    "setrange_failed", "setbit_failed",
                    "pfadd_failed", "hset_failed", "hdel_failed")
                and resp.get("index") is not None):
            self.last_write_index = int(resp["index"])
            self.last_write_term = int(resp.get("term") or 0)
        return resp

    def set(self, key: str, value: str,
            ttl_entries: int | None = None,
            nx: bool = False, xx: bool = False,
            keepttl: bool = False, get: bool = False,
            ex: int | None = None, px: int | None = None,
            exat: int | None = None, pxat: int | None = None) -> dict:
        """Write one key. With ``ttl_entries`` (a positive number
        of log entries) the key expires when the log reaches this
        entry's index + ``ttl_entries`` — a distance in the log,
        not a duration in seconds, so every node agrees on when
        the key goes (see KVStore). Without it the write clears
        any deadline the key had. With ``nx`` the write happens
        only when the key does not exist, with ``xx`` only when
        it does — Redis SET NX / XX — and the reply carries
        ``set`` 1 when the key was written, 0 when the condition
        failed and nothing changed. With ``keepttl`` the write
        keeps whatever deadline the key had instead of clearing
        it — Redis SET KEEPTTL. With ``get`` the reply also
        carries ``old_value`` — the value the key held before
        this write, None when it was absent — so one round trip
        swaps the value and reports what it replaced, Redis
        SET ... GET; a losing conditional set answers
        ``old_value`` None. With ``ex`` (seconds) or ``px``
        (milliseconds) the write carries an inline wall-clock
        TTL — Redis SET ... EX / PX: the leader fixes the
        absolute deadline at log time, so every replica applies
        the same one and the key dies when the wall clock reaches
        it, like a ``setex`` written as a plain set. With
        ``exat`` (Unix seconds) or ``pxat`` (Unix milliseconds)
        the write carries an absolute wall-clock deadline
        instead — Redis SET ... EXAT / PXAT: the timestamp
        itself is the deadline, recorded as-is by the leader,
        so the key dies when the wall clock reaches it; a
        deadline already past deletes the key at apply time
        instead, like an ``expireat`` whose deadline was already
        past, and the reply carries ``deleted`` when a key
        actually went. The outcome is decided at apply time, at
        the entry's exact position in the log, like a CAS: a
        retry with the same session id returns the first
        attempt's result instead of writing twice. NX and XX
        together, any two of EX / PX / EXAT / PXAT together,
        EX/PX/EXAT/PXAT with a log-entry TTL, or with KEEPTTL,
        is a client error — the server rejects it and appends no
        log entry."""
        msg: dict = {"type": "client", "op": "set",
                     "key": key, "value": value}
        if ttl_entries is not None:
            msg["ttl_entries"] = ttl_entries
        if nx:
            msg["nx"] = True
        if xx:
            msg["xx"] = True
        if keepttl:
            msg["keepttl"] = True
        if get:
            msg["get"] = True
        if ex is not None:
            msg["ex"] = ex
        if px is not None:
            msg["px"] = px
        if exat is not None:
            msg["exat"] = exat
        if pxat is not None:
            msg["pxat"] = pxat
        return self._write(msg)

    def setex(self, key: str, value: str, seconds: int) -> dict:
        """Write one key with a wall-clock TTL, Redis SETEX: the
        key dies ``seconds`` wall-clock seconds after the leader
        logs the entry — an absolute deadline fixed by the leader
        at log time, so every replica applies the same deadline.
        The opt-in alternative to a log-entry TTL: use it when the
        expiry must be a duration in seconds, not a distance in
        the log — and accept that two nodes' clocks may disagree
        briefly about whether the key is alive, which a log-entry
        TTL can never do. Like ``set`` it is a fresh life for the
        key: any deadline it had (log or wall-clock) is dropped.
        A write like ``set``: it appends a log entry, carries a
        session request id, and advances the write watermark from
        the reply's index."""
        return self._write({"type": "client", "op": "setex",
                            "key": key, "value": value,
                            "seconds": seconds})

    def expire(self, key: str, entries: int) -> dict:
        """Set a deadline on a key that already exists: the key is
        purged when the log reaches this entry's index + ``entries``
        — the same log-index units as a ``set`` with ``ttl_entries``,
        so the deadline is deterministic across replicas. Returns
        ``expire`` 1 when the key exists (the deadline is set, and
        the reply also carries it) and 0 when it does not — like
        Redis's EXPIRE, a missing key is a no-op, not an error. A
        write like ``set``: it appends a log entry, carries a session
        request id (a retry returns the first attempt's result
        instead of setting the deadline twice), and advances the
        write watermark from the reply's index."""
        return self._write({"type": "client", "op": "expire",
                            "key": key, "ttl_entries": entries})

    def expireat(self, key: str, timestamp: int) -> dict:
        """Set an absolute wall-clock deadline on a key that
        already exists, Redis EXPIREAT: the key dies when the wall
        clock reaches ``timestamp`` — Unix seconds, fixed by the
        leader at log time, so every replica applies the same
        deadline. The wall-clock analogue of ``expire``: the value
        is untouched, only the deadline changes, and a log-entry
        deadline the key had is dropped, so a key holds at most
        one kind of TTL. Returns ``expireat`` 1 when the key
        exists (the deadline is set, and the reply also carries
        it) and 0 when it does not — like Redis, a missing key is
        a no-op, not an error. A deadline already past deletes
        the key outright and still answers 1, with ``deleted``
        set. A write like ``expire``: it appends a log entry,
        carries a session request id (a retry returns the first
        attempt's result instead of setting the deadline twice),
        and advances the write watermark from the reply's index."""
        return self._write({"type": "client", "op": "expireat",
                            "key": key, "timestamp": timestamp})

    def pexpireat(self, key: str, timestamp_ms: int) -> dict:
        """Set an absolute wall-clock deadline on a key that
        already exists, Redis PEXPIREAT: the same as
        ``expireat``, but ``timestamp_ms`` names the deadline in
        Unix milliseconds — the leader divides by 1000 before the
        entry is logged, so the deadline the replicas apply is
        still an absolute Unix time in seconds. Returns
        ``pexpireat`` 1 when the key exists (the reply also
        carries ``wall_deadline``, the deadline in seconds) and 0
        when it does not; a deadline already past deletes the key
        outright and still answers 1, with ``deleted`` set. A
        write like ``expireat``: it appends a log entry, carries
        a session request id, and advances the write watermark
        from the reply's index."""
        return self._write({"type": "client", "op": "pexpireat",
                            "key": key, "timestamp_ms": timestamp_ms})

    def mset(self, pairs: list,
             ttl_entries: int | None = None) -> dict:
        """Write many keys with one ``mset`` RPC: the whole batch
        appends as a single log entry and applies atomically, so a
        read after it commits sees every pair. It carries a session
        request id like ``set`` — a retry returns the first
        attempt's result instead of appending a second entry — and
        the response's index advances the write watermark exactly
        as a single write does. With ``ttl_entries`` the batch
        carries one deadline for every pair — this entry's index
        + the TTL — so the whole batch expires together at one log
        position, exactly like a single ``set`` with a TTL;
        without it every pair loses any deadline it had. An empty
        batch is a client error — the server rejects it — so this
        returns the error response instead of sending it."""
        if not pairs:
            return {"type": "client_response", "status": "error",
                    "error": "mset takes a list of 1-1000 "
                             "[key, value] pairs"}
        msg: dict = {"type": "client", "op": "mset",
                     "pairs": [[k, v] for k, v in pairs]}
        if ttl_entries is not None:
            msg["ttl_entries"] = ttl_entries
        return self._write(msg)

    def pipeline(self, ops: list, depth: int = 50,
                 timeout: float = 12.0) -> list[dict]:
        """Send many writes pipelined over one connection.

        ``ops`` is a list of write operations, one dict each:

            {"op": "set", "key": k, "value": v}
            {"op": "set", "key": k, "value": v, "ttl_entries": n}
            {"op": "set", "key": k, "value": v, "nx": True}
            {"op": "set", "key": k, "value": v, "xx": True}
            {"op": "set", "key": k, "value": v, "keepttl": True}
            {"op": "set", "key": k, "value": v, "ex": n}
            {"op": "set", "key": k, "value": v, "px": n}
            {"op": "set", "key": k, "value": v, "exat": t}
            {"op": "set", "key": k, "value": v, "pxat": t}
            {"op": "del", "key": k}
            {"op": "cas", "key": k, "expected": e, "value": v}
            {"op": "incr", "key": k, "by": n}
            {"op": "incrbyfloat", "key": k, "by": x}
            {"op": "setnx", "key": k, "value": v}
            {"op": "getset", "key": k, "value": v}
            {"op": "append", "key": k, "value": v}
            {"op": "getdel", "key": k}
            {"op": "getex", "key": k}
            {"op": "getex", "key": k, "ex": n}
            {"op": "getex", "key": k, "persist": True}
            {"op": "setrange", "key": k, "offset": n, "value": v}
            {"op": "setbit", "key": k, "offset": n, "bit": b}
            {"op": "pfadd", "key": k, "elements": [e1, e2, ...]}
            {"op": "hset", "key": k, "pairs": [[f1, v1], [f2, v2]]}
            {"op": "setex", "key": k, "value": v, "seconds": n}
            {"op": "expireat", "key": k, "timestamp": t}
            {"op": "pexpireat", "key": k, "timestamp_ms": t}

        The whole list goes out in batches of ``depth`` on a single
        persistent socket without waiting between requests, and the
        replies come back aligned by position: the i-th reply
        answers the i-th op. A failed op is an ``error`` response in
        its position, never an exception, so a partial batch is
        still usable — and a batch that hits a follower is re-sent
        to the leader it names, like ``request`` follows a redirect.

        Each op carries its own session request id, so the batch is
        safe to re-send: the server dedups by (client_id,
        request_id), and a retry returns the first attempt's
        result instead of appending a second entry. That is what
        makes pipelining writes safe where pipelining bare writes
        would not be. A reply lost to a dead connection is retried
        once, with the same request ids, on a fresh connection —
        so a write whose reply was lost but which did apply still
        comes back ``ok`` with its original index instead of
        applying twice.

        This is NOT ``mset``: every op appends its own log entry
        and pays its own fsync, so the batch is not atomic — a
        failure part-way leaves the earlier ops applied — and a
        batch of pure sets is cheaper as one ``mset`` (one entry,
        one fsync, atomic). Pipelining removes the client's
        round-trip wait between writes, not the server's per-entry
        fsync: on localhost that wait is ~0.2ms against an ~8-9ms
        commit, so expect a modest gain here and a bigger one over
        a real network RTT. The write watermark advances to the
        highest committed index any reply in the batch carries, so
        a later read through this session still sees every write
        the batch acknowledged.

        An op that is not a well-formed dict (unknown op, missing
        or non-string key/value) never goes on the wire: its
        position comes back an ``error`` response, and the ops
        around it still run. An empty list sends nothing and
        returns an empty list.
        """
        if not ops:
            return []
        msgs: list[dict] = []
        positions: list[int] = []
        replies: list[dict | None] = [None] * len(ops)
        for i, op in enumerate(ops):
            msg, err = _pipeline_op_message(op)
            if err is not None:
                replies[i] = err
                continue
            self._next_request += 1
            msg["client_id"] = self.client_id
            msg["request_id"] = str(self._next_request)
            msgs.append(msg)
            positions.append(i)
        if msgs:
            sent = request_many(self.node_addr, msgs,
                                timeout=timeout, depth=depth)
            # A reply lost to a dead connection is retried once
            # with the same request ids — dedup makes the re-send
            # safe even if the first attempt applied and only its
            # reply was lost. Anything the server actually answered
            # (including an error) is final and is not retried.
            failed = [j for j, resp in enumerate(sent)
                      if resp.get("status") == "error"
                      and "did not answer"
                      in (resp.get("error") or "")]
            if failed:
                retried = request_many(
                    self.node_addr, [msgs[j] for j in failed],
                    timeout=timeout, depth=depth)
                for j, resp in zip(failed, retried):
                    sent[j] = resp
            for j, resp in enumerate(sent):
                replies[positions[j]] = resp
                if (resp.get("status")
                        in ("ok", "cas_failed", "incr_failed",
                            "incrbyfloat_failed",
                            "append_failed", "setrange_failed",
                            "setbit_failed", "pfadd_failed",
                            "hset_failed", "hdel_failed")
                        and resp.get("index") is not None):
                    index = int(resp["index"])
                    if index > self.last_write_index:
                        self.last_write_index = index
                        self.last_write_term = int(resp.get("term") or 0)
        return [r if r is not None else
                {"type": "client_response", "status": "error",
                 "error": "no reply for pipeline op"}
                for r in replies]

    def delete(self, key: str) -> dict:
        return self._write({"type": "client", "op": "del", "key": key})

    def cas(self, key: str, expected: str, value: str) -> dict:
        return self._write({"type": "client", "op": "cas", "key": key,
                            "expected": expected, "value": value})

    def incr(self, key: str, by: int = 1) -> dict:
        """Atomically add ``by`` (default 1) to the key's integer
        value, Redis INCR/INCRBY: a missing key counts as 0, so
        the first incr creates it at ``by``. The outcome is decided
        at apply time, at the entry's position in the log, so the
        reply carries the new value as a string (``value``) — or
        ``incr_failed`` with a reason (``not_an_integer`` when the
        current value is not an integer, ``overflow`` when the
        result would leave the signed 64-bit range) and the current
        value. A failed incr still committed its entry, so its
        index is a valid watermark, exactly like a failed CAS; a
        retry with the same session id returns the first attempt's
        result instead of incrementing twice, and a successful
        incr keeps the key's TTL deadline, like a CAS."""
        return self._write({"type": "client", "op": "incr", "key": key,
                            "by": by})

    def incrbyfloat(self, key: str, by: float = 1.0) -> dict:
        """Atomically add ``by`` (default 1.0) to the key's float
        value, Redis INCRBYFLOAT: a missing key counts as 0, so
        the first incrbyfloat creates it at ``by``. The outcome is
        decided at apply time, at the entry's position in the log,
        so the reply carries the new value as a string (``value``)
        — or ``incrbyfloat_failed`` with a reason
        (``not_a_float`` when the current value is not a valid
        float, ``overflow`` when the result would be NaN or
        Infinity) and the current value. A failed incrbyfloat
        still committed its entry, so its index is a valid
        watermark, exactly like a failed incr; a retry with the
        same session id returns the first attempt's result
        instead of adding twice, and a successful incrbyfloat
        keeps the key's TTL deadline, like an incr. An integer
        ``by`` counts as a float, and the stored value is the
        formatted float, so a later incr on it fails — "2.5" is
        not an integer string."""
        return self._write({"type": "client", "op": "incrbyfloat",
                            "key": key, "by": by})

    def setnx(self, key: str, value: str) -> dict:
        """Set the key only if it does not already exist, Redis
        SETNX: returns ``setnx`` 1 when the key was created and 0
        when it already existed — and the existing value is left
        untouched. The outcome is decided at apply time, at the
        entry's position in the log, so two concurrent setnx on
        the same key race in log order and exactly one wins; a
        retry with the same session id returns the first
        attempt's result instead of setting twice. A successful
        setnx advances the write watermark from the reply's
        index, like any other write."""
        return self._write({"type": "client", "op": "setnx", "key": key,
                            "value": value})

    def getset(self, key: str, value: str) -> dict:
        """Atomically set the key and return its old value, Redis
        GETSET: returns ``old_value`` — the value the key held
        before this write, or None when the key did not exist —
        and the key holds ``value`` after. The old value is
        decided at apply time, at the entry's position in the log,
        so a retry with the same session id returns the first
        attempt's old value instead of setting twice, and a
        second getset sees the first one's value as the old value
        — the atomic swap Redis promises. A getset always writes,
        even on a missing key, which it creates, and it keeps the
        key's TTL deadline, like a CAS. A successful getset
        advances the write watermark from the reply's index, like
        any other write."""
        return self._write({"type": "client", "op": "getset", "key": key,
                            "value": value})

    def append(self, key: str, value: str) -> dict:
        """Append ``value`` to the key's string value, Redis APPEND:
        a missing key is created holding just ``value``, and the
        reply carries the new value and its length (``length``).
        The outcome is decided at apply time, at the entry's
        position in the log, so a retry with the same session id
        returns the first attempt's length instead of appending
        twice, and a successful append keeps the key's TTL
        deadline, like a CAS. A non-string value fails with
        ``append_failed`` and reason ``wrongtype`` instead of
        appending. A successful append advances the write
        watermark from the reply's index, like any other write —
        and so does a failed one, since it still committed its
        entry."""
        return self._write({"type": "client", "op": "append", "key": key,
                            "value": value})

    def strlen(self, key: str) -> dict:
        """Ask how long the string value at ``key`` is: the reply
        carries ``length`` — 0 when the key does not exist, Redis
        STRLEN. It rides the same ReadIndex path as ``get``, so it
        carries the same session watermark and advances the read
        watermark from the reply's ``read_index`` exactly the same
        way: a length query after your own write sees the length
        that write produced, and a later read never goes backwards
        past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "strlen", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def getdel(self, key: str) -> dict:
        """Atomically get a key's value and delete it, Redis GETDEL:
        returns ``value`` — the value the key held, or None when
        it did not exist — and ``getdel`` 1 when the key was there
        and is now gone, 0 when there was nothing to delete. The
        outcome is decided at apply time, at the entry's position
        in the log, so a retry with the same session id returns
        the first attempt's value instead of deleting a key a
        later write recreated, and the key takes its TTL deadline
        with it when it goes. A successful getdel advances the
        write watermark from the reply's index, like any other
        write."""
        return self._write({"type": "client", "op": "getdel", "key": key})

    def getex(self, key: str, ex: int | None = None,
              px: int | None = None, exat: int | None = None,
              pxat: int | None = None, persist: bool = False) -> dict:
        """Get the value at ``key`` and optionally set, refresh, or
        clear its expiry in the same atomic operation, Redis GETEX:
        returns ``value`` — the value the key held, or None when
        it did not exist — and ``getex`` 1 when the key was there,
        0 when there was nothing to get, plus ``deadline`` — the
        key's deadline after the op, or None without one. At most
        one option: ``ex`` / ``px`` expire the key after that many
        further log entries (the store's log-entry units, like
        ``ttl_entries`` — the Redis seconds / milliseconds are
        read as log entries, since this store has no wall clock);
        ``exat`` / ``pxat`` expire it when the log reaches that
        absolute index; ``persist`` clears the deadline. With no
        option the value and the deadline are both left alone. The
        outcome is decided at apply time, at the entry's position
        in the log, so a retry with the same session id returns
        the first attempt's value and deadline instead of
        re-expiring, and the value is never changed — a pure
        expiry change fires no watch event. A successful getex
        advances the write watermark from the reply's index, like
        any other write."""
        msg: dict = {"type": "client", "op": "getex", "key": key}
        if ex is not None:
            msg["ex"] = ex
        if px is not None:
            msg["px"] = px
        if exat is not None:
            msg["exat"] = exat
        if pxat is not None:
            msg["pxat"] = pxat
        if persist:
            msg["persist"] = True
        return self._write(msg)

    def getrange(self, key: str, start: int, end: int) -> dict:
        """Ask for the substring of the string value at ``key``
        from ``start`` to ``end`` inclusive, Redis GETRANGE: a
        negative index counts back from the end of the string,
        an index past either end is clamped, and a missing key
        holds the empty string, so any range of it is "". It
        rides the same ReadIndex path as ``get``, so it carries
        the same session watermark and advances the read
        watermark from the reply's ``read_index`` exactly the
        same way: a substring query after your own write sees
        the substring that write produced, and a later read never
        goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "getrange", "key": key,
                     "start": start, "end": end,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def setrange(self, key: str, offset: int, value: str) -> dict:
        """Overwrite part of the string value at ``key`` starting
        at ``offset``, Redis SETRANGE: a missing key counts as an
        empty string, and when the offset lies past the end the
        gap is padded with zero bytes. The reply carries the new
        value and its length (``length``). The outcome is decided
        at apply time, at the entry's position in the log, so a
        retry with the same session id returns the first
        attempt's value and length instead of overwriting twice,
        and a successful setrange keeps the key's TTL deadline,
        like a CAS. A non-string value fails with
        ``setrange_failed`` and reason ``wrongtype`` instead of
        overwriting. A successful setrange advances the write
        watermark from the reply's index, like any other write —
        and so does a failed one, since it still committed its
        entry."""
        return self._write({"type": "client", "op": "setrange",
                            "key": key, "offset": offset,
                            "value": value})

    def setbit(self, key: str, offset: int, bit: int) -> dict:
        """Set the bit at ``offset`` in the string value at ``key``
        to ``bit`` (0 or 1), Redis SETBIT: bit 0 is the most
        significant bit of the first byte, a missing key counts
        as an empty string, and when the offset lies past the end
        the gap is padded with zero bytes. The reply carries the
        old bit value (``old_bit``) — 0 when the bit was clear or
        the key did not exist — and the new value. The outcome is
        decided at apply time, at the entry's position in the log,
        so a retry with the same session id returns the first
        attempt's old bit instead of setting twice, and a
        successful setbit keeps the key's TTL deadline, like a
        CAS. A value that is not a byte string fails with
        ``setbit_failed`` and reason ``wrongtype`` instead of
        writing. A successful setbit advances the write watermark
        from the reply's index, like any other write — and so
        does a failed one, since it still committed its entry."""
        return self._write({"type": "client", "op": "setbit",
                            "key": key, "offset": offset, "bit": bit})

    def getbit(self, key: str, offset: int) -> dict:
        """Ask for the bit at ``offset`` in the string value at
        ``key``, Redis GETBIT: bit 0 is the most significant bit
        of the first byte, a missing key holds no bits so any
        offset of it is 0, and an offset past the end of the
        value is 0 too. It rides the same ReadIndex path as
        ``get``, so it carries the same session watermark and
        advances the read watermark from the reply's
        ``read_index`` exactly the same way: a bit query after
        your own write sees the bit that write set, and a later
        read never goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "getbit", "key": key,
                     "offset": offset,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def pfadd(self, key: str, *elements: str) -> dict:
        """Add ``elements`` to the HyperLogLog at ``key``, Redis
        PFADD: each element hashes into one of 16384 registers
        (SHA-256, deterministic across nodes), and the reply
        carries ``pfadd`` 1 when at least one register rose — the
        HLL was modified — and 0 when every element was already
        counted, plus ``estimate`` — the cardinality estimate
        after the add, good to under 1% once the count is past a
        few thousand. A missing key is created as an empty HLL;
        at most 1000 elements per call, like an mget batch. The
        outcome is decided at apply time, at the entry's position
        in the log, so a retry with the same session id returns
        the first attempt's pfadd flag and estimate instead of
        adding twice, and a successful pfadd keeps the key's TTL
        deadline, like a CAS. A plain string value fails with
        ``pfadd_failed`` and reason ``wrongtype`` instead of
        writing. A successful pfadd advances the write watermark
        from the reply's index, like any other write — and so
        does a failed one, since it still committed its entry."""
        return self._write({"type": "client", "op": "pfadd",
                            "key": key, "elements": list(elements)})

    def pfcount(self, key: str) -> dict:
        """Ask for the HyperLogLog cardinality estimate at ``key``,
        Redis PFCOUNT: 0 when the key does not exist, and a
        client error — not an estimate — when the key holds a
        plain string. It rides the same ReadIndex path as
        ``get``, so it carries the same session watermark and
        advances the read watermark from the reply's
        ``read_index`` exactly the same way: an estimate query
        after your own write sees the estimate that write
        produced, and a later read never goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "pfcount", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hset(self, key: str, fields: dict[str, str]) -> dict:
        """Set ``fields`` — a dict of field to value — in the hash
        at ``key``, Redis HSET: a missing key is created as an
        empty hash, and the reply carries ``hset`` — the number of
        fields that did not exist before (a field overwritten with
        a new value is not counted). The outcome is decided at
        apply time, at the entry's position in the log, so a retry
        with the same session id returns the first attempt's count
        instead of setting twice, and a successful hset keeps the
        key's TTL deadline, like a CAS. A plain string value fails
        with ``hset_failed`` and reason ``wrongtype`` instead of
        writing. A successful hset advances the write watermark
        from the reply's index, like any other write — and so
        does a failed one, since it still committed its entry."""
        return self._write({"type": "client", "op": "hset",
                            "key": key,
                            "pairs": [[f, v]
                                      for f, v in fields.items()]})

    def hdel(self, key: str, *fields: str) -> dict:
        """Delete ``fields`` from the hash at ``key``, Redis HDEL:
        the reply carries ``hdel`` — the number of fields actually
        removed (a missing key or a missing field counts 0, and
        removing the last field deletes the key outright). The
        outcome is decided at apply time, at the entry's position
        in the log, so a retry with the same session id returns
        the first attempt's count instead of deleting twice, and
        an hdel that leaves fields behind keeps the key's TTL
        deadline, like an hset. A plain string value fails with
        ``hdel_failed`` and reason ``wrongtype`` instead of
        writing. A successful hdel advances the write watermark
        from the reply's index, like any other write — and so
        does a failed one, since it still committed its entry."""
        return self._write({"type": "client", "op": "hdel",
                            "key": key, "fields": list(fields)})

    def hget(self, key: str, field: str) -> dict:
        """Ask for the value of ``field`` in the hash at ``key``,
        Redis HGET: None when the key does not exist or the field
        is not set, and a client error — not a field — when the key
        holds a plain string. It rides the same ReadIndex path as
        ``get``, so it carries the same session watermark and
        advances the read watermark from the reply's
        ``read_index`` exactly the same way: a field query after
        your own write sees the field that write set, and a later
        read never goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hget", "key": key,
                     "field": field,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hgetall(self, key: str) -> dict:
        """Ask for every field and value in the hash at ``key``,
        Redis HGETALL: an empty dict when the key does not exist,
        and a client error — not a field map — when the key holds
        a plain string. It rides the same ReadIndex path as
        ``get``, so it carries the same session watermark and
        advances the read watermark from the reply's
        ``read_index`` exactly the same way: a field map queried
        after your own write shows the fields that write set,
        and a later read never goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hgetall", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hlen(self, key: str) -> dict:
        """Ask for the number of fields in the hash at ``key``,
        Redis HLEN: 0 when the key does not exist, and a client
        error — not a count — when the key holds a plain string.
        It rides the same ReadIndex path as ``get``, so it
        carries the same session watermark and advances the
        read watermark from the reply's ``read_index`` exactly
        the same way: a length queried after your own write
        counts the fields that write set, and a later read
        never goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hlen", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hexists(self, key: str, field: str) -> dict:
        """Ask whether ``field`` is set in the hash at ``key``,
        Redis HEXISTS: 1 when it is, 0 when the key does not
        exist or the field is not set, and a client error — not
        a 0 — when the key holds a plain string. It rides the
        same ReadIndex path as ``get``, so it carries the same
        session watermark and advances the read watermark from
        the reply's ``read_index`` exactly the same way: a
        presence query after your own write sees the field
        that write set, and a later read never goes backwards
        past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hexists", "key": key,
                     "field": field,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hkeys(self, key: str) -> dict:
        """Ask for every field name in the hash at ``key``,
        Redis HKEYS: the field names, sorted, an empty list
        when the key does not exist, and a client error — not
        a list — when the key holds a plain string. It rides
        the same ReadIndex path as ``get``, so it carries the
        same session watermark and advances the read watermark
        from the reply's ``read_index`` exactly the same way:
        a field list queried after your own write shows the
        fields that write set, and a later read never goes
        backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hkeys", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hvals(self, key: str) -> dict:
        """Ask for every field value in the hash at ``key``,
        Redis HVALS: the values, in sorted field order, an
        empty list when the key does not exist, and a client
        error — not a list — when the key holds a plain
        string. The values are aligned with ``hkeys`` — the
        value at position i is the value of the field at
        position i in the sorted field list. It rides the same
        ReadIndex path as ``get``, so it carries the same
        session watermark and advances the read watermark from
        the reply's ``read_index`` exactly the same way: a
        value list queried after your own write shows the
        values that write set, and a later read never goes
        backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hvals", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def hmget(self, key: str, fields: list[str]) -> dict:
        """Ask for the values of ``fields`` in the hash at
        ``key``, Redis HMGET: one value per requested field,
        aligned with the request — None in a position whose
        field is not set, and None in every position when the
        key does not exist — and a client error — not a list —
        when the key holds a plain string. At most 1000 fields
        per call, like an hdel batch. It rides the same
        ReadIndex path as ``get``, so it carries the same
        session watermark and advances the read watermark from
        the reply's ``read_index`` exactly the same way: a
        value list queried after your own write shows the
        values that write set, and a later read never goes
        backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "hmget", "key": key,
                     "fields": list(fields),
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def get(self, key: str) -> dict:
        # The read carries the higher of the two watermarks — the
        # last acknowledged write, or the newest state a previous
        # read returned — with the term of whichever index wins, so
        # the serving node checks the right log position.
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "get", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        # A served read advances the read watermark to the applied
        # index the value reflects — never backwards: a read that
        # was served from a newer applied state raises the floor
        # for every read after it. A response without the fields
        # (an older server) leaves the watermark where it was.
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def ttl(self, key: str) -> dict:
        """Ask how many more log entries ``key`` has left before it
        expires: the reply carries ``ttl`` — the remaining entries
        in Redis's convention (-2 the key does not exist, -1 it
        exists with no deadline, otherwise the entries left) —
        plus ``deadline``, the absolute log index at which the key
        is purged (None without one). It rides the same ReadIndex
        path as ``get``, so it carries the same session watermark
        and advances the read watermark from the reply's
        ``read_index`` exactly the same way: a TTL query after
        your own write sees the deadline that write set, and a
        later read never goes backwards past it."""
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "ttl", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def mget(self, keys: list[str]) -> dict:
        """Read many keys with one ``mget`` RPC: one leadership
        confirmation and one apply wait for the whole batch, and the
        reply carries ``values`` aligned with ``keys`` (None for a
        missing key). Carries the same session watermark as ``get``
        and advances the read watermark from the reply's
        ``read_index`` exactly the same way, so a multi-get is
        read-your-writes and monotonic like any other session read.
        An empty list is a client error — the server rejects it —
        so this returns the error response instead of sending it."""
        if not keys:
            return {"type": "client_response", "status": "error",
                    "error": "mget takes a list of 1-1000 keys"}
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "mget", "keys": list(keys),
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def keys(self, prefix: str = "", limit: int = 1000,
             start_after: str | None = None) -> dict:
        """Enumerate keys with one ``keys`` RPC: every key in the
        applied state machine starting with ``prefix`` (empty
        prefix means all keys) and strictly after ``start_after``
        when one is given, sorted, at most ``limit`` of them
        (default 1000, the server cap). ``start_after`` is an
        exclusive lower bound on the sorted key order — pass the
        previous page's last key (or the reply's
        ``next_start_after``) to fetch the next page, with no
        repeats and no gaps while the state is unchanged. The reply
        carries ``keys`` plus ``truncated`` — True when more keys
        matched than the limit returned. Carries the same session
        watermark as ``get`` and advances the read watermark from
        the reply's ``read_index`` exactly the same way, so a scan
        is read-your-writes and monotonic like any other session
        read. A bad limit or cursor is a client error — the server
        rejects it — so this returns the error response instead of
        sending it."""
        if not isinstance(prefix, str):
            return {"type": "client_response", "status": "error",
                    "error": "keys prefix must be a string"}
        if not isinstance(limit, int) or isinstance(limit, bool) \
                or limit < 1 or limit > 1000:
            return {"type": "client_response", "status": "error",
                    "error": "keys limit must be between 1 and 1000"}
        if start_after is not None \
                and not isinstance(start_after, str):
            return {"type": "client_response", "status": "error",
                    "error": "keys start_after must be a string"}
        if self.last_read_index > self.last_write_index:
            min_index = self.last_read_index
            term = self.last_read_term
        else:
            min_index = self.last_write_index
            term = self.last_write_term
        msg: dict = {"type": "client", "op": "keys",
                     "prefix": prefix, "limit": limit,
                     "client_id": self.client_id,
                     "min_commit_index": min_index,
                     "session_term": term}
        if start_after is not None:
            msg["start_after"] = start_after
        resp = request(self.node_addr, msg, pool=self._pooled)
        if (resp.get("status") == "ok"
                and resp.get("read_index") is not None):
            read_index = int(resp["read_index"])
            if read_index > self.last_read_index:
                self.last_read_index = read_index
                self.last_read_term = int(resp.get("read_term") or 0)
        return resp

    def keys_all(self, prefix: str = "", limit: int = 1000) -> dict:
        """Enumerate every key under ``prefix`` by paging through
        the ``keys`` RPC: each page fetches at most ``limit`` keys,
        then resumes from the page's ``next_start_after`` until a
        page comes back untruncated. The reply is one merged
        ``keys`` response — every key in sorted order, ``truncated``
        False — plus ``pages``, how many RPCs the walk took, and
        the ``read_index`` / ``read_term`` of the last page. Each
        page is linearizable on its own, but the walk as a whole
        is not a snapshot: a key written or deleted mid-walk can
        be missed or appear once the cursor has passed it. An
        error on any page stops the walk and returns that error
        response instead of a partial list."""
        all_keys: list[str] = []
        start_after: str | None = None
        pages = 0
        last_resp: dict = {}
        while True:
            resp = self.keys(prefix=prefix, limit=limit,
                             start_after=start_after)
            if resp.get("status") != "ok":
                return resp
            pages += 1
            all_keys.extend(resp.get("keys") or [])
            last_resp = resp
            if not resp.get("truncated"):
                break
            start_after = resp.get("next_start_after")
            if start_after is None:
                # A truncated reply without a cursor cannot be
                # resumed — treat it as a server bug, not a
                # partial answer.
                return {"type": "client_response", "status": "error",
                        "error": "keys page truncated without a "
                                 "next_start_after cursor"}
        last_resp = dict(last_resp)
        last_resp["keys"] = all_keys
        last_resp["pages"] = pages
        return last_resp

    def watch(self, key: str, prefix: bool = False,
              timeout: float = 5.0) -> dict:
        """Long-poll for committed changes to ``key`` (or to every key
        starting with ``key`` when ``prefix=True``).

        One call is one long-poll: the node waits up to ``timeout``
        seconds for the first change and returns ``{"status": "ok" |
        "timeout", "events": [...]}``; each event carries ``key``,
        ``value`` (``None`` on delete), ``op`` (``"set"`` | ``"del"``)
        and the log ``index`` of the entry that caused it. Any node
        can serve a watch — call this again to keep watching. The RPC
        timeout is the watch timeout plus headroom, so a slow node
        reports a timeout instead of the client giving up first.
        """
        msg: dict = {"type": "watch", "key": key, "prefix": prefix,
                     "timeout": timeout}
        host, port = self.node_addr
        # A watch is a long-poll, not a pooled request: it would pin
        # a pooled connection for up to ``timeout`` seconds, so it
        # stays one-shot and never touches the session's pool.
        resp = send_rpc(host, port, msg, timeout=timeout + 5.0)
        if resp is None:
            return {"type": "watch_response", "status": "error",
                    "error": f"node {host}:{port} did not answer"}
        return resp


def membership_status(node_addr: tuple[str, int]) -> dict:
    """Ask one node for its status response, which carries the
    ``membership`` object: the current config, the joint config
    while a transition is open, which phase it is in, and the
    change itself (op, server, entry indexes). Any node answers —
    no redirect following, since this reads local state, not a
    client-protocol op."""
    host, port = node_addr
    resp = send_rpc(host, port, {"type": "status"}, timeout=5.0)
    if resp is None:
        return {"type": "status_response", "status": "error",
                "error": f"node {host}:{port} did not answer"}
    return resp


def _format_membership_status(status: dict) -> list[str]:
    """Human-readable lines for a status response's membership
    object: one line naming the node, one for the config, one for
    the phase and the change in flight, then the joint config and
    any servers still catching up or pending removal."""
    m = status.get("membership") or {}
    lines = [
        f"{status.get('node_id')} ({status.get('role')}, "
        f"term {status.get('term')})",
        "config: " + ", ".join(m.get("config") or []),
    ]
    phase = m.get("phase", "stable")
    pending = m.get("pending_change") or {}
    detail = f"membership: {phase}"
    if pending:
        detail += f" — {pending.get('op')} {pending.get('server_id')}"
        joint_index = pending.get("joint_index")
        final_index = pending.get("final_index")
        if phase == "catching_up":
            detail += (" (replicating the log as a non-voter; joint "
                       "entry not yet appended)")
        elif phase == "joint_pending":
            detail += f" (joint entry {joint_index} not yet committed)"
        elif phase == "joint_committed":
            detail += (f" (joint entry {joint_index} committed; final "
                       "entry not yet appended)")
        elif phase == "final_pending":
            detail += (f" (joint entry {joint_index} committed; final "
                       f"entry {final_index} not yet committed)")
    lines.append(detail)
    if m.get("joint_new") is not None:
        lines.append("joint config: " + ", ".join(m["joint_new"]))
    if m.get("non_voting"):
        lines.append("non-voting (catching up): "
                     + ", ".join(m["non_voting"]))
    if m.get("removal_pending"):
        lines.append("removal pending: " + ", ".join(
            f"{sid} (entry {idx})"
            for sid, idx in sorted(m["removal_pending"].items())))
    return lines


def _pipeline_op_message(op) -> tuple[dict | None, dict | None]:
    """Validate one pipeline op dict into its wire message.

    Returns (message, None) for a well-formed op, or (None, error
    response) when the op is malformed — the error goes in the
    op's reply position and nothing goes on the wire for it.
    ``set`` needs a string key and value, and may carry
    ``ttl_entries`` (an integer number of log entries,
    1..1000000) and the bare boolean flags ``nx`` / ``xx`` /
    ``keepttl`` / ``get`` — NX and XX never together, KEEPTTL never with
    a TTL — ``del`` a string key,
    ``cas`` a string key, expected, and value, ``incr`` a string
    key and an integer ``by`` (default 1), ``incrbyfloat`` a string
    key and a finite number ``by`` (default 1.0), ``setnx`` a string key
    and value, ``getset`` a string key and value, ``append`` a
    string key and value, ``getdel`` a string key, ``getex`` a
    string key and at most one expiry option (``ex`` / ``px`` a
    relative count of log entries, ``exat`` / ``pxat`` an absolute
    log index, ``persist`` a bare flag), ``setrange`` a
    string key, an integer ``offset`` and a string value,
    ``setbit`` a string key, an integer ``offset`` and a bit
    that is 0 or 1, ``pfadd`` a string key and a non-empty list
    of at most 1000 string elements, ``hset`` a string key and a
    non-empty list of at most 1000 [field, value] string pairs,
    ``hdel`` a string key and a non-empty list of at most 1000
    string fields, ``setex`` a string key and
    value plus an integer ``seconds`` from 1 to 1000000 — the
    wall-clock TTL, whose absolute deadline the leader fixes at
    log time — ``expireat`` a string key and a positive integer
    ``timestamp`` (Unix seconds), ``pexpireat`` a string key and
    a positive integer ``timestamp_ms`` (Unix milliseconds)."""
    if not isinstance(op, dict):
        return None, {"type": "client_response", "status": "error",
                      "error": "pipeline op must be an object with "
                               "op, key, and value fields"}
    name = op.get("op")
    if name not in ("set", "del", "cas", "incr", "incrbyfloat",
                    "setnx", "getset",
                    "append", "getdel", "getex", "setrange", "setbit",
                    "pfadd", "hset", "hdel", "setex", "expireat",
                    "pexpireat"):
        return None, {"type": "client_response", "status": "error",
                      "error": "pipeline op must be set, del, cas, "
                               "incr, incrbyfloat, setnx, getset, append, "
                               "getdel, getex, setrange, setbit, "
                               "pfadd, hset, hdel, setex, expireat, or "
                               "pexpireat"}
    key = op.get("key")
    if not isinstance(key, str):
        return None, {"type": "client_response", "status": "error",
                      "error": f"pipeline {name} needs a string key"}
    msg: dict = {"type": "client", "op": name, "key": key}
    if name in ("set", "cas", "setnx", "getset", "append", "setrange",
                "setex"):
        value = op.get("value")
        if not isinstance(value, str):
            return None, {"type": "client_response", "status": "error",
                          "error": f"pipeline {name} needs a string "
                                   "value"}
        msg["value"] = value
    if name == "set" and op.get("ttl_entries") is not None:
        # A TTL rides along with a pipelined set, validated here
        # so a malformed one never goes on the wire: an integer
        # number of log entries, 1..1,000,000, the same bounds
        # the server enforces.
        ttl = op.get("ttl_entries")
        if (not isinstance(ttl, int) or isinstance(ttl, bool)
                or ttl < 1 or ttl > 1_000_000):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set ttl_entries must be "
                                   "an integer between 1 and 1000000"}
        msg["ttl_entries"] = ttl
    if name == "set":
        # NX / XX / KEEPTTL / GET ride along with a pipelined set,
        # validated here so a malformed one never goes on the
        # wire: bare boolean flags, NX and XX never together,
        # KEEPTTL never with a TTL — the same shapes the server
        # enforces.
        for field in ("nx", "xx", "keepttl", "get"):
            if (op.get(field) is not None
                    and op[field] is not True):
                return None, {"type": "client_response",
                              "status": "error",
                              "error": f"pipeline set {field.upper()} "
                                       "takes no argument"}
        if op.get("nx") and op.get("xx"):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set NX and XX are "
                                   "mutually exclusive"}
        if (op.get("keepttl")
                and op.get("ttl_entries") is not None):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set KEEPTTL cannot be "
                                   "combined with ttl_entries"}
        for field in ("nx", "xx", "keepttl", "get"):
            if op.get(field):
                msg[field] = True
        # EX / PX ride along with a pipelined set, validated here
        # so a malformed one never goes on the wire: an integer
        # number of seconds / milliseconds, 1..1,000,000 /
        # 1..1,000,000,000, never together, never with a
        # log-entry TTL and never with KEEPTTL — the same shapes
        # the server enforces. EXAT / PXAT ride along the same
        # way: an absolute Unix time in seconds / milliseconds,
        # a positive integer like ``expireat`` takes, never with
        # each other, never with EX or PX, never with a log-entry
        # TTL and never with KEEPTTL.
        ex = op.get("ex")
        if (ex is not None
                and (not isinstance(ex, int) or isinstance(ex, bool)
                     or ex < 1 or ex > 1_000_000)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EX must be an "
                                   "integer between 1 and 1000000"}
        px = op.get("px")
        if (px is not None
                and (not isinstance(px, int) or isinstance(px, bool)
                     or px < 1 or px > 1_000_000_000)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set PX must be an "
                                   "integer between 1 and 1000000000"}
        exat = op.get("exat")
        if (exat is not None
                and (not isinstance(exat, int) or isinstance(exat, bool)
                     or exat < 1)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EXAT must be a "
                                   "positive integer"}
        pxat = op.get("pxat")
        if (pxat is not None
                and (not isinstance(pxat, int) or isinstance(pxat, bool)
                     or pxat < 1)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set PXAT must be a "
                                   "positive integer"}
        if ex is not None and px is not None:
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EX and PX are "
                                   "mutually exclusive"}
        if exat is not None and pxat is not None:
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EXAT and PXAT are "
                                   "mutually exclusive"}
        if ((exat is not None or pxat is not None)
                and (ex is not None or px is not None)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EXAT/PXAT cannot be "
                                   "combined with EX/PX"}
        if ((ex is not None or px is not None)
                and op.get("ttl_entries") is not None):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EX/PX cannot be "
                                   "combined with ttl_entries"}
        if ((exat is not None or pxat is not None)
                and op.get("ttl_entries") is not None):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EXAT/PXAT cannot be "
                                   "combined with ttl_entries"}
        if ((ex is not None or px is not None)
                and op.get("keepttl")):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EX/PX cannot be "
                                   "combined with KEEPTTL"}
        if ((exat is not None or pxat is not None)
                and op.get("keepttl")):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline set EXAT/PXAT cannot be "
                                   "combined with KEEPTTL"}
        if ex is not None:
            msg["ex"] = ex
        if px is not None:
            msg["px"] = px
        if exat is not None:
            msg["exat"] = exat
        if pxat is not None:
            msg["pxat"] = pxat
    if name == "cas":
        expected = op.get("expected")
        if not isinstance(expected, str):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline cas needs a string "
                                   "expected value"}
        msg["expected"] = expected
    if name == "incr":
        # ``by`` rides along with a pipelined incr, validated here
        # so a malformed one never goes on the wire: an integer,
        # default 1 — the same shape the server enforces.
        by = op.get("by", 1)
        if not isinstance(by, int) or isinstance(by, bool):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline incr by must be an "
                                   "integer"}
        msg["by"] = by
    if name == "incrbyfloat":
        # ``by`` rides along with a pipelined incrbyfloat,
        # validated here so a malformed one never goes on the
        # wire: a finite number, default 1.0 — an integer counts,
        # a bool does not — the same shape the server enforces.
        by = op.get("by", 1.0)
        if (not isinstance(by, (int, float))
                or isinstance(by, bool)
                or not math.isfinite(by)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline incrbyfloat by must be "
                                   "a finite number"}
        msg["by"] = by
    if name == "setrange":
        # ``offset`` rides along with a pipelined setrange,
        # validated here so a malformed one never goes on the
        # wire: a non-negative integer — the same shape the
        # server enforces (the server also caps it, so a huge
        # offset is still a client error there, not a log entry).
        offset = op.get("offset")
        if (not isinstance(offset, int) or isinstance(offset, bool)
                or offset < 0):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline setrange offset must be "
                                   "a non-negative integer"}
        msg["offset"] = offset
    if name == "setbit":
        # ``offset`` and ``bit`` ride along with a pipelined
        # setbit, validated here so a malformed one never goes
        # on the wire: a non-negative integer offset and a bit
        # that is 0 or 1 — the same shapes the server enforces
        # (the server also caps the offset, so a huge offset is
        # still a client error there, not a log entry).
        offset = op.get("offset")
        if (not isinstance(offset, int) or isinstance(offset, bool)
                or offset < 0):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline setbit offset must be "
                                   "a non-negative integer"}
        bit = op.get("bit")
        if (not isinstance(bit, int) or isinstance(bit, bool)
                or bit not in (0, 1)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline setbit bit must be 0 "
                                   "or 1"}
        msg["offset"] = offset
        msg["bit"] = bit
    if name == "pfadd":
        # ``elements`` rides along with a pipelined pfadd,
        # validated here so a malformed one never goes on the
        # wire: a non-empty list of at most 1000 strings — the
        # same shape the server enforces (the server also caps
        # it, so a huge list is still a client error there, not
        # a log entry).
        elements = op.get("elements")
        if (not isinstance(elements, list) or not elements
                or len(elements) > 1000
                or any(not isinstance(e, str) for e in elements)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline pfadd elements must be "
                                   "a list of 1 to 1000 strings"}
        msg["elements"] = list(elements)
    if name == "hset":
        # ``pairs`` rides along with a pipelined hset, validated
        # here so a malformed one never goes on the wire: a
        # non-empty list of at most 1000 [field, value] string
        # pairs — the same shape the server enforces (the server
        # also caps it, so a huge list is still a client error
        # there, not a log entry).
        pairs = op.get("pairs")
        if (not isinstance(pairs, list) or not pairs
                or len(pairs) > 1000
                or any(not isinstance(p, (list, tuple))
                       or len(p) != 2
                       or not isinstance(p[0], str)
                       or not isinstance(p[1], str)
                       for p in pairs)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline hset pairs must be a "
                                   "list of 1 to 1000 [field, value] "
                                   "string pairs"}
        msg["pairs"] = [[f, v] for f, v in pairs]
    if name == "hdel":
        # ``fields`` rides along with a pipelined hdel, validated
        # here so a malformed one never goes on the wire: a
        # non-empty list of at most 1000 strings — the same shape
        # the server enforces (the server also caps it, so a huge
        # list is still a client error there, not a log entry).
        fields = op.get("fields")
        if (not isinstance(fields, list) or not fields
                or len(fields) > 1000
                or any(not isinstance(f, str) for f in fields)):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline hdel fields must be a "
                                   "list of 1 to 1000 strings"}
        msg["fields"] = list(fields)
    if name == "setex":
        # ``seconds`` rides along with a pipelined setex,
        # validated here so a malformed one never goes on the
        # wire: an integer number of wall-clock seconds,
        # 1..1,000,000, the same bounds the server enforces.
        seconds = op.get("seconds")
        if (not isinstance(seconds, int) or isinstance(seconds, bool)
                or seconds < 1 or seconds > 1_000_000):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline setex seconds must be "
                                   "an integer between 1 and 1000000"}
        msg["seconds"] = seconds
    if name == "expireat":
        # ``timestamp`` rides along with a pipelined expireat,
        # validated here so a malformed one never goes on the
        # wire: a positive integer number of Unix seconds — the
        # same shape the server enforces.
        ts = op.get("timestamp")
        if (not isinstance(ts, int) or isinstance(ts, bool)
                or ts < 1):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline expireat timestamp "
                                   "must be a positive integer"}
        msg["timestamp"] = ts
    if name == "pexpireat":
        # ``timestamp_ms`` rides along with a pipelined
        # pexpireat, validated here so a malformed one never
        # goes on the wire: a positive integer number of Unix
        # milliseconds — the same shape the server enforces.
        ts = op.get("timestamp_ms")
        if (not isinstance(ts, int) or isinstance(ts, bool)
                or ts < 1):
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline pexpireat timestamp_ms "
                                   "must be a positive integer"}
        msg["timestamp_ms"] = ts
    if name == "getex":
        # At most one expiry option rides along with a pipelined
        # getex, validated here so a malformed one never goes on
        # the wire: EX / PX a relative count of log entries,
        # 1..1,000,000, like ttl_entries; EXAT / PXAT an absolute
        # log index, a positive integer; PERSIST a bare flag —
        # the same shapes the server enforces. Anything else on
        # the op is an unknown option, not a silently ignored
        # field.
        options = [f for f in ("ex", "px", "exat", "pxat", "persist")
                   if op.get(f) is not None]
        extra = [f for f in op
                 if f not in ("op", "key", "ex", "px", "exat",
                              "pxat", "persist")
                 and op.get(f) is not None]
        if extra:
            return None, {"type": "client_response", "status": "error",
                          "error": f"unknown getex option: {extra[0]}"}
        if len(options) > 1:
            return None, {"type": "client_response", "status": "error",
                          "error": "pipeline getex takes at most one "
                                   "of EX, PX, EXAT, PXAT, PERSIST"}
        if "ex" in options or "px" in options:
            field = "ex" if "ex" in options else "px"
            n = op[field]
            if (not isinstance(n, int) or isinstance(n, bool)
                    or n < 1 or n > 1_000_000):
                return None, {"type": "client_response",
                              "status": "error",
                              "error": f"pipeline getex {field.upper()} "
                                       "must be an integer between 1 "
                                       "and 1000000"}
            msg[field] = n
        elif "exat" in options or "pxat" in options:
            field = "exat" if "exat" in options else "pxat"
            t = op[field]
            if (not isinstance(t, int) or isinstance(t, bool)
                    or t < 1):
                return None, {"type": "client_response",
                              "status": "error",
                              "error": f"pipeline getex {field.upper()} "
                                       "must be a positive integer"}
            msg[field] = t
        elif "persist" in options:
            if op["persist"] is not True:
                return None, {"type": "client_response",
                              "status": "error",
                              "error": "pipeline getex PERSIST takes "
                                       "no argument"}
            msg["persist"] = True
    return msg, None


def pipeline(node_addr: tuple[str, int], ops: list,
             timeout: float = 12.0, depth: int = 50) -> list[dict]:
    """Send many writes pipelined over one connection.

    One ``set`` / ``del`` / ``cas`` / ``incr`` / ``setnx`` /
    ``getset`` / ``append`` / ``getdel`` / ``getex`` / ``setrange`` /
    ``setbit`` / ``pfadd`` / ``setex`` / ``expireat`` / ``pexpireat``
    op per
    dict (see
    ``Session.pipeline`` for the shapes): the ops go out in
    batches of ``depth`` on a single persistent socket and the
    replies come back aligned by position. Each op carries a
    dedup id from a fresh one-shot session, so a re-send after a
    redirect or a lost reply cannot apply twice — but the session
    is discarded when the call returns, so a retry of the whole
    call later is a new session and its writes are new writes.
    An empty list sends nothing and returns an empty list.
    """
    if not ops:
        return []
    with Session(node_addr, pool_size=0) as session:
        return session.pipeline(ops, depth=depth, timeout=timeout)


def request_many(node_addr: tuple[str, int], msgs: list[dict],
                 timeout: float = 12.0, depth: int = 50) -> list[dict]:
    """Send many idempotent requests pipelined over one connection.

    The messages go out in batches of ``depth`` on a single
    persistent socket and the replies come back aligned by position:
    the i-th reply answers the i-th message. A batch that hits a
    follower is re-sent once to the leader it names, like
    ``request`` follows a redirect; a batch that fails mid-way (a
    ``None`` reply) is returned as-is with an ``error`` response in
    that position, and the caller decides whether to retry it — the
    connection is gone by then, so a retry starts fresh.

    Only for idempotent messages: reads, and writes that carry a
    dedup session id (``client_id`` + ``request_id``) — a batch
    re-sent after a redirect or a lost reply repeats every message
    in it, so a write without a dedup id could apply twice, while
    one with an id returns the first attempt's result instead.
    ``Session.pipeline`` assigns one id per op and retries lost
    replies with the same ids for exactly this reason.
    """
    if not msgs:
        return []
    conn = PipelinedConnection(node_addr[0], node_addr[1],
                               timeout=timeout)
    host, port = node_addr
    replies: list[dict] = []
    try:
        for start in range(0, len(msgs), depth):
            batch = msgs[start:start + depth]
            batch_replies = _request_batch(conn, host, port, batch,
                                           timeout)
            replies.extend(batch_replies)
    finally:
        conn.close()
    return replies


def _request_batch(conn: PipelinedConnection, host: str, port: int,
                   batch: list[dict], timeout: float) -> list[dict]:
    """One pipelined batch, following a leader redirect once.

    A ``not_leader`` reply names the leader; the whole batch is
    re-sent to it on a fresh connection, since the follower answered
    nothing from the batch. After one redirect the replies are
    returned as they came back, redirect or not — the caller sees
    them. A ``None`` reply (the connection died mid-batch) becomes
    an ``error`` response in that position."""
    for _ in range(MAX_REDIRECTS):
        raw = conn.call_many(batch)
        responses = [
            r if r is not None else
            {"type": "client_response", "status": "error",
             "error": f"node {host}:{port} did not answer"}
            for r in raw
        ]
        leader = None
        for resp in responses:
            if (resp.get("status") == "not_leader"
                    and resp.get("leader_addr")):
                leader = resp["leader_addr"]
                break
        if leader is None:
            return responses
        host, port = leader[0], int(leader[1])
        conn.close()
        conn.host, conn.port = host, port
    return [{"type": "client_response", "status": "error",
             "error": "too many leader redirects"}] * len(batch)


def get_many(node_addr: tuple[str, int], keys: list[str],
             timeout: float = 12.0, depth: int = 50) -> list[dict]:
    """Read many keys with one pipelined connection.

    One ``get`` per key, sent in batches of ``depth`` without
    waiting between them; the replies come back in key order. Each
    reply is the plain ``get`` response (``status`` ``ok`` with the
    value, or ``ok`` with ``value`` ``None`` for a missing key) —
    a failed read is an ``error`` response in that position, never
    an exception, so a partial batch is still usable.
    """
    msgs = [{"type": "client", "op": "get", "key": key} for key in keys]
    return request_many(node_addr, msgs, timeout=timeout, depth=depth)


def mget(node_addr: tuple[str, int], keys: list[str],
         timeout: float = 12.0) -> dict:
    """Read many keys with one ``mget`` RPC, following leader
    redirects like ``request``.

    One request serves the whole batch through the leader's
    ReadIndex path, so the per-key cost is one state machine lookup
    instead of one dispatch each — the single-request equivalent of
    ``get_many``'s pipelined batch, without the round trips between
    requests. The reply is the plain ``mget`` response
    (``status`` ``ok`` with ``values`` aligned with ``keys``, None
    for a missing key); an empty list never goes on the wire and
    returns the server's error shape instead."""
    if not keys:
        return {"type": "client_response", "status": "error",
                "error": "mget takes a list of 1-1000 keys"}
    return request(node_addr, {"type": "client", "op": "mget",
                               "keys": list(keys)}, timeout=timeout)


def ttl(node_addr: tuple[str, int], key: str,
        timeout: float = 12.0) -> dict:
    """Ask how many more log entries ``key`` has left before it
    expires, following leader redirects like ``request``.

    One request serves the query through the leader's ReadIndex
    path, so the number reflects the committed state as of one
    read index. The reply is the plain ``ttl`` response
    (``status`` ``ok`` with ``ttl`` — the remaining entries in
    Redis's convention: -2 the key does not exist, -1 it exists
    with no deadline, otherwise the entries left — plus
    ``deadline``, the absolute log index at which the key is
    purged, or None without one)."""
    return request(node_addr, {"type": "client", "op": "ttl",
                               "key": key}, timeout=timeout)


def expire(node_addr: tuple[str, int], key: str, entries: int,
           timeout: float = 12.0) -> dict:
    """Set a deadline on an existing key, following leader redirects
    like ``request``.

    One request appends the expire entry, so the deadline is this
    entry's log index + ``entries`` — the same log-index units as a
    ``set`` with ``ttl_entries``. The reply is the plain expire
    response (``status`` ``ok`` with ``expire`` 1 when the key
    exists — plus ``deadline``, the absolute log index at which the
    key is purged — and 0 when it does not, like Redis's EXPIRE)."""
    return request(node_addr, {"type": "client", "op": "expire",
                               "key": key, "ttl_entries": entries},
                   timeout=timeout)


def setex(node_addr: tuple[str, int], key: str, value: str,
          seconds: int, timeout: float = 12.0) -> dict:
    """Write one key with a wall-clock TTL, Redis SETEX, following
    leader redirects like ``request``.

    One request appends the setex entry, so the deadline is the
    leader's wall clock at log time + ``seconds`` — an absolute
    Unix time every replica applies, so the key dies everywhere at
    the same wall-clock moment (up to clock skew between nodes).
    The reply is the plain write response (``status`` ``ok`` with
    the entry's index and term)."""
    return request(node_addr, {"type": "client", "op": "setex",
                               "key": key, "value": value,
                               "seconds": seconds}, timeout=timeout)


def expireat(node_addr: tuple[str, int], key: str, timestamp: int,
             timeout: float = 12.0) -> dict:
    """Set an absolute wall-clock deadline on an existing key,
    following leader redirects like ``request``.

    One request appends the expireat entry, so the deadline is the
    absolute Unix time ``timestamp`` (seconds) — fixed by the
    leader at log time, so every replica applies the same
    deadline. The reply is the plain expireat response (``status``
    ``ok`` with ``expireat`` 1 when the key exists — plus
    ``wall_deadline``, the absolute Unix time at which the key is
    purged — and 0 when it does not, like Redis's EXPIREAT). A
    timestamp already past deletes the key outright and still
    answers 1, with ``deleted`` set."""
    return request(node_addr, {"type": "client", "op": "expireat",
                               "key": key, "timestamp": timestamp},
                   timeout=timeout)


def pexpireat(node_addr: tuple[str, int], key: str,
              timestamp_ms: int, timeout: float = 12.0) -> dict:
    """Set an absolute wall-clock deadline on an existing key,
    following leader redirects like ``request``.

    One request appends the pexpireat entry, so the deadline is the
    absolute Unix time ``timestamp_ms`` (milliseconds) — the leader
    divides by 1000 before the entry is logged, so the deadline
    the replicas apply is still an absolute Unix time in seconds.
    The reply is the plain pexpireat response (``status`` ``ok``
    with ``pexpireat`` 1 when the key exists — plus
    ``wall_deadline``, the deadline in seconds — and 0 when it
    does not, like Redis's PEXPIREAT). A timestamp already past
    deletes the key outright and still answers 1, with ``deleted``
    set."""
    return request(node_addr, {"type": "client", "op": "pexpireat",
                               "key": key,
                               "timestamp_ms": timestamp_ms},
                   timeout=timeout)


def incr(node_addr: tuple[str, int], key: str, by: int = 1,
         timeout: float = 12.0) -> dict:
    """Atomically add ``by`` (default 1) to the key's integer value,
    following leader redirects like ``request``.

    One request appends the incr entry, so the new value is decided
    at the entry's position in the log — the single-request
    equivalent of a read-modify-write, without the race. The reply
    is the plain incr response (``status`` ``ok`` with the new
    value as a string, or ``incr_failed`` with a reason —
    ``not_an_integer`` when the current value is not an integer,
    ``overflow`` when the result would leave the signed 64-bit
    range — and the current value). A missing key counts as 0, so
    the first incr creates it at ``by``."""
    return request(node_addr, {"type": "client", "op": "incr",
                               "key": key, "by": by},
                   timeout=timeout)


def incrbyfloat(node_addr: tuple[str, int], key: str,
                by: float = 1.0, timeout: float = 12.0) -> dict:
    """Atomically add ``by`` (default 1.0) to the key's float value,
    following leader redirects like ``request``.

    One request appends the incrbyfloat entry, so the new value is
    decided at the entry's position in the log — the single-request
    equivalent of a read-modify-write, without the race. The reply
    is the plain incrbyfloat response (``status`` ``ok`` with the
    new value as a string, or ``incrbyfloat_failed`` with a reason —
    ``not_a_float`` when the current value is not a valid float,
    ``overflow`` when the result would be NaN or Infinity — and
    the current value). A missing key counts as 0, so the first
    incrbyfloat creates it at ``by``."""
    return request(node_addr, {"type": "client", "op": "incrbyfloat",
                               "key": key, "by": by},
                   timeout=timeout)


def setnx(node_addr: tuple[str, int], key: str, value: str,
          timeout: float = 12.0) -> dict:
    """Set the key only if it does not already exist, following
    leader redirects like ``request`` — Redis SETNX.

    One request appends the setnx entry, so the outcome is decided
    at the entry's position in the log: two concurrent setnx on
    the same key race in log order and exactly one wins. The reply
    is the plain setnx response (``status`` ``ok`` with ``setnx``
    1 when the key was created and 0 when it already existed —
    and the existing value is left untouched)."""
    return request(node_addr, {"type": "client", "op": "setnx",
                               "key": key, "value": value},
                   timeout=timeout)


def getset(node_addr: tuple[str, int], key: str, value: str,
           timeout: float = 12.0) -> dict:
    """Atomically set the key and return its old value, following
    leader redirects like ``request`` — Redis GETSET.

    One request appends the getset entry, so the old value is
    decided at the entry's position in the log: the reply is the
    plain getset response (``status`` ``ok`` with ``old_value``
    — the value the key held before this write, or None when the
    key did not exist — and the key holds ``value`` after). A
    getset always writes, even on a missing key, which it
    creates, and it keeps the key's TTL deadline, like a CAS."""
    return request(node_addr, {"type": "client", "op": "getset",
                               "key": key, "value": value},
                   timeout=timeout)


def append(node_addr: tuple[str, int], key: str, value: str,
           timeout: float = 12.0) -> dict:
    """Append ``value`` to the key's string value, following leader
    redirects like ``request`` — Redis APPEND.

    One request appends the append entry, so the new value and
    length are decided at the entry's position in the log: the
    reply is the plain append response (``status`` ``ok`` with
    the new ``value`` and its ``length`` — or ``append_failed``
    with reason ``wrongtype`` when the current value is not a
    string). A missing key is created holding just ``value``, and
    a successful append keeps the key's TTL deadline, like a
    CAS."""
    return request(node_addr, {"type": "client", "op": "append",
                               "key": key, "value": value},
                   timeout=timeout)


def strlen(node_addr: tuple[str, int], key: str,
           timeout: float = 12.0) -> dict:
    """Ask how long the string value at ``key`` is, following leader
    redirects like ``request`` — Redis STRLEN.

    One request serves the length through the leader's ReadIndex
    path, so it reflects the committed state as of one read
    index, exactly as linearizable as a ``get``. The reply is the
    plain strlen response (``status`` ``ok`` with ``length`` — 0
    when the key does not exist — plus the ``read_index`` /
    ``read_term`` the length was measured at)."""
    return request(node_addr, {"type": "client", "op": "strlen",
                               "key": key}, timeout=timeout)


def getdel(node_addr: tuple[str, int], key: str,
           timeout: float = 12.0) -> dict:
    """Atomically get a key's value and delete it, following leader
    redirects like ``request`` — Redis GETDEL.

    One request appends the getdel entry, so the value it returns
    is decided at the entry's position in the log: the reply is
    the plain getdel response (``status`` ``ok`` with ``value``
    — the value the key held, or None when it did not exist —
    and ``getdel`` 1 when the key was there and is now gone, 0
    when there was nothing to delete). The key takes its TTL
    deadline with it when it goes."""
    return request(node_addr, {"type": "client", "op": "getdel",
                               "key": key}, timeout=timeout)


def getex(node_addr: tuple[str, int], key: str,
          ex: int | None = None, px: int | None = None,
          exat: int | None = None, pxat: int | None = None,
          persist: bool = False, timeout: float = 12.0) -> dict:
    """Get the value at ``key`` and optionally set, refresh, or
    clear its expiry in the same atomic operation, following
    leader redirects like ``request`` — Redis GETEX.

    One request appends the getex entry, so the value it returns
    and the deadline it sets are decided at the entry's position
    in the log: the reply is the plain getex response (``status``
    ``ok`` with ``value`` — the value the key held, or None when
    it did not exist — and ``getex`` 1 when the key was there, 0
    when there was nothing to get, plus ``deadline``, the key's
    deadline after the op, or None without one). At most one
    option: ``ex`` / ``px`` expire the key after that many
    further log entries (the store's log-entry units — the Redis
    seconds / milliseconds are read as log entries, since this
    store has no wall clock); ``exat`` / ``pxat`` expire it when
    the log reaches that absolute index; ``persist`` clears the
    deadline. With no option the value and the deadline are both
    left alone, and the value is never changed — a pure expiry
    change fires no watch event."""
    msg: dict = {"type": "client", "op": "getex", "key": key}
    if ex is not None:
        msg["ex"] = ex
    if px is not None:
        msg["px"] = px
    if exat is not None:
        msg["exat"] = exat
    if pxat is not None:
        msg["pxat"] = pxat
    if persist:
        msg["persist"] = True
    return request(node_addr, msg, timeout=timeout)


def getrange(node_addr: tuple[str, int], key: str, start: int,
             end: int, timeout: float = 12.0) -> dict:
    """Ask for the substring of the string value at ``key`` from
    ``start`` to ``end`` inclusive, following leader redirects
    like ``request`` — Redis GETRANGE.

    One request serves the substring through the leader's ReadIndex
    path, so it reflects the committed state as of one read
    index, exactly as linearizable as a ``get``. The reply is the
    plain getrange response (``status`` ``ok`` with ``value`` — the
    substring, "" when the key does not exist or the range holds
    nothing — plus the ``read_index`` / ``read_term`` it was
    measured at). A negative index counts back from the end of
    the string, and an index past either end is clamped."""
    return request(node_addr, {"type": "client", "op": "getrange",
                               "key": key, "start": start, "end": end},
                   timeout=timeout)


def setrange(node_addr: tuple[str, int], key: str, offset: int,
             value: str, timeout: float = 12.0) -> dict:
    """Overwrite part of the string value at ``key`` starting at
    ``offset``, following leader redirects like ``request`` —
    Redis SETRANGE.

    One request appends the setrange entry, so the new value and
    length are decided at the entry's position in the log: the
    reply is the plain setrange response (``status`` ``ok`` with
    the new ``value`` and its ``length`` — or ``setrange_failed``
    with reason ``wrongtype`` when the current value is not a
    string). A missing key counts as an empty string, and when
    the offset lies past the end the gap is padded with zero
    bytes; a successful setrange keeps the key's TTL deadline,
    like a CAS."""
    return request(node_addr, {"type": "client", "op": "setrange",
                               "key": key, "offset": offset,
                               "value": value}, timeout=timeout)


def setbit(node_addr: tuple[str, int], key: str, offset: int,
           bit: int, timeout: float = 12.0) -> dict:
    """Set the bit at ``offset`` in the string value at ``key`` to
    ``bit`` (0 or 1), following leader redirects like ``request`` —
    Redis SETBIT.

    One request appends the setbit entry, so the old bit it returns
    is decided at the entry's position in the log: the reply is the
    plain setbit response (``status`` ``ok`` with ``old_bit`` — the
    bit the key held, 0 when it was clear or the key did not exist —
    and the new ``value`` — or ``setbit_failed`` with reason
    ``wrongtype`` when the current value is not a byte string).
    Bit 0 is the most significant bit of the first byte; a missing
    key counts as an empty string, and when the offset lies past
    the end the gap is padded with zero bytes; a successful setbit
    keeps the key's TTL deadline, like a CAS."""
    return request(node_addr, {"type": "client", "op": "setbit",
                               "key": key, "offset": offset,
                               "bit": bit}, timeout=timeout)


def getbit(node_addr: tuple[str, int], key: str, offset: int,
           timeout: float = 12.0) -> dict:
    """Ask for the bit at ``offset`` in the string value at ``key``,
    following leader redirects like ``request`` — Redis GETBIT.

    One request serves the bit through the leader's ReadIndex path,
    so it reflects the committed state as of one read index,
    exactly as linearizable as a ``get``. The reply is the plain
    getbit response (``status`` ``ok`` with ``bit`` — 0 or 1 — plus
    the ``read_index`` / ``read_term`` it was measured at). Bit 0
    is the most significant bit of the first byte; a missing key
    holds no bits, so any offset of it is 0, and an offset past the
    end of the value is 0 too."""
    return request(node_addr, {"type": "client", "op": "getbit",
                               "key": key, "offset": offset},
                   timeout=timeout)


def pfadd(node_addr: tuple[str, int], key: str, *elements: str,
          timeout: float = 12.0) -> dict:
    """Add ``elements`` to the HyperLogLog at ``key``, following
    leader redirects like ``request`` — Redis PFADD.

    One request appends the pfadd entry, so the pfadd flag and the
    estimate it returns are decided at the entry's position in the
    log: the reply is the plain pfadd response (``status`` ``ok``
    with ``pfadd`` — 1 when at least one register rose, 0 when
    every element was already counted — and ``estimate``, the
    cardinality estimate after the add — or ``pfadd_failed`` with
    reason ``wrongtype`` when the current value is a plain
    string). A missing key is created as an empty HLL; at most
    1000 elements per call. A successful pfadd keeps the key's
    TTL deadline, like a CAS."""
    return request(node_addr, {"type": "client", "op": "pfadd",
                               "key": key,
                               "elements": list(elements)},
                   timeout=timeout)


def pfcount(node_addr: tuple[str, int], key: str,
            timeout: float = 12.0) -> dict:
    """Ask for the HyperLogLog cardinality estimate at ``key``,
    following leader redirects like ``request`` — Redis PFCOUNT.

    One request serves the estimate through the leader's ReadIndex
    path, so it reflects the committed state as of one read index,
    exactly as linearizable as a ``get``. The reply is the plain
    pfcount response (``status`` ``ok`` with ``estimate`` — the
    cardinality estimate, 0 when the key does not exist — plus the
    ``read_index`` / ``read_term`` it was measured at). A plain
    string value is a client error, not an estimate."""
    return request(node_addr, {"type": "client", "op": "pfcount",
                               "key": key}, timeout=timeout)


def hset(node_addr: tuple[str, int], key: str,
         fields: dict[str, str], timeout: float = 12.0) -> dict:
    """Set ``fields`` — a dict of field to value — in the hash at
    ``key``, following leader redirects like ``request`` — Redis
    HSET.

    One request appends the hset entry, so the count it returns is
    decided at the entry's position in the log: the reply is the
    plain hset response (``status`` ``ok`` with ``hset`` — the
    number of fields that did not exist before — or
    ``hset_failed`` with reason ``wrongtype`` when the current
    value is a plain string). A missing key is created as an empty
    hash; at most 1000 pairs per call. A successful hset keeps the
    key's TTL deadline, like a CAS."""
    return request(node_addr, {"type": "client", "op": "hset",
                               "key": key,
                               "pairs": [[f, v]
                                         for f, v in fields.items()]},
                   timeout=timeout)


def hget(node_addr: tuple[str, int], key: str, field: str,
         timeout: float = 12.0) -> dict:
    """Ask for the value of ``field`` in the hash at ``key``,
    following leader redirects like ``request`` — Redis HGET.

    One request serves the field through the leader's ReadIndex
    path, so it reflects the committed state as of one read index,
    exactly as linearizable as a ``get``. The reply is the plain
    hget response (``status`` ``ok`` with ``value`` — the field's
    value, None when the key does not exist or the field is not
    set — plus the ``read_index`` / ``read_term`` it was measured
    at). A plain string value is a client error, not a field."""
    return request(node_addr, {"type": "client", "op": "hget",
                               "key": key, "field": field},
                   timeout=timeout)


def hdel(node_addr: tuple[str, int], key: str,
         fields: list[str], timeout: float = 12.0) -> dict:
    """Delete ``fields`` from the hash at ``key``, following leader
    redirects like ``request`` — Redis HDEL.

    One request appends the hdel entry, so the count it returns is
    decided at the entry's position in the log: the reply is the
    plain hdel response (``status`` ``ok`` with ``hdel`` — the
    number of fields actually removed, 0 for a missing key or a
    missing field — or ``hdel_failed`` with reason ``wrongtype``
    when the current value is a plain string). Removing the last
    field deletes the key outright; at most 1000 fields per call.
    An hdel that leaves fields behind keeps the key's TTL deadline,
    like an hset."""
    return request(node_addr, {"type": "client", "op": "hdel",
                               "key": key, "fields": list(fields)},
                   timeout=timeout)


def hgetall(node_addr: tuple[str, int], key: str,
            timeout: float = 12.0) -> dict:
    """Ask for every field and value in the hash at ``key``,
    following leader redirects like ``request`` — Redis HGETALL.

    One request serves the field map through the leader's ReadIndex
    path, so it reflects the committed state as of one read index,
    exactly as linearizable as a ``get``. The reply is the plain
    hgetall response (``status`` ``ok`` with ``fields`` — the whole
    field map, an empty dict when the key does not exist — plus the
    ``read_index`` / ``read_term`` it was measured at). A plain
    string value is a client error, not a field map."""
    return request(node_addr, {"type": "client", "op": "hgetall",
                               "key": key},
                   timeout=timeout)


def hlen(node_addr: tuple[str, int], key: str,
         timeout: float = 12.0) -> dict:
    """Ask for the number of fields in the hash at ``key``,
    following leader redirects like ``request`` — Redis HLEN.

    One request serves the count through the leader's ReadIndex
    path, so it reflects the committed state as of one read index,
    exactly as linearizable as a ``get``. The reply is the plain
    hlen response (``status`` ``ok`` with ``hlen`` — the number of
    fields, 0 when the key does not exist — plus the
    ``read_index`` / ``read_term`` it was measured at). A plain
    string value is a client error, not a count."""
    return request(node_addr, {"type": "client", "op": "hlen",
                               "key": key},
                   timeout=timeout)


def hexists(node_addr: tuple[str, int], key: str, field: str,
            timeout: float = 12.0) -> dict:
    """Ask whether ``field`` is set in the hash at ``key``,
    following leader redirects like ``request`` — Redis HEXISTS.

    One request serves the answer through the leader's ReadIndex
    path, so it reflects the committed state as of one read index,
    exactly as linearizable as a ``get``. The reply is the plain
    hexists response (``status`` ``ok`` with ``hexists`` — 1 when
    the field is set, 0 when the key does not exist or the field
    is not set — plus the ``read_index`` / ``read_term`` it was
    measured at). A plain string value is a client error, not
    a 0."""
    return request(node_addr, {"type": "client", "op": "hexists",
                               "key": key, "field": field},
                   timeout=timeout)


def hkeys(node_addr: tuple[str, int], key: str,
          timeout: float = 12.0) -> dict:
    """Ask for every field name in the hash at ``key``,
    following leader redirects like ``request`` — Redis HKEYS.

    One request serves the field names through the leader's
    ReadIndex path, so they reflect the committed state as of
    one read index, exactly as linearizable as a ``get``. The
    reply is the plain hkeys response (``status`` ``ok`` with
    ``fields`` — the field names, sorted, an empty list when
    the key does not exist — plus the ``read_index`` /
    ``read_term`` it was measured at). A plain string value is
    a client error, not a list."""
    return request(node_addr, {"type": "client", "op": "hkeys",
                               "key": key},
                   timeout=timeout)


def hvals(node_addr: tuple[str, int], key: str,
          timeout: float = 12.0) -> dict:
    """Ask for every field value in the hash at ``key``,
    following leader redirects like ``request`` — Redis HVALS.

    One request serves the values through the leader's ReadIndex
    path, so they reflect the committed state as of one read
    index, exactly as linearizable as a ``get``. The reply is
    the plain hvals response (``status`` ``ok`` with ``values``
    — the field values, in sorted field order, an empty list
    when the key does not exist — plus the ``read_index`` /
    ``read_term`` it was measured at). The values are aligned
    with ``hkeys`` — the value at position i is the value of
    the field at position i in the sorted field list. A plain
    string value is a client error, not a list."""
    return request(node_addr, {"type": "client", "op": "hvals",
                               "key": key},
                   timeout=timeout)


def hmget(node_addr: tuple[str, int], key: str,
          fields: list[str], timeout: float = 12.0) -> dict:
    """Ask for the values of ``fields`` in the hash at ``key``,
    following leader redirects like ``request`` — Redis HMGET.

    One request serves the values through the leader's ReadIndex
    path, so they reflect the committed state as of one read
    index, exactly as linearizable as a ``get``. The reply is
    the plain hmget response (``status`` ``ok`` with ``values``
    — one value per requested field, aligned with the request,
    None in a position whose field is not set, and None in
    every position when the key does not exist — plus the
    ``read_index`` / ``read_term`` it was measured at). At most
    1000 fields per call. A plain string value is a client
    error, not a list."""
    return request(node_addr, {"type": "client", "op": "hmget",
                               "key": key, "fields": list(fields)},
                   timeout=timeout)


def mset(node_addr: tuple[str, int], pairs: list,
         ttl_entries: int | None = None,
         timeout: float = 12.0) -> dict:
    """Write many keys with one ``mset`` RPC, following leader
    redirects like ``request``.

    One request appends a single log entry carrying the whole
    batch, so the batch applies atomically at one log position —
    the single-request equivalent of many ``set`` calls, without a
    round trip and an fsync per key. With ``ttl_entries`` the batch
    carries one deadline for every pair — this entry's index + the
    TTL — so the whole batch expires together at one log position,
    exactly like a single ``set`` with a TTL; without it every
    pair loses any deadline it had. The reply is the plain write
    response (``status`` ``ok`` with the entry's ``index`` /
    ``term``); an empty batch never goes on the wire and returns
    the server's error shape instead."""
    if not pairs:
        return {"type": "client_response", "status": "error",
                "error": "mset takes a list of 1-1000 [key, value] "
                         "pairs"}
    msg: dict = {"type": "client", "op": "mset",
                 "pairs": [[k, v] for k, v in pairs]}
    if ttl_entries is not None:
        msg["ttl_entries"] = ttl_entries
    return request(node_addr, msg, timeout=timeout)


def keys(node_addr: tuple[str, int], prefix: str = "",
         limit: int = 1000, start_after: str | None = None,
         timeout: float = 12.0) -> dict:
    """Enumerate keys with one ``keys`` RPC, following leader
    redirects like ``request``.

    One request serves the whole scan through the leader's
    ReadIndex path, so the list reflects the committed state as of
    one read index — never a half-applied prefix. The reply is the
    plain ``keys`` response (``status`` ``ok`` with ``keys`` in
    sorted order, ``truncated`` True when more matched than the
    limit, and ``next_start_after`` — the last key returned — to
    resume the next page with). ``start_after`` is an exclusive
    lower bound on the sorted key order: pass the previous page's
    last key to fetch the next page. A bad prefix, limit, or
    cursor never goes on the wire and returns the server's error
    shape instead."""
    if not isinstance(prefix, str):
        return {"type": "client_response", "status": "error",
                "error": "keys prefix must be a string"}
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or limit < 1 or limit > 1000:
        return {"type": "client_response", "status": "error",
                "error": "keys limit must be between 1 and 1000"}
    if start_after is not None and not isinstance(start_after, str):
        return {"type": "client_response", "status": "error",
                "error": "keys start_after must be a string"}
    msg: dict = {"type": "client", "op": "keys",
                 "prefix": prefix, "limit": limit}
    if start_after is not None:
        msg["start_after"] = start_after
    return request(node_addr, msg, timeout=timeout)


def keys_all(node_addr: tuple[str, int], prefix: str = "",
             limit: int = 1000, timeout: float = 12.0) -> dict:
    """Enumerate every key under ``prefix`` by paging through the
    ``keys`` RPC: each page fetches at most ``limit`` keys, then
    resumes from the page's ``next_start_after`` until a page
    comes back untruncated. The reply is one merged ``keys``
    response — every key in sorted order, ``truncated`` False —
    plus ``pages``, how many RPCs the walk took. Each page is
    linearizable on its own, but the walk as a whole is not a
    snapshot: a key written or deleted mid-walk can be missed or
    appear once the cursor has passed it. An error on any page
    stops the walk and returns that error response instead of a
    partial list."""
    all_keys: list[str] = []
    start_after: str | None = None
    pages = 0
    last_resp: dict = {}
    while True:
        resp = keys(node_addr, prefix=prefix, limit=limit,
                    start_after=start_after, timeout=timeout)
        if resp.get("status") != "ok":
            return resp
        pages += 1
        all_keys.extend(resp.get("keys") or [])
        last_resp = resp
        if not resp.get("truncated"):
            break
        start_after = resp.get("next_start_after")
        if start_after is None:
            return {"type": "client_response", "status": "error",
                    "error": "keys page truncated without a "
                             "next_start_after cursor"}
    last_resp = dict(last_resp)
    last_resp["keys"] = all_keys
    last_resp["pages"] = pages
    return last_resp


def request(node_addr: tuple[str, int], msg: dict, timeout: float = 12.0,
            pool: ConnectionPool | None = None) -> dict:
    """Send one client request, following leader redirects.

    Pass a ``ConnectionPool`` to reuse persistent connections; with
    no pool every request opens a fresh TCP connection, as before.
    A redirect reuses the same pool — the leader's address just
    becomes another pooled host."""
    host, port = node_addr
    for _ in range(MAX_REDIRECTS):
        resp = send_rpc(host, port, msg, timeout=timeout, pool=pool)
        if resp is None:
            return {"type": "client_response", "status": "error",
                    "error": f"node {host}:{port} did not answer"}
        if resp.get("status") == "not_leader" and resp.get("leader_addr"):
            host, port = resp["leader_addr"][0], int(resp["leader_addr"][1])
            continue
        return resp
    return {"type": "client_response", "status": "error",
            "error": "too many leader redirects"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="raftkv client")
    parser.add_argument("--node", required=True,
                        help="any cluster node as host:port")
    parser.add_argument("--timeout", type=float, default=12.0)
    sub = parser.add_subparsers(dest="op", required=True)
    p_set = sub.add_parser("set")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.add_argument("--ttl-entries", type=int, default=None,
                       help="expire the key after this many further "
                            "log entries: the key lives until the log "
                            "reaches this entry's index + ttl-entries "
                            "(a distance in the log, not seconds)")
    g_set = p_set.add_mutually_exclusive_group()
    g_set.add_argument("--nx", action="store_true",
                       help="set only when the key does not exist "
                            "(Redis NX): prints 1 when the key was "
                            "written, 0 when it already existed")
    g_set.add_argument("--xx", action="store_true",
                       help="set only when the key exists (Redis "
                            "XX): prints 1 when the key was written, "
                            "0 when it did not exist")
    p_set.add_argument("--keepttl", action="store_true",
                       help="keep the key's existing TTL instead of "
                            "clearing it (Redis KEEPTTL)")
    p_set.add_argument("--get", action="store_true",
                       help="return the old value the key held before "
                            "this write (Redis SET GET): prints the old "
                            "value, or (nil) when the key was absent or "
                            "a conditional set lost")
    g_ex = p_set.add_mutually_exclusive_group()
    g_ex.add_argument("--ex", type=int, default=None,
                      help="expire the key this many seconds after "
                           "the write (Redis SET EX): the leader "
                           "fixes the absolute deadline at log time, "
                           "so every replica applies the same one")
    g_ex.add_argument("--px", type=int, default=None,
                      help="expire the key this many milliseconds "
                           "after the write (Redis SET PX): the same "
                           "wall-clock deadline as --ex, in "
                           "milliseconds")
    g_ex.add_argument("--exat", type=int, default=None,
                      help="expire the key at this Unix time in "
                           "seconds (Redis SET EXAT): the timestamp "
                           "itself is the deadline, recorded as-is "
                           "by the leader, so every replica applies "
                           "the same one")
    g_ex.add_argument("--pxat", type=int, default=None,
                      help="expire the key at this Unix time in "
                           "milliseconds (Redis SET PXAT): the same "
                           "absolute deadline as --exat, in "
                           "milliseconds")
    p_set.add_argument("--client-id", default=None,
                       help="session id for at-most-once writes: a retry "
                            "with the same client id + request id returns "
                            "the first attempt's result instead of writing "
                            "twice")
    p_set.add_argument("--request-id", default=None,
                       help="id of this write within the client session")
    p_mset = sub.add_parser(
        "mset",
        help="write many keys with one request: mset k1 v1 k2 v2 "
             "... appends one log entry carrying the whole batch, "
             "so the batch applies atomically")
    p_mset.add_argument("pairs", nargs="+",
                        help="alternating keys and values: k1 v1 k2 v2 ...")
    p_mset.add_argument("--ttl-entries", type=int, default=None,
                        help="expire every key in the batch after this "
                             "many further log entries: the deadline is "
                             "this entry's index + ttl-entries, so the "
                             "whole batch expires together (a distance "
                             "in the log, not seconds)")
    p_mset.add_argument("--client-id", default=None,
                        help="session id for at-most-once writes: a retry "
                             "with the same client id + request id returns "
                             "the first attempt's result instead of writing "
                             "twice")
    p_mset.add_argument("--request-id", default=None,
                        help="id of this write within the client session")
    p_pipe = sub.add_parser(
        "pipeline",
        help="send many writes pipelined over one connection: "
             "reads JSON ops (one per line) from stdin — "
             '{"op": "set", "key": k, "value": v}, '
             '{"op": "del", "key": k}, or '
             '{"op": "cas", "key": k, "expected": e, "value": v} '
             "— and prints one JSON response per line, in order")
    p_pipe.add_argument("--client-id", default=None,
                        help="session id for at-most-once writes: ops "
                             "retried with the same client id and "
                             "request ids return the first attempt's "
                             "result instead of writing twice")
    p_get = sub.add_parser("get")
    p_get.add_argument("key")
    p_get.add_argument("--client-id", default=None,
                       help="session id for a read-your-writes read")
    p_get.add_argument("--min-index", type=int, default=0,
                       help="index of the client's last acknowledged "
                            "write: the serving node waits until it has "
                            "applied that far before answering")
    p_get.add_argument("--session-term", type=int, default=0,
                       help="term of the entry at --min-index, checked "
                            "against the serving node's log")
    p_get.add_argument("--min-read-index", type=int, default=0,
                       help="applied index of the newest state a previous "
                            "read of this session returned: the serving "
                            "node waits until it has applied that far, so "
                            "the read never goes backwards to older state")
    p_get.add_argument("--read-term", type=int, default=0,
                       help="term of the entry at --min-read-index, "
                            "checked against the serving node's log")
    p_get.add_argument("--stale", action="store_true",
                       help="allow a follower holding a fresh leader "
                            "lease to serve the read from its applied "
                            "state: bounded staleness (at most ~one "
                            "heartbeat behind the leader), no redirect "
                            "hop. Ignored when a session watermark is "
                            "given — that read still goes to the leader")
    p_ttl = sub.add_parser(
        "ttl",
        help="how many more log entries until this key expires: "
             "prints the remaining entries (-2 the key does not "
             "exist, -1 it exists with no deadline)")
    p_ttl.add_argument("key")
    p_expire = sub.add_parser(
        "expire",
        help="set a deadline on an existing key: prints 1 when the "
             "key exists (its deadline is now this entry's log "
             "index + entries) and 0 when it does not, like Redis's "
             "EXPIRE — a missing key is a no-op, not an error")
    p_expire.add_argument("key")
    p_expire.add_argument("entries", type=int,
                          help="log entries until the key is purged: "
                               "the deadline is this entry's log "
                               "index + entries (1-1000000)")
    p_expire.add_argument("--client-id", default=None,
                          help="session id for at-most-once writes: a "
                               "retry with the same client id + request "
                               "id returns the first attempt's result "
                               "instead of writing twice")
    p_expire.add_argument("--request-id", default=None,
                          help="id of this write within the client "
                               "session")
    p_setex = sub.add_parser(
        "setex",
        help="set a key with a wall-clock TTL, Redis SETEX: "
             "prints OK; the key dies SECONDS wall-clock seconds "
             "after the leader logs the entry, at the same absolute "
             "deadline on every replica (up to clock skew)")
    p_setex.add_argument("key")
    p_setex.add_argument("value")
    p_setex.add_argument("seconds", type=int,
                         help="wall-clock seconds until the key is "
                              "purged (1-1000000)")
    p_setex.add_argument("--client-id", default=None,
                         help="session id for at-most-once writes: a "
                              "retry with the same client id + request "
                              "id returns the first attempt's result "
                              "instead of writing twice")
    p_setex.add_argument("--request-id", default=None,
                         help="id of this write within the client "
                              "session")
    p_expireat = sub.add_parser(
        "expireat",
        help="set an absolute wall-clock deadline on an existing "
             "key, Redis EXPIREAT: prints 1 when the key exists "
             "(it dies when the wall clock reaches TIMESTAMP, "
             "Unix seconds) and 0 when it does not; a timestamp "
             "already past deletes the key outright and still "
             "prints 1")
    p_expireat.add_argument("key")
    p_expireat.add_argument("timestamp", type=int,
                            help="absolute Unix time in seconds at "
                                 "which the key is purged")
    p_expireat.add_argument("--client-id", default=None,
                            help="session id for at-most-once writes: a "
                                 "retry with the same client id + request "
                                 "id returns the first attempt's result "
                                 "instead of writing twice")
    p_expireat.add_argument("--request-id", default=None,
                            help="id of this write within the client "
                                 "session")
    p_pexpireat = sub.add_parser(
        "pexpireat",
        help="set an absolute wall-clock deadline on an existing "
             "key, Redis PEXPIREAT: prints 1 when the key exists "
             "(it dies when the wall clock reaches TIMESTAMP_MS, "
             "Unix milliseconds) and 0 when it does not; a "
             "timestamp already past deletes the key outright and "
             "still prints 1")
    p_pexpireat.add_argument("key")
    p_pexpireat.add_argument("timestamp_ms", type=int,
                             help="absolute Unix time in milliseconds "
                                  "at which the key is purged")
    p_pexpireat.add_argument("--client-id", default=None,
                             help="session id for at-most-once writes: a "
                                  "retry with the same client id + request "
                                  "id returns the first attempt's result "
                                  "instead of writing twice")
    p_pexpireat.add_argument("--request-id", default=None,
                             help="id of this write within the client "
                                  "session")
    p_mget = sub.add_parser(
        "mget",
        help="read many keys with one request: prints the values as "
             "one JSON list, null for a missing key, in key order")
    p_mget.add_argument("keys", nargs="+",
                        help="keys to read, in order")
    p_keys = sub.add_parser(
        "keys",
        help="list keys in the store, sorted: prints them as one "
             "JSON list, optionally restricted to a prefix and "
             "capped at --limit (default 1000)")
    p_keys.add_argument("--prefix", default="",
                        help="only keys starting with PREFIX "
                             "(default: all keys)")
    p_keys.add_argument("--limit", type=int, default=1000,
                        help="at most this many keys (1-1000, "
                             "default 1000)")
    p_keys.add_argument("--start-after", default=None,
                        help="resume past this key: only keys "
                             "strictly after it in sorted order are "
                             "returned — pass the last key of the "
                             "previous page to page through a large "
                             "keyspace")
    p_del = sub.add_parser("del")
    p_del.add_argument("key")
    p_del.add_argument("--client-id", default=None)
    p_del.add_argument("--request-id", default=None)
    p_cas = sub.add_parser(
        "cas",
        help="compare-and-swap: set key to value only if its current "
             "value equals expected (fails distinctly on a missing key)")
    p_cas.add_argument("key")
    p_cas.add_argument("expected")
    p_cas.add_argument("value")
    p_cas.add_argument("--client-id", default=None,
                       help="session id for at-most-once writes: a retry "
                            "with the same client id + request id returns "
                            "the first attempt's result instead of writing "
                            "twice")
    p_cas.add_argument("--request-id", default=None,
                       help="id of this write within the client session")
    p_incr = sub.add_parser(
        "incr",
        help="atomically add BY (default 1) to the key's integer value "
             "and print the new value; a missing key counts as 0, a "
             "non-integer value or a 64-bit overflow fails")
    p_incr.add_argument("key")
    p_incr.add_argument("by", nargs="?", type=int, default=1,
                        help="amount to add (default 1, may be negative)")
    p_incr.add_argument("--client-id", default=None,
                        help="session id for at-most-once writes: a retry "
                             "with the same client id + request id returns "
                             "the first attempt's result instead of writing "
                             "twice")
    p_incr.add_argument("--request-id", default=None,
                        help="id of this write within the client session")
    p_incrbyfloat = sub.add_parser(
        "incrbyfloat",
        help="atomically add BY (default 1.0) to the key's float value "
             "and print the new value; a missing key counts as 0, a "
             "non-float value or a non-finite result fails")
    p_incrbyfloat.add_argument("key")
    p_incrbyfloat.add_argument("by", nargs="?", type=float, default=1.0,
                               help="amount to add (default 1.0, may be "
                                    "negative)")
    p_incrbyfloat.add_argument("--client-id", default=None,
                               help="session id for at-most-once writes: a retry "
                                    "with the same client id + request id returns "
                                    "the first attempt's result instead of writing "
                                    "twice")
    p_incrbyfloat.add_argument("--request-id", default=None,
                               help="id of this write within the client session")
    p_setnx = sub.add_parser(
        "setnx",
        help="set the key only if it does not already exist and "
             "print 1 when it was set, 0 when the key already "
             "existed (its value is left untouched)")
    p_setnx.add_argument("key")
    p_setnx.add_argument("value")
    p_setnx.add_argument("--client-id", default=None,
                         help="session id for at-most-once writes: a retry "
                              "with the same client id + request id returns "
                              "the first attempt's result instead of writing "
                              "twice")
    p_setnx.add_argument("--request-id", default=None,
                         help="id of this write within the client session")
    p_getset = sub.add_parser(
        "getset",
        help="atomically set the key and print its old value "
             "(or (nil) when the key did not exist); the key holds "
             "the new value after")
    p_getset.add_argument("key")
    p_getset.add_argument("value")
    p_getset.add_argument("--client-id", default=None,
                          help="session id for at-most-once writes: a retry "
                               "with the same client id + request id returns "
                               "the first attempt's result instead of writing "
                               "twice")
    p_getset.add_argument("--request-id", default=None,
                          help="id of this write within the client session")
    p_append = sub.add_parser(
        "append",
        help="append value to the string at key and print the new "
             "length; a missing key is created holding just value, "
             "and a non-string value fails")
    p_append.add_argument("key")
    p_append.add_argument("value")
    p_append.add_argument("--client-id", default=None,
                          help="session id for at-most-once writes: a retry "
                               "with the same client id + request id returns "
                               "the first attempt's result instead of writing "
                               "twice")
    p_append.add_argument("--request-id", default=None,
                          help="id of this write within the client session")
    p_strlen = sub.add_parser(
        "strlen",
        help="print the length of the string at key (0 when the key "
             "does not exist); a non-string value fails")
    p_strlen.add_argument("key")
    p_getdel = sub.add_parser(
        "getdel",
        help="atomically print the value at key and delete it "
             "(or (nil) when the key did not exist)")
    p_getdel.add_argument("key")
    p_getdel.add_argument("--client-id", default=None,
                          help="session id for at-most-once writes: a retry "
                               "with the same client id + request id returns "
                               "the first attempt's result instead of writing "
                               "twice")
    p_getdel.add_argument("--request-id", default=None,
                          help="id of this write within the client session")
    p_getex = sub.add_parser(
        "getex",
        help="print the value at key and optionally set, refresh, "
             "or clear its expiry in one atomic operation (or "
             "(nil) when the key did not exist); at most one "
             "expiry option")
    p_getex.add_argument("key")
    g_getex = p_getex.add_mutually_exclusive_group()
    g_getex.add_argument("--ex", type=int, default=None,
                         help="expire the key after this many further "
                              "log entries")
    g_getex.add_argument("--px", type=int, default=None,
                         help="expire the key after this many further "
                              "log entries")
    g_getex.add_argument("--exat", type=int, default=None,
                         help="expire the key when the log reaches "
                              "this index")
    g_getex.add_argument("--pxat", type=int, default=None,
                         help="expire the key when the log reaches "
                              "this index")
    g_getex.add_argument("--persist", action="store_true",
                         help="clear the key's expiry")
    p_getex.add_argument("--client-id", default=None,
                         help="session id for at-most-once writes: a retry "
                              "with the same client id + request id returns "
                              "the first attempt's result instead of writing "
                              "twice")
    p_getex.add_argument("--request-id", default=None,
                         help="id of this write within the client session")
    p_getrange = sub.add_parser(
        "getrange",
        help="print the substring of the string at key from start "
             "to end inclusive (negative indexes count back from "
             "the end, out-of-range indexes are clamped); empty "
             "when the key does not exist or the range holds "
             "nothing")
    p_getrange.add_argument("key")
    p_getrange.add_argument("start", type=int,
                            help="first character, 0-based; negative "
                                 "counts back from the end")
    p_getrange.add_argument("end", type=int,
                            help="last character, inclusive; negative "
                                 "counts back from the end")
    p_setrange = sub.add_parser(
        "setrange",
        help="overwrite the string at key starting at offset with "
             "value and print the new length; a missing key "
             "counts as empty, and a gap past the end is padded "
             "with zero bytes")
    p_setrange.add_argument("key")
    p_setrange.add_argument("offset", type=int,
                            help="character to start overwriting at, "
                                 "0-based, at most 1000000")
    p_setrange.add_argument("value")
    p_setrange.add_argument("--client-id", default=None,
                            help="session id for at-most-once writes: a retry "
                                 "with the same client id + request id returns "
                                 "the first attempt's result instead of writing "
                                 "twice")
    p_setrange.add_argument("--request-id", default=None,
                            help="id of this write within the client session")
    p_setbit = sub.add_parser(
        "setbit",
        help="set the bit at offset in the string at key to 0 or 1 "
             "and print the old bit value; a missing key counts "
             "as empty, and a gap past the end is padded with "
             "zero bytes")
    p_setbit.add_argument("key")
    p_setbit.add_argument("offset", type=int,
                          help="bit to set, 0-based, at most 1000000; "
                               "bit 0 is the most significant bit of "
                               "the first byte")
    p_setbit.add_argument("bit", type=int,
                          help="the bit to set: 0 or 1")
    p_setbit.add_argument("--client-id", default=None,
                          help="session id for at-most-once writes: a retry "
                               "with the same client id + request id returns "
                               "the first attempt's result instead of writing "
                               "twice")
    p_setbit.add_argument("--request-id", default=None,
                          help="id of this write within the client session")
    p_getbit = sub.add_parser(
        "getbit",
        help="print the bit at offset in the string at key (0 or 1); "
             "0 when the key does not exist or the offset lies "
             "past the end")
    p_getbit.add_argument("key")
    p_getbit.add_argument("offset", type=int,
                          help="bit to read, 0-based; bit 0 is the most "
                               "significant bit of the first byte")
    p_pfadd = sub.add_parser(
        "pfadd",
        help="add elements to the HyperLogLog at key and print 1 "
             "when at least one register rose (the HLL was "
             "modified) or 0 when every element was already "
             "counted; a missing key is created as an empty HLL")
    p_pfadd.add_argument("key")
    p_pfadd.add_argument("elements", nargs="+",
                         help="elements to add (at most 1000)")
    p_pfadd.add_argument("--client-id", default=None,
                         help="session id for at-most-once writes: a retry "
                              "with the same client id + request id returns "
                              "the first attempt's result instead of writing "
                              "twice")
    p_pfadd.add_argument("--request-id", default=None,
                         help="id of this write within the client session")
    p_pfcount = sub.add_parser(
        "pfcount",
        help="print the HyperLogLog cardinality estimate at key; "
             "0 when the key does not exist")
    p_pfcount.add_argument("key")
    p_hset = sub.add_parser(
        "hset",
        help="set fields in the hash at key and print the number of "
             "fields that did not exist before; a missing key is "
             "created as an empty hash")
    p_hset.add_argument("key")
    p_hset.add_argument("pairs", nargs="+",
                        help="field value pairs: f1 v1 f2 v2 ... "
                             "(at most 1000 pairs)")
    p_hset.add_argument("--client-id", default=None,
                        help="session id for at-most-once writes: a retry "
                             "with the same client id + request id returns "
                             "the first attempt's result instead of writing "
                             "twice")
    p_hset.add_argument("--request-id", default=None,
                        help="id of this write within the client session")
    p_hget = sub.add_parser(
        "hget",
        help="print the value of field in the hash at key (or (nil) "
             "when the key does not exist or the field is not set)")
    p_hget.add_argument("key")
    p_hget.add_argument("field")
    p_hdel = sub.add_parser(
        "hdel",
        help="delete fields from the hash at key and print the number "
             "of fields actually removed; removing the last field "
             "deletes the key")
    p_hdel.add_argument("key")
    p_hdel.add_argument("fields", nargs="+",
                        help="fields to delete: f1 f2 ... "
                             "(at most 1000 fields)")
    p_hdel.add_argument("--client-id", default=None,
                        help="session id for at-most-once writes: a retry "
                             "with the same client id + request id returns "
                             "the first attempt's result instead of writing "
                             "twice")
    p_hdel.add_argument("--request-id", default=None,
                        help="id of this write within the client session")
    p_hgetall = sub.add_parser(
        "hgetall",
        help="print every field and value in the hash at key as JSON "
             "({} when the key does not exist)")
    p_hgetall.add_argument("key")
    p_hlen = sub.add_parser(
        "hlen",
        help="print the number of fields in the hash at key "
             "(0 when the key does not exist)")
    p_hlen.add_argument("key")
    p_hexists = sub.add_parser(
        "hexists",
        help="print 1 when field is set in the hash at key, 0 when "
             "the key does not exist or the field is not set")
    p_hexists.add_argument("key")
    p_hexists.add_argument("field")
    p_hkeys = sub.add_parser(
        "hkeys",
        help="print every field name in the hash at key as JSON "
             "([] when the key does not exist)")
    p_hkeys.add_argument("key")
    p_hvals = sub.add_parser(
        "hvals",
        help="print every field value in the hash at key as JSON, "
             "in sorted field order ([] when the key does not "
             "exist)")
    p_hvals.add_argument("key")
    p_hmget = sub.add_parser(
        "hmget",
        help="print the values of fields in the hash at key as "
             "JSON, aligned with the requested fields (null per "
             "missing field, all null when the key does not "
             "exist)")
    p_hmget.add_argument("key")
    p_hmget.add_argument("fields", nargs="+",
                         help="fields to read: f1 f2 ... "
                              "(at most 1000 fields)")
    p_add = sub.add_parser("add-server",
                           help="add a server to the cluster (Raft §6)")
    p_add.add_argument("server_id")
    p_add.add_argument("addr", help="the new server as host:port")
    p_watch = sub.add_parser(
        "watch",
        help="long-poll for committed changes to a key (or key prefix): "
             "waits up to --timeout for the first change and prints "
             "each event as one JSON line; run again to keep watching")
    p_watch.add_argument("key")
    p_watch.add_argument("--prefix", action="store_true",
                         help="watch every key starting with KEY")
    p_watch.add_argument("--timeout", type=float, default=5.0,
                         help="seconds to wait for the first event "
                              "(default 5, max 25)")
    p_remove = sub.add_parser("remove-server",
                              help="remove a server from the cluster")
    p_remove.add_argument("server_id")
    p_ms = sub.add_parser(
        "membership-status",
        help="show the cluster membership as this node sees it: the "
             "current config, and any change in flight — which server, "
             "which phase, and whether its entries have committed")
    p_ms.add_argument("--json", action="store_true",
                      help="print the membership object as JSON "
                           "instead of the human-readable summary")
    args = parser.parse_args(argv)
    # Subcommand names keep their hyphens (add-server), but the wire
    # protocol and the server use underscores (add_server): normalize
    # once here so every branch below can compare against the
    # underscored form.
    op = args.op.replace("-", "_")

    host, _, port = args.node.rpartition(":")
    if op == "watch":
        # A watch is a long-poll, not a client-protocol op: any node
        # serves it, so no redirect following, and the RPC timeout
        # must outlive the watch timeout or the client gives up first.
        resp = send_rpc(host, int(port),
                        {"type": "watch", "key": args.key,
                         "prefix": args.prefix, "timeout": args.timeout},
                        timeout=args.timeout + 5.0)
        if resp is None:
            print(f"error: node {args.node} did not answer",
                  file=sys.stderr)
            return 1
        if resp.get("status") == "error":
            print(f"error: {resp.get('error') or resp}", file=sys.stderr)
            return 1
        for event in resp.get("events") or []:
            print(json.dumps(event))
        return 0
    if op == "membership_status":
        # A status read, not a client-protocol op: any node serves
        # it, so no redirect following, and the answer is the node's
        # own view of the membership — which may lag the leader's
        # during a transition, by design.
        resp = membership_status((host, int(port)))
        if resp.get("status") == "error":
            print(f"error: {resp.get('error') or resp}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(resp.get("membership") or {},
                             sort_keys=True))
        else:
            for line in _format_membership_status(resp):
                print(line)
        return 0
    if op == "pipeline":
        # Many writes pipelined over one connection: one JSON op
        # per stdin line, one JSON response per stdout line, in
        # order. A line that is not JSON, or not an object, is
        # passed through as-is — Session.pipeline rejects it
        # client-side and its position comes back an error
        # response, so a bad line never aborts the batch.
        ops = []
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                ops.append(json.loads(line))
            except ValueError:
                ops.append(line)
        with Session((host, int(port)),
                     client_id=args.client_id) as session:
            for resp in session.pipeline(ops):
                print(json.dumps(resp))
        return 0
    msg: dict = {"type": "client", "op": op}
    if op in ("set", "get", "del", "cas", "ttl", "expire", "incr",
              "incrbyfloat",
              "setnx", "getset", "append", "strlen", "getdel",
              "getex", "getrange", "setrange", "setbit", "getbit",
              "pfadd", "pfcount", "hset", "hget", "hdel", "hgetall",
              "hlen", "hexists", "hkeys", "hvals", "hmget",
              "setex", "expireat",
              "pexpireat"):
        msg["key"] = args.key
    elif op == "mset":
        if len(args.pairs) % 2:
            parser.error("mset takes alternating keys and values: "
                         "k1 v1 k2 v2 ...")
        msg["pairs"] = [[args.pairs[i], args.pairs[i + 1]]
                        for i in range(0, len(args.pairs), 2)]
    elif op == "mget":
        msg["keys"] = args.keys
    elif op == "keys":
        msg["prefix"] = args.prefix
        msg["limit"] = args.limit
        if args.start_after is not None:
            msg["start_after"] = args.start_after
    elif op == "add_server":
        msg["server_id"] = args.server_id
        ahost, _, aport = args.addr.rpartition(":")
        msg["addr"] = [ahost, int(aport)]
    elif op == "remove_server":
        msg["server_id"] = args.server_id
    if op == "set":
        msg["value"] = args.value
        if args.ttl_entries is not None:
            msg["ttl_entries"] = args.ttl_entries
        if args.nx:
            msg["nx"] = True
        if args.xx:
            msg["xx"] = True
        if args.keepttl:
            msg["keepttl"] = True
        if args.get:
            msg["get"] = True
        if args.ex is not None:
            msg["ex"] = args.ex
        if args.px is not None:
            msg["px"] = args.px
        if args.exat is not None:
            msg["exat"] = args.exat
        if args.pxat is not None:
            msg["pxat"] = args.pxat
    if op == "mset":
        if args.ttl_entries is not None:
            msg["ttl_entries"] = args.ttl_entries
    if op == "cas":
        msg["expected"] = args.expected
        msg["value"] = args.value
    if op == "expire":
        msg["ttl_entries"] = args.entries
    if op == "setex":
        msg["value"] = args.value
        msg["seconds"] = args.seconds
    if op == "expireat":
        msg["timestamp"] = args.timestamp
    if op == "pexpireat":
        msg["timestamp_ms"] = args.timestamp_ms
    if op == "incr":
        msg["by"] = args.by
    if op == "incrbyfloat":
        msg["by"] = args.by
    if op == "setnx":
        msg["value"] = args.value
    if op == "getset":
        msg["value"] = args.value
    if op == "append":
        msg["value"] = args.value
    if op == "getrange":
        msg["start"] = args.start
        msg["end"] = args.end
    if op == "getex":
        if args.ex is not None:
            msg["ex"] = args.ex
        if args.px is not None:
            msg["px"] = args.px
        if args.exat is not None:
            msg["exat"] = args.exat
        if args.pxat is not None:
            msg["pxat"] = args.pxat
        if args.persist:
            msg["persist"] = True
    if op == "setrange":
        msg["offset"] = args.offset
        msg["value"] = args.value
    if op == "setbit":
        msg["offset"] = args.offset
        msg["bit"] = args.bit
    if op == "getbit":
        msg["offset"] = args.offset
    if op == "pfadd":
        msg["elements"] = args.elements
    if op == "hset":
        if len(args.pairs) % 2:
            parser.error("hset takes alternating fields and values: "
                         "f1 v1 f2 v2 ...")
        msg["pairs"] = [[args.pairs[i], args.pairs[i + 1]]
                        for i in range(0, len(args.pairs), 2)]
    if op == "hget":
        msg["field"] = args.field
    if op == "hexists":
        msg["field"] = args.field
    if op == "hdel":
        msg["fields"] = args.fields
    if op == "hmget":
        msg["fields"] = args.fields
    if op in ("set", "del", "cas", "mset", "expire", "incr",
              "incrbyfloat", "setnx",
              "getset", "append", "getdel", "getex", "setrange",
              "setbit", "pfadd", "hset", "hdel", "setex", "expireat",
              "pexpireat") \
            and args.request_id:
        # A request id without an explicit client id still dedups
        # within the default "cli" session; the same id retried from
        # another session is a different write.
        msg["client_id"] = args.client_id or "cli"
        msg["request_id"] = args.request_id
    if op == "get":
        # A read-your-writes read from the shell: --min-index names
        # the last write the reader saw acknowledged (its index, from
        # the write response), --session-term its term. A monotonic
        # read adds --min-read-index / --read-term, the applied
        # index a previous read returned; the read carries the
        # higher of the two watermarks, with the term of whichever
        # index wins.
        if args.client_id:
            msg["client_id"] = args.client_id
        if args.min_read_index > args.min_index:
            msg["min_commit_index"] = args.min_read_index
            msg["session_term"] = args.read_term
        elif args.min_index:
            msg["min_commit_index"] = args.min_index
            msg["session_term"] = args.session_term
        if args.stale:
            msg["stale_ok"] = True
    resp = request((host, int(port)), msg, timeout=args.timeout)
    status = resp.get("status")
    if status == "ok":
        if op == "get":
            print(resp.get("value") if resp.get("value") is not None else "(nil)")
        elif op == "ttl":
            print(resp.get("ttl"))
        elif op == "expire":
            print(resp.get("expire"))
        elif op == "expireat":
            print(resp.get("expireat"))
        elif op == "pexpireat":
            print(resp.get("pexpireat"))
        elif op == "incr":
            print(resp.get("value"))
        elif op == "incrbyfloat":
            print(resp.get("value"))
        elif op == "setnx":
            print(resp.get("setnx"))
        elif op == "getset":
            old = resp.get("old_value")
            print(old if old is not None else "(nil)")
        elif op == "append":
            print(resp.get("length"))
        elif op == "strlen":
            print(resp.get("length"))
        elif op == "getdel":
            old = resp.get("value")
            print(old if old is not None else "(nil)")
        elif op == "getex":
            value = resp.get("value")
            print(value if value is not None else "(nil)")
        elif op == "getrange":
            print(resp.get("value") or "")
        elif op == "setrange":
            print(resp.get("length"))
        elif op == "setbit":
            print(resp.get("old_bit"))
        elif op == "getbit":
            print(resp.get("bit"))
        elif op == "pfadd":
            print(resp.get("pfadd"))
        elif op == "pfcount":
            print(resp.get("estimate"))
        elif op == "hset":
            print(resp.get("hset"))
        elif op == "hget":
            value = resp.get("value")
            print(value if value is not None else "(nil)")
        elif op == "hdel":
            print(resp.get("hdel"))
        elif op == "hgetall":
            print(json.dumps(resp.get("fields")))
        elif op == "hlen":
            print(resp.get("hlen"))
        elif op == "hexists":
            print(resp.get("hexists"))
        elif op == "hkeys":
            print(json.dumps(resp.get("fields")))
        elif op == "hvals":
            print(json.dumps(resp.get("values")))
        elif op == "hmget":
            print(json.dumps(resp.get("values")))
        elif op == "mget":
            print(json.dumps(resp.get("values")))
        elif op == "keys":
            print(json.dumps(resp.get("keys")))
        elif op == "set" and args.get:
            # A GET set prints the old value, like getset does —
            # (nil) when the key was absent or a conditional set
            # lost — instead of OK or the 1/0 a plain or
            # conditional set prints.
            old = resp.get("old_value")
            print(old if old is not None else "(nil)")
        elif op == "set" and (args.nx or args.xx):
            # A conditional set prints whether it wrote, like
            # setnx does — a plain set prints OK below.
            print(resp.get("set"))
        else:
            print("OK")
        return 0
    print(f"error: {resp.get('error') or resp}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
