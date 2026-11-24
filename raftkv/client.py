"""raftkv client: set / get / del / cas against a cluster.

The client talks to any node; writes and reads sent to a follower get a
redirect naming the current leader, which the client follows
automatically (up to 10 hops).

Examples:

    python -m raftkv.client --node 127.0.0.1:5001 set mykey myvalue
    python -m raftkv.client --node 127.0.0.1:5001 get mykey
    python -m raftkv.client --node 127.0.0.1:5001 mget mykey otherkey
    python -m raftkv.client --node 127.0.0.1:5001 keys --prefix my
    python -m raftkv.client --node 127.0.0.1:5001 del mykey
    python -m raftkv.client --node 127.0.0.1:5001 cas mykey myvalue newvalue
    python -m raftkv.client --node 127.0.0.1:5001 watch mykey --timeout 10
"""

from __future__ import annotations

import argparse
import json
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
        if (resp.get("status") in ("ok", "cas_failed")
                and resp.get("index") is not None):
            self.last_write_index = int(resp["index"])
            self.last_write_term = int(resp.get("term") or 0)
        return resp

    def set(self, key: str, value: str) -> dict:
        return self._write({"type": "client", "op": "set",
                            "key": key, "value": value})

    def mset(self, pairs: list) -> dict:
        """Write many keys with one ``mset`` RPC: the whole batch
        appends as a single log entry and applies atomically, so a
        read after it commits sees every pair. It carries a session
        request id like ``set`` — a retry returns the first
        attempt's result instead of appending a second entry — and
        the response's index advances the write watermark exactly
        as a single write does. An empty batch is a client error —
        the server rejects it — so this returns the error response
        instead of sending it."""
        if not pairs:
            return {"type": "client_response", "status": "error",
                    "error": "mset takes a list of 1-1000 "
                             "[key, value] pairs"}
        return self._write({"type": "client", "op": "mset",
                            "pairs": [[k, v] for k, v in pairs]})

    def delete(self, key: str) -> dict:
        return self._write({"type": "client", "op": "del", "key": key})

    def cas(self, key: str, expected: str, value: str) -> dict:
        return self._write({"type": "client", "op": "cas", "key": key,
                            "expected": expected, "value": value})

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

    Only for idempotent messages (reads): a batch re-sent after a
    redirect or a lost reply repeats every message in it, so a write
    without a dedup session id could apply twice.
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


def mset(node_addr: tuple[str, int], pairs: list,
         timeout: float = 12.0) -> dict:
    """Write many keys with one ``mset`` RPC, following leader
    redirects like ``request``.

    One request appends a single log entry carrying the whole
    batch, so the batch applies atomically at one log position —
    the single-request equivalent of many ``set`` calls, without a
    round trip and an fsync per key. The reply is the plain write
    response (``status`` ``ok`` with the entry's ``index`` /
    ``term``); an empty batch never goes on the wire and returns
    the server's error shape instead."""
    if not pairs:
        return {"type": "client_response", "status": "error",
                "error": "mset takes a list of 1-1000 [key, value] "
                         "pairs"}
    return request(node_addr, {"type": "client", "op": "mset",
                               "pairs": [[k, v] for k, v in pairs]},
                   timeout=timeout)


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
    p_mset.add_argument("--client-id", default=None,
                        help="session id for at-most-once writes: a retry "
                             "with the same client id + request id returns "
                             "the first attempt's result instead of writing "
                             "twice")
    p_mset.add_argument("--request-id", default=None,
                        help="id of this write within the client session")
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
    msg: dict = {"type": "client", "op": op}
    if op in ("set", "get", "del", "cas"):
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
    if op == "cas":
        msg["expected"] = args.expected
        msg["value"] = args.value
    if op in ("set", "del", "cas", "mset") and args.request_id:
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
        elif op == "mget":
            print(json.dumps(resp.get("values")))
        elif op == "keys":
            print(json.dumps(resp.get("keys")))
        else:
            print("OK")
        return 0
    print(f"error: {resp.get('error') or resp}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
