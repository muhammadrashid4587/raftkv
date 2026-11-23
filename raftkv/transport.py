"""Length-prefixed JSON transport for raftkv.

Every message on the wire is:

    4-byte big-endian length | UTF-8 JSON payload

This module is deliberately boring: it knows nothing about Raft, it just
moves dicts between sockets. Both node-to-node RPC and the client protocol
use the same framing.
"""

import json
import socket
import socketserver
import struct
import threading
import time

_HEADER = struct.Struct(">I")
_MAX_MESSAGE_BYTES = 64 * 1024 * 1024  # 64 MiB, a sanity bound
# A pooled connection the server has already closed (its handler idles
# out after 30s) fails on first use; one retry on a fresh connection
# makes that invisible to the caller.
_POOL_RETRIES = 1
# Idle connections older than this are dropped on checkout instead of
# handed out: the server closes a connection after 30s idle, so a
# pooled socket older than that is dead on arrival.
_DEFAULT_MAX_IDLE_SECONDS = 20.0


def encode_message(obj: dict) -> bytes:
    payload = json.dumps(obj).encode("utf-8")
    return _HEADER.pack(len(payload)) + payload


def _read_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly n bytes, or None if the connection closed / errored."""
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def recv_message(sock: socket.socket) -> dict | None:
    """Read one framed message. Returns None on close/error/bad frame."""
    header = _read_exact(sock, _HEADER.size)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    if length > _MAX_MESSAGE_BYTES:
        return None
    payload = _read_exact(sock, length)
    if payload is None:
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def send_message(sock: socket.socket, obj: dict) -> bool:
    try:
        sock.sendall(encode_message(obj))
        return True
    except OSError:
        return False


def send_rpc(host: str, port: int, msg: dict, timeout: float = 2.0,
             pool: "ConnectionPool | None" = None) -> dict | None:
    """One-shot RPC: connect, send one message, read one reply, close.

    Returns the reply dict, or None if the peer is unreachable, the call
    timed out, or the reply was malformed. Callers treat None as "peer
    did not answer", never as a vote or an ack.

    Pass a ``ConnectionPool`` to reuse a persistent connection for this
    peer instead of opening a fresh TCP connection per call; with no
    pool the call stays one-shot, exactly as before.
    """
    if pool is not None:
        return pool.rpc(host, port, msg, timeout=timeout)
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            if not send_message(sock, msg):
                return None
            return recv_message(sock)
    except OSError:
        return None


class _PooledConnection:
    """One socket in a pool: the socket, where it goes, whether it
    counts against the pool's cap, and when it was last used."""

    __slots__ = ("sock", "host", "port", "counted", "last_used")

    def __init__(self, sock: socket.socket, host: str, port: int,
                 counted: bool):
        self.sock = sock
        self.host = host
        self.port = port
        self.counted = counted
        self.last_used = time.monotonic()


class ConnectionPool:
    """Reusable TCP connections, one pool shared by many callers.

    The server already serves many messages on one connection (its
    handler loops until the client closes), so a client that reuses
    its connections stops paying a TCP handshake — and a fresh server
    thread — per request. That is the whole win: the benchmark's
    concurrent reads sat at ~185 ops/s with a ~1s p99 because every
    read opened a new connection.

    One connection is checked out per in-flight request, so threads
    never share a socket: concurrent callers get concurrent
    connections, up to ``max_size`` pooled connections per host. Past
    the cap, extra callers open an ephemeral connection that is
    closed after the call instead of blocking — the cap bounds idle
    sockets, never throughput. A connection that fails mid-call is
    closed and the call is retried once on a fresh connection, so a
    stale pooled socket (the server closed it, or restarted) is
    invisible to the caller. Idle connections older than
    ``max_idle_seconds`` are dropped on checkout for the same reason.

    Not a general connection manager: it knows nothing about Raft or
    redirects, it just moves one framed message and back per call.
    Call ``close()`` when done to release idle sockets; a pool left
    open only leaks idle sockets, which the server closes after 30s.
    """

    def __init__(self, max_size: int = 8,
                 max_idle_seconds: float = _DEFAULT_MAX_IDLE_SECONDS):
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self.max_size = max_size
        self.max_idle_seconds = max_idle_seconds
        self._lock = threading.Lock()
        # (host, port) -> {"idle": [_PooledConnection, ...], "live": int}
        self._hosts: dict[tuple[str, int], dict] = {}
        self._closed = False

    def _host_state(self, host: str, port: int) -> dict:
        return self._hosts.setdefault((host, port),
                                      {"idle": [], "live": 0})

    def _acquire(self, host: str, port: int,
                 timeout: float) -> _PooledConnection | None:
        """Check out a connection: an idle one, a new counted one, or
        an ephemeral one past the cap. Returns None only if no
        connection could be opened at all."""
        now = time.monotonic()
        with self._lock:
            if self._closed:
                return None
            state = self._host_state(host, port)
            while state["idle"]:
                conn = state["idle"].pop()
                if now - conn.last_used <= self.max_idle_seconds:
                    return conn
                # A stale idle socket is dead on arrival: close it and
                # give its counted slot back, or the live count leaks
                # up to the cap and every later call goes ephemeral.
                state["live"] -= 1
                self._close_silently(conn)
            if state["live"] < self.max_size:
                state["live"] += 1
                counted = True
            else:
                counted = False
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
        except OSError:
            if counted:
                with self._lock:
                    self._host_state(host, port)["live"] -= 1
            return None
        return _PooledConnection(sock, host, port, counted)

    @staticmethod
    def _close_silently(conn: _PooledConnection) -> None:
        try:
            conn.sock.close()
        except OSError:
            pass

    def _release(self, conn: _PooledConnection, healthy: bool) -> None:
        """Return a connection to the idle list, or close it. A
        connection that errored is never reused; an idle list already
        at the cap closes the extra instead of growing."""
        if not healthy:
            self._close_silently(conn)
            if conn.counted:
                with self._lock:
                    self._host_state(conn.host, conn.port)["live"] -= 1
            return
        conn.last_used = time.monotonic()
        with self._lock:
            state = self._host_state(conn.host, conn.port)
            if (conn.counted and not self._closed
                    and len(state["idle"]) < self.max_size):
                state["idle"].append(conn)
                return
            if conn.counted:
                state["live"] -= 1
        self._close_silently(conn)

    def rpc(self, host: str, port: int, msg: dict,
            timeout: float = 2.0) -> dict | None:
        """One request over a pooled connection, retried once on a
        fresh connection if the pooled one fails. Returns the reply
        dict, or None if the peer did not answer on either attempt."""
        for attempt in range(_POOL_RETRIES + 1):
            conn = self._acquire(host, port, timeout)
            if conn is None:
                return None
            try:
                conn.sock.settimeout(timeout)
                if not send_message(conn.sock, msg):
                    raise OSError("send failed")
                resp = recv_message(conn.sock)
                if resp is None:
                    raise OSError("no reply")
            except OSError:
                self._release(conn, healthy=False)
                if attempt == _POOL_RETRIES:
                    return None
                # Retry on a brand-new connection, not another idle
                # one: if the failure was a stale socket, the next
                # idle one may be just as stale.
                fresh = self._connect_fresh(host, port, timeout)
                if fresh is None:
                    return None
                try:
                    fresh.sock.settimeout(timeout)
                    if not send_message(fresh.sock, msg):
                        raise OSError("send failed")
                    resp = recv_message(fresh.sock)
                    if resp is None:
                        raise OSError("no reply")
                except OSError:
                    self._release(fresh, healthy=False)
                    return None
                self._release(fresh, healthy=True)
                return resp
            self._release(conn, healthy=True)
            return resp
        return None

    def _connect_fresh(self, host: str, port: int,
                       timeout: float) -> _PooledConnection | None:
        """Open a connection that bypasses the idle list, counted
        against the cap when there is room, ephemeral otherwise."""
        with self._lock:
            if self._closed:
                return None
            state = self._host_state(host, port)
            counted = state["live"] < self.max_size
            if counted:
                state["live"] += 1
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
        except OSError:
            if counted:
                with self._lock:
                    self._host_state(host, port)["live"] -= 1
            return None
        return _PooledConnection(sock, host, port, counted)

    def close(self) -> None:
        """Close every idle connection. In-flight calls keep their
        sockets and return them to a closed pool, which closes them
        instead of re-pooling."""
        with self._lock:
            self._closed = True
            idle = [c for state in self._hosts.values()
                    for c in state["idle"]]
            for state in self._hosts.values():
                state["idle"].clear()
        for conn in idle:
            self._close_silently(conn)

    @property
    def idle_count(self) -> int:
        """Idle pooled connections, for tests and debugging."""
        with self._lock:
            return sum(len(s["idle"]) for s in self._hosts.values())


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        self.request.settimeout(30.0)
        while True:
            msg = recv_message(self.request)
            if msg is None:
                return
            try:
                resp = self.server.dispatch(msg)
            except Exception as exc:  # never kill a connection on a bad handler
                resp = {"type": "error", "error": f"handler failed: {exc}"}
            if not send_message(self.request, resp):
                return


class NodeServer(socketserver.ThreadingTCPServer):
    """Threaded TCP server that dispatches each message to ``dispatch``."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, host: str, port: int, dispatch):
        self.dispatch = dispatch
        super().__init__((host, port), _Handler)


def serve_in_background(host: str, port: int, dispatch) -> NodeServer:
    """Start a NodeServer on a daemon thread. Caller must call
    ``shutdown()`` + ``server_close()`` to stop it."""
    server = NodeServer(host, port, dispatch)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    return server
