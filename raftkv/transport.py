"""Length-prefixed JSON transport for raftkv.

Every message on the wire is:

    4-byte big-endian length | UTF-8 JSON payload

This module is deliberately boring: it knows nothing about Raft, it just
moves dicts between sockets. Both node-to-node RPC and the client protocol
use the same framing.
"""

import concurrent.futures
import json
import queue
import selectors
import socket
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


# ---------------------------------------------------------------------------
# Server: one reactor thread + a bounded pool of dispatch workers.
# ---------------------------------------------------------------------------

# Dispatch workers. A long dispatch (a watch long-poll, a write waiting
# on replication) occupies one worker for its duration, exactly as it
# occupied one handler thread before; 32 leaves room for a handful of
# concurrent watches without ever starving reads, while staying far
# below one thread per connection.
_DEFAULT_POOL_WORKERS = 32
# An idle connection is closed after this long with no bytes either
# way, like the old handler's 30s socket timeout. A pending watch
# (max 25s) always answers inside it.
_DEFAULT_IDLE_TIMEOUT = 30.0
_RECV_SIZE = 65536


class _TaskPool:
    """Bounded pool of daemon worker threads running dispatch tasks.

    A plain ThreadPoolExecutor is the wrong tool here: its workers are
    non-daemon, so a node stopping while a 25s watch is in flight
    would hang its process exit on that worker. These workers are
    daemon like the old thread-per-connection handlers, so stop()
    never waits on them. Each task gets a Future, so a caller can wait
    on an earlier task — the per-connection ordering below relies on
    that.
    """

    def __init__(self, max_workers: int):
        self._queue: queue.Queue = queue.Queue()
        self._threads = []
        for i in range(max_workers):
            thread = threading.Thread(
                target=self._run, name=f"raftkv-worker-{i}", daemon=True)
            thread.start()
            self._threads.append(thread)

    def submit(self, fn) -> concurrent.futures.Future:
        fut: concurrent.futures.Future = concurrent.futures.Future()
        self._queue.put((fn, fut))
        return fut

    def _run(self) -> None:
        while True:
            fn, fut = self._queue.get()
            if fn is None:
                return
            try:
                fut.set_result(fn())
            except BaseException as exc:  # noqa: BLE001 - a failed task must settle its future
                fut.set_exception(exc)

    def shutdown(self) -> None:
        """Release the workers. Queued tasks drain first (a watch in
        flight finishes on its own time); the caller does not wait,
        and the daemon threads never block process exit."""
        for _ in self._threads:
            self._queue.put((None, None))


class _Connection:
    """One client socket on the reactor: its read buffer, its queued
    reply bytes, and the future of the last frame submitted from it.

    ``inbuf`` and ``last_future`` are reactor-only. ``outbuf``,
    ``closed`` and ``want_write`` are touched by worker threads too
    (a worker appends a reply), so they live under ``guard``.
    ``last_active`` is reactor-only: it moves whenever bytes arrive
    or a reply is written.
    """

    __slots__ = ("sock", "inbuf", "outbuf", "closed", "last_future",
                 "last_active", "want_write", "guard")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.inbuf = bytearray()
        self.outbuf = bytearray()
        self.closed = False
        self.last_future: concurrent.futures.Future | None = None
        self.last_active = time.monotonic()
        self.want_write = False
        self.guard = threading.Lock()


class NodeServer:
    """TCP server that dispatches each framed message to ``dispatch``.

    One reactor thread owns every socket (accept, read, write) via a
    selector; the actual dispatch runs on a bounded pool of daemon
    worker threads. Idle connections hold no thread at all, so 50
    pooled client connections cost one reactor thread plus a few
    workers instead of 50 handler threads contending on the GIL —
    that contention was the read ceiling (see README Benchmark).

    Frames from one connection still dispatch in order, like the old
    one-thread-per-connection handler: each submitted task waits on
    the previous frame's future first, and the pool queue is FIFO,
    so the wait can only ever be for a task already dequeued. A bad
    frame closes the connection, exactly as a failed ``recv_message``
    ended the old handler. Idle connections are closed after
    ``idle_timeout`` seconds with no bytes either way.

    Only the reactor thread touches the selector; a worker that
    queued a reply sends it straight from its own thread when the
    socket takes it, and only pokes the wakeup socketpair when
    bytes remain queued, so the reactor re-syncs its write
    registrations. ``shutdown()`` / ``server_close()`` mirror
    ``socketserver``: stop the reactor, then release everything.
    """

    def __init__(self, host: str, port: int, dispatch,
                 max_workers: int = _DEFAULT_POOL_WORKERS,
                 idle_timeout: float = _DEFAULT_IDLE_TIMEOUT):
        self.dispatch = dispatch
        self.max_workers = max_workers
        self.idle_timeout = idle_timeout
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((host, port))
        self._listener.listen(128)
        self._listener.setblocking(False)
        self.server_address = self._listener.getsockname()
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._listener, selectors.EVENT_READ, None)
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self._selector.register(self._wake_r, selectors.EVENT_READ, None)
        self._pool = _TaskPool(max_workers)
        self._conns: dict[socket.socket, _Connection] = {}
        # Connections a worker found broken (a send failed): workers
        # must never touch the selector themselves — it is only safe
        # on the reactor thread — so they queue the connection here
        # and poke the reactor, which closes it on wakeup.
        self._close_queue: queue.SimpleQueue = queue.SimpleQueue()
        self._shutdown = False
        self._shutdown_event = threading.Event()
        self._last_sweep = time.monotonic()

    # -- reactor ------------------------------------------------------

    def serve_forever(self, poll_interval: float = 0.05) -> None:
        """Run the reactor until shutdown() is called."""
        try:
            while not self._shutdown:
                try:
                    events = self._selector.select(timeout=poll_interval)
                except OSError:
                    break
                for key, mask in events:
                    if key.fileobj is self._listener:
                        self._accept()
                    elif key.fileobj is self._wake_r:
                        self._drain_wake()
                    else:
                        conn = key.data
                        if conn.closed:
                            continue
                        if mask & selectors.EVENT_READ:
                            self._readable(conn)
                        if not conn.closed and mask & selectors.EVENT_WRITE:
                            self._writable(conn)
                now = time.monotonic()
                if now - self._last_sweep >= 1.0:
                    self._last_sweep = now
                    self._sweep_idle(now)
        finally:
            self._shutdown_event.set()

    def _accept(self) -> None:
        while True:
            try:
                sock, _ = self._listener.accept()
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                return
            sock.setblocking(False)
            conn = _Connection(sock)
            self._conns[sock] = conn
            try:
                self._selector.register(sock, selectors.EVENT_READ, conn)
            except (KeyError, ValueError, OSError):
                self._close(conn)

    def _readable(self, conn: _Connection) -> None:
        try:
            data = conn.sock.recv(_RECV_SIZE)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            self._close(conn)
            return
        if not data:
            self._close(conn)
            return
        conn.last_active = time.monotonic()
        conn.inbuf += data
        for msg in self._extract_frames(conn):
            if conn.closed:
                return
            self._submit(conn, msg)

    def _extract_frames(self, conn: _Connection) -> list:
        """Each complete framed message in the read buffer.

        A frame is a 4-byte big-endian length plus that many JSON
        bytes — the same framing ``recv_message`` reads. A bad frame
        closes the connection and the frames already extracted still
        dispatch (their replies are dropped: the connection is gone).
        """
        frames = []
        buf = conn.inbuf
        while len(buf) >= _HEADER.size:
            (length,) = _HEADER.unpack(buf[:_HEADER.size])
            if length > _MAX_MESSAGE_BYTES:
                self._close(conn)
                return frames
            if len(buf) < _HEADER.size + length:
                return frames
            payload = bytes(buf[_HEADER.size:_HEADER.size + length])
            del buf[:_HEADER.size + length]
            try:
                frames.append(json.loads(payload.decode("utf-8")))
            except (ValueError, UnicodeDecodeError):
                self._close(conn)
                return frames
        return frames

    def _submit(self, conn: _Connection, msg: dict) -> None:
        prev = conn.last_future

        def run() -> None:
            if prev is not None:
                try:
                    prev.result()
                except Exception:
                    pass
            with conn.guard:
                if conn.closed:
                    return
            try:
                resp = self.dispatch(msg)
            except Exception as exc:  # never kill a connection on a bad handler
                resp = {"type": "error", "error": f"handler failed: {exc}"}
            self._queue_reply(conn, encode_message(resp))

        conn.last_future = self._pool.submit(run)

    def _queue_reply(self, conn: _Connection, data: bytes) -> None:
        with conn.guard:
            if conn.closed:
                return
            conn.outbuf += data
        # Best effort: send the reply straight from this worker
        # instead of waking the reactor for it. The socket is
        # non-blocking, so a full kernel buffer just leaves the
        # remainder queued — the reactor drains it on EVENT_WRITE.
        # Without this, every reply paid a full reactor round-trip
        # (wakeup + select + send), which showed up as ~50-100us of
        # extra latency on sequential reads.
        self._flush(conn)
        with conn.guard:
            if not conn.closed and conn.outbuf:
                self._poke()

    def _flush(self, conn: _Connection) -> None:
        """Send as much of the queued reply bytes as the socket takes
        right now. Called from the worker that queued the reply and
        from the reactor on EVENT_WRITE. The copy, send and delete
        happen under the connection guard, so two flushers can never
        send the same bytes twice; the socket is non-blocking, so the
        send itself never blocks while the guard is held. A failed
        send queues the connection for the reactor to close — the
        selector is reactor-only, so a worker never closes a
        connection itself.
        """
        error = False
        with conn.guard:
            if conn.closed or not conn.outbuf:
                return
            data = bytes(conn.outbuf)
            try:
                sent = conn.sock.send(data)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                error = True
            else:
                del conn.outbuf[:sent]
                conn.last_active = time.monotonic()
        if error:
            # A failed send means the peer is gone. Do not close
            # here: the selector is reactor-only, and this runs on a
            # worker thread — queue the close for the reactor.
            self._request_close(conn)

    def _writable(self, conn: _Connection) -> None:
        self._flush(conn)
        with conn.guard:
            if not conn.closed and not conn.outbuf and conn.want_write:
                conn.want_write = False
                try:
                    self._selector.modify(conn.sock,
                                          selectors.EVENT_READ, conn)
                except (KeyError, ValueError, OSError):
                    pass

    def _poke(self) -> None:
        """Wake the reactor so it re-syncs write registrations. If the
        pipe is full a wakeup is already pending, so a failed send is
        fine."""
        try:
            self._wake_w.send(b"\x00")
        except (BlockingIOError, InterruptedError, OSError):
            pass

    def _request_close(self, conn: _Connection) -> None:
        """Ask the reactor to close a connection. Safe from any
        thread: the close itself (selector unregister, socket close)
        happens on the reactor thread when it wakes."""
        self._close_queue.put(conn)
        self._poke()

    def _drain_wake(self) -> None:
        try:
            while self._wake_r.recv(4096):
                pass
        except (BlockingIOError, InterruptedError, OSError):
            pass
        while True:
            try:
                conn = self._close_queue.get_nowait()
            except queue.Empty:
                break
            self._close(conn)
        self._sync_registrations()

    def _sync_registrations(self) -> None:
        for sock, conn in list(self._conns.items()):
            if conn.closed:
                continue
            with conn.guard:
                want_write = bool(conn.outbuf)
                if want_write == conn.want_write:
                    continue
                conn.want_write = want_write
            events = selectors.EVENT_READ
            if want_write:
                events |= selectors.EVENT_WRITE
            try:
                self._selector.modify(sock, events, conn)
            except (KeyError, ValueError, OSError):
                pass

    def _sweep_idle(self, now: float) -> None:
        for conn in list(self._conns.values()):
            if not conn.closed and now - conn.last_active > self.idle_timeout:
                self._close(conn)

    def _close(self, conn: _Connection) -> None:
        with conn.guard:
            if conn.closed:
                return
            conn.closed = True
        try:
            self._selector.unregister(conn.sock)
        except (KeyError, ValueError, OSError):
            pass
        self._conns.pop(conn.sock, None)
        try:
            conn.sock.close()
        except OSError:
            pass

    # -- lifecycle ----------------------------------------------------

    def shutdown(self) -> None:
        """Stop the reactor loop; blocks until serve_forever has
        returned, like ``socketserver``'s shutdown."""
        self._shutdown = True
        self._poke()
        self._shutdown_event.wait(timeout=5.0)

    def server_close(self) -> None:
        """Close the listener, every connection and the selector, and
        release the worker pool. In-flight dispatches are daemon
        threads and are not waited on — a node stopping mid-watch
        behaves like the old server, whose handler threads were
        daemon too."""
        try:
            self._selector.unregister(self._listener)
        except (KeyError, ValueError, OSError):
            pass
        try:
            self._listener.close()
        except OSError:
            pass
        for conn in list(self._conns.values()):
            self._close(conn)
        try:
            self._selector.unregister(self._wake_r)
        except (KeyError, ValueError, OSError):
            pass
        for sock in (self._wake_r, self._wake_w):
            try:
                sock.close()
            except OSError:
                pass
        try:
            self._selector.close()
        except OSError:
            pass
        self._pool.shutdown()


def serve_in_background(host: str, port: int, dispatch,
                        max_workers: int = _DEFAULT_POOL_WORKERS) -> NodeServer:
    """Start a NodeServer on a daemon thread. Caller must call
    ``shutdown()`` + ``server_close()`` to stop it."""
    server = NodeServer(host, port, dispatch, max_workers=max_workers)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05},
        name="raftkv-reactor", daemon=True,
    )
    thread.start()
    return server
