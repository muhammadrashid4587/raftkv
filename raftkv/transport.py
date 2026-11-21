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

_HEADER = struct.Struct(">I")
_MAX_MESSAGE_BYTES = 64 * 1024 * 1024  # 64 MiB, a sanity bound


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


def send_rpc(host: str, port: int, msg: dict, timeout: float = 2.0) -> dict | None:
    """One-shot RPC: connect, send one message, read one reply, close.

    Returns the reply dict, or None if the peer is unreachable, the call
    timed out, or the reply was malformed. Callers treat None as "peer
    did not answer", never as a vote or an ack.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            if not send_message(sock, msg):
                return None
            return recv_message(sock)
    except OSError:
        return None


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
