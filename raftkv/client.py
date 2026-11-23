"""raftkv client: set / get / del / cas against a cluster.

The client talks to any node; writes and reads sent to a follower get a
redirect naming the current leader, which the client follows
automatically (up to 10 hops).

Examples:

    python -m raftkv.client --node 127.0.0.1:5001 set mykey myvalue
    python -m raftkv.client --node 127.0.0.1:5001 get mykey
    python -m raftkv.client --node 127.0.0.1:5001 del mykey
    python -m raftkv.client --node 127.0.0.1:5001 cas mykey myvalue newvalue
"""

from __future__ import annotations

import argparse
import sys
import uuid

from .transport import send_rpc

MAX_REDIRECTS = 10


class Session:
    """A read-your-writes client session.

    A session is one client talking to the cluster under one
    ``client_id``. Every write it acks advances its watermark to the
    log index (and term) of that write's entry, and every read it
    issues carries the watermark — so a client that wrote through the
    leader and then reads from any node, or from a new leader after a
    failover, never sees its own write missing: the serving node waits
    until it has applied up to the watermark before answering.

    The watermark is also the dedup session: each write carries a
    unique request id within the session, so a retried write returns
    the first attempt's result (and the same index) instead of
    appending a second entry. Reads need no request id — they write
    nothing.
    """

    def __init__(self, node_addr: tuple[str, int],
                 client_id: str | None = None):
        self.node_addr = node_addr
        self.client_id = client_id or uuid.uuid4().hex[:12]
        # Index/term of the last write this session saw acknowledged
        # (0/0 = no write yet: reads are plain ReadIndex reads).
        self.last_write_index = 0
        self.last_write_term = 0
        self._next_request = 0

    @property
    def token(self) -> dict:
        """The session token: what identifies this client and how far
        its acknowledged writes reach in the log."""
        return {"client_id": self.client_id,
                "last_write_index": self.last_write_index,
                "last_write_term": self.last_write_term}

    def _write(self, msg: dict) -> dict:
        self._next_request += 1
        msg["client_id"] = self.client_id
        msg["request_id"] = str(self._next_request)
        resp = request(self.node_addr, msg)
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

    def delete(self, key: str) -> dict:
        return self._write({"type": "client", "op": "del", "key": key})

    def cas(self, key: str, expected: str, value: str) -> dict:
        return self._write({"type": "client", "op": "cas", "key": key,
                            "expected": expected, "value": value})

    def get(self, key: str) -> dict:
        msg: dict = {"type": "client", "op": "get", "key": key,
                     "client_id": self.client_id,
                     "min_commit_index": self.last_write_index,
                     "session_term": self.last_write_term}
        return request(self.node_addr, msg)


def request(node_addr: tuple[str, int], msg: dict, timeout: float = 12.0) -> dict:
    """Send one client request, following leader redirects."""
    host, port = node_addr
    for _ in range(MAX_REDIRECTS):
        resp = send_rpc(host, port, msg, timeout=timeout)
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
    p_remove = sub.add_parser("remove-server",
                              help="remove a server from the cluster")
    p_remove.add_argument("server_id")
    args = parser.parse_args(argv)

    host, _, port = args.node.rpartition(":")
    msg: dict = {"type": "client", "op": args.op}
    if args.op in ("set", "get", "del", "cas"):
        msg["key"] = args.key
    elif args.op == "add_server":
        msg["server_id"] = args.server_id
        ahost, _, aport = args.addr.rpartition(":")
        msg["addr"] = [ahost, int(aport)]
    elif args.op == "remove_server":
        msg["server_id"] = args.server_id
    if args.op == "set":
        msg["value"] = args.value
    if args.op == "cas":
        msg["expected"] = args.expected
        msg["value"] = args.value
    if args.op in ("set", "del", "cas") and args.request_id:
        # A request id without an explicit client id still dedups
        # within the default "cli" session; the same id retried from
        # another session is a different write.
        msg["client_id"] = args.client_id or "cli"
        msg["request_id"] = args.request_id
    if args.op == "get":
        # A read-your-writes read from the shell: --min-index names
        # the last write the reader saw acknowledged (its index, from
        # the write response), --session-term its term.
        if args.client_id:
            msg["client_id"] = args.client_id
        if args.min_index:
            msg["min_commit_index"] = args.min_index
            msg["session_term"] = args.session_term
    resp = request((host, int(port)), msg, timeout=args.timeout)
    status = resp.get("status")
    if status == "ok":
        if args.op == "get":
            print(resp.get("value") if resp.get("value") is not None else "(nil)")
        else:
            print("OK")
        return 0
    print(f"error: {resp.get('error') or resp}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
