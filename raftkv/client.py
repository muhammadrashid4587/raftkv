"""raftkv client: set / get / del against a cluster.

The client talks to any node; writes and reads sent to a follower get a
redirect naming the current leader, which the client follows
automatically (up to 10 hops).

Examples:

    python -m raftkv.client --node 127.0.0.1:5001 set mykey myvalue
    python -m raftkv.client --node 127.0.0.1:5001 get mykey
    python -m raftkv.client --node 127.0.0.1:5001 del mykey
"""

from __future__ import annotations

import argparse
import sys

from .transport import send_rpc

MAX_REDIRECTS = 10


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
    p_get = sub.add_parser("get")
    p_get.add_argument("key")
    p_del = sub.add_parser("del")
    p_del.add_argument("key")
    args = parser.parse_args(argv)

    host, _, port = args.node.rpartition(":")
    msg: dict = {"type": "client", "op": args.op, "key": args.key}
    if args.op == "set":
        msg["value"] = args.value
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
