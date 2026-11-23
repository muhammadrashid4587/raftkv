"""Tests for persistent client connections (ConnectionPool).

The server already serves many messages on one connection — its
handler loops until the client closes — so the pool only has to stop
opening a fresh TCP connection per request. These tests prove the
reuse (a counting server sees one connection for many RPCs), the
recovery (a stale pooled socket is retried on a fresh connection
after a restart), the cap (past ``max_size`` extra callers open an
ephemeral connection instead of blocking), and that a shared pool
serves concurrent reads against a real cluster correctly.
"""

import concurrent.futures
import socketserver
import threading
import time
import unittest

from harness import Cluster
from raftkv.client import Session
from raftkv.transport import (
    ConnectionPool,
    recv_message,
    send_message,
    send_rpc,
)


class _CountingHandler(socketserver.BaseRequestHandler):
    """Echo handler that counts connections: one ``setup`` per TCP
    connection, however many messages ride on it."""

    def setup(self):
        self.server.connections[0] += 1

    def handle(self):
        self.request.settimeout(10.0)
        while True:
            msg = recv_message(self.request)
            if msg is None:
                return
            if msg.get("sleep"):
                time.sleep(msg["sleep"])
            if not send_message(self.request, {
                    "type": "echo", "status": "ok",
                    "key": msg.get("key")}):
                return


class _CountingServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr):
        self.connections = [0]
        super().__init__(addr, _CountingHandler)


def _start_server():
    server = _CountingServer(("127.0.0.1", 0))
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    return server


class TestConnectionPool(unittest.TestCase):
    def test_pool_reuses_one_connection(self):
        server = _start_server()
        try:
            host, port = server.server_address
            pool = ConnectionPool(max_size=4)
            try:
                for i in range(5):
                    resp = pool.rpc(host, port, {"key": f"k{i}"})
                    self.assertEqual(resp.get("status"), "ok", resp)
                    self.assertEqual(resp.get("key"), f"k{i}")
            finally:
                pool.close()
            self.assertEqual(server.connections[0], 1)
        finally:
            server.shutdown()
            server.server_close()

    def test_pool_recovers_after_server_restart(self):
        server = _start_server()
        host, port = server.server_address
        pool = ConnectionPool(max_size=4)
        try:
            resp = pool.rpc(host, port, {"key": "before"})
            self.assertEqual(resp.get("status"), "ok", resp)
            server.shutdown()
            server.server_close()
            # The pooled socket is now dead; the next call must retry
            # on a fresh connection instead of failing.
            restarted = _CountingServer((host, port))
            thread = threading.Thread(
                target=restarted.serve_forever,
                kwargs={"poll_interval": 0.05}, daemon=True)
            thread.start()
            try:
                resp = pool.rpc(host, port, {"key": "after"})
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("key"), "after")
            finally:
                restarted.shutdown()
                restarted.server_close()
        finally:
            pool.close()

    def test_idle_connection_expires(self):
        server = _start_server()
        try:
            host, port = server.server_address
            pool = ConnectionPool(max_size=4, max_idle_seconds=0.05)
            try:
                self.assertEqual(
                    pool.rpc(host, port, {"key": "a"}).get("status"), "ok")
                time.sleep(0.15)
                # The idle socket is past its age, so this opens a
                # second connection instead of reusing the dead one.
                self.assertEqual(
                    pool.rpc(host, port, {"key": "b"}).get("status"), "ok")
            finally:
                pool.close()
            self.assertEqual(server.connections[0], 2)
        finally:
            server.shutdown()
            server.server_close()

    def test_pool_serves_concurrent_calls_past_the_cap(self):
        server = _start_server()
        try:
            host, port = server.server_address
            pool = ConnectionPool(max_size=1)
            try:
                # Five slow calls at once against a cap of one: the
                # first takes the pooled connection, the rest open
                # ephemeral ones instead of queueing behind it.
                def call(i):
                    return pool.rpc(host, port,
                                    {"key": f"k{i}", "sleep": 0.2},
                                    timeout=5.0)

                start = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=5) as ex:
                    results = list(ex.map(call, range(5)))
                elapsed = time.monotonic() - start
                for i, resp in enumerate(results):
                    self.assertEqual(resp.get("status"), "ok", resp)
                    self.assertEqual(resp.get("key"), f"k{i}")
                # Sequential would take ~1s; concurrent finishes in
                # ~one round of slow calls.
                self.assertLess(elapsed, 0.8, elapsed)
            finally:
                pool.close()
        finally:
            server.shutdown()
            server.server_close()

    def test_one_shot_send_rpc_unchanged(self):
        # send_rpc with no pool still opens a fresh connection per
        # call — the counting server sees one per RPC.
        server = _start_server()
        try:
            host, port = server.server_address
            for i in range(3):
                resp = send_rpc(host, port, {"key": f"k{i}"})
                self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(server.connections[0], 3)
        finally:
            server.shutdown()
            server.server_close()

    def test_pool_rejects_bad_size(self):
        with self.assertRaises(ValueError):
            ConnectionPool(max_size=0)


class TestPooledSession(unittest.TestCase):
    def test_session_uses_pool_and_close_releases_it(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            session = Session(cluster.addrs[leader.id], "pool-s1",
                              pool_size=4)
            resp = session.set("pk", "pv")
            self.assertEqual(resp.get("status"), "ok", resp)
            resp = session.get("pk")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "pv")
            # The requests rode pooled connections, not fresh ones.
            self.assertIsNotNone(session._pool)
            self.assertGreaterEqual(session._pool.idle_count, 1)
            session.close()
            self.assertIsNone(session._pool)
            # A closed session is still usable: the next request
            # opens a fresh pool.
            resp = session.get("pk")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("value"), "pv")
            session.close()
        finally:
            cluster.stop_all()

    def test_session_pool_size_zero_means_one_shot(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            session = Session(cluster.addrs[leader.id], "pool-s0",
                              pool_size=0)
            resp = session.set("zk", "zv")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertIsNone(session._pool)
            resp = session.get("zk")
            self.assertEqual(resp.get("value"), "zv")
        finally:
            cluster.stop_all()

    def test_pooled_concurrent_reads_are_correct(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            session = Session(addr, "pool-reader", pool_size=4)
            for i in range(20):
                self.assertEqual(session.set(f"ck:{i}", "cv").get("status"),
                                 "ok")
            pool = session._pool
            self.assertIsNotNone(pool)

            def do_get(i):
                from raftkv.client import request
                resp = request(addr, {"type": "client", "op": "get",
                                      "key": f"ck:{i}"},
                               timeout=12.0, pool=pool)
                return resp.get("value")

            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=10) as ex:
                values = list(ex.map(do_get, range(20)))
            self.assertEqual(values, ["cv"] * 20)
            session.close()
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
