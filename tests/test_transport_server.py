"""Tests for the reactor + worker-pool server (transport.NodeServer).

The server replaced the old thread-per-connection handler in v0.28.0:
one reactor thread owns the sockets, dispatch runs on a bounded pool
of daemon workers. These tests prove the properties the change
promises: idle connections cost no thread, concurrent requests are
all served correctly and in order per connection, frames may arrive
split across reads, a slow dispatch cannot starve the pool, a watch
long-polling on the leader cannot starve reads, and idle connections
are closed.
"""

import concurrent.futures
import socket
import threading
import time
import unittest

from harness import Cluster
from raftkv.transport import (
    ConnectionPool,
    NodeServer,
    encode_message,
    recv_message,
    send_rpc,
)


class _ServerCase(unittest.TestCase):
    """A bare NodeServer with an echo dispatch, for transport-level
    tests that need no Raft cluster."""

    dispatch = staticmethod(lambda msg: {"type": "echo",
                                         "value": msg.get("value")})

    def setUp(self):
        self.server = NodeServer("127.0.0.1", 0, self.dispatch)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]
        self.socks: list[socket.socket] = []

    def tearDown(self):
        for sock in self.socks:
            try:
                sock.close()
            except OSError:
                pass
        self.server.shutdown()
        self.server.server_close()

    def _open(self) -> socket.socket:
        sock = socket.create_connection(("127.0.0.1", self.port))
        self.socks.append(sock)
        return sock


class TestThreadPoolServer(_ServerCase):
    def test_idle_connections_use_bounded_threads(self):
        # Thirty idle connections must not cost thirty threads: the
        # reactor owns them all and no worker spawns until a request
        # arrives.
        before = threading.active_count()
        for _ in range(30):
            self._open()
        time.sleep(0.5)  # let the reactor accept them all
        self.assertLessEqual(threading.active_count() - before, 3)

    def test_concurrent_requests_all_correct(self):
        pool = ConnectionPool(max_size=20)
        try:
            def call(i):
                resp = send_rpc("127.0.0.1", self.port,
                                {"type": "echo", "value": i},
                                timeout=10.0, pool=pool)
                return resp.get("value") == i

            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=20) as ex:
                self.assertTrue(all(ex.map(call, range(200))))
        finally:
            pool.close()

    def test_split_frame_is_reassembled(self):
        sock = self._open()
        data = encode_message({"type": "echo", "value": 7})
        sock.sendall(data[:3])
        time.sleep(0.2)
        sock.sendall(data[3:])
        sock.settimeout(5.0)
        self.assertEqual(recv_message(sock).get("value"), 7)

    def test_pipelined_frames_reply_in_order(self):
        # The first frame takes 0.2s to dispatch, the second none: if
        # the pool ran them out of order the replies would swap.
        def slow_first(msg):
            if msg.get("value") == "first":
                time.sleep(0.2)
            return {"type": "echo", "value": msg.get("value")}

        self.server.shutdown()
        self.server.server_close()
        self.server = NodeServer("127.0.0.1", 0, slow_first)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]
        sock = self._open()
        sock.settimeout(5.0)
        sock.sendall(encode_message({"type": "echo", "value": "first"}))
        sock.sendall(encode_message({"type": "echo", "value": "second"}))
        self.assertEqual(recv_message(sock).get("value"), "first")
        self.assertEqual(recv_message(sock).get("value"), "second")

    def test_slow_dispatch_does_not_starve_pool(self):
        # Eight 1s dispatches in parallel plus fast ones: serial
        # would take 8s, the pool takes about 1s.
        def slow(msg):
            if msg.get("slow"):
                time.sleep(1.0)
            return {"type": "echo", "value": msg.get("value")}

        self.server.shutdown()
        self.server.server_close()
        self.server = NodeServer("127.0.0.1", 0, slow)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]
        pool = ConnectionPool(max_size=16)
        try:
            start = time.perf_counter()

            def call(i):
                resp = send_rpc("127.0.0.1", self.port,
                                {"type": "echo", "value": i,
                                 "slow": i < 8},
                                timeout=10.0, pool=pool)
                return resp.get("value") == i

            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=16) as ex:
                self.assertTrue(all(ex.map(call, range(16))))
            self.assertLess(time.perf_counter() - start, 5.0)
        finally:
            pool.close()

    def test_idle_connection_is_closed(self):
        server = NodeServer("127.0.0.1", 0,
                            lambda msg: {"type": "ok"}, idle_timeout=0.5)
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        try:
            sock = socket.create_connection(
                ("127.0.0.1", server.server_address[1]))
            sock.settimeout(5.0)
            time.sleep(1.5)
            self.assertEqual(sock.recv(1), b"")
            sock.close()
        finally:
            server.shutdown()
            server.server_close()


class TestWatchDoesNotStarveReads(unittest.TestCase):
    def test_watch_does_not_starve_reads(self):
        cluster = Cluster()
        cluster.start_all()
        try:
            leader = cluster.wait_for_stable_leader()
            leader.handle_rpc({"type": "client", "op": "set",
                               "key": "k", "value": "v"})
            # A 5s watch pins one pool worker on the leader.
            results: dict = {}

            def watch():
                results["watch"] = send_rpc(
                    *cluster.addrs[leader.id],
                    {"type": "watch", "key": "other", "timeout": 5.0},
                    timeout=10.0)

            watcher = threading.Thread(target=watch, daemon=True)
            watcher.start()
            time.sleep(0.3)
            pool = ConnectionPool(max_size=8)
            try:
                latencies = []
                for _ in range(20):
                    start = time.perf_counter()
                    resp = send_rpc(
                        *cluster.addrs[leader.id],
                        {"type": "client", "op": "get", "key": "k"},
                        timeout=10.0, pool=pool)
                    latencies.append(time.perf_counter() - start)
                    self.assertEqual(resp.get("value"), "v")
                # Reads stay fast while the watch waits: the pool is
                # not starved by one long-polling dispatch.
                self.assertLess(max(latencies), 2.0)
            finally:
                pool.close()
            watcher.join(timeout=8)
            self.assertEqual(results["watch"].get("status"), "timeout")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
