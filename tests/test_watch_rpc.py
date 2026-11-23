"""Tests for watch-over-RPC (v0.17.0).

A client sends ``{"type": "watch", "key": ..., "prefix": ...,
"timeout": ...}`` to any node and the node long-polls: it registers a
callback on its local WatchManager, waits up to ``timeout`` seconds
for the first committed change, and replies with every event that
arrived — each carrying the log index of the entry that caused it.
A watch on a follower fires just like one on the leader, because the
apply path notifies on every node; a watch with nothing to report
times out with an empty event list and leaves nothing registered.
"""

import io
import json
import threading
import time
import unittest
from contextlib import redirect_stdout

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session
from raftkv.transport import send_rpc
from raftkv import client as client_mod


def write(addr, key="k", value="v1"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def delete(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "del", "key": key})


def watch_rpc(addr, key, prefix=False, timeout=5.0):
    host, port = addr
    return send_rpc(host, port, {"type": "watch", "key": key,
                                 "prefix": prefix, "timeout": timeout},
                    timeout=timeout + 5.0)


def watch_in_thread(addr, key, prefix=False, timeout=5.0):
    """Issue a watch RPC on a thread (it blocks until an event or
    the timeout) and return the thread plus a slot for the reply."""
    slot: dict = {}

    def _run():
        slot["resp"] = watch_rpc(addr, key, prefix, timeout)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t, slot


class TestWatchRpc(unittest.TestCase):
    def test_watch_rpc_returns_event_on_set(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            t, slot = watch_in_thread(addr, "k", timeout=5.0)
            time.sleep(0.3)  # let the watch register before the write
            self.assertEqual(write(addr).get("status"), "ok")
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive(), "watch RPC did not return")
            resp = slot["resp"]
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(resp["events"]), 1)
            event = resp["events"][0]
            self.assertEqual(event["key"], "k")
            self.assertEqual(event["value"], "v1")
            self.assertEqual(event["op"], "set")
            self.assertIsInstance(event.get("index"), int)
            self.assertGreater(event["index"], 0)
        finally:
            cluster.stop_all()

    def test_watch_rpc_times_out_with_no_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.watch)
            started = time.monotonic()
            resp = watch_rpc(addr, "k", timeout=0.5)
            elapsed = time.monotonic() - started
            self.assertEqual(resp.get("status"), "timeout", resp)
            self.assertEqual(resp.get("events"), [])
            self.assertGreaterEqual(elapsed, 0.4)
            self.assertLess(elapsed, 3.0)
            # A timed-out watch leaves nothing registered behind.
            self.assertEqual(len(leader.watch), before)
        finally:
            cluster.stop_all()

    def test_watch_rpc_on_follower_sees_committed_write(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            t, slot = watch_in_thread(cluster.addrs[follower.id], "k",
                                      timeout=5.0)
            time.sleep(0.3)
            self.assertEqual(write(cluster.addrs[leader.id]).get("status"),
                             "ok")
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive(), "follower watch did not return")
            resp = slot["resp"]
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(resp["events"]), 1)
            self.assertEqual(resp["events"][0]["value"], "v1")
        finally:
            cluster.stop_all()

    def test_watch_rpc_prefix_matches_only_matching_keys(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            t, slot = watch_in_thread(addr, "user:", prefix=True,
                                      timeout=5.0)
            time.sleep(0.3)
            self.assertEqual(write(addr, key="user:1", value="a").get("status"),
                             "ok")
            self.assertEqual(write(addr, key="order:1", value="b").get("status"),
                             "ok")
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive(), "prefix watch did not return")
            resp = slot["resp"]
            self.assertEqual(resp.get("status"), "ok", resp)
            # The first event ends the long-poll; it must be the
            # matching key, never the unrelated one.
            self.assertEqual(resp["events"][0]["key"], "user:1")
            self.assertTrue(all(e["key"].startswith("user:")
                              for e in resp["events"]))
        finally:
            cluster.stop_all()

    def test_watch_rpc_delete_event_has_none_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            self.assertTrue(wait_for_key(leader, "k", "v1"))
            t, slot = watch_in_thread(addr, "k", timeout=5.0)
            time.sleep(0.3)
            self.assertEqual(delete(addr).get("status"), "ok")
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive(), "delete watch did not return")
            resp = slot["resp"]
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp["events"],
                             [{"key": "k", "value": None, "op": "del",
                               "index": resp["events"][0]["index"]}])
        finally:
            cluster.stop_all()

    def test_watch_rpc_rejects_empty_key(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            resp = watch_rpc(cluster.addrs[leader.id], "", timeout=1.0)
            self.assertEqual(resp.get("status"), "error", resp)
            resp = watch_rpc(cluster.addrs[leader.id], None, timeout=1.0)
            self.assertEqual(resp.get("status"), "error", resp)
        finally:
            cluster.stop_all()

    def test_watch_rpc_event_index_matches_log(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, key="a", value="1").get("status"),
                             "ok")
            self.assertTrue(wait_for_key(leader, "a", "1"))
            t, slot = watch_in_thread(addr, "k", timeout=5.0)
            time.sleep(0.3)
            resp = write(addr, key="k", value="v1")
            self.assertEqual(resp.get("status"), "ok", resp)
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive())
            event = slot["resp"]["events"][0]
            # The event's index is the index of the write's own entry.
            self.assertEqual(event["index"], resp.get("index"), resp)
        finally:
            cluster.stop_all()

    def test_session_watch_returns_events(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            session = Session(addr)
            slot: dict = {}

            def _run():
                slot["resp"] = session.watch("k", timeout=5.0)

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            time.sleep(0.3)
            self.assertEqual(write(addr).get("status"), "ok")
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive(), "session watch did not return")
            resp = slot["resp"]
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp["events"][0]["key"], "k")
            self.assertEqual(resp["events"][0]["value"], "v1")
        finally:
            cluster.stop_all()

    def test_watch_cli_prints_event_as_json(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            out = io.StringIO()
            slot: dict = {}

            def _run():
                with redirect_stdout(out):
                    slot["rc"] = client_mod.main(
                        ["--node", f"{host}:{port}", "watch", "k",
                         "--timeout", "5"])

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            time.sleep(0.3)
            self.assertEqual(write(addr).get("status"), "ok")
            t.join(timeout=6.0)
            self.assertFalse(t.is_alive(), "CLI watch did not return")
            self.assertEqual(slot["rc"], 0)
            lines = [ln for ln in out.getvalue().splitlines() if ln.strip()]
            self.assertEqual(len(lines), 1, out.getvalue())
            event = json.loads(lines[0])
            self.assertEqual(event["key"], "k")
            self.assertEqual(event["value"], "v1")
            self.assertEqual(event["op"], "set")
            self.assertIsInstance(event.get("index"), int)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
