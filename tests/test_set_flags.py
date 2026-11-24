"""Tests for raftkv SET NX / XX / KEEPTTL flags.

A ``set`` may carry Redis's SET options: NX sets only when the key
does not exist, XX only when it does, KEEPTTL preserves the deadline
a plain set would clear. The outcome is decided when the entry
applies — at its exact position in the log — so two concurrent
conditional sets on the same key race in log order and exactly one
wins, and a retry with the same session id returns the first
attempt's result instead of setting twice. A losing set writes
nothing at all: the value and any deadline stay exactly as they
were, and no watch event fires.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.store import KVStore


def write(addr, key="k", value="v", **flags):
    msg = {"type": "client", "op": "set", "key": key, "value": value}
    msg.update(flags)
    return client_request(addr, msg)


def read(addr, key="k"):
    return client_request(addr, {"type": "client", "op": "get", "key": key})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestSetFlagsStore(unittest.TestCase):
    def test_store_plain_set_answers_one(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v"}),
            {"type": "client_response", "status": "ok", "set": 1})
        self.assertEqual(store.get("k"), "v")

    def test_store_set_nx_wins_on_missing_key(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "nx": True})["set"], 1)
        self.assertEqual(store.get("k"), "v")

    def test_store_set_nx_loses_on_existing_key(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "nx": True}),
            {"type": "client_response", "status": "ok", "set": 0})
        # The losing set wrote nothing: value and deadline both
        # untouched.
        self.assertEqual(store.get("k"), "first")
        self.assertEqual(store.expiries(), {"k": 15})

    def test_store_set_xx_wins_on_existing_key(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "xx": True})["set"], 1)
        self.assertEqual(store.get("k"), "second")

    def test_store_set_xx_loses_on_missing_key(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "xx": True}),
            {"type": "client_response", "status": "ok", "set": 0})
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.keys(), ([], False))

    def test_store_set_nx_after_delete_wins(self):
        # A key that was deleted is absent again, so a later NX
        # wins — the "exists" check is against live state, not
        # history.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "first"})
        store.apply({"op": "del", "key": "k"})
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "second",
                         "nx": True})["set"], 1)
        self.assertEqual(store.get("k"), "second")

    def test_store_set_keepttl_keeps_log_deadline(self):
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(store.expiries(), {"k": 15})
        # A KEEPTTL overwrite changes the value but not the
        # deadline: the key still dies at 15.
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "keepttl": True}, index=11)["set"], 1)
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.expiries(), {"k": 15})
        # And a plain set after it clears the deadline, like
        # always.
        store.apply({"op": "set", "key": "k", "value": "v3"},
                    index=12)
        self.assertEqual(store.expiries(), {})

    def test_store_set_keepttl_keeps_wall_deadline(self):
        store = KVStore()
        store.apply({"op": "setex", "key": "k", "value": "v",
                     "wall_deadline": 1_800_000_000.0})
        self.assertEqual(store.wall_expiries(),
                         {"k": 1_800_000_000.0})
        store.apply({"op": "set", "key": "k", "value": "v2",
                     "keepttl": True})
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.wall_expiries(),
                         {"k": 1_800_000_000.0})
        # A plain set clears the wall deadline, like always.
        store.apply({"op": "set", "key": "k", "value": "v3"})
        self.assertEqual(store.wall_expiries(), {})

    def test_store_set_keepttl_on_missing_key_records_no_deadline(self):
        store = KVStore()
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v",
                         "keepttl": True})["set"], 1)
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.expiries(), {})
        self.assertEqual(store.wall_expiries(), {})

    def test_store_set_xx_with_keepttl_keeps_deadline(self):
        # The flags compose: XX decides whether the write
        # happens, KEEPTTL decides what happens to the deadline.
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "v",
                     "ttl_entries": 5}, index=10)
        self.assertEqual(
            store.apply({"op": "set", "key": "k", "value": "v2",
                         "xx": True, "keepttl": True},
                        index=11)["set"], 1)
        self.assertEqual(store.get("k"), "v2")
        self.assertEqual(store.expiries(), {"k": 15})
        # And a losing XX with KEEPTTL writes nothing at all.
        self.assertEqual(
            store.apply({"op": "set", "key": "missing", "value": "v",
                         "xx": True, "keepttl": True})["set"], 0)
        self.assertIsNone(store.get("missing"))


class TestSetFlagsCluster(unittest.TestCase):
    def test_set_nx_xx_replicate(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, nx=True).get("set"), 1)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v"))
            self.assertEqual(write(addr, key="k", value="v2",
                                   xx=True).get("set"), 1)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v2"))
        finally:
            cluster.stop_all()

    def test_set_nx_loses_on_existing_key_everywhere(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr).get("status"), "ok")
            resp = write(addr, value="other", nx=True)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 0, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "k", "v"))
        finally:
            cluster.stop_all()

    def test_set_xx_loses_on_missing_key_everywhere(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = write(addr, xx=True)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 0, resp)
            for node_id in cluster.live:
                self.assertIsNone(
                    cluster.nodes[node_id].sm.get("k"))
        finally:
            cluster.stop_all()

    def test_set_flags_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            # NX and XX together are mutually exclusive.
            resp = write(addr, nx=True, xx=True)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("mutually exclusive",
                          resp.get("error", ""), resp)
            # KEEPTTL cannot ride with a fresh TTL.
            resp = write(addr, ttl_entries=5, keepttl=True)
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("KEEPTTL", resp.get("error", ""), resp)
            # A flag that is not a bare boolean is malformed.
            resp = write(addr, nx="yes")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("NX", resp.get("error", ""), resp)
            # None of the three appended a log entry.
            self.assertEqual(len(leader.log), before)
            self.assertIsNone(read(addr).get("value"))
        finally:
            cluster.stop_all()

    def test_set_retry_returns_first_outcome(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            msg = {"type": "client", "op": "set", "key": "k",
                   "value": "v", "nx": True,
                   "client_id": "c1", "request_id": "r1"}
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("set"), 1, resp)
            # The retry returns the first attempt's 1, and appends
            # no second entry — even though the key now exists, so
            # a fresh NX would lose.
            resp = client_request(addr, dict(msg, value="other"))
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("set"), 1, resp)
            self.assertEqual(read(addr).get("value"), "v")
            self.assertEqual(count_entries(leader, "r1"), 1)
            # And a losing set's retry returns the same 0, not a
            # fresh attempt that might win after a delete.
            msg = {"type": "client", "op": "set", "key": "k",
                   "value": "other", "nx": True,
                   "client_id": "c1", "request_id": "r2"}
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("set"), 0, resp)
            resp = client_request(addr, msg)
            self.assertEqual(resp.get("set"), 0, resp)
            self.assertEqual(count_entries(leader, "r2"), 1)
            self.assertEqual(read(addr).get("value"), "v")
        finally:
            cluster.stop_all()

    def test_set_keepttl_keeps_deadline_on_cluster(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                self.assertEqual(
                    session.set("k", "v", ttl_entries=100)
                    .get("status"), "ok")
                deadline = session.ttl("k").get("deadline")
                self.assertIsNotNone(deadline)
                # A KEEPTTL overwrite keeps the same deadline.
                self.assertEqual(
                    session.set("k", "v2", keepttl=True).get("set"), 1)
                self.assertEqual(session.ttl("k").get("deadline"),
                                 deadline)
                self.assertEqual(session.get("k").get("value"), "v2")
                # A plain set clears it.
                self.assertEqual(
                    session.set("k", "v3").get("set"), 1)
                self.assertEqual(session.ttl("k").get("ttl"), -1)
                self.assertEqual(session.get("k").get("value"), "v3")
        finally:
            cluster.stop_all()

    def test_set_keepttl_keeps_wall_deadline_on_cluster(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                self.assertEqual(
                    session.setex("k", "v", 3600).get("status"), "ok")
                wall = session.ttl("k").get("wall_deadline")
                self.assertIsNotNone(wall)
                self.assertEqual(
                    session.set("k", "v2", keepttl=True).get("set"), 1)
                self.assertEqual(session.ttl("k").get("wall_deadline"),
                                 wall)
                self.assertEqual(session.get("k").get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_set_fires_watch_event_only_on_win(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("k", events.append)
            follower.watch.watch("k", events.append)
            self.assertEqual(write(addr, nx=True).get("set"), 1)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "k", event)
                self.assertEqual(event["value"], "v", event)
                self.assertEqual(event["op"], "set", event)
            # A losing NX fires nothing, and neither does a losing
            # XX on a missing key.
            self.assertEqual(write(addr, value="other", nx=True)
                             .get("set"), 0)
            self.assertEqual(write(addr, key="missing", xx=True)
                             .get("set"), 0)
            time.sleep(0.3)
            self.assertEqual(len(events), 2, events)
        finally:
            cluster.stop_all()

    def test_set_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, nx=True).get("set"), 1)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            # The key exists on the new leader too, so this loses.
            self.assertEqual(write(addr, value="other", nx=True)
                             .get("set"), 0)
            self.assertEqual(read(addr).get("value"), "v")
            # And XX still wins there.
            self.assertEqual(write(addr, value="v2", xx=True)
                             .get("set"), 1)
            self.assertEqual(read(addr).get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_set_flags_survive_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(write(addr, nx=True).get("set"), 1)
            # Enough writes to compact the log past the set.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x")
                    .get("status"), "ok")
            self.assertEqual(write(addr, value="other", nx=True)
                             .get("set"), 0)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(read(addr).get("value"), "v")
            self.assertEqual(write(addr, value="other", nx=True)
                             .get("set"), 0)
        finally:
            cluster.stop_all()

    def test_session_set_flags_is_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.set("k", "v", nx=True)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("set"), 1, resp)
                self.assertGreater(session.last_write_index, 0)
                self.assertEqual(session.get("k").get("value"), "v")
                # A losing set through the same session still
                # advances the watermark: it committed an entry.
                before = session.last_write_index
                resp = session.set("k", "other", nx=True)
                self.assertEqual(resp.get("set"), 0, resp)
                self.assertGreater(session.last_write_index, before)
                self.assertEqual(session.get("k").get("value"), "v")
                # And XX wins on the key that is there.
                resp = session.set("k", "v2", xx=True)
                self.assertEqual(resp.get("set"), 1, resp)
                self.assertEqual(session.get("k").get("value"), "v2")
        finally:
            cluster.stop_all()

    def test_pipeline_set_flags(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "a", "value": "1",
                     "nx": True},
                    {"op": "set", "key": "a", "value": "2",
                     "nx": True},
                    {"op": "set", "key": "a", "value": "3",
                     "xx": True},
                    {"op": "set", "key": "b", "value": "x",
                     "xx": True},
                    {"op": "set", "key": "c", "value": "y",
                     "nx": True, "xx": True},
                    {"op": "set", "key": "d", "value": "z",
                     "keepttl": True, "ttl_entries": 5},
                    {"op": "set", "key": "e", "value": "w",
                     "nx": "yes"},
                ])
            self.assertEqual(len(replies), 7, replies)
            self.assertEqual(replies[0].get("set"), 1, replies)
            self.assertEqual(replies[1].get("set"), 0, replies)
            self.assertEqual(replies[2].get("set"), 1, replies)
            self.assertEqual(replies[3].get("set"), 0, replies)
            self.assertEqual(replies[4].get("status"), "error",
                             replies)
            self.assertEqual(replies[5].get("status"), "error",
                             replies)
            self.assertEqual(replies[6].get("status"), "error",
                             replies)
            self.assertEqual(read(addr, key="a").get("value"), "3")
            self.assertIsNone(read(addr, key="b").get("value"))
        finally:
            cluster.stop_all()

    def test_set_flags_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "v",
                                  "--nx"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "k", "other",
                                  "--nx"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # XX loses on a missing key, wins once it exists.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "missing",
                                  "v", "--xx"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "0")
            # The losing XX created nothing, so a second XX still
            # loses — the key has to be created first.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "missing",
                                  "v", "--nx"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "missing",
                                  "v2", "--xx"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "1")
            # --nx and --xx together are a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "set", "k", "v",
                                 "--nx", "--xx"])
            self.assertEqual(cm.exception.code, 2)
            # --keepttl with --ttl-entries is a server error, not
            # a write.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "set", "k", "v",
                                  "--keepttl", "--ttl-entries", "5"])
            self.assertEqual(rc, 1)
            self.assertIn("KEEPTTL", err.getvalue())
            self.assertEqual(read(cluster.addrs[leader.id], "k")
                             .get("value"), "v")
            # --keepttl keeps the deadline a plain set would clear.
            with Session(cluster.addrs[leader.id]) as session:
                session.set("t", "v", ttl_entries=100)
                deadline = session.ttl("t").get("deadline")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "set", "t", "v2",
                                  "--keepttl"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "OK")
            with Session(cluster.addrs[leader.id]) as session:
                self.assertEqual(session.ttl("t").get("deadline"),
                                 deadline)
                self.assertEqual(session.get("t").get("value"), "v2")
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
