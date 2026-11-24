"""Pipelined batch writes: many set/del/cas ops over one connection.

``Session.pipeline`` sends a list of write ops in batches on a single
persistent socket without waiting between requests, and the replies
come back aligned by position. Every op carries its own session
request id, so a re-send after a redirect or a lost reply is
dedup-safe — the server returns the first attempt's result instead
of appending a second entry. These spin up real nodes (real TCP,
real threads, real disk) on localhost, like the rest of the suite,
except the lost-reply retry test, which talks to a small fake
framing server so the dropped connection is deterministic.
"""

import json
import socket
import subprocess
import sys
import threading
import unittest

from harness import Cluster, client_request
from raftkv.client import Session, mget, pipeline
from raftkv.transport import encode_message, recv_message


class TestPipelineWrites(unittest.TestCase):
    def test_pipeline_mixed_set_del_applies_in_order(self):
        """A mixed batch — set, set, del, set — applies in order:
        every reply is ok and the store ends up with the last
        write to each key, the deleted key gone."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "pw:a", "value": "1"},
                    {"op": "set", "key": "pw:b", "value": "2"},
                    {"op": "del", "key": "pw:a"},
                    {"op": "set", "key": "pw:c", "value": "3"},
                    {"op": "set", "key": "pw:b", "value": "22"},
                ])
            self.assertEqual(len(replies), 5)
            for resp in replies:
                self.assertEqual(resp.get("status"), "ok", resp)
            got = mget(addr, ["pw:a", "pw:b", "pw:c"])
            self.assertEqual(got.get("values"), [None, "22", "3"])
        finally:
            cluster.stop_all()

    def test_pipeline_replies_aligned_with_failure_in_middle(self):
        """A CAS with the wrong expected value fails in its own
        position and the ops after it still run — the batch is
        not atomic, and a failure never shifts the replies after
        it. The ops before the failure are applied, which is the
        honest non-atomicity of a pipelined batch: use mset when
        the batch must apply as one unit."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            client_request(addr, {"type": "client", "op": "set",
                                  "key": "pw:x", "value": "old"})
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "pw:y", "value": "1"},
                    {"op": "cas", "key": "pw:x", "expected": "wrong",
                     "value": "new"},
                    {"op": "set", "key": "pw:z", "value": "3"},
                ])
            self.assertEqual(len(replies), 3)
            self.assertEqual(replies[0].get("status"), "ok", replies[0])
            self.assertEqual(replies[1].get("status"), "cas_failed",
                             replies[1])
            self.assertEqual(replies[2].get("status"), "ok", replies[2])
            got = mget(addr, ["pw:x", "pw:y", "pw:z"])
            self.assertEqual(got.get("values"), ["old", "1", "3"])
        finally:
            cluster.stop_all()

    def test_pipeline_malformed_op_is_error_in_position(self):
        """A malformed op — unknown op, missing value, non-string
        key — never goes on the wire: its position comes back an
        error response and the well-formed ops around it still
        apply, in order."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "pw:m1", "value": "1"},
                    {"op": "explode", "key": "pw:m2"},
                    {"op": "set", "key": "pw:m3"},
                    {"op": "set", "key": 42, "value": "x"},
                    {"op": "set", "key": "pw:m5", "value": "5"},
                ])
            self.assertEqual(len(replies), 5)
            self.assertEqual(replies[0].get("status"), "ok", replies[0])
            for i in (1, 2, 3):
                self.assertEqual(replies[i].get("status"), "error",
                                 replies[i])
            self.assertEqual(replies[4].get("status"), "ok", replies[4])
            got = mget(addr, ["pw:m1", "pw:m5"])
            self.assertEqual(got.get("values"), ["1", "5"])
        finally:
            cluster.stop_all()

    def test_pipeline_empty_returns_empty(self):
        """No ops means no requests and no connection — an empty
        list back, without touching the server."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            with Session(cluster.addrs[leader.id]) as session:
                self.assertEqual(session.pipeline([]), [])
        finally:
            cluster.stop_all()

    def test_pipeline_follows_redirect_from_follower(self):
        """Pipelining at a follower re-sends the batch to the leader
        it names, and every op applies exactly once — the leader's
        log grows by exactly the number of ops, not twice, so the
        redirect re-send did not double-apply."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.nodes.values()
                            if n is not leader)
            before = len(leader.log)
            with Session(cluster.addrs[follower.id]) as session:
                replies = session.pipeline([
                    {"op": "set", "key": f"pw:r{i}", "value": str(i)}
                    for i in range(5)
                ])
            for resp in replies:
                self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(leader.log), before + 5)
            got = mget(cluster.addrs[leader.id],
                       [f"pw:r{i}" for i in range(5)])
            self.assertEqual(got.get("values"),
                             [str(i) for i in range(5)])
        finally:
            cluster.stop_all()

    def test_pipeline_advances_session_watermark(self):
        """A pipeline advances the session's write watermark to the
        highest index its replies carry, so a read through the
        same session afterwards is read-your-writes: it sees
        every key the batch acknowledged."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "pw:w", "value": "v"},
                ])
                self.assertEqual(replies[0].get("status"), "ok",
                                 replies[0])
                self.assertGreater(session.last_write_index, 0)
                got = session.get("pw:w")
            self.assertEqual(got.get("status"), "ok", got)
            self.assertEqual(got.get("value"), "v")
        finally:
            cluster.stop_all()

    def test_pipeline_large_batch_splits_by_depth(self):
        """A 120-op batch — deeper than the default depth of 50, so
        it splits into several batches on one connection — still
        returns every reply in order and applies every op."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            ops = [{"op": "set", "key": f"pw:d{i}", "value": str(i)}
                   for i in range(120)]
            with Session(addr) as session:
                replies = session.pipeline(ops, depth=50)
            self.assertEqual(len(replies), 120)
            for resp in replies:
                self.assertEqual(resp.get("status"), "ok", resp)
            got = mget(addr, [f"pw:d{i}" for i in range(120)])
            self.assertEqual(got.get("values"),
                             [str(i) for i in range(120)])
        finally:
            cluster.stop_all()

    def test_pipeline_module_level_function(self):
        """The module-level ``pipeline`` is the same batch through a
        fresh one-shot session: replies aligned, values landed."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            replies = pipeline(addr, [
                {"op": "set", "key": "pw:f1", "value": "a"},
                {"op": "set", "key": "pw:f2", "value": "b"},
                {"op": "del", "key": "pw:f1"},
            ])
            self.assertEqual(len(replies), 3)
            for resp in replies:
                self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(pipeline(addr, []), [])
            got = mget(addr, ["pw:f1", "pw:f2"])
            self.assertEqual(got.get("values"), [None, "b"])
        finally:
            cluster.stop_all()

    def test_pipeline_retries_lost_replies_with_same_ids(self):
        """A connection that swallows the batch — reads every frame
        and closes without replying — is retried once with the
        same request ids, on a fresh connection, and the retry's
        replies come back ok. The fake server sees each op twice
        with identical (client_id, request_id), which is exactly
        what makes the retry safe: a real server dedups by that
        pair and returns the first attempt's result instead of
        applying twice."""
        received: list[list[dict]] = []
        ready = threading.Event()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        port = srv.getsockname()[1]
        srv.listen(2)
        srv.settimeout(10.0)

        def serve():
            try:
                for attempt in range(2):
                    conn, _ = srv.accept()
                    conn.settimeout(5.0)
                    frames = []
                    try:
                        while len(frames) < 3:
                            msg = recv_message(conn)
                            if msg is None:
                                break
                            frames.append(msg)
                    finally:
                        received.append(frames)
                        if attempt == 1:
                            for msg in frames:
                                conn.sendall(encode_message(
                                    {"type": "client_response",
                                     "status": "ok", "index": 7,
                                     "term": 1}))
                        conn.close()
            except OSError:
                pass
            finally:
                ready.set()
                srv.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            with Session(("127.0.0.1", port)) as session:
                replies = session.pipeline([
                    {"op": "set", "key": "pw:s1", "value": "a"},
                    {"op": "set", "key": "pw:s2", "value": "b"},
                    {"op": "del", "key": "pw:s1"},
                ], timeout=5.0)
            self.assertTrue(ready.wait(timeout=10.0),
                            "fake server did not finish")
            self.assertEqual(len(replies), 3)
            for resp in replies:
                self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(len(received), 2)
            self.assertEqual(len(received[0]), 3)
            self.assertEqual(len(received[1]), 3)
            first_ids = [(m.get("client_id"), m.get("request_id"))
                         for m in received[0]]
            second_ids = [(m.get("client_id"), m.get("request_id"))
                          for m in received[1]]
            self.assertEqual(first_ids, second_ids)
            self.assertEqual(len(set(first_ids)), 3,
                             "each op needs its own request id")
        finally:
            try:
                srv.close()
            except OSError:
                pass

    def test_pipeline_cli(self):
        """The ``pipeline`` CLI subcommand reads JSON ops (one per
        line) from stdin and prints one JSON response per line, in
        order — including an error line for a malformed line, which
        does not abort the batch."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            host, port = addr
            stdin = "\n".join([
                json.dumps({"op": "set", "key": "pw:c1",
                            "value": "a"}),
                json.dumps({"op": "set", "key": "pw:c2",
                            "value": "b"}),
                "not json at all",
                json.dumps({"op": "del", "key": "pw:c1"}),
            ]) + "\n"
            out = subprocess.run(
                [sys.executable, "-m", "raftkv.client",
                 "--node", f"{host}:{port}", "pipeline"],
                input=stdin, capture_output=True, text=True,
                timeout=15)
            self.assertEqual(out.returncode, 0, out.stderr)
            lines = [json.loads(line)
                     for line in out.stdout.strip().splitlines()]
            self.assertEqual(len(lines), 4)
            self.assertEqual(lines[0].get("status"), "ok", lines[0])
            self.assertEqual(lines[1].get("status"), "ok", lines[1])
            self.assertEqual(lines[2].get("status"), "error", lines[2])
            self.assertEqual(lines[3].get("status"), "ok", lines[3])
            got = mget(addr, ["pw:c1", "pw:c2"])
            self.assertEqual(got.get("values"), [None, "b"])
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
