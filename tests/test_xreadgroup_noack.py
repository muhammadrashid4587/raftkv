"""Tests for raftkv XREADGROUP NOACK re-reads (v0.75.0).

A NOACK re-read of a consumer's own pending entries — an
explicit-ID ``xreadgroup`` with ``noack`` — hands the entries back
without touching the pending record at all: the delivery count
stays where the last real delivery left it and the delivery stamp
is not re-stamped, so the entry's idle keeps running from that
delivery instead of resetting on a mere peek, and the re-read is
distinguishable from a fresh ">" delivery and from a plain
re-read in XPENDING's per-entry form. The consumer's own
last-seen stamp still moves — the consumer interacted and
received entries — and the entries stay pending afterwards,
still ackable and still claimable once idle long enough. This is
a deliberate departure from Redis, where NOACK is ignored for a
history read and the re-read still counts as a delivery: here
NOACK means the delivery is not recorded, whichever ID form the
read takes. The store half runs against a bare KVStore with
fixed stamps, so the idle arithmetic is exact; the cluster half
spins up real nodes (real TCP, real threads, real disk) on
localhost, like the rest of the suite.
"""

import unittest

from harness import Cluster, wait_for_key
from raftkv.client import Session
from raftkv.store import KVStore
from test_xreadgroup import groups_of


def _seed(store, entry_ids=("100-5", "100-6")):
    for entry_id in entry_ids:
        store.apply({"op": "xadd", "key": "s",
                     "pairs": [["a", entry_id]], "id": entry_id})
    store.apply({"op": "xgroup_create", "key": "s",
                 "group": "g", "start_id": "0"})


class TestXReadGroupNoackRereadStore(unittest.TestCase):
    def test_noack_reread_leaves_count_and_stamp_alone(self):
        """A NOACK re-read returns the entries but leaves the
        pending record untouched: the delivery count stays 1, the
        delivery stamp stays at the first delivery, and the idle
        in XPENDING's per-entry form keeps running from that
        delivery — while the consumer's own last-seen stamp still
        moves, since the consumer interacted and received
        entries."""
        store = KVStore()
        _seed(store)
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">",
                     "seen_ms": 1700000000000})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "0", "noack": True,
                              "seen_ms": 1700000005000})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5", "100-6"])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["deliveries"], 1)
        self.assertEqual(pending["100-6"]["deliveries"], 1)
        self.assertEqual(pending["100-5"]["delivered_ms"],
                         1700000000000)
        self.assertEqual(pending["100-6"]["delivered_ms"],
                         1700000000000)
        # The idle is measured from the first delivery, not the
        # re-read: 10000ms at now 1700000010000, not 5000.
        entries = store.xpending_entries("s", "g", 1700000010000)
        self.assertEqual([(e["id"], e["idle"], e["deliveries"])
                          for e in entries],
                         [("100-5", 10000, 1), ("100-6", 10000, 1)])
        # The consumer's own last-seen stamp still moved to the
        # re-read — it did interact — and its lag record still
        # names the newest entry handed, never moving backwards.
        consumer = groups_of(store.get("s"))["g"]["consumers"]["c1"]
        self.assertEqual(consumer["last_seen_ms"], 1700000005000)
        self.assertEqual(consumer["last_delivered_id"], "100-6")

    def test_plain_reread_still_counts_as_a_delivery(self):
        """The regression guard for the NOACK exception: a plain
        re-read still raises the delivery count by one and
        re-stamps the delivery time, so the two kinds of re-read
        are distinguishable in XPENDING's per-entry form."""
        store = KVStore()
        _seed(store)
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">",
                     "seen_ms": 1700000000000})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "0", "seen_ms": 1700000005000})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5", "100-6"])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["deliveries"], 2)
        self.assertEqual(pending["100-5"]["delivered_ms"],
                         1700000005000)
        entries = store.xpending_entries("s", "g", 1700000010000)
        self.assertEqual([(e["id"], e["idle"], e["deliveries"])
                          for e in entries],
                         [("100-5", 5000, 2), ("100-6", 5000, 2)])

    def test_noack_reread_capped_by_count_and_entries_stay_pending(
            self):
        """COUNT caps a NOACK re-read too, and the entries stay
        pending afterwards — still ackable, since a NOACK re-read
        neither acks nor drops them."""
        store = KVStore()
        _seed(store)
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">",
                     "seen_ms": 1700000000000})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "0", "count": 1, "noack": True,
                              "seen_ms": 1700000005000})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5"])
        self.assertEqual(store.xpending("s", "g")["pending"], 2)
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["deliveries"], 1)
        self.assertEqual(pending["100-6"]["deliveries"], 1)
        result = store.apply({"op": "xack", "key": "s", "group": "g",
                              "ids": ["100-5", "100-6"]})
        self.assertEqual(result["xack"], 2, result)
        self.assertEqual(store.xpending("s", "g")["pending"], 0)

    def test_noack_reread_only_returns_own_pending(self):
        """A NOACK re-read still only sees the consumer's own
        pending entries: another consumer's entries are not
        returned, and their delivery counts are untouched."""
        store = KVStore()
        _seed(store)
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c1", "id": ">", "count": 1,
                     "seen_ms": 1700000000000})
        store.apply({"op": "xreadgroup", "key": "s", "group": "g",
                     "consumer": "c2", "id": ">", "count": 1,
                     "seen_ms": 1700000001000})
        result = store.apply({"op": "xreadgroup", "key": "s",
                              "group": "g", "consumer": "c1",
                              "id": "0", "noack": True,
                              "seen_ms": 1700000005000})
        self.assertEqual([e[0] for e in result["entries"]],
                         ["100-5"])
        pending = groups_of(store.get("s"))["g"]["pending"]
        self.assertEqual(pending["100-5"]["deliveries"], 1)
        self.assertEqual(pending["100-5"]["delivered_ms"],
                         1700000000000)
        self.assertEqual(pending["100-6"]["deliveries"], 1)
        self.assertEqual(pending["100-6"]["delivered_ms"],
                         1700000001000)


class TestXReadGroupNoackRereadCluster(unittest.TestCase):
    def test_noack_reread_replicates_to_every_node(self):
        """A NOACK re-read through a Session replicates to every
        node with the delivery counts and stamps unchanged on
        each one — and a plain re-read after it still raises the
        count everywhere, so the two are distinguishable live."""
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                for entry_id in ("100-5", "100-6"):
                    resp = session.xadd("events", {"a": entry_id},
                                        id=entry_id)
                    self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xgroup_create("events", "workers",
                                             start_id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                resp = session.xreadgroup("events", "workers", "c1")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5", "100-6"])
                before = {
                    eid: dict(info) for eid, info in
                    groups_of(leader.sm.get("events"))["workers"]
                    ["pending"].items()}
                resp = session.xreadgroup("events", "workers", "c1",
                                          id="0", noack=True)
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual([e[0] for e in resp.get("entries")],
                                 ["100-5", "100-6"])
                # The entries are still pending, still held once,
                # with the same stamps, on every node.
                for node_id in cluster.live:
                    self.assertTrue(
                        wait_for_key(cluster.nodes[node_id], "events",
                                     cluster.nodes[leader.id].sm.get(
                                         "events")))
                    pending = groups_of(
                        cluster.nodes[node_id].sm.get("events")
                    )["workers"]["pending"]
                    self.assertEqual(
                        {eid: info["deliveries"]
                         for eid, info in pending.items()},
                        {"100-5": 1, "100-6": 1})
                    self.assertEqual(
                        {eid: info["delivered_ms"]
                         for eid, info in pending.items()},
                        {eid: info["delivered_ms"]
                         for eid, info in before.items()})
                # And a plain re-read after it still counts as a
                # delivery on every node.
                resp = session.xreadgroup("events", "workers", "c1",
                                          id="0")
                self.assertEqual(resp.get("status"), "ok", resp)
                for node_id in cluster.live:
                    self.assertTrue(
                        wait_for_key(cluster.nodes[node_id], "events",
                                     cluster.nodes[leader.id].sm.get(
                                         "events")))
                    pending = groups_of(
                        cluster.nodes[node_id].sm.get("events")
                    )["workers"]["pending"]
                    self.assertEqual(
                        {eid: info["deliveries"]
                         for eid, info in pending.items()},
                        {"100-5": 2, "100-6": 2})
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
