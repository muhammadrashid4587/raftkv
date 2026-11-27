"""Tests for raftkv GEOADD / GEODIST (Redis geo operations, v0.62.0).

``geoadd key lon lat member [lon lat member ...]`` adds members with
positions to the geo set at the key and answers with the number of
members that did not exist before — Redis GEOADD. A missing key is
created as an empty geo set; a member whose position is updated is
not counted, and a member repeated in one call counts once, at its
last position. ``geodist key member1 member2 [unit]`` answers the
distance between two members — the haversine over the stored
coordinates, in metres by default, or km, mi, ft — Redis GEODIST,
None when the key does not exist or either member is not in the set.
A geoadd is a write replicated through the Raft log like any other,
its outcome decided when the entry applies — at its exact position
in the log — so a retry with the same session id returns the first
attempt's count instead of adding twice. A geodist is a read through
the same ReadIndex path as a ``get``. A successful geoadd keeps the
key's TTL deadline, like an hset, and fires a ``set`` watch event
with the new value. A plain string value fails a geoadd with
``geoadd_failed`` / ``wrongtype`` instead of writing, and a geodist
on one is a client error, not a distance. Positions are stored as
plain longitude/latitude pairs, not as Redis's 52-bit geohash scores,
so a distance computed here is the exact haversine — within a
fraction of a metre of Redis's geohash-quantized answer. These spin
up real nodes (real TCP, real threads, real disk) on localhost, like
the rest of the suite.
"""

import contextlib
import io
import time
import unittest

from harness import Cluster, client_request, wait_for_key
from raftkv.client import Session, main as client_main
from raftkv.client import geodist as client_geodist
from raftkv.client import geoadd as client_geoadd
from raftkv.store import KVStore

# Redis's own GEOADD / GEODIST example: Palermo and Catania in
# Sicily, 166274.1516 m apart by Redis's geohash-quantized measure —
# the exact haversine over the stored coordinates is 166274.2578 m,
# within 0.11 m of it.
PALERMO = (13.361389, 38.115556, "Palermo")
CATANIA = (15.087269, 37.502669, "Catania")
PARIS = (2.349014, 48.864716, "Paris")


def geoadd(addr, key="Sicily", members=(PALERMO, CATANIA),
           client_id=None, request_id=None):
    msg = {"type": "client", "op": "geoadd", "key": key,
           "members": [[lon, lat, member]
                       for lon, lat, member in members]}
    if client_id is not None:
        msg["client_id"] = client_id
    if request_id is not None:
        msg["request_id"] = request_id
    return client_request(addr, msg)


def geodist(addr, key="Sicily", member1="Palermo", member2="Catania",
            unit=None):
    msg = {"type": "client", "op": "geodist", "key": key,
           "member1": member1, "member2": member2}
    if unit is not None:
        msg["unit"] = unit
    return client_request(addr, msg)


def write(addr, key="k", value="old"):
    return client_request(addr, {"type": "client", "op": "set",
                                 "key": key, "value": value})


def count_entries(node, request_id):
    """How many entries in a node's in-memory log carry request_id."""
    return sum(1 for e in node.log
               if e["command"].get("request_id") == request_id)


class TestGeoStore(unittest.TestCase):
    def test_geoadd_adds_members_and_counts_new_only(self):
        """A geoadd on a missing key creates the set and answers 2
        for two new members; a second geoadd with one new member
        and one existing member answers 1."""
        store = KVStore()
        result = store.apply({"op": "geoadd", "key": "Sicily",
                              "members": [list(PALERMO),
                                          list(CATANIA)]})
        self.assertEqual(result, {"type": "client_response",
                                  "status": "ok", "geoadd": 2})
        result = store.apply({"op": "geoadd", "key": "Sicily",
                              "members": [list(PALERMO),
                                          list(PARIS)]})
        self.assertEqual(result["geoadd"], 1)
        self.assertAlmostEqual(store.geodist("Sicily", "Palermo",
                                            "Catania"),
                               166274.1516, delta=1.0)
        self.assertAlmostEqual(store.geodist("Sicily", "Palermo",
                                            "Paris"),
                               1486391.0, delta=500.0)

    def test_geoadd_updates_position_without_counting(self):
        """Moving an existing member to a new position answers 0 —
        the member already existed — but the distance changes to
        the new position."""
        store = KVStore()
        store.apply({"op": "geoadd", "key": "Sicily",
                     "members": [list(PALERMO), list(CATANIA)]})
        before = store.geodist("Sicily", "Palermo", "Catania")
        result = store.apply({"op": "geoadd", "key": "Sicily",
                              "members": [[13.361389, 38.115556,
                                           "Palermo"],
                                          [15.087269, 38.115556,
                                           "Catania"]]})
        self.assertEqual(result["geoadd"], 0)
        after = store.geodist("Sicily", "Palermo", "Catania")
        self.assertNotAlmostEqual(before, after, delta=1000.0)
        # Same latitude now: the distance is the longitude gap at
        # that latitude, ~151 km.
        self.assertAlmostEqual(after, 151000.0, delta=5000.0)

    def test_geoadd_repeated_member_in_one_call_counts_once(self):
        """A member repeated in one geoadd counts once, at its last
        position — like a field repeated in one hset."""
        store = KVStore()
        result = store.apply({"op": "geoadd", "key": "Sicily",
                              "members": [list(PALERMO),
                                          [15.087269, 37.502669,
                                           "Palermo"]]})
        self.assertEqual(result["geoadd"], 1)
        # Palermo now sits at Catania's coordinates: the distance
        # between them is 0.
        self.assertEqual(store.geodist("Sicily", "Palermo",
                                       "Palermo"), 0.0)

    def test_geoadd_keeps_ttl_deadline(self):
        # A geoadd is an update, not a fresh set: the deadline
        # survives it, like an hset. The deadline is put on the
        # geo key with expire, since a set would leave a plain
        # string a geoadd rightly refuses.
        store = KVStore()
        store.apply({"op": "geoadd", "key": "Sicily",
                     "members": [list(PALERMO)]}, index=10)
        store.apply({"op": "expire", "key": "Sicily",
                     "ttl_entries": 10}, index=11)
        self.assertEqual(store.expiries(), {"Sicily": 21})
        result = store.apply({"op": "geoadd", "key": "Sicily",
                              "members": [list(CATANIA)]}, index=12)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(store.expiries(), {"Sicily": 21})
        self.assertEqual(store.expire_up_to(20), [])
        self.assertEqual(store.expire_up_to(21), ["Sicily"])

    def test_geoadd_new_key_joins_sorted_index(self):
        """A geoadd that creates a key inserts it in the sorted
        index, so a scan finds it in order."""
        store = KVStore()
        store.apply({"op": "set", "key": "b", "value": "x"})
        store.apply({"op": "geoadd", "key": "a",
                     "members": [list(PALERMO)]})
        store.apply({"op": "geoadd", "key": "c",
                     "members": [list(CATANIA)]})
        self.assertEqual(store.keys(), (["a", "b", "c"], False))

    def test_geoadd_wrongtype_leaves_value_untouched(self):
        """A plain string value fails a geoadd with geoadd_failed /
        wrongtype and writes nothing, and a geodist on it raises
        TypeError."""
        store = KVStore()
        store.apply({"op": "set", "key": "k", "value": "x"})
        result = store.apply({"op": "geoadd", "key": "k",
                              "members": [list(PALERMO)]})
        self.assertEqual(result["status"], "geoadd_failed")
        self.assertEqual(result["reason"], "wrongtype")
        self.assertEqual(store.get("k"), "x")
        with self.assertRaises(TypeError):
            store.geodist("k", "Palermo", "Catania")

    def test_geoadd_bad_members_raise(self):
        """Bad members are a programming error at apply time — the
        leader validates them before logging, so a hand-built entry
        with any of them raises instead of writing nonsense."""
        store = KVStore()
        bad = [
            [],
            [[200.0, 0.0, "a"]],
            [[-180.1, 0.0, "a"]],
            [[0.0, 85.05112879, "a"]],
            [[0.0, -85.05112879, "a"]],
            [[float("inf"), 0.0, "a"]],
            [[0.0, float("nan"), "a"]],
            [[True, 0.0, "a"]],
            [[0.0, 0.0, 1]],
            [[0.0, 0.0]],
            [["0", "0", "a"]],
            [[0.0, 0.0, "a"]] * 1001,
        ]
        for members in bad:
            with self.assertRaises(ValueError, msg=str(members[:1])):
                store.apply({"op": "geoadd", "key": "k",
                             "members": members})
        self.assertIsNone(store.get("k"))

    def test_geodist_missing_key_and_member_are_none(self):
        """A geodist on a missing key, or between a member and one
        that is not in the set, answers None — like a get on a
        missing key, not an error."""
        store = KVStore()
        self.assertIsNone(store.geodist("missing", "a", "b"))
        store.apply({"op": "geoadd", "key": "Sicily",
                     "members": [list(PALERMO)]})
        self.assertIsNone(store.geodist("Sicily", "Palermo",
                                        "Catania"))
        self.assertIsNone(store.geodist("Sicily", "Catania",
                                        "Palermo"))
        # The same member is 0, not None — it is in the set.
        self.assertEqual(store.geodist("Sicily", "Palermo",
                                       "Palermo"), 0.0)

    def test_geodist_units_agree(self):
        """The same pair in m, km, mi, and ft is the same distance
        divided by the unit's metres: the units agree with each
        other exactly, and the metre value is within a metre of
        Redis's documented 166274.1516."""
        store = KVStore()
        store.apply({"op": "geoadd", "key": "Sicily",
                     "members": [list(PALERMO), list(CATANIA)]})
        metres = store.geodist("Sicily", "Palermo", "Catania")
        self.assertAlmostEqual(metres, 166274.1516, delta=1.0)
        km = store.geodist("Sicily", "Palermo", "Catania", "km")
        self.assertAlmostEqual(km, metres / 1000.0, delta=0.001)
        self.assertAlmostEqual(km, 166.2742, delta=0.001)
        mi = store.geodist("Sicily", "Palermo", "Catania", "mi")
        self.assertAlmostEqual(mi, metres / 1609.344, delta=0.001)
        ft = store.geodist("Sicily", "Palermo", "Catania", "ft")
        self.assertAlmostEqual(ft, metres / 0.3048, delta=0.01)
        # The unit is matched case-insensitively.
        self.assertAlmostEqual(
            store.geodist("Sicily", "Palermo", "Catania", "KM"),
            km, delta=0.001)

    def test_geodist_bad_unit_raises(self):
        store = KVStore()
        store.apply({"op": "geoadd", "key": "Sicily",
                     "members": [list(PALERMO), list(CATANIA)]})
        for unit in ("parsecs", "", "M ", 5, None):
            with self.assertRaises(ValueError, msg=str(unit)):
                store.geodist("Sicily", "Palermo", "Catania", unit)

    def test_geodist_survives_snapshot_load(self):
        """The geo blob is a plain string in the store, so a
        snapshot load rebuilds the same members and the same
        distances."""
        store = KVStore()
        store.apply({"op": "geoadd", "key": "Sicily",
                     "members": [list(PALERMO), list(CATANIA)]})
        loaded = KVStore()
        loaded.load(store.items())
        self.assertAlmostEqual(loaded.geodist("Sicily", "Palermo",
                                             "Catania"),
                               166274.1516, delta=1.0)


class TestGeoCluster(unittest.TestCase):
    def test_geoadd_replicates_to_every_node(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = geoadd(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("geoadd"), 2, resp)
            for node_id in cluster.live:
                self.assertTrue(
                    wait_for_key(cluster.nodes[node_id], "Sicily",
                                 cluster.nodes[leader.id].sm.get(
                                     "Sicily")))
                self.assertAlmostEqual(
                    cluster.nodes[node_id].sm.geodist(
                        "Sicily", "Palermo", "Catania"),
                    166274.1516, delta=1.0)
            resp = geodist(addr)
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertAlmostEqual(resp.get("distance"),
                                   166274.1516, delta=1.0)
            self.assertAlmostEqual(
                geodist(addr, unit="km").get("distance"),
                166.2742, delta=0.001)
        finally:
            cluster.stop_all()

    def test_geoadd_from_follower_redirects(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            follower = next(n for n in cluster.live
                            if n != leader.id)
            resp = client_geoadd(cluster.addrs[follower], "Sicily",
                                 [PALERMO, CATANIA])
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("geoadd"), 2, resp)
            self.assertAlmostEqual(
                client_geodist(cluster.addrs[follower], "Sicily",
                               "Palermo", "Catania").get("distance"),
                166274.1516, delta=1.0)
        finally:
            cluster.stop_all()

    def test_invalid_geoadd_and_geodist_rejected_with_no_log_entry(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            before = len(leader.log)
            resp = geoadd(addr, members=[(200.0, 0.0, "a")])
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("lon", resp.get("error", ""), resp)
            resp = geoadd(addr, members=[(0.0, 90.0, "a")])
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("lat", resp.get("error", ""), resp)
            resp = geoadd(addr, members=[])
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "geoadd", "key": "k",
                       "members": [[0.0, 0.0, 1]]})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = client_request(
                addr, {"type": "client", "op": "geoadd", "key": "k",
                       "members": [[0.0, 0.0, "a"]] * 1001})
            self.assertEqual(resp.get("status"), "error", resp)
            resp = geodist(addr, unit="parsecs")
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("unit", resp.get("error", ""), resp)
            resp = client_request(
                addr, {"type": "client", "op": "geodist", "key": "k",
                       "member1": 1, "member2": "b"})
            self.assertEqual(resp.get("status"), "error", resp)
            self.assertIn("member1", resp.get("error", ""), resp)
            # None of the rejected writes left a key behind, and
            # none appended a log entry.
            self.assertIsNone(
                client_request(addr, {"type": "client", "op": "get",
                                      "key": "Sicily"}).get("value"))
            self.assertEqual(len(leader.log), before)
        finally:
            cluster.stop_all()

    def test_geoadd_retry_returns_first_count_and_writes_once(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            resp = geoadd(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("geoadd"), 2, resp)
            resp = geoadd(addr, client_id="c1", request_id="r1")
            self.assertEqual(resp.get("status"), "ok", resp)
            self.assertEqual(resp.get("geoadd"), 2, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
            # A retry after the member moved still returns the
            # first attempt's count, not a recount.
            resp = geoadd(addr, members=[list(PALERMO)],
                          client_id="c1", request_id="r1")
            self.assertEqual(resp.get("geoadd"), 2, resp)
            self.assertEqual(count_entries(leader, "r1"), 1)
        finally:
            cluster.stop_all()

    def test_geoadd_fires_watch_event_with_new_value(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            follower = next(cluster.nodes[n] for n in cluster.live
                            if n != leader.id)
            events = []
            leader.watch.watch("Sicily", events.append)
            follower.watch.watch("Sicily", events.append)
            resp = geoadd(addr)
            self.assertEqual(resp.get("geoadd"), 2, resp)
            deadline = time.monotonic() + 5.0
            while len(events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(events), 2, events)
            for event in events:
                self.assertEqual(event["key"], "Sicily", event)
                self.assertEqual(event["value"],
                                 leader.sm.get("Sicily"), event)
                self.assertEqual(event["op"], "set", event)
            # A failed geoadd — wrongtype on a plain string —
            # writes nothing, so it fires nothing.
            w_events = []
            leader.watch.watch("w", w_events.append)
            follower.watch.watch("w", w_events.append)
            self.assertEqual(write(addr, key="w", value="x")
                             .get("status"), "ok")
            deadline = time.monotonic() + 5.0
            while len(w_events) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(w_events), 2, w_events)
            resp = client_geoadd(addr, "w", [PALERMO])
            self.assertEqual(resp.get("status"), "geoadd_failed", resp)
            time.sleep(0.3)
            self.assertEqual(len(w_events), 2, w_events)
        finally:
            cluster.stop_all()

    def test_geoadd_continues_after_leader_change(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(geoadd(addr).get("geoadd"), 2)
            cluster.stop_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertEqual(geoadd(addr, members=[PARIS])
                             .get("geoadd"), 1)
            self.assertAlmostEqual(geodist(addr).get("distance"),
                                   166274.1516, delta=1.0)
            self.assertAlmostEqual(
                geodist(addr, member1="Palermo", member2="Paris",
                        unit="km").get("distance"),
                1486.4, delta=1.0)
        finally:
            cluster.stop_all()

    def test_geoadd_survives_snapshot_and_restart(self):
        cluster = Cluster(3, snapshot_threshold=5)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(geoadd(addr).get("geoadd"), 2)
            # Enough writes to compact the log past the geoadd.
            for i in range(10):
                self.assertEqual(
                    write(addr, key=f"pad{i}", value="x").get("status"),
                    "ok")
            self.assertAlmostEqual(geodist(addr).get("distance"),
                                   166274.1516, delta=1.0)
            cluster.stop_node(leader.id)
            cluster.restart_node(leader.id)
            new_leader = cluster.wait_for_leader()
            addr = cluster.addrs[new_leader.id]
            self.assertAlmostEqual(geodist(addr).get("distance"),
                                   166274.1516, delta=1.0)
            self.assertEqual(geoadd(addr, members=[PARIS])
                             .get("geoadd"), 1)
        finally:
            cluster.stop_all()

    def test_session_geoadd_geodist_are_read_your_writes(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                resp = session.geoadd("Sicily", [PALERMO, CATANIA])
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertEqual(resp.get("geoadd"), 2, resp)
                resp = session.geodist("Sicily", "Palermo", "Catania")
                self.assertEqual(resp.get("status"), "ok", resp)
                self.assertAlmostEqual(resp.get("distance"),
                                       166274.1516, delta=1.0)
                # A session read from a follower still sees the
                # session's own geoadd.
                follower = next(n for n in cluster.live
                                if n != leader.id)
                with Session(cluster.addrs[follower],
                             client_id=session.client_id) as other:
                    other.last_write_index = session.last_write_index
                    other.last_write_term = session.last_write_term
                    resp = other.geodist("Sicily", "Palermo",
                                         "Catania")
                    self.assertEqual(resp.get("status"), "ok", resp)
                    self.assertAlmostEqual(resp.get("distance"),
                                           166274.1516, delta=1.0)
        finally:
            cluster.stop_all()

    def test_pipeline_geoadd_ops(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            with Session(addr) as session:
                replies = session.pipeline([
                    {"op": "geoadd", "key": "Sicily",
                     "members": [[13.361389, 38.115556, "Palermo"],
                                 [15.087269, 37.502669, "Catania"]]},
                    {"op": "geoadd", "key": "Sicily",
                     "members": [[2.349014, 48.864716, "Paris"]]},
                    {"op": "geoadd", "key": "Sicily",
                     "members": [[200.0, 0.0, "a"]]},
                    {"op": "geoadd", "key": "Sicily",
                     "members": []},
                    {"op": "geoadd", "key": "Sicily",
                     "members": [[0.0, 0.0, 1]]},
                    {"op": "geoadd", "key": "Elsewhere",
                     "members": [[0.0, 0.0, "origin"]]},
                ])
            self.assertEqual(len(replies), 6, replies)
            self.assertEqual(replies[0].get("geoadd"), 2, replies)
            self.assertEqual(replies[1].get("geoadd"), 1, replies)
            self.assertEqual(replies[2].get("status"), "error", replies)
            self.assertEqual(replies[3].get("status"), "error", replies)
            self.assertEqual(replies[4].get("status"), "error", replies)
            self.assertEqual(replies[5].get("geoadd"), 1, replies)
            self.assertAlmostEqual(geodist(addr).get("distance"),
                                   166274.1516, delta=1.0)
            self.assertAlmostEqual(
                geodist(addr, member1="Palermo", member2="Paris")
                .get("distance"), 1486391.0, delta=500.0)
        finally:
            cluster.stop_all()

    def test_geo_module_functions(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            addr = cluster.addrs[leader.id]
            self.assertEqual(client_geoadd(addr, "Sicily",
                                           [PALERMO, CATANIA])
                             .get("geoadd"), 2)
            self.assertAlmostEqual(
                client_geodist(addr, "Sicily", "Palermo", "Catania")
                .get("distance"), 166274.1516, delta=1.0)
            self.assertIsNone(
                client_geodist(addr, "Sicily", "Palermo", "Nowhere")
                .get("distance"))
            self.assertIsNone(
                client_geodist(addr, "missing", "a", "b")
                .get("distance"))
            self.assertEqual(
                client_geodist(addr, "Sicily", "Palermo", "Catania",
                               "KM").get("distance"),
                client_geodist(addr, "Sicily", "Palermo", "Catania",
                               "km").get("distance"))
        finally:
            cluster.stop_all()

    def test_geo_cli(self):
        cluster = Cluster(3)
        try:
            cluster.start_all()
            leader = cluster.wait_for_leader()
            host, port = cluster.addrs[leader.id]
            node = f"{host}:{port}"
            # Geodist on a missing key prints (nil).
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "geodist", "Sicily",
                                  "Palermo", "Catania"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            # Geoadd prints the number of new members.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "geoadd", "Sicily",
                                  "13.361389", "38.115556", "Palermo",
                                  "15.087269", "37.502669", "Catania"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "2")
            # Geodist prints the distance in metres.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "geodist", "Sicily",
                                  "Palermo", "Catania"])
            self.assertEqual(rc, 0)
            self.assertAlmostEqual(float(out.getvalue().strip()),
                                   166274.1516, delta=1.0)
            # ... and in kilometres with --unit.
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "geodist", "Sicily",
                                  "Palermo", "Catania",
                                  "--unit", "km"])
            self.assertEqual(rc, 0)
            self.assertAlmostEqual(float(out.getvalue().strip()),
                                   166.2742, delta=0.001)
            # A member that is not in the set prints (nil).
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = client_main(["--node", node, "geodist", "Sicily",
                                  "Palermo", "Nowhere"])
            self.assertEqual(rc, 0)
            self.assertEqual(out.getvalue().strip(), "(nil)")
            # A bad longitude is a server error, not a write.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = client_main(["--node", node, "geoadd", "Sicily",
                                  "200", "0", "a"])
            self.assertEqual(rc, 1)
            self.assertIn("lon", err.getvalue())
            # Triples must come in threes: a CLI usage error.
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    client_main(["--node", node, "geoadd", "Sicily",
                                 "13.361389", "38.115556"])
            self.assertEqual(cm.exception.code, 2)
        finally:
            cluster.stop_all()


if __name__ == "__main__":
    unittest.main()
