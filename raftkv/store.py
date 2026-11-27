"""The key-value state machine that sits on top of Raft.

Raft guarantees every node applies the same committed log entries in the
same order, so this state machine is intentionally trivial and
deterministic: applying the same command log always produces the same
state. All it needs to be is correct and idempotent under replay — a node
re-applies its committed prefix after a restart, so ``apply`` must be a
pure function of (current state, command).
"""

import bisect
import fnmatch
import hashlib
import json
import math
import re

# Signed 64-bit bounds, the range Redis's INCR/INCRBY works in: a
# value outside it is not an integer Redis can count, and a result
# outside it is an overflow, not a bigger number.
_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1

# HyperLogLog (PFADD / PFCOUNT), Redis-style probabilistic
# cardinality. Precision 14: 2**14 = 16384 registers, one byte each
# (a register holds a rho of 0..51, so a byte is plenty) — about
# 16 KiB per key, standard error ~1.04/sqrt(m) ≈ 0.81%. The
# registers ride in _data as a latin-1 string — the same string
# keyspace every other value lives in, so snapshots, the sorted
# index, TTL expiry, and `get` all treat an HLL key like any other
# key (Redis stores its HLL the same way, as a string-encoded blob
# in the keyspace) — with a marker prefix so a PFADD / PFCOUNT can
# tell an HLL apart from a plain string and answer WRONGTYPE on the
# latter instead of counting bytes as registers. The hash is SHA-256
# of the element's UTF-8 bytes: deterministic across processes and
# nodes — Python's hash() is salted per process, so it could never
# be used here: every node applying the same log must compute the
# same registers, and a replay after restart must rebuild the
# identical blob. The tradeoff is the precision itself: a smaller p
# would halve the memory per step but double the error, a larger p
# the reverse — 14 is Redis's default, and the estimate is good to
# under 1% once the cardinality is past a few thousand.
HLL_PRECISION = 14
HLL_REGISTERS = 1 << HLL_PRECISION
HLL_PREFIX = "\x00raftkv-hll\x00"
HLL_VALUE_LENGTH = len(HLL_PREFIX) + HLL_REGISTERS

# Largest number of source keys one ``bitop`` may carry. The keys
# ride in one log entry and one frame, so an unbounded list would
# let one entry grow without limit (and replicate that growth to
# every node). 1,000 keys is far past a real use of one BITOP call
# — a client combining more splits the batch, like an mget client
# splits a batch past 1000 keys — so it only rejects nonsense,
# never a real use — the same scale as the pfadd cap above, for the
# same reason.
BITOP_MAX_KEYS = 1000

# Hashes (HSET / HGET), Redis-style field-value maps. A hash rides in
# _data as a JSON object with a marker prefix, the same trick the HLL
# uses — one string in the same keyspace every other value lives in,
# so snapshots, the sorted index, TTL expiry, and `get` all treat a
# hash key like any other key — with the prefix so an HSET / HGET can
# tell a hash apart from a plain string and answer WRONGTYPE on the
# latter instead of parsing bytes as fields. The JSON is dumped with
# sorted keys and no spaces, so the encoding is a pure function of
# the fields: every node applying the same log computes the same
# bytes, and a replay after restart rebuilds the identical blob. A
# plain string can only pass as a hash by being hand-built with the
# exact prefix and a valid JSON object of string fields — the same
# class of collision the HLL marker has — so HSET / HGET trust the
# shape. The honest limit is the encoding itself: a plain `get` on
# a hash key returns the encoded blob, not a field, and the string
# ops (append, setrange, strlen) see the blob as a plain string,
# exactly as they see an HLL blob.
HASH_PREFIX = "\x00raftkv-hash\x00"


def is_hash(value) -> bool:
    """Whether ``value`` is a hash blob: a string carrying the
    marker prefix and a JSON object of string fields behind it. A
    plain string can only pass this by being hand-built to the exact
    shape — the same class of collision the HLL check has — so an
    HSET / HGET trusts the shape."""
    if not isinstance(value, str) or not value.startswith(HASH_PREFIX):
        return False
    try:
        fields = json.loads(value[len(HASH_PREFIX):])
    except (ValueError, TypeError):
        return False
    return (isinstance(fields, dict)
            and all(isinstance(f, str) and isinstance(v, str)
                    for f, v in fields.items()))


def _hash_fields(value: str) -> dict[str, str]:
    """The field map behind a hash blob. Only called on values
    ``is_hash`` already accepted, so the JSON is a dict of strings
    and the parse cannot fail."""
    return json.loads(value[len(HASH_PREFIX):])


def _hash_value(fields: dict[str, str]) -> str:
    """A field map as a stored blob: the marker prefix plus the
    fields as JSON, keys sorted and no spaces, so the same fields
    always encode to the same bytes on every node."""
    return HASH_PREFIX + json.dumps(fields, sort_keys=True,
                                    separators=(",", ":"))


# Geo (GEOADD / GEODIST), Redis-style member positions. A geo set
# rides in _data as a JSON object with a marker prefix, the same
# trick the hash uses — one string in the same keyspace every other
# value lives in, so snapshots, the sorted index, TTL expiry, and
# `get` all treat a geo key like any other key — with the prefix so
# a GEOADD / GEODIST can tell a geo set apart from a plain string
# and answer WRONGTYPE on the latter instead of parsing bytes as
# coordinates. The JSON maps member names to [lon, lat] pairs,
# dumped with sorted keys and no spaces, so the encoding is a pure
# function of the members: every node applying the same log
# computes the same bytes. The honest limit is the encoding
# itself: positions are stored as plain longitude/latitude pairs,
# not as Redis's 52-bit geohash scores in a sorted set, so there
# is no radius search yet (GEORADIUS / GEOSEARCH would need the
# geohash ordering) — and a distance computed here is the exact
# haversine over the stored coordinates, which can differ from
# Redis's by a fraction of a metre, since Redis quantizes each
# coordinate through its geohash first. A plain `get` on a geo key
# returns the encoded blob, not a member, exactly as it does for a
# hash key.
GEO_PREFIX = "\x00raftkv-geo\x00"

# Largest number of members one ``geoadd`` may carry. The members
# ride in one log entry and one frame, so an unbounded list would
# let one entry grow without limit — the same scale as the pfadd
# cap, for the same reason.
GEOADD_MAX_MEMBERS = 1000

# Redis's coordinate ranges: longitude -180..180, latitude
# -85.05112878..85.05112878 (the latitude of the Mercator
# projection's edge — beyond it the geohash math breaks down, and
# Redis rejects it, so this does too).
GEO_LON_MIN = -180.0
GEO_LON_MAX = 180.0
GEO_LAT_MIN = -85.05112878
GEO_LAT_MAX = 85.05112878

# The Earth's radius Redis's GEODIST uses, in metres — the WGS-84
# equatorial radius. Distances in other units are the metre
# distance divided by the unit's metres, so every unit agrees on
# the same underlying distance.
GEO_EARTH_RADIUS_M = 6372797.560856
GEO_UNIT_METRES = {"m": 1.0, "km": 1000.0, "mi": 1609.344,
                   "ft": 0.3048}


def is_geo(value) -> bool:
    """Whether ``value`` is a geo blob: a string carrying the
    marker prefix and a JSON object of member to [lon, lat] pairs
    behind it. A plain string can only pass this by being
    hand-built to the exact shape — the same class of collision
    the hash check has — so a GEOADD / GEODIST trusts the shape."""
    if not isinstance(value, str) or not value.startswith(GEO_PREFIX):
        return False
    try:
        members = json.loads(value[len(GEO_PREFIX):])
    except (ValueError, TypeError):
        return False
    return (isinstance(members, dict)
            and all(isinstance(m, str) and isinstance(p, list)
                    and len(p) == 2
                    and all(isinstance(c, (int, float))
                            and not isinstance(c, bool) for c in p)
                    for m, p in members.items()))


def _geo_members(value: str) -> dict[str, list[float]]:
    """The member map behind a geo blob. Only called on values
    ``is_geo`` already accepted, so the JSON is a dict of member
    to [lon, lat] pairs and the parse cannot fail."""
    return json.loads(value[len(GEO_PREFIX):])


def _geo_value(members: dict[str, list[float]]) -> str:
    """A member map as a stored blob: the marker prefix plus the
    members as JSON, keys sorted and no spaces, so the same
    members always encode to the same bytes on every node."""
    return GEO_PREFIX + json.dumps(members, sort_keys=True,
                                   separators=(",", ":"))


def geo_distance(lon1: float, lat1: float, lon2: float,
                 lat2: float) -> float:
    """The great-circle distance between two points, in metres —
    the haversine formula over Redis's Earth radius. Two
    identical points are 0, antipodes are half the circumference;
    the formula is a pure function of the coordinates, so every
    node applying the same log computes the same distance."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2)
         * math.sin(dlambda / 2) ** 2)
    return 2 * GEO_EARTH_RADIUS_M * math.asin(math.sqrt(a))


# Streams (XADD), Redis-style append-only logs. A stream rides in
# _data as a JSON object with a marker prefix, the same trick the
# hash and the geo set use — one string in the same keyspace every
# other value lives in, so snapshots, the sorted index, TTL expiry,
# and `get` all treat a stream key like any other key — with the
# prefix so an XADD can tell a stream apart from a plain string
# and answer WRONGTYPE on the latter instead of appending to it.
# The JSON is {"last": "ms-seq", "entries": [["ms-seq", {field:
# value, ...}], ...]}: the entries in ID order, "last" the
# greatest ID appended, dumped with sorted keys and no spaces, so
# the encoding is a pure function of the entries and every node
# applying the same log computes the same bytes. A plain `get` on
# a stream key returns the encoded blob, not an entry, exactly as
# it does for a hash key. XTRIM (v0.66.0) trims the entries from
# the front — MAXLEN keeps at most N, MINID drops every entry
# below an ID — and the top ID ("last") never moves when entries
# are trimmed, so an xadd after a trim still refuses an ID at or
# below the old top; trimming the last entry deletes the key,
# like an hdel that removes the last field. XRANGE / XLEN
# (v0.64.0) read the entries back; XREAD (v0.65.0) reads the
# entries appended after one ID per stream, blocking for new ones
# when asked. XREADGROUP (v0.67.0) adds consumer groups to the
# same blob: an optional "groups" key mapping a group name to
# {"last_delivered": "ms-seq" or None, "pending": {"ms-seq":
# {"consumer": name, "deliveries": n, "delivered_ms": ms}}} — the
# last entry the group was handed (None when it was created on an
# empty stream with "$", so every later entry counts as new), and
# the pending entries list: every entry handed out and not yet
# acked, with which consumer holds it, how many times it has been
# delivered, and — since v0.71.0 — the wall-clock millisecond of
# its last delivery, so XPENDING in per-entry form can report how
# long each entry has been idle. Since v0.72.0 XCLAIM can move a
# pending entry to another consumer once it has been idle long
# enough, re-stamping that same delivery time. Since v0.69.0 a group may also
# carry "entries_read", the count an XGROUP SETID ... ENTRIESREAD
# set — XGROUP SETID moves last_delivered (and optionally
# entries_read), XGROUP DESTROY drops the whole group, and XGROUP
# DELCONSUMER drops one consumer and its pending entries. Since
# v0.70.0 a group may also carry "consumers", a map of consumer
# name to {"last_seen_ms": n} — the wall-clock millisecond of the
# last delivery that consumer was handed, fixed by the leader at
# log time (like an xadd auto ID), so XINFO CONSUMERS can report
# how long each consumer has been idle. Since v0.74.0 the record
# also carries "last_delivered_id" — the newest entry ID ever
# handed to that consumer, so XINFO CONSUMERS can report the
# consumer's lag: the entries in the stream past that ID. A
# consumer only appears there
# once it has been handed at least one entry: a readgroup that
# hands out nothing writes nothing at all, so a consumer that
# only ever polled an empty group is not listed. The groups ride
# in the same blob, so they survive snapshots and replay exactly
# like the entries — a group created before a snapshot is still
# there after one — and the blob stays a pure function of
# (entries, groups), so every node computes the same bytes. A
# stream created before groups existed carries no "groups" key at
# all, and reads like one with no groups.
STREAM_PREFIX = "\x00raftkv-stream\x00"

# Largest number of fields one ``xadd`` may carry. The pairs ride
# in one log entry and one frame, so an unbounded list would let
# one entry grow without limit — the same scale as the hset cap,
# for the same reason.
XADD_MAX_FIELDS = 1000

# Largest number of IDs one ``xack`` may carry. The IDs ride in
# one log entry and one frame, so an unbounded list would let one
# entry grow without limit — the same scale as the xadd cap, for
# the same reason.
XACK_MAX_IDS = 1000

# How many pending entries one ``xautoclaim`` examines when the
# caller gives no COUNT — Redis's default of 100. The count bounds
# the work per call, not the claims: a call can return fewer than
# this many claimed entries when the ones it examined were too
# fresh to take.
XAUTOCLAIM_DEFAULT_COUNT = 100

# A stream ID is two 64-bit halves, milliseconds and sequence,
# Redis's shape: each half is an unsigned 64-bit integer, and 0-0
# is not a valid ID — the first entry in a stream is never 0-0, so
# an ID of 0-0 is smaller than every real entry.
STREAM_ID_MAX = 2 ** 64 - 1


def _parse_stored_stream_id(id_str) -> tuple[int, int]:
    """Parse a stored stream ID — "ms-seq" with both halves digits,
    0-0 allowed: a group's last_delivered can be 0-0 (created with
    start "0", deliver from the very first entry), where an entry
    ID never is. Raises ValueError on any other shape, on a half
    past 2**64 - 1, or on a non-string. Unlike
    ``parse_stream_id``, "*" and "ms-*" are not IDs here — a
    stored ID is always fully explicit."""
    if not isinstance(id_str, str):
        raise ValueError("stored stream id must be a string")
    parts = id_str.split("-")
    if (len(parts) != 2 or not parts[0].isdigit()
            or not parts[1].isdigit()):
        raise ValueError("stored stream id must be \"ms-seq\"")
    ms = int(parts[0])
    seq = int(parts[1])
    if ms > STREAM_ID_MAX or seq > STREAM_ID_MAX:
        raise ValueError(
            "stored stream id halves must be at most 2**64 - 1")
    return ms, seq


def parse_stream_id(id_str) -> tuple[int | str, int | None]:
    """Parse a stream ID: "*" (auto-generate), "ms-*" (auto
    sequence), or "ms-seq" (explicit) — returning ("*", None) for
    a fully automatic ID, (ms, None) for "ms-*", and (ms, seq) for
    an explicit ID. Raises ValueError on any other shape, on a
    half past 2**64 - 1, or on the 0-0 ID, which is smaller than
    every real entry. A leading zero is fine — "007-01" parses as
    (7, 1) — since the parts are integers, not strings."""
    if not isinstance(id_str, str):
        raise ValueError("stream id must be a string")
    if id_str == "*":
        return "*", None
    parts = id_str.split("-")
    if (len(parts) != 2 or not parts[0].isdigit()
            or (parts[1] != "*" and not parts[1].isdigit())):
        raise ValueError(
            "stream id must be \"*\", \"ms-seq\", or \"ms-*\"")
    ms = int(parts[0])
    if ms > STREAM_ID_MAX:
        raise ValueError(
            "stream id milliseconds must be at most 2**64 - 1")
    if parts[1] == "*":
        return ms, None
    seq = int(parts[1])
    if seq > STREAM_ID_MAX:
        raise ValueError(
            "stream id sequence must be at most 2**64 - 1")
    if ms == 0 and seq == 0:
        raise ValueError("stream id must be greater than 0-0")
    return ms, seq


def parse_stream_range_bound(bound: str) -> tuple[int, int]:
    """Parse one XRANGE bound — "-", "+", a bare millisecond, or a
    full "ms-seq" — returning the (ms, seq) pair the bound names.
    "-" is the start of the stream (0, 0), "+" the end
    (2**64 - 1, 2**64 - 1), and a bare millisecond names that
    millisecond with sequence 0, like Redis. Raises ValueError
    on any other shape, on a half past 2**64 - 1, or on a
    non-string. Unlike ``parse_stream_id``, 0-0 is a legal bound:
    it names the very start, and no real entry is ever 0-0, so a
    range from 0-0 covers every entry. A leading zero is fine —
    "007" parses as (7, 0) — since the parts are integers, not
    strings."""
    if not isinstance(bound, str):
        raise ValueError("stream range bound must be a string")
    if bound == "-":
        return 0, 0
    if bound == "+":
        return STREAM_ID_MAX, STREAM_ID_MAX
    parts = bound.split("-")
    if (len(parts) not in (1, 2) or not parts[0]
            or not parts[0].isdigit()):
        raise ValueError(
            "stream range bound must be \"-\", \"+\", \"ms\", or "
            "\"ms-seq\"")
    ms = int(parts[0])
    if ms > STREAM_ID_MAX:
        raise ValueError(
            "stream range bound milliseconds must be at most "
            "2**64 - 1")
    if len(parts) == 1:
        return ms, 0
    if not parts[1].isdigit():
        raise ValueError(
            "stream range bound must be \"-\", \"+\", \"ms\", or "
            "\"ms-seq\"")
    seq = int(parts[1])
    if seq > STREAM_ID_MAX:
        raise ValueError(
            "stream range bound sequence must be at most "
            "2**64 - 1")
    return ms, seq


def _stamp_consumer_delivery(state, consumer, seen_ms,
                             delivered_id=None):
    """Record that ``consumer`` was handed an entry at wall-clock
    millisecond ``seen_ms`` — fixed by the leader at log time, like
    an xadd auto ID — in the group's "consumers" map:
    ``last_seen_ms`` is replaced, and ``last_delivered_id`` keeps
    the greatest entry ID ever handed to that consumer, so a
    re-read or a claim of an older entry never moves it backwards
    — XINFO CONSUMERS reads the consumer's lag off it. A
    ``delivered_id`` of None leaves the last-delivered ID alone,
    so a stamp that names no entry still records the sighting.
    A record written before v0.74.0 carries no last-delivered ID;
    stamping it adds one, and until then its lag reads None."""
    consumers = state.setdefault("consumers", {})
    record = consumers.get(consumer)
    if record is None:
        record = {}
        consumers[consumer] = record
    record["last_seen_ms"] = seen_ms
    if delivered_id is not None:
        previous = record.get("last_delivered_id")
        if (previous is None
                or _parse_stored_stream_id(delivered_id)
                > _parse_stored_stream_id(previous)):
            record["last_delivered_id"] = delivered_id


def _is_group_map(groups) -> bool:
    """Whether ``groups`` is a well-formed consumer-group map:
    a dict of non-empty group name to {"last_delivered": "ms-seq"
    or None, "pending": {"ms-seq": {"consumer": name,
    "deliveries": n, "delivered_ms": ms}}} — the last entry handed
    to the group (None when it was created on an empty stream with
    "$"), and the pending entries: every entry handed out and not
    yet acked, with which consumer holds it, how many times it has
    been delivered (at least 1 — an entry with 0 deliveries was
    never handed out), and — since v0.71.0 — the wall-clock
    millisecond of its last delivery, absent for an entry whose
    deliveries all came from replays of pre-v0.71.0 log entries,
    which carry no stamp. Since v0.69.0 a group may also carry
    "entries_read" — the count an XGROUP SETID ... ENTRIESREAD
    set, a non-negative integer, absent when no SETID ever set
    it. Since v0.70.0 a group may also carry "consumers" — a map
    of consumer name to {"last_seen_ms": n, "last_delivered_id":
    "ms-seq"} — the wall-clock millisecond of that consumer's
    last delivery, a non-negative integer, and the newest entry
    ID ever handed to that consumer, absent for a consumer whose
    deliveries all came from replays of pre-v0.74.0 log entries,
    which carry no last-delivered ID. A plain string can only pass
    this by being hand-built to the exact shape — the same class of
    collision the hash check has — so an XREADGROUP trusts the
    shape."""
    if not isinstance(groups, dict):
        return False
    for name, group in groups.items():
        if not isinstance(name, str) or not name:
            return False
        if (not isinstance(group, dict)
                or not {"last_delivered", "pending"} <= set(group)
                or not set(group) <= {"last_delivered", "pending",
                                      "entries_read", "consumers"}):
            return False
        last = group["last_delivered"]
        if last is not None:
            try:
                _parse_stored_stream_id(last)
            except ValueError:
                return False
        if "entries_read" in group:
            entries_read = group["entries_read"]
            if (not isinstance(entries_read, int)
                    or isinstance(entries_read, bool)
                    or entries_read < 0):
                return False
        if "consumers" in group:
            consumers = group["consumers"]
            if not isinstance(consumers, dict):
                return False
            for consumer, seen in consumers.items():
                if not isinstance(consumer, str) or not consumer:
                    return False
                if (not isinstance(seen, dict)
                        or not {"last_seen_ms"} <= set(seen)
                        or not set(seen) <= {"last_seen_ms",
                                             "last_delivered_id"}):
                    return False
                last_seen = seen["last_seen_ms"]
                if (not isinstance(last_seen, int)
                        or isinstance(last_seen, bool)
                        or last_seen < 0):
                    return False
                if "last_delivered_id" in seen:
                    try:
                        _parse_stored_stream_id(
                            seen["last_delivered_id"])
                    except ValueError:
                        return False
        pending = group["pending"]
        if not isinstance(pending, dict):
            return False
        for entry_id, info in pending.items():
            try:
                _parse_stored_stream_id(entry_id)
            except ValueError:
                return False
            if (not isinstance(info, dict)
                    or not {"consumer", "deliveries"} <= set(info)
                    or not set(info) <= {"consumer", "deliveries",
                                         "delivered_ms"}):
                return False
            if (not isinstance(info["consumer"], str)
                    or not info["consumer"]):
                return False
            deliveries = info["deliveries"]
            if (not isinstance(deliveries, int)
                    or isinstance(deliveries, bool)
                    or deliveries < 1):
                return False
            if "delivered_ms" in info:
                delivered_ms = info["delivered_ms"]
                if (not isinstance(delivered_ms, int)
                        or isinstance(delivered_ms, bool)
                        or delivered_ms < 0):
                    return False
    return True


def is_stream(value) -> bool:
    """Whether ``value`` is a stream blob: a string carrying the
    marker prefix and a JSON object with "last" and "entries" —
    and, since v0.67.0, an optional "groups" consumer-group map —
    behind it. A plain string can only pass this by being
    hand-built to the exact shape — the same class of collision
    the hash check has — so an XADD trusts the shape. A stream
    created before groups existed carries no "groups" key, and
    reads like one with no groups."""
    if not isinstance(value, str) or not value.startswith(STREAM_PREFIX):
        return False
    try:
        data = json.loads(value[len(STREAM_PREFIX):])
    except (ValueError, TypeError):
        return False
    if (not isinstance(data, dict)
            or set(data) not in ({"last", "entries"},
                                 {"last", "entries", "groups"})):
        return False
    last = data["last"]
    if last is not None:
        try:
            _, last_seq = parse_stream_id(last)
        except ValueError:
            return False
        if last_seq is None:
            # A stored top ID is always fully explicit — "ms-*"
            # is a request, never a stored ID.
            return False
    entries = data["entries"]
    if not (isinstance(entries, list)
            and all(isinstance(e, list) and len(e) == 2
                    and isinstance(e[0], str)
                    and isinstance(e[1], dict)
                    and all(isinstance(f, str)
                            and isinstance(v, str)
                            for f, v in e[1].items())
                    for e in entries)):
        return False
    if "groups" in data and not _is_group_map(data["groups"]):
        return False
    return True


def _stream_data(value: str) -> dict:
    """The {"last": ..., "entries": ...} dict behind a stream blob.
    Only called on values ``is_stream`` already accepted, so the
    JSON is a dict with a valid last ID and a list of [id, fields]
    entries and the parse cannot fail."""
    return json.loads(value[len(STREAM_PREFIX):])


def _stream_value(stream: dict) -> str:
    """A stream dict as a stored blob: the marker prefix plus the
    JSON, keys sorted and no spaces, so the same entries always
    encode to the same bytes on every node."""
    return STREAM_PREFIX + json.dumps(stream, sort_keys=True,
                                      separators=(",", ":"))


def _next_seq_at(ms: int, seq: int) -> tuple[int, int]:
    """(ms, seq + 1), rolling into the next millisecond when the
    sequence would pass 2**64 - 1 — like Redis, which never lets
    a sequence overflow its half of the ID."""
    if seq >= STREAM_ID_MAX:
        return ms + 1, 0
    return ms, seq + 1


def _auto_stream_id(auto_ms: int, last_ms: int | None,
                    last_seq: int | None) -> tuple[int, int]:
    """The ID an auto-generated ("*") xadd gets: the leader's
    millisecond when it is past the stream's last one, else the
    last millisecond with the next sequence — so the ID is always
    greater than the last one, however far behind the leader's
    clock the stream's top has run. A bare 0 millisecond on an
    empty stream is 0-1, since 0-0 is not a valid ID."""
    if last_ms is None:
        return (0, 1) if auto_ms == 0 else (auto_ms, 0)
    if auto_ms > last_ms:
        return auto_ms, 0
    return _next_seq_at(last_ms, last_seq)


def _is_integer_string(s: str) -> bool:
    """Whether ``s`` is a Redis-style integer: an optional sign
    followed by digits only. ``int()`` alone is too lenient — it
    accepts whitespace and underscores ("1_0", " 1"), which Redis
    rejects — so the shape is checked first and ``int()`` only
    parses what already looks like an integer."""
    if s.startswith(("+", "-")):
        s = s[1:]
    return bool(s) and s.isdigit()


_FLOAT_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")


def _parse_float_string(s: str) -> float | None:
    """Parse ``s`` as a Redis-style float, or None when it is not
    one. ``float()`` alone is too lenient — it accepts "inf",
    "nan", whitespace, and underscores, which Redis rejects — so
    the shape is checked first and ``float()`` only parses what
    already looks like a number. The result is still checked for
    finiteness by the caller: "1e999" has the right shape but no
    finite value."""
    if not _FLOAT_RE.match(s):
        return None
    return float(s)


def _format_float(value: float) -> str:
    """Format a float the way Redis formats an INCRBYFLOAT result:
    the shortest string that round-trips, with a trailing ".0"
    dropped so an integral result reads "2", not "2.0" — the same
    "%.17Lg" output Redis produces for one."""
    s = repr(value)
    if s.endswith(".0"):
        s = s[:-2]
    return s


def is_hll(value) -> bool:
    """Whether ``value`` is a HyperLogLog blob: a string of exactly
    the HLL length carrying the marker prefix. A plain string can
    only pass this by being hand-built to the exact length with
    the exact prefix — the same class of collision Redis's own
    encoding check has — so a PFADD / PFCOUNT trusts the shape."""
    return (isinstance(value, str)
            and len(value) == HLL_VALUE_LENGTH
            and value.startswith(HLL_PREFIX))


def _hll_registers(value: str) -> bytearray:
    """The register bytes behind an HLL blob, as a mutable
    bytearray — the prefix stripped, the rest decoded latin-1,
    one byte per register. Only called on values ``is_hll``
    already accepted, so every character is at most U+00FF and
    the decode cannot fail."""
    return bytearray(value[len(HLL_PREFIX):].encode("latin-1"))


def _hll_value(registers) -> str:
    """Registers as a stored blob: the marker prefix plus the
    registers encoded latin-1, one byte each."""
    return HLL_PREFIX + bytes(registers).decode("latin-1")


def _hll_add(registers: bytearray, element: str) -> bool:
    """Hash one element into ``registers``: the top
    ``HLL_PRECISION`` bits of the SHA-256 pick the register, the
    remaining bits give rho — the position of the first 1-bit,
    1..51 — and the register keeps the largest rho it has seen.
    Returns True when a register rose, False when the element was
    already counted (or hashed no higher than what was there):
    that is what PFADD answers 1 or 0 on. Pure function of
    (registers, element): the same elements in any order build
    the same registers, which is what makes the op replay-safe."""
    digest = hashlib.sha256(element.encode("utf-8")).digest()
    x = int.from_bytes(digest[:8], "big")
    j = x >> (64 - HLL_PRECISION)
    w = x & ((1 << (64 - HLL_PRECISION)) - 1)
    rho = (64 - HLL_PRECISION) - w.bit_length() + 1 if w else \
        (64 - HLL_PRECISION) + 1
    if rho > registers[j]:
        registers[j] = rho
        return True
    return False


def hll_estimate(registers) -> int:
    """The cardinality estimate for a register set: the standard
    HyperLogLog raw estimate — alpha * m**2 / sum(2**-register) —
    with the small-range correction (linear counting off the empty
    registers, while the estimate is under 2.5 * m) and the
    large-range correction (for a 64-bit hash, past 2**64 / 30 —
    unreachable in practice, but the formula is the honest one).
    Rounded to the nearest integer: the estimate is a float, and
    callers compare integers. Pure function of the registers."""
    m = len(registers)
    alpha = 0.7213 / (1 + 1.079 / m)
    estimate = alpha * m * m / sum(2.0 ** -r for r in registers)
    if estimate <= 2.5 * m:
        zeros = sum(1 for r in registers if r == 0)
        if zeros:
            estimate = m * math.log(m / zeros)
    elif estimate > (1 << 64) / 30.0:
        estimate = -(1 << 64) * math.log(
            1.0 - estimate / (1 << 64))
    return int(round(estimate))


class KVStore:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        # Sorted index of the store's keys, maintained on every
        # write: a ``set`` of a new key inserts it in sorted order
        # (bisect finds the position in O(log n)); a ``del``
        # removes it; a ``set`` of a key already present and a CAS
        # touch no index entry, since the key is already there.
        # A scan then seeks to its cursor with bisect and walks
        # forward, so one page costs O(page + log n) instead of
        # sorting the whole matching keyspace per page. The index
        # is derived state, exactly like ``_data``: replaying the
        # log rebuilds it entry by entry, and ``load`` rebuilds it
        # from the snapshot state, so every node holding the same
        # log computes the same index.
        self._keys: list[str] = []
        # Expiry deadlines, in log-index units: ``_expiry[key]`` is
        # the log index at which the key stops existing. A ``set``
        # carrying ``ttl_entries`` (see ``apply``) records
        # ``index + ttl_entries`` here; when the node applies any
        # entry at index ``i`` it first drops every key whose
        # deadline is <= ``i`` (see ``expire_up_to``). The unit is
        # deliberately log entries, not wall-clock seconds: every
        # node applies the same log in the same order, so every
        # node computes the same deadline and purges the key at
        # the same log position — replicas can never disagree
        # about whether a key is alive, which a wall-clock TTL
        # (clocks skew, apply times differ) could never promise.
        # The honest limit is the unit itself: on a quiet cluster
        # a key can live arbitrarily long in wall-clock time, and
        # on a busy cluster it can expire quickly — the TTL is a
        # distance in the log, not a duration. Derived state like
        # ``_keys``: replaying the log rebuilds it entry by entry,
        # and ``load`` restores it from the snapshot.
        self._expiry: dict[str, int] = {}
        # Wall-clock expiry deadlines, the opt-in variant of the
        # same idea: ``_wall_expiry[key]`` is the absolute Unix
        # time (``time.time()`` seconds) at which the key stops
        # existing, fixed once by the leader when the ``setex``
        # entry was logged — or when a ``set`` carried EX / PX /
        # EXAT / PXAT — and stored in the entry itself — or
        # when an ``expireat`` / ``pexpireat`` entry fixed one
        # on a key that already exists — so every replica
        # applies the same deadline, and replaying the log or
        # installing a snapshot restores it exactly.
        # The leader's clock is the only one that matters for
        # the deadline; each node enforces it against its own
        # clock (a background sweep plus a purge before serving
        # a read — see RaftNode), so two nodes' clocks may
        # disagree briefly about whether a key is alive, the
        # way a wall-clock TTL always can. That is the honest
        # limit of this variant, and why it is opt-in: the
        # log-entry deadline above stays the default, and the
        # two kinds never mix on one key — a fresh ``set`` (with
        # or without a log TTL) drops the wall deadline, and a
        # ``setex`` drops the log deadline, so a key holds at
        # most one kind at a time. Derived state like ``_expiry``:
        # replaying the log rebuilds it entry by entry, and
        # ``load`` restores it from the snapshot.
        self._wall_expiry: dict[str, float] = {}

    # Sentinel for "key absent": distinct from any value a client can
    # store, so a CAS against a missing key is distinguishable from a
    # CAS against a stored value (including an empty string).
    _MISSING = object()

    def apply(self, command: dict, index: int | None = None) -> dict | None:
        """Apply one committed log command. Commands are dicts like
        {"op": "set", "key": "k", "value": "v"}, {"op": "mset",
        "pairs": [["k1", "v1"], ["k2", "v2"]]}, or {"op": "del",
        "key": "k"}.

        ``index`` is the log index of the entry being applied: a
        ``set`` carrying ``ttl_entries`` (a positive number of log
        entries) records a deadline of ``index + ttl_entries`` for
        the key, so the key lives for exactly that many further
        entries and is purged when the log reaches the deadline
        (see ``expire_up_to``). A TTL without a log position is
        meaningless, so a ``set`` with ``ttl_entries`` and no
        index raises; callers that apply commands outside a log
        (unit tests, mostly) pass no TTL. A ``set`` carrying a
        ``wall_deadline`` — SET ... EX / PX / EXAT / PXAT, fixed
        by the leader at log time — records a wall-clock deadline
        instead, and drops any log-entry deadline, so a key still
        holds at most one kind of TTL; a ``wall_deadline``
        already past at log time (``delete_now``) deletes the key
        outright instead of recording it. A plain ``set`` without
        a TTL clears
        any deadline the key had — like Redis, a fresh SET drops
        the old TTL — and so do an ``mset`` without a TTL and
        ``del``. A ``set`` carrying NX or XX is
        conditional instead of unconditional: NX writes only
        when the key does not exist, XX only when it does, and
        a losing set writes nothing at all — the value and any
        deadline stay exactly as they were — and answers ``set``
        0, while a winning one answers ``set`` 1. A ``set``
        carrying KEEPTTL preserves whatever deadline the key
        had (log-entry or wall-clock) instead of clearing it
        like a plain set; a set carrying ``ttl_entries`` records
        a fresh deadline instead, and drops the wall one, so a
        key still holds at most one kind of TTL. A ``set``
        carrying GET answers with the old value the key held
        at this exact point in the log — None when the key was
        absent — alongside ``set`` 1, so one round trip both
        swaps the value and reports what it replaced, like
        Redis SET ... GET; a losing conditional set answers
        ``old_value`` None, because the condition failed and
        there is no old value to report. GET changes nothing
        about the write itself: a winning GET set clears the
        deadline like a plain set (or keeps it with KEEPTTL),
        and a losing one writes nothing at all. An ``mset`` carrying
        ``ttl_entries`` sets the same deadline (its entry's
        index + the TTL) on every pair it touches, so the whole
        batch expires together. A successful CAS keeps the key's
        deadline, and so does a successful incr, an incrbyfloat,
        a getset, an hset, and an hdel that leaves fields behind:        all five are updates of the value, not fresh sets, so
        none must resurrect a key that is about to expire. (An
        hdel that removes the last field deletes the key, and
        the deadline goes with it.) A bitop is a fresh set of its
        destination, so it clears the destination's deadline, like
        a plain set — and when every input is empty it deletes the
        destination outright, deadline and all. A
        ``setex`` carries an absolute wall-clock deadline
        (``wall_deadline``, fixed by the leader before the entry
        was logged) instead of a log distance: it is recorded
        the same way, purged by the wall-clock path (see
        ``expire_wall_up_to``) instead of the log path, and a
        plain ``set`` clears it just as it clears a log deadline.

        Returns a result dict for commands whose outcome depends on the
        state at apply time — currently "set", "cas", "expire", "expireat",
        "pexpireat", "incr", "incrbyfloat", "setnx", "getset", "append", "getdel",
        "setrange", "getex", "setbit", "bitop", "pfadd", "hset", "hdel",
        "geoadd", "xadd", "xtrim", "xgroup_create", "xreadgroup", "xack",
        "xclaim", "xautoclaim", "xgroup_setid", "xgroup_destroy", and
        "xgroup_delconsumer" —
        and None for del, whose outcome is always ok. The result is
        what the leader hands back to the waiting client and what the
        dedup table caches for retries, so it must be a pure function
        of (state before, command): every node applying the same log
        computes the same result.
        """
        op = command.get("op")
        if op == "set":
            # A plain set writes unconditionally, but a set may
            # also carry Redis's conditional flags, decided here,
            # at this exact point in the log, like a CAS: NX sets
            # only when the key does not exist, XX only when it
            # does — a losing set writes nothing, leaves the value
            # and any deadline untouched, and answers ``set`` 0,
            # while a winning one answers ``set`` 1. KEEPTTL
            # preserves whatever deadline the key had (log-entry
            # or wall-clock) instead of clearing it like a plain
            # set; without it a fresh set is a new life for the
            # key and both deadlines are dropped, and a set
            # carrying ``ttl_entries`` records a fresh deadline
            # instead. GET answers with the old value alongside
            # ``set`` 1 — None when the key was absent — and a
            # losing conditional set answers ``old_value`` None.
            # The flags were validated on the leader
            # before the entry was logged — NX and XX never
            # together, KEEPTTL never with a TTL, GET a bare
            # boolean — so by apply time they are well-formed.
            key = command["key"]
            get = bool(command.get("get"))
            if command.get("nx") and key in self._data:
                # A losing conditional set answers nil for the
                # old value, like Redis: the condition failed,
                # so there is no old value to report.
                result = {"type": "client_response", "status": "ok",
                          "set": 0}
                if get:
                    result["old_value"] = None
                return result
            if command.get("xx") and key not in self._data:
                result = {"type": "client_response", "status": "ok",
                          "set": 0}
                if get:
                    result["old_value"] = None
                return result
            keepttl = bool(command.get("keepttl"))
            # The old value is whatever the key held at this
            # exact point in the log — None when it was absent —
            # read before the write overwrites it.
            old = self._data.get(key)
            wall = command.get("wall_deadline")
            if wall is not None and command.get("delete_now"):
                # SET ... EXAT / PXAT with a deadline already
                # past at log time: the key dies at once, like
                # an ``expireat`` / ``getex`` whose absolute
                # deadline was already past — the value is never
                # recorded, and an existing key is deleted
                # outright instead of living with a deadline
                # nobody could honour. The set still answers
                # ``set`` 1, with ``deleted`` set when a key
                # actually went, so the apply path fires the
                # ``del`` event for it; a missing key answers
                # ``set`` 1 with nothing deleted and fires
                # nothing, since there was nothing to delete.
                # A losing conditional set never reaches here —
                # NX / XX are decided above — so a past
                # deadline on a losing set deletes nothing.
                result = {"type": "client_response", "status": "ok",
                          "set": 1}
                if key in self._data:
                    del self._data[key]
                    idx = bisect.bisect_left(self._keys, key)
                    if idx < len(self._keys) and self._keys[idx] == key:
                        del self._keys[idx]
                    self._expiry.pop(key, None)
                    self._wall_expiry.pop(key, None)
                    result["deleted"] = True
                if get:
                    result["old_value"] = old
                return result
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = command["value"]
            ttl = command.get("ttl_entries")
            if wall is not None:
                # SET ... EX / PX / EXAT / PXAT: an inline
                # wall-clock TTL — the leader fixed the absolute
                # deadline at log time, so every replica applies
                # the same one, like a setex. A fresh life on
                # the log side too: any log-entry deadline the
                # key had is dropped, so a key still holds at
                # most one kind of TTL.
                self._wall_expiry[key] = float(wall)
                self._expiry.pop(key, None)
            elif ttl is not None:
                if index is None:
                    raise ValueError(
                        "ttl_entries needs the log index of the "
                        "entry being applied")
                self._expiry[key] = index + ttl
                # An explicit TTL is a fresh life on the wall
                # side too: the wall deadline is dropped, so a
                # key still holds at most one kind of TTL.
                self._wall_expiry.pop(key, None)
            elif keepttl:
                # KEEPTTL keeps both deadlines exactly as they
                # were — a missing key has none to keep, so a
                # KEEPTTL set of a new key simply records none.
                pass
            else:
                self._expiry.pop(key, None)
                # A fresh set drops the wall-clock deadline too:
                # a key holds at most one kind of TTL, and a
                # plain set is a new life for the key either way.
                self._wall_expiry.pop(key, None)
            result = {"type": "client_response", "status": "ok",
                      "set": 1}
            if get:
                result["old_value"] = old
            return result
        elif op == "setex":
            # Set a key with a wall-clock TTL, Redis SETEX: the
            # value is written like a ``set``, and the key dies
            # when the wall clock reaches ``wall_deadline`` — an
            # absolute Unix time, fixed once by the leader when
            # the entry was logged and carried in the command
            # itself, so every replica applies the same deadline
            # and a replay or a snapshot restores it exactly.
            # Like a plain set it is a fresh life for the key: any
            # log-entry deadline the key had is dropped, so the
            # two TTL kinds never mix on one key. The deadline
            # needs no log index — it is already absolute — so
            # ``index`` is unused here; a command without one
            # raises instead of recording a deadline nobody
            # could agree on. Returns None, like a del: the
            # outcome is always ok once committed.
            key = command["key"]
            deadline = command.get("wall_deadline")
            if deadline is None:
                raise ValueError(
                    "setex needs a wall_deadline computed by the "
                    "leader before the entry was logged")
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = command["value"]
            self._expiry.pop(key, None)
            self._wall_expiry[key] = float(deadline)
            return None
        elif op == "mset":
            # A batch set: every pair applies in order at this one
            # log position, so the whole batch is atomic — a read
            # after the entry commits sees all of it, never a
            # prefix. A duplicate key inside the batch keeps the
            # last value, like successive sets. The sorted index
            # is maintained per pair, exactly as for a single set.
            # With ``ttl_entries`` the batch carries one deadline
            # for every pair it touches — the entry's index + the
            # TTL — so the whole batch expires together at one log
            # position, purged by the same ``expire_up_to`` path
            # (and the same ``del`` watch event) as a single set's
            # TTL, and the deadlines ride snapshots with the rest
            # of ``_expiry``. Without a TTL every pair it touches
            # loses any deadline it had, exactly like a plain set.
            # A TTL with no log position is meaningless, like for
            # a single set, so it raises instead of recording a
            # deadline nobody could agree on.
            ttl = command.get("ttl_entries")
            if ttl is not None and index is None:
                raise ValueError(
                    "ttl_entries needs the log index of the "
                    "entry being applied")
            for key, value in command["pairs"]:
                if key not in self._data:
                    bisect.insort(self._keys, key)
                self._data[key] = value
                if ttl is not None:
                    self._expiry[key] = index + ttl
                else:
                    self._expiry.pop(key, None)
                # A fresh batch set drops any wall-clock deadline
                # too, like a plain set: a key holds at most one
                # kind of TTL.
                self._wall_expiry.pop(key, None)
            return None
        elif op == "del":
            key = command["key"]
            if key in self._data:
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
            self._expiry.pop(key, None)
            self._wall_expiry.pop(key, None)
            return None
        elif op == "expire":
            # Set a deadline on an existing key: the key is purged
            # when the log reaches ``index + ttl_entries``, exactly
            # like a ``set`` that carried the same TTL — same units,
            # same purge path (``expire_up_to`` runs before the entry
            # at the deadline applies, on every node), same ``del``
            # watch event when it goes. Unlike a set it does not
            # touch the value, and unlike a set it is a no-op on a
            # missing key: Redis's EXPIRE answers 0 there, so this
            # returns ``expire: 0`` and records nothing instead of
            # an error — the entry still committed (the outcome is
            # decided at apply time, like a CAS), it just did not
            # set a deadline. A key that already has a deadline gets
            # it replaced; a key with none gets one. The deadline
            # rides snapshots with the rest of ``_expiry``, so a
            # node that installs one keeps expiring the key on
            # schedule. Needs the log index, like a set with a TTL:
            # applying one outside a log raises.
            key = command["key"]
            ttl = command.get("ttl_entries")
            if index is None or ttl is None:
                raise ValueError(
                    "expire needs the log index of the entry being "
                    "applied and a ttl_entries count")
            if key not in self._data:
                return {"type": "client_response", "status": "ok",
                        "expire": 0}
            deadline = index + ttl
            self._expiry[key] = deadline
            return {"type": "client_response", "status": "ok",
                    "expire": 1, "deadline": deadline}
        elif op in ("expireat", "pexpireat"):
            # Set an absolute wall-clock deadline on an existing
            # key, Redis EXPIREAT / PEXPIREAT: the wall-clock
            # analogue of ``expire`` — the value is untouched,
            # only the deadline changes. The deadline is the
            # absolute Unix time in ``wall_deadline`` (seconds; a
            # PEXPIREAT millisecond timestamp was divided by
            # 1000 by the leader before the entry was logged),
            # fixed once by the leader, so every replica applies
            # the same deadline and a replay or a snapshot
            # restores it exactly. Like a ``setex`` it drops the
            # log-entry deadline, so a key holds at most one kind
            # of TTL. A missing key answers 0 and records
            # nothing, like ``expire`` — the entry still
            # committed, it just did not set a deadline. A
            # deadline already past at log time (``delete_now``,
            # set by the leader when it fixed the deadline)
            # deletes the key outright instead of recording a
            # deadline nobody could honour, like a ``getex``
            # whose absolute deadline was already past — the
            # reply is 1 with ``deleted`` set, so the apply path
            # fires the ``del`` event for it. The deadline needs
            # no log index — it is already absolute — so a
            # command without one raises instead of recording a
            # deadline nobody could agree on.
            key = command["key"]
            deadline = command.get("wall_deadline")
            if deadline is None:
                raise ValueError(
                    "expireat needs a wall_deadline computed by "
                    "the leader before the entry was logged")
            if key not in self._data:
                return {"type": "client_response", "status": "ok",
                        op: 0}
            if command.get("delete_now"):
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
                self._expiry.pop(key, None)
                self._wall_expiry.pop(key, None)
                return {"type": "client_response", "status": "ok",
                        op: 1, "deleted": True}
            self._expiry.pop(key, None)
            self._wall_expiry[key] = float(deadline)
            return {"type": "client_response", "status": "ok",
                    op: 1, "wall_deadline": float(deadline)}
        elif op == "cas":
            key = command["key"]
            expected = command["expected"]
            current = self._data.get(key, self._MISSING)
            if current is self._MISSING:
                return {"type": "client_response", "status": "cas_failed",
                        "reason": "missing_key", "current": None,
                        "error": f"cas failed: key {key!r} does not exist"}
            if current != expected:
                return {"type": "client_response", "status": "cas_failed",
                        "reason": "mismatch", "current": current,
                        "error": f"cas failed: expected {expected!r} but "
                                 f"current value is {current!r}"}
            # A successful CAS writes a key that already exists, so
            # the sorted index is untouched; a failed or missing-key
            # CAS writes nothing at all.
            self._data[key] = command["value"]
            return {"type": "client_response", "status": "ok",
                    "cas": "success"}
        elif op == "incr":
            # Atomic counter increment, Redis INCR/INCRBY: add
            # ``by`` (default 1) to the key's current value, which
            # must be a base-10 integer string — a missing key
            # counts as 0, so the first incr creates it at ``by`` —
            # and the result must fit a signed 64-bit integer, or
            # the increment errors instead of wrapping. The outcome
            # is decided here, at this exact point in the log, so
            # every node computes the same result and a retry with
            # the same session id returns the first attempt's
            # outcome from the dedup table instead of incrementing
            # twice. A failed incr writes nothing at all, like a
            # failed CAS, but it still committed its entry — the
            # failure is the entry's result, not a client error.
            # A successful incr keeps the key's deadline, like a
            # CAS: it is an update of the value, not a fresh set,
            # so it must not resurrect a key that is about to
            # expire. A new key joins the sorted index, like a set
            # of a new key; the value is stored as its decimal
            # string, so a later get reads back exactly what incr
            # returned.
            key = command["key"]
            by = command.get("by", 1)
            current = self._data.get(key)
            if current is None:
                current_int = 0
            elif (not _is_integer_string(current)
                    or not _INT64_MIN <= int(current) <= _INT64_MAX):
                return {"type": "client_response",
                        "status": "incr_failed",
                        "reason": "not_an_integer",
                        "current": current,
                        "error": f"incr failed: value of {key!r} is "
                                 "not an integer"}
            else:
                current_int = int(current)
            new = current_int + by
            if not _INT64_MIN <= new <= _INT64_MAX:
                return {"type": "client_response",
                        "status": "incr_failed",
                        "reason": "overflow",
                        "current": current,
                        "error": "incr failed: increment would "
                                 "overflow"}
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = str(new)
            return {"type": "client_response", "status": "ok",
                    "value": str(new)}
        elif op == "incrbyfloat":
            # Atomic float increment, Redis INCRBYFLOAT: add
            # ``by`` to the key's current value, which must be a
            # float string — a missing key counts as 0, so the
            # first incrbyfloat creates it at ``by`` — and the
            # result must be finite, or the increment errors
            # instead of producing NaN or Infinity. The outcome
            # is decided here, at this exact point in the log, so
            # every node computes the same result and a retry with
            # the same session id returns the first attempt's
            # outcome from the dedup table instead of adding
            # twice. A failed incrbyfloat writes nothing at all,
            # like a failed incr, but it still committed its
            # entry — the failure is the entry's result, not a
            # client error. A successful one keeps the key's
            # deadline, like an incr, and the value is stored as
            # its formatted string, so a later get reads back
            # exactly what incrbyfloat returned — and a later
            # incr on it fails, since "2.5" is not an integer
            # string.
            key = command["key"]
            by = command.get("by", 1.0)
            current = self._data.get(key)
            if current is None:
                current_float = 0.0
            else:
                current_float = _parse_float_string(current)
                if (current_float is None
                        or not math.isfinite(current_float)):
                    return {"type": "client_response",
                            "status": "incrbyfloat_failed",
                            "reason": "not_a_float",
                            "current": current,
                            "error": "incrbyfloat failed: value of "
                                     f"{key!r} is not a valid float"}
            new = current_float + by
            if not math.isfinite(new):
                return {"type": "client_response",
                        "status": "incrbyfloat_failed",
                        "reason": "overflow",
                        "current": current,
                        "error": "incrbyfloat failed: increment "
                                 "would produce NaN or Infinity"}
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = _format_float(new)
            return {"type": "client_response", "status": "ok",
                    "value": _format_float(new)}
        elif op == "setnx":
            # Set-if-not-exists, Redis SETNX: set the key only
            # when it is absent, and answer 1 when it was set,
            # 0 when the key already existed (and the value is
            # left untouched). The outcome is decided here, at
            # this exact point in the log, so two concurrent
            # setnx on the same key race in log order — exactly
            # one wins — and a retry with the same session id
            # returns the first attempt's outcome from the
            # dedup table instead of setting twice. A key that
            # already exists keeps its value and its deadline
            # (the failed attempt writes nothing); a key that
            # is created has no deadline, like a plain set
            # without a TTL, and any stale deadline entry is
            # cleared. A new key joins the sorted index, like a
            # set of a new key.
            key = command["key"]
            if key in self._data:
                return {"type": "client_response", "status": "ok",
                        "setnx": 0}
            bisect.insort(self._keys, key)
            self._data[key] = command["value"]
            self._expiry.pop(key, None)
            self._wall_expiry.pop(key, None)
            return {"type": "client_response", "status": "ok",
                    "setnx": 1}
        elif op == "getset":
            # Atomically set a key and return its old value, Redis
            # GETSET: the old value is whatever the key held at
            # this exact point in the log — None when the key was
            # absent — and the new value is what the key holds
            # after. The outcome is decided here, at apply time,
            # like a CAS, so a retry with the same session id
            # returns the first attempt's old value from the dedup
            # table instead of setting twice (and a second getset
            # with a different id sees the first one's value as
            # the old value, which is exactly the atomic swap
            # Redis promises). It always writes — even on a
            # missing key, which it creates — so it always fires
            # a set watch event. A key that already exists keeps
            # its deadline, like a CAS or an incr: a getset is an
            # update of the value, not a fresh set, so it must
            # not resurrect a key that is about to expire. A new
            # key joins the sorted index, like a set of a new key.
            key = command["key"]
            old = self._data.get(key)
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = command["value"]
            return {"type": "client_response", "status": "ok",
                    "old_value": old}
        elif op == "append":
            # Append to a string value, Redis APPEND: the key holds
            # its old value plus ``value`` — a missing key is
            # created holding just ``value`` — and the reply
            # carries the new value and its length, the length
            # Redis answers with. The outcome is decided here, at
            # this exact point in the log, so a retry with the
            # same session id returns the first attempt's length
            # from the dedup table instead of appending twice.
            # A value that is not a string fails with WRONGTYPE,
            # like Redis: every value this store holds is a
            # string, so only a hand-built state can do that, but
            # the check keeps the op honest about what it
            # appended to. A failed append writes nothing at all,
            # like a failed incr. A successful append keeps the
            # key's deadline, like a CAS or an incr: it is an
            # update of the value, not a fresh set, so it must
            # not resurrect a key that is about to expire. A new
            # key joins the sorted index, like a set of a new key.
            key = command["key"]
            current = self._data.get(key)
            if current is not None and not isinstance(current, str):
                return {"type": "client_response",
                        "status": "append_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "append failed: value of "
                                 f"{key!r} is not a string"}
            new = (current or "") + command["value"]
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = new
            return {"type": "client_response", "status": "ok",
                    "value": new, "length": len(new)}
        elif op == "getdel":
            # Atomically get a key's value and delete it, Redis
            # GETDEL: the reply carries the value the key held —
            # None when it did not exist — and ``getdel`` 1 when
            # the key was there and is now gone, 0 when there was
            # nothing to delete. The outcome is decided here, at
            # this exact point in the log, so a retry with the
            # same session id returns the first attempt's value
            # from the dedup table instead of deleting a key a
            # later write recreated. A deleted key leaves the
            # sorted index and takes its deadline with it — the
            # key is gone, so there is nothing left to expire.
            # A getdel on a missing key is a no-op, not an error:
            # Redis answers nil there, and the entry still
            # committed (the outcome is the entry's result, not
            # a client error).
            key = command["key"]
            old = self._data.get(key)
            if old is None:
                return {"type": "client_response", "status": "ok",
                        "value": None, "getdel": 0}
            del self._data[key]
            idx = bisect.bisect_left(self._keys, key)
            if idx < len(self._keys) and self._keys[idx] == key:
                del self._keys[idx]
            self._expiry.pop(key, None)
            self._wall_expiry.pop(key, None)
            return {"type": "client_response", "status": "ok",
                    "value": old, "getdel": 1}
        elif op == "getex":
            # Get a key's value and optionally set, refresh, or
            # clear its expiry in the same atomic operation, Redis
            # GETEX: the value is whatever the key held at this
            # exact point in the log — None when it did not
            # exist — and the deadline comes from the option the
            # command carries. EX / PX name a relative deadline
            # (this entry's index + N, in log entries, like
            # ttl_entries — this store has no wall clock, so the
            # Redis seconds / milliseconds are read as log
            # entries); EXAT / PXAT name the absolute log index
            # of the deadline; PERSIST clears it; no option
            # leaves the deadline alone. The outcome is decided
            # here, at apply time, like a CAS: a retry with the
            # same session id returns the first attempt's value
            # and deadline from the dedup table instead of
            # re-expiring, and a missing key answers None with
            # ``getex`` 0 — a no-op, not an error. A GETEX never
            # changes the value, so it fires no watch event for
            # the expiry change itself — only the purge fires a
            # ``del`` event, when the deadline entry lands, like
            # an expire. The one exception is an absolute
            # deadline already past at apply time: Redis deletes
            # the key there instead of recording a deadline
            # nobody could ever honour, so this deletes it too
            # — the reply is None with ``getex`` 0 and
            # ``deleted`` set, so the apply path fires the
            # ``del`` event for it. A relative deadline needs
            # the log index, like a set with a TTL: applying one
            # outside a log raises.
            key = command["key"]
            if key not in self._data:
                return {"type": "client_response", "status": "ok",
                        "value": None, "getex": 0}
            value = self._data[key]
            if command.get("persist"):
                self._expiry.pop(key, None)
                return {"type": "client_response", "status": "ok",
                        "value": value, "getex": 1, "deadline": None}
            deadline = None
            if "ex" in command or "px" in command:
                if index is None:
                    raise ValueError(
                        "getex needs the log index of the entry "
                        "being applied")
                n = (command["ex"] if "ex" in command
                     else command["px"])
                deadline = index + n
            elif "exat" in command or "pxat" in command:
                if index is None:
                    raise ValueError(
                        "getex needs the log index of the entry "
                        "being applied")
                deadline = (command["exat"] if "exat" in command
                            else command["pxat"])
            if deadline is not None:
                if deadline <= index:
                    # Already past: the key dies now, not at some
                    # later entry — a deadline at or before this
                    # entry's index can never be honoured, and
                    # recording it would leave ttl() answering 0
                    # or negative for a live key.
                    del self._data[key]
                    idx = bisect.bisect_left(self._keys, key)
                    if idx < len(self._keys) and self._keys[idx] == key:
                        del self._keys[idx]
                    self._expiry.pop(key, None)
                    self._wall_expiry.pop(key, None)
                    return {"type": "client_response", "status": "ok",
                            "value": None, "getex": 0, "deleted": True}
                self._expiry[key] = deadline
            return {"type": "client_response", "status": "ok",
                    "value": value, "getex": 1,
                    "deadline": self._expiry.get(key)}
        elif op == "setbit":
            # Set one bit in a string value, Redis SETBIT: bit 0
            # is the most significant bit of the first byte, bit
            # 7 the least significant, bit 8 the most significant
            # bit of the second byte, and so on — the bit
            # numbering Redis uses. A missing key counts as an
            # empty string, and when the offset lies past the end
            # the gap is padded with zero bytes, exactly like a
            # setrange pads with zero bytes. The reply carries
            # the old bit value (``old_bit``) — 0 when the bit
            # was clear or the key did not exist — and the new
            # value, which is what the watch event fires with.
            # The outcome is decided here, at this exact point
            # in the log, so a retry with the same session id
            # returns the first attempt's old bit from the dedup
            # table instead of setting twice. A value that is
            # not a byte string fails with WRONGTYPE, like
            # append and setrange: every value this store holds
            # is a string, and a bit op reads it as bytes — one
            # byte per character, latin-1 — so a value holding
            # a character above U+00FF has no byte form to set
            # a bit in. A failed setbit writes nothing at all,
            # like a failed incr. A successful setbit keeps the
            # key's deadline, like a CAS or an append: it is an
            # update of the value, not a fresh set, so it must
            # not resurrect a key that is about to expire. A new
            # key joins the sorted index, like a set of a new key.
            # The offset itself was validated on the leader
            # before the entry was logged (an integer from 0 to
            # SETBIT_MAX_OFFSET) and the bit as 0 or 1, so by
            # apply time both are in range — a hand-built log
            # entry with a bad offset or bit still raises rather
            # than silently setting a nonsense bit.
            key = command["key"]
            offset = command["offset"]
            if (not isinstance(offset, int)
                    or isinstance(offset, bool) or offset < 0):
                raise ValueError(
                    "setbit offset must be a non-negative integer")
            bit = command["bit"]
            if (not isinstance(bit, int) or isinstance(bit, bool)
                    or bit not in (0, 1)):
                raise ValueError("setbit bit must be 0 or 1")
            current = self._data.get(key)
            if current is not None and not isinstance(current, str):
                return {"type": "client_response",
                        "status": "setbit_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "setbit failed: value of "
                                 f"{key!r} is not a byte string"}
            try:
                raw = bytearray((current or "").encode("latin-1"))
            except UnicodeEncodeError:
                return {"type": "client_response",
                        "status": "setbit_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "setbit failed: value of "
                                 f"{key!r} is not a byte string"}
            byte_index = offset // 8
            if byte_index >= len(raw):
                raw.extend(b"\x00" * (byte_index - len(raw) + 1))
            mask = 1 << (7 - offset % 8)
            old = 1 if raw[byte_index] & mask else 0
            if bit:
                raw[byte_index] |= mask
            else:
                raw[byte_index] &= ~mask
            new = raw.decode("latin-1")
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = new
            return {"type": "client_response", "status": "ok",
                    "old_bit": old, "value": new}
        elif op == "bitop":
            # Bitwise AND / OR / XOR / NOT across string keys,
            # Redis BITOP: the result is a string of the same
            # length as the longest input, missing keys count as
            # empty (all-zero) strings, and the result overwrites
            # the destination key — a fresh set, so any deadline
            # the destination had is cleared, log-entry or
            # wall-clock, like a plain set. Bit 0 is the most
            # significant bit of the first byte, like a setbit.
            # The reply carries ``bitop`` — the length of the
            # result — and the result ``value`` itself, which is
            # what the watch event fires with. When every input
            # is missing or empty the result is the empty string:
            # Redis deletes the destination there instead of
            # storing an empty value, so this does too — the
            # reply is ``bitop`` 0 with ``deleted`` set when a
            # key actually went, and the apply path fires a
            # ``del`` event for it. The outcome is decided here,
            # at this exact point in the log, so a retry with
            # the same session id returns the first attempt's
            # length instead of combining twice. A source value
            # that is not a byte string fails with WRONGTYPE,
            # like a setbit on a value that is not a byte
            # string — a bit op reads each value as bytes, one
            # byte per character (latin-1), so a value holding
            # a character above U+00FF has no byte form to
            # combine. A failed bitop writes nothing at all,
            # like a failed setbit — and the destination is
            # untouched even when it is also a source, since the
            # failure is decided before anything is written.
            # The operation and the key list were validated on
            # the leader before the entry was logged (one of
            # AND / OR / XOR / NOT, 1 to BITOP_MAX_KEYS string
            # keys, NOT with exactly one), so by apply time
            # they are well-formed — a hand-built log entry with
            # a bad operation or key list still raises rather
            # than silently combining nonsense.
            key = command["key"]
            operation = command.get("operation")
            if (not isinstance(operation, str)
                    or operation.upper()
                    not in ("AND", "OR", "XOR", "NOT")):
                raise ValueError(
                    "bitop operation must be AND, OR, XOR, or NOT")
            operation = operation.upper()
            keys = command.get("keys")
            if (not isinstance(keys, list) or not keys
                    or len(keys) > BITOP_MAX_KEYS
                    or any(not isinstance(k, str) for k in keys)):
                raise ValueError(
                    "bitop keys must be a list of 1 to "
                    f"{BITOP_MAX_KEYS} strings")
            if operation == "NOT" and len(keys) != 1:
                raise ValueError(
                    "bitop NOT takes exactly one source key")
            raws: list[bytes] = []
            for source in keys:
                current = self._data.get(source)
                if current is not None and not isinstance(current, str):
                    return {"type": "client_response",
                            "status": "bitop_failed",
                            "reason": "wrongtype",
                            "current": current,
                            "error": "bitop failed: value of "
                                     f"{source!r} is not a byte string"}
                try:
                    raws.append((current or "").encode("latin-1"))
                except UnicodeEncodeError:
                    return {"type": "client_response",
                            "status": "bitop_failed",
                            "reason": "wrongtype",
                            "current": current,
                            "error": "bitop failed: value of "
                                     f"{source!r} is not a byte string"}
            length = max((len(raw) for raw in raws), default=0)
            if length == 0:
                # Every input missing or empty: the destination
                # goes, like Redis — no empty value is stored.
                deleted = key in self._data
                if deleted:
                    del self._data[key]
                    idx = bisect.bisect_left(self._keys, key)
                    if idx < len(self._keys) and self._keys[idx] == key:
                        del self._keys[idx]
                self._expiry.pop(key, None)
                self._wall_expiry.pop(key, None)
                return {"type": "client_response", "status": "ok",
                        "bitop": 0, "value": "",
                        "deleted": deleted}
            # Every input is padded with zero bytes to the longest
            # one's length before combining: a missing key is an
            # all-zero string, and a shorter key's missing tail is
            # zeros too — so AND zeroes the tail, OR / XOR keep
            # the long key's tail whole, exactly as Redis does.
            padded = [raw + b"\x00" * (length - len(raw))
                      for raw in raws]
            result = bytearray(padded[0])
            for raw in padded[1:]:
                if operation == "AND":
                    for i in range(length):
                        result[i] &= raw[i]
                elif operation == "OR":
                    for i in range(length):
                        result[i] |= raw[i]
                elif operation == "XOR":
                    for i in range(length):
                        result[i] ^= raw[i]
            if operation == "NOT":
                for i in range(length):
                    result[i] = ~padded[0][i] & 0xFF
            new = bytes(result).decode("latin-1")
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = new
            self._expiry.pop(key, None)
            self._wall_expiry.pop(key, None)
            return {"type": "client_response", "status": "ok",
                    "bitop": length, "value": new}
        elif op == "pfadd":
            # Add elements to a HyperLogLog, Redis PFADD: each
            # element hashes (SHA-256, deterministic across nodes)
            # into one of 16384 registers, and the register keeps
            # the longest run of leading zeros it has seen — the
            # estimate is the standard HLL one, with the small- and
            # large-range corrections (see hll_estimate). The reply
            # carries ``pfadd`` 1 when at least one register rose
            # — the HLL was modified — and 0 when every element
            # was already counted, plus the estimate after the add.
            # The outcome is decided here, at this exact point in
            # the log, so a retry with the same session id returns
            # the first attempt's pfadd flag and estimate from the
            # dedup table instead of adding twice. A missing key
            # is created as an empty HLL; a key holding a plain
            # string fails with WRONGTYPE, like a setbit on a value
            # that is not a byte string — a pfadd must not count
            # bytes as registers. A failed pfadd writes nothing at
            # all, like a failed incr. A successful pfadd keeps the
            # key's deadline, like a CAS or a setbit: it is an
            # update of the value, not a fresh set, so it must not
            # resurrect a key that is about to expire. A new key
            # joins the sorted index, like a set of a new key. The
            # registers ride in _data as a latin-1 string with the
            # HLL marker prefix, so they survive snapshots and
            # replay exactly like any other value — and the stored
            # blob is a pure function of the elements added, so
            # every node computes the same bytes. The elements
            # were validated on the leader before the entry was
            # logged (a non-empty list of at most
            # PFADD_MAX_ELEMENTS strings), so by apply time they
            # are well-formed — a hand-built log entry with a bad
            # elements list still raises rather than silently
            # counting nonsense.
            key = command["key"]
            elements = command.get("elements")
            if (not isinstance(elements, list) or not elements
                    or any(not isinstance(e, str) for e in elements)):
                raise ValueError(
                    "pfadd elements must be a non-empty list of "
                    "strings")
            current = self._data.get(key)
            if current is not None and not is_hll(current):
                return {"type": "client_response",
                        "status": "pfadd_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "pfadd failed: value of "
                                 f"{key!r} is not a hyperloglog"}
            registers = (_hll_registers(current)
                         if current is not None
                         else bytearray(HLL_REGISTERS))
            changed = False
            for element in elements:
                if _hll_add(registers, element):
                    changed = True
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = _hll_value(registers)
            return {"type": "client_response", "status": "ok",
                    "pfadd": 1 if changed else 0,
                    "estimate": hll_estimate(registers)}
        elif op == "hset":
            # Set fields in a hash, Redis HSET: each pair writes
            # one field, and the reply carries ``hset`` — the
            # number of fields that did not exist before, the
            # count Redis answers with (a field overwritten with
            # a new value is not counted, and a repeated field in
            # one call counts once, at its last value). The
            # outcome is decided here, at this exact point in
            # the log, so a retry with the same session id
            # returns the first attempt's count from the dedup
            # table instead of setting twice. A missing key is
            # created as an empty hash; a key holding a plain
            # string fails with WRONGTYPE, like a pfadd on a
            # plain string — an hset must not parse bytes as
            # fields. A failed hset writes nothing at all, like
            # a failed incr. A successful hset keeps the key's
            # deadline, like a CAS or a pfadd: it is an update
            # of the value, not a fresh set, so it must not
            # resurrect a key that is about to expire. A new key
            # joins the sorted index, like a set of a new key.
            # The fields ride in _data as a JSON blob with the
            # hash marker prefix, so they survive snapshots and
            # replay exactly like any other value — and the
            # stored blob is a pure function of the fields, so
            # every node computes the same bytes. The pairs were
            # validated on the leader before the entry was
            # logged (a non-empty list of at most HSET_MAX_PAIRS
            # string pairs), so by apply time they are
            # well-formed — a hand-built log entry with bad
            # pairs still raises rather than silently writing
            # nonsense.
            key = command["key"]
            pairs = command.get("pairs")
            if (not isinstance(pairs, list) or not pairs
                    or any(not isinstance(p, (list, tuple))
                           or len(p) != 2
                           or not isinstance(p[0], str)
                           or not isinstance(p[1], str)
                           for p in pairs)):
                raise ValueError(
                    "hset pairs must be a non-empty list of "
                    "[field, value] string pairs")
            current = self._data.get(key)
            if current is not None and not is_hash(current):
                return {"type": "client_response",
                        "status": "hset_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "hset failed: value of "
                                 f"{key!r} is not a hash"}
            fields = (_hash_fields(current)
                      if current is not None else {})
            added = 0
            for field, value in pairs:
                if field not in fields:
                    added += 1
                fields[field] = value
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = _hash_value(fields)
            return {"type": "client_response", "status": "ok",
                    "hset": added}
        elif op == "hdel":
            # Delete fields from a hash, Redis HDEL: each field
            # named is removed when present, and the reply
            # carries ``hdel`` — the number of fields actually
            # removed (a field repeated in one call counts once,
            # a field that was never there counts not at all).
            # The outcome is decided here, at this exact point
            # in the log, so a retry with the same session id
            # returns the first attempt's count from the dedup
            # table instead of deleting twice. A missing key is
            # a no-op, not an error: Redis answers 0 there, and
            # the entry still committed (the outcome is the
            # entry's result, not a client error). Removing the
            # last field deletes the key outright, like Redis —
            # the key leaves the sorted index and takes its
            # deadline with it, so there is nothing left to
            # expire — and the reply carries ``deleted`` so the
            # apply path fires a ``del`` watch event for it
            # instead of a ``set`` one. Removing fields while
            # some remain keeps the key's deadline, like an
            # hset: it is an update of the value, not a fresh
            # set, so it must not resurrect a key that is about
            # to expire. A key holding a plain string fails
            # with WRONGTYPE, like an hset — an hdel must not
            # parse bytes as fields — and writes nothing at
            # all, like a failed hset. The fields were validated
            # on the leader before the entry was logged (a
            # non-empty list of at most HDEL_MAX_FIELDS
            # strings), so by apply time they are well-formed —
            # a hand-built log entry with bad fields still raises
            # rather than silently writing nonsense.
            key = command["key"]
            fields = command.get("fields")
            if (not isinstance(fields, list) or not fields
                    or any(not isinstance(f, str) for f in fields)):
                raise ValueError(
                    "hdel fields must be a non-empty list of "
                    "strings")
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response", "status": "ok",
                        "hdel": 0}
            if not is_hash(current):
                return {"type": "client_response",
                        "status": "hdel_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "hdel failed: value of "
                                 f"{key!r} is not a hash"}
            remaining = _hash_fields(current)
            removed = 0
            for field in fields:
                if field in remaining:
                    del remaining[field]
                    removed += 1
            if not remaining:
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
                self._expiry.pop(key, None)
                self._wall_expiry.pop(key, None)
                return {"type": "client_response", "status": "ok",
                        "hdel": removed, "deleted": True}
            self._data[key] = _hash_value(remaining)
            return {"type": "client_response", "status": "ok",
                    "hdel": removed}
        elif op == "geoadd":
            # Add members with positions to a geo set, Redis
            # GEOADD: each member is a [lon, lat, member] triple,
            # and the reply carries ``geoadd`` — the number of
            # members that did not exist before (a member whose
            # position is updated is not counted, and a member
            # repeated in one call counts once, at its last
            # position). The outcome is decided here, at this
            # exact point in the log, so a retry with the same
            # session id returns the first attempt's count from
            # the dedup table instead of adding twice. A missing
            # key is created as an empty geo set; a key holding
            # a plain string fails with WRONGTYPE, like an hset
            # — a geoadd must not parse bytes as coordinates —
            # and writes nothing at all, like a failed hset. A
            # successful geoadd keeps the key's deadline, like
            # an hset: it is an update of the value, not a fresh
            # set, so it must not resurrect a key that is about
            # to expire. A new key joins the sorted index, like
            # a set of a new key. The members ride in _data as
            # a JSON blob with the geo marker prefix, so they
            # survive snapshots and replay exactly like any
            # other value — and the stored blob is a pure
            # function of the members, so every node computes
            # the same bytes. The members were validated on the
            # leader before the entry was logged (a non-empty
            # list of at most GEOADD_MAX_MEMBERS triples, each
            # a finite longitude in -180..180, a finite
            # latitude in -85.05112878..85.05112878, and a
            # string member), so by apply time they are
            # well-formed — a hand-built log entry with bad
            # members still raises rather than silently
            # writing nonsense.
            key = command["key"]
            members = command.get("members")
            if (not isinstance(members, list) or not members
                    or len(members) > GEOADD_MAX_MEMBERS
                    or any(not isinstance(m, (list, tuple))
                           or len(m) != 3
                           or not isinstance(m[0], (int, float))
                           or isinstance(m[0], bool)
                           or not math.isfinite(m[0])
                           or m[0] < GEO_LON_MIN or m[0] > GEO_LON_MAX
                           or not isinstance(m[1], (int, float))
                           or isinstance(m[1], bool)
                           or not math.isfinite(m[1])
                           or m[1] < GEO_LAT_MIN or m[1] > GEO_LAT_MAX
                           or not isinstance(m[2], str)
                           for m in members)):
                raise ValueError(
                    "geoadd members must be a non-empty list of "
                    f"at most {GEOADD_MAX_MEMBERS} "
                    "[lon, lat, member] triples with lon in "
                    "-180..180 and lat in "
                    "-85.05112878..85.05112878")
            current = self._data.get(key)
            if current is not None and not is_geo(current):
                return {"type": "client_response",
                        "status": "geoadd_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "geoadd failed: value of "
                                 f"{key!r} is not a geo set"}
            positions = (_geo_members(current)
                         if current is not None else {})
            added = 0
            for lon, lat, member in members:
                if member not in positions:
                    added += 1
                positions[member] = [float(lon), float(lat)]
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = _geo_value(positions)
            return {"type": "client_response", "status": "ok",
                    "geoadd": added}
        elif op == "xadd":
            # Append one entry to a stream, Redis XADD: the entry
            # is a list of [field, value] pairs, and the reply
            # carries ``xadd`` — the ID the entry got, "ms-seq".
            # The ID is "*", an explicit "ms-seq", or "ms-*" (the
            # sequence auto-generated): the leader fixed the auto
            # millisecond at log time (command "auto_ms"), so
            # every replica resolves the same ID — a follower must
            # never read its own clock here, or two nodes could
            # append the same entry under different IDs. The
            # resolution is a pure function of the request and the
            # stream's last ID: an auto millisecond past the last
            # one takes sequence 0, one equal to it or behind it
            # takes the last millisecond with the next sequence —
            # so an auto ID is always greater than the last ID,
            # however far behind the leader's clock the stream's
            # top has run — and "ms-*" takes sequence 0 past the
            # last millisecond, the next sequence within it, and
            # fails when its millisecond is behind the last one.
            # An explicit ID must be greater than the last ID,
            # like Redis: one equal or smaller fails with
            # ``xadd_failed`` / ``id_not_greater`` and writes
            # nothing — but it still committed its entry, so a
            # retry with the same session id returns the first
            # attempt's failure instead of appending again. The
            # outcome is decided here, at this exact point in the
            # log, like a CAS. A missing key is created as an
            # empty stream; a key holding a plain string fails
            # with WRONGTYPE, like an hset — an xadd must not
            # parse bytes as entries — and writes nothing at all.
            # A field repeated in one call counts once, at its
            # last value, like an hset. A successful xadd keeps
            # the key's deadline, like an hset: it is an append
            # to the value, not a fresh set, so it must not
            # resurrect a key that is about to expire. A new key
            # joins the sorted index, like a set of a new key.
            # The entries ride in _data as a JSON blob with the
            # stream marker prefix, so they survive snapshots and
            # replay exactly like any other value — and the stored
            # blob is a pure function of the entries, so every
            # node computes the same bytes. The pairs and the ID
            # were validated on the leader before the entry was
            # logged (a non-empty list of at most XADD_MAX_FIELDS
            # [field, value] string pairs, an ID of "*", "ms-seq",
            # or "ms-*" with 64-bit halves and never 0-0), so by
            # apply time they are well-formed — a hand-built log
            # entry with bad pairs or a bad ID still raises rather
            # than silently writing nonsense.
            key = command["key"]
            pairs = command.get("pairs")
            if (not isinstance(pairs, list) or not pairs
                    or len(pairs) > XADD_MAX_FIELDS
                    or any(not isinstance(p, (list, tuple))
                           or len(p) != 2
                           or not isinstance(p[0], str)
                           or not isinstance(p[1], str)
                           for p in pairs)):
                raise ValueError(
                    "xadd pairs must be a non-empty list of at "
                    f"most {XADD_MAX_FIELDS} [field, value] string "
                    "pairs")
            ms, seq = parse_stream_id(command.get("id", "*"))
            current = self._data.get(key)
            if current is not None and not is_stream(current):
                return {"type": "client_response",
                        "status": "xadd_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xadd failed: value of "
                                 f"{key!r} is not a stream"}
            stream = (_stream_data(current)
                      if current is not None
                      else {"last": None, "entries": []})
            last = stream["last"]
            last_ms, last_seq = (parse_stream_id(last)
                                 if last is not None else (None, None))
            if ms == "*":
                auto_ms = command.get("auto_ms")
                if (not isinstance(auto_ms, int)
                        or isinstance(auto_ms, bool)
                        or auto_ms < 0 or auto_ms > STREAM_ID_MAX):
                    raise ValueError(
                        "xadd with id \"*\" needs auto_ms, a "
                        "millisecond from 0 to 2**64 - 1, fixed by "
                        "the leader at log time")
                new_ms, new_seq = _auto_stream_id(auto_ms, last_ms,
                                                  last_seq)
            elif seq is None:
                # "ms-*": the sequence is the next one at that
                # millisecond — 0 past the last millisecond, the
                # last sequence + 1 within it — and a millisecond
                # behind the last one fails, like an explicit ID
                # behind it. On an empty stream 0-* is 0-1, since
                # 0-0 is not a valid ID.
                if last_ms is None:
                    new_ms, new_seq = ((0, 1) if ms == 0
                                       else (ms, 0))
                elif ms < last_ms:
                    return {"type": "client_response",
                            "status": "xadd_failed",
                            "reason": "id_not_greater",
                            "last_id": last,
                            "error": "xadd failed: the ID specified "
                                     "in XADD is equal or smaller "
                                     "than the target stream top "
                                     "item"}
                elif ms == last_ms:
                    new_ms, new_seq = _next_seq_at(ms, last_seq)
                else:
                    new_ms, new_seq = ms, 0
            else:
                if (last_ms is not None
                        and (ms, seq) <= (last_ms, last_seq)):
                    return {"type": "client_response",
                            "status": "xadd_failed",
                            "reason": "id_not_greater",
                            "last_id": last,
                            "error": "xadd failed: the ID specified "
                                     "in XADD is equal or smaller "
                                     "than the target stream top "
                                     "item"}
                new_ms, new_seq = ms, seq
            fields = {}
            for field, value in pairs:
                fields[field] = value
            new_id = f"{new_ms}-{new_seq}"
            stream["entries"].append([new_id, fields])
            stream["last"] = new_id
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = _stream_value(stream)
            return {"type": "client_response", "status": "ok",
                    "xadd": new_id}
        elif op == "xtrim":
            # Trim a stream from the front, Redis XTRIM: MAXLEN
            # keeps at most ``threshold`` entries (the newest
            # ones), MINID drops every entry with an ID below
            # ``threshold``. The reply carries ``xtrim`` — the
            # number of entries removed. A missing key answers 0
            # and writes nothing, like an expire on a missing key
            # — the entry still committed, it just had nothing to
            # trim. A key holding a plain string fails with
            # WRONGTYPE, like an xadd — an xtrim must not parse
            # bytes as entries — and writes nothing at all. The
            # outcome is decided here, at this exact point in the
            # log, like a CAS, so a retry with the same session
            # id returns the first attempt's count instead of
            # trimming twice. The top ID ("last") never moves:
            # trimming removes entries, it does not rewind the
            # stream, so an xadd after a trim still refuses an ID
            # at or below the old top — and a trim that empties
            # the stream deletes the key outright, deadline and
            # all, like an hdel that removes the last field, so
            # a later xadd starts a fresh stream — the old top is
            # forgotten with the key, exactly as Redis forgets it
            # once the stream is gone. A successful xtrim
            # keeps the key's deadline when entries remain, like
            # an xadd: it is an update of the value, not a fresh
            # set. The ``approximate`` flag ("~" in Redis) is
            # accepted and ignored — this store trims exactly,
            # since there is no radix tree whose nodes need a
            # visit budget. The ``mode`` (KEEPREF / DELREF /
            # ACKED) governs what happens to the pending entries
            # of consumer groups when their entry is trimmed:
            # DELREF — and no mode at all, the default — drops
            # the pending reference, so the entry stops being
            # pending in every group; KEEPREF keeps it, so the
            # entry stays pending (and still counts in XPENDING)
            # even though its data is gone, and a later XREADGROUP
            # with an explicit ID simply skips it, since there is
            # nothing left to return. ACKED is accepted and
            # behaves like KEEPREF in this version: dropping only
            # entries already acked by every group would need
            # per-entry read tracking the store does not keep yet.
            # The strategy and threshold were validated on the
            # leader before the entry was logged (MAXLEN with a
            # non-negative integer threshold, MINID with a bound
            # a range bound can parse), so by apply time they
            # are well-formed — a hand-built log entry with a bad
            # strategy or threshold still raises rather than
            # silently trimming nonsense.
            key = command["key"]
            strategy = command.get("strategy")
            threshold = command.get("threshold")
            if strategy not in ("MAXLEN", "MINID"):
                raise ValueError(
                    "xtrim strategy must be \"MAXLEN\" or \"MINID\"")
            if strategy == "MAXLEN":
                if (not isinstance(threshold, int)
                        or isinstance(threshold, bool)
                        or threshold < 0):
                    raise ValueError(
                        "xtrim MAXLEN threshold must be a "
                        "non-negative integer")
            else:
                if not isinstance(threshold, str):
                    raise ValueError(
                        "xtrim MINID threshold must be a string")
                min_id = parse_stream_range_bound(threshold)
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response", "status": "ok",
                        "xtrim": 0}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xtrim_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xtrim failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            entries = stream["entries"]
            if strategy == "MAXLEN":
                removed = max(0, len(entries) - threshold)
                kept = entries[removed:]
            else:
                removed = 0
                for entry_id, _fields in entries:
                    if parse_stream_id(entry_id) < min_id:
                        removed += 1
                    else:
                        break
                kept = entries[removed:]
            if removed:
                # Trimming drops the pending references of the
                # entries that went — DELREF, and no mode at all
                # — so a trimmed entry stops being pending in
                # every group; KEEPREF (and ACKED, which behaves
                # like it here) keeps them, so the entry stays
                # pending even though its data is gone. A trim
                # that empties the stream deletes the key
                # outright, groups and all, so there is nothing
                # left to drop there.
                mode = command.get("mode")
                if mode not in ("KEEPREF", "ACKED"):
                    gone = {entry_id for entry_id, _f
                            in entries[:removed]}
                    for group in stream.get("groups", {}).values():
                        pending = group["pending"]
                        for entry_id in list(pending):
                            if entry_id in gone:
                                del pending[entry_id]
            if not kept:
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
                self._expiry.pop(key, None)
                self._wall_expiry.pop(key, None)
                return {"type": "client_response", "status": "ok",
                        "xtrim": removed, "deleted": True}
            stream["entries"] = kept
            self._data[key] = _stream_value(stream)
            return {"type": "client_response", "status": "ok",
                    "xtrim": removed}
        elif op == "xgroup_create":
            # Create a consumer group on a stream, Redis XGROUP
            # CREATE: the group starts reading from ``start_id``
            # — "$" (only entries appended after the group is
            # created: last_delivered is the stream's top, or
            # None on an empty stream, so every later entry
            # counts as new), "0" (from the very first entry:
            # last_delivered "0-0"), a bare millisecond, or a
            # full "ms-seq". The reply carries ``xgroup_create``
            # 1. A missing key is created as an empty stream when
            # ``mkstream`` rides along, like an xadd creating its
            # key; without it the create fails with
            # ``xgroup_create_failed`` / ``no_group`` — a group
            # cannot exist on a stream that does not. A group
            # that already exists fails the same way, reason
            # ``group_exists`` — Redis's BUSYGROUP — and a key
            # holding a plain string fails with WRONGTYPE, like
            # an xadd. The outcome is decided here, at this exact
            # point in the log, like a CAS, so a retry with the
            # same session id returns the first attempt's result
            # instead of creating the group twice. The group rides
            # in the stream blob, so it survives snapshots and
            # replay exactly like the entries, and a successful
            # create keeps the key's deadline, like an xadd: it
            # is an update of the value, not a fresh set. The
            # group name and start ID were validated on the leader
            # before the entry was logged (a non-empty string
            # name; "$" or a bound a range bound can parse), so
            # by apply time they are well-formed — a hand-built
            # log entry with a bad name or ID still raises rather
            # than silently creating nonsense.
            key = command["key"]
            group = command.get("group")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xgroup_create group must be a non-empty string")
            start_id = command.get("start_id", "$")
            if not isinstance(start_id, str):
                raise ValueError("xgroup_create start_id must be a "
                                 "string")
            current = self._data.get(key)
            if current is not None and not is_stream(current):
                return {"type": "client_response",
                        "status": "xgroup_create_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xgroup_create failed: value of "
                                 f"{key!r} is not a stream"}
            if current is None and not command.get("mkstream"):
                return {"type": "client_response",
                        "status": "xgroup_create_failed",
                        "reason": "no_group",
                        "error": "xgroup_create failed: no such key "
                                 f"{key!r} (use MKSTREAM to create "
                                 "the stream)"}
            stream = (_stream_data(current)
                      if current is not None
                      else {"last": None, "entries": []})
            groups = stream.setdefault("groups", {})
            if group in groups:
                return {"type": "client_response",
                        "status": "xgroup_create_failed",
                        "reason": "group_exists",
                        "error": "xgroup_create failed: consumer "
                                 f"group {group!r} already exists"}
            if start_id == "$":
                last_delivered = stream["last"]
            else:
                start_ms, start_seq = parse_stream_range_bound(
                    start_id)
                last_delivered = f"{start_ms}-{start_seq}"
            groups[group] = {"last_delivered": last_delivered,
                             "pending": {}}
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = _stream_value(stream)
            return {"type": "client_response", "status": "ok",
                    "xgroup_create": 1}
        elif op == "xreadgroup":
            # Read entries for one consumer of one group, Redis
            # XREADGROUP: with ``id`` ">" the group hands the
            # consumer the entries appended after its
            # last_delivered — in ID order, at most ``count`` of
            # them when a count is given — and marks each one
            # pending for that consumer (delivery count 1, unless
            # ``noack`` rides along, which still advances
            # last_delivered but records nothing pending), moving
            # last_delivered to the last entry handed out; with
            # any other ID it re-reads the consumer's own pending
            # entries — the ones already handed to this consumer
            # and not yet acked, with an ID at or after the given
            # one — in ID order, at most ``count`` of them, and
            # each re-read raises its delivery count by one, so a
            # consumer can tell a first delivery from a retry —
            # unless ``noack`` rides along: a NOACK re-read hands
            # the entries back without touching the pending record
            # at all, no delivery count and no new delivery stamp,
            # so it reads as a peek rather than a redelivery and
            # the entry's idle keeps running from its last real
            # delivery. That is a deliberate departure from Redis,
            # where NOACK is ignored for a history read and the
            # re-read still counts as a delivery: here NOACK means
            # the delivery is not recorded, whichever ID form the
            # read takes.
            # The reply carries ``entries`` — the [id, {field:
            # value}] pairs in ID order, an empty list when the
            # group has nothing for this consumer. A missing key
            # or a missing group fails with
            # ``xreadgroup_failed`` / ``no_group`` — Redis's
            # NOGROUP — and a key holding a plain string fails
            # with WRONGTYPE, like an xadd. The outcome is
            # decided here, at this exact point in the log, like
            # a CAS, so a retry with the same session id returns
            # the first attempt's entries instead of delivering
            # them twice — a redelivery only ever comes from a
            # fresh explicit-ID read, never from a retry. This is
            # a write, not a read: handing entries out mutates the
            # pending list and last_delivered, and the mutation
            # must replicate through the log like any other, so
            # every node agrees on who holds what. A successful
            # read keeps the key's deadline, like an xadd, and a
            # read that hands out nothing writes nothing at all.
            # The group, consumer, and ID were validated on the
            # leader before the entry was logged (non-empty
            # strings; ">" or a bound a range bound can parse;
            # a non-negative integer count), so by apply time
            # they are well-formed — a hand-built log entry with
            # a bad name, ID, or count still raises rather than
            # silently delivering nonsense. A delivery also
            # stamps the consumer's last-seen time — the
            # wall-clock millisecond the leader fixed at log time
            # (command "seen_ms", like an xadd auto ID) — in the
            # group's "consumers" map, so XINFO CONSUMERS can
            # report how long each consumer has been idle, and
            # records the newest entry ID it handed that
            # consumer there too, so XINFO CONSUMERS can report
            # the consumer's lag — and stamps the same millisecond
            # on each pending entry it creates or re-delivers — its
            # "delivered_ms" — so XPENDING in per-entry form can
            # report how long each entry has been idle; an entry
            # logged before v0.71.0 carries no stamp on the entry
            # itself, so replaying one simply records no delivery
            # time instead of failing, and a hand-built entry with
            # a bad one still raises. A NOACK delivery stamps the
            # consumer's last-seen time too — the consumer
            # interacted and received entries — and so does a
            # re-read; only a read that hands out nothing
            # records nothing, since it writes nothing at all.
            key = command["key"]
            group = command.get("group")
            consumer = command.get("consumer")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xreadgroup group must be a non-empty string")
            if not isinstance(consumer, str) or not consumer:
                raise ValueError(
                    "xreadgroup consumer must be a non-empty "
                    "string")
            id_str = command.get("id", ">")
            if not isinstance(id_str, str):
                raise ValueError("xreadgroup id must be a string")
            count = command.get("count")
            if count is not None and (
                    not isinstance(count, int)
                    or isinstance(count, bool) or count < 0):
                raise ValueError(
                    "xreadgroup count must be a non-negative "
                    "integer")
            seen_ms = command.get("seen_ms")
            if seen_ms is not None and (
                    not isinstance(seen_ms, int)
                    or isinstance(seen_ms, bool) or seen_ms < 0):
                raise ValueError(
                    "xreadgroup seen_ms must be a non-negative "
                    "integer number of milliseconds")
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xreadgroup_failed",
                        "reason": "no_group",
                        "error": "xreadgroup failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xreadgroup_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xreadgroup failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response",
                        "status": "xreadgroup_failed",
                        "reason": "no_group",
                        "error": "xreadgroup failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            state = groups[group]
            entries = stream["entries"]
            noack = bool(command.get("noack"))
            if id_str == ">":
                # New entries: everything after the group's last
                # delivery — None means the group was created on
                # an empty stream with "$", so every entry is
                # new. A bare (0, 0) floor is the same thing: no
                # real entry is ever 0-0.
                last = state["last_delivered"]
                after = ((0, 0) if last is None
                         else _parse_stored_stream_id(last))
                chosen = [(eid, fields) for eid, fields in entries
                          if parse_stream_id(eid) > after]
                if count is not None:
                    chosen = chosen[:count]
                if chosen:
                    if not noack:
                        for eid, _fields in chosen:
                            info = {"consumer": consumer,
                                    "deliveries": 1}
                            if seen_ms is not None:
                                info["delivered_ms"] = seen_ms
                            state["pending"][eid] = info
                    state["last_delivered"] = chosen[-1][0]
            else:
                # Pending entries: the consumer's own unacked
                # ones, at or after the given ID — in ID order,
                # each re-read raising its delivery count, unless
                # NOACK rides along: a NOACK re-read hands the
                # entries back without touching the pending record
                # at all, so the delivery count stays where the
                # last real delivery left it and the delivery
                # stamp is not re-stamped, and the entry's idle
                # keeps running from that delivery. An entry
                # trimmed away under KEEPREF is still
                # pending but its data is gone, so it is skipped:
                # there is nothing left to return for it.
                start = parse_stream_range_bound(id_str)
                by_id = {eid: fields for eid, fields in entries}
                chosen = []
                for eid in sorted(state["pending"],
                                  key=parse_stream_id):
                    if parse_stream_id(eid) < start:
                        continue
                    info = state["pending"][eid]
                    if info["consumer"] != consumer:
                        continue
                    if eid not in by_id:
                        continue
                    if count is not None and len(chosen) >= count:
                        break
                    chosen.append((eid, by_id[eid]))
                    if not noack:
                        info["deliveries"] += 1
                        if seen_ms is not None:
                            info["delivered_ms"] = seen_ms
            if chosen:
                if seen_ms is not None:
                    _stamp_consumer_delivery(state, consumer, seen_ms,
                                             chosen[-1][0])
                self._data[key] = _stream_value(stream)
            return {"type": "client_response", "status": "ok",
                    "entries": [[eid, dict(fields)]
                                for eid, fields in chosen]}
        elif op == "xack":
            # Acknowledge entries for a consumer group, Redis
            # XACK: every ID in ``ids`` that is pending in the
            # group — for any consumer, since an ack names the
            # entry, not its holder — leaves the pending list,
            # and the reply carries ``xack`` — how many entries
            # actually went, 0 when none of them was pending. An
            # ID that was never delivered, or was already acked,
            # simply does not count. A missing key or a missing
            # group fails with ``xack_failed`` / ``no_group`` —
            # Redis's NOGROUP — and a key holding a plain string
            # fails with WRONGTYPE, like an xadd. The outcome is
            # decided here, at this exact point in the log, like
            # a CAS, so a retry with the same session id returns
            # the first attempt's count instead of acking twice.
            # A successful ack keeps the key's deadline, like an
            # xadd, and an ack that removes nothing writes nothing
            # at all. The group and IDs were validated on the
            # leader before the entry was logged (a non-empty
            # string group; a non-empty list of at most 1000
            # explicit "ms-seq" IDs), so by apply time they are
            # well-formed — a hand-built log entry with a bad
            # group or ID still raises rather than silently
            # acking nonsense.
            key = command["key"]
            group = command.get("group")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xack group must be a non-empty string")
            ids = command.get("ids")
            if (not isinstance(ids, list) or not ids
                    or len(ids) > XACK_MAX_IDS
                    or any(not isinstance(i, str) for i in ids)):
                raise ValueError(
                    "xack ids must be a non-empty list of at most "
                    f"{XACK_MAX_IDS} stream IDs")
            for entry_id in ids:
                _ms, seq = parse_stream_id(entry_id)
                if seq is None:
                    raise ValueError(
                        "xack ids must be explicit \"ms-seq\" IDs")
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xack_failed",
                        "reason": "no_group",
                        "error": "xack failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xack_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xack failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response",
                        "status": "xack_failed",
                        "reason": "no_group",
                        "error": "xack failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            pending = groups[group]["pending"]
            acked = 0
            for entry_id in ids:
                if entry_id in pending:
                    del pending[entry_id]
                    acked += 1
            if acked:
                self._data[key] = _stream_value(stream)
            return {"type": "client_response", "status": "ok",
                    "xack": acked}
        elif op == "xclaim":
            # Claim pending entries for a new consumer, Redis
            # XCLAIM: every ID in ``ids`` that is pending in the
            # group and has been idle at least ``min_idle_ms``
            # — the milliseconds since its last delivery, measured
            # against ``seen_ms``, the wall-clock millisecond the
            # leader fixed at log time, like an xreadgroup stamp —
            # moves to ``consumer``: its holder changes, its
            # delivery count rises by one (or is set to
            # ``retrycount`` when one is given), and its delivery
            # stamp is re-set to ``seen_ms``, so the idle measures
            # since this claim. With ``force`` the idle check is
            # skipped — even an entry delivered a moment ago, or
            # one whose deliveries all came from replays of
            # pre-v0.71.0 log entries and so carries no stamp and
            # no provable idle, can be taken over. With
            # ``justid`` the reply carries only the claimed IDs,
            # not the entries. An ID that is not pending, or whose
            # entry was trimmed away under KEEPREF so there is no
            # data left to return, is simply skipped: there is
            # nothing to claim for it. A missing key or a missing
            # group fails with ``xclaim_failed`` / ``no_group`` —
            # Redis's NOGROUP — and a key holding a plain string
            # fails with WRONGTYPE, like an xadd. The outcome is
            # decided here, at this exact point in the log, like
            # a CAS, so a retry with the same session id returns
            # the first attempt's entries instead of claiming
            # them twice. A successful claim keeps the key's
            # deadline, like an xadd, stamps the claiming
            # consumer's last-seen time and records the newest
            # entry it claimed as that consumer's last delivery,
            # and a claim that takes nothing writes nothing at all. The group, consumer,
            # IDs, minimum idle, and options were validated on
            # the leader before the entry was logged (non-empty
            # string group and consumer; a non-empty list of at
            # most 1000 explicit "ms-seq" IDs; a non-negative
            # integer minimum idle and retry count), so by apply
            # time they are well-formed — a hand-built log entry
            # with a bad group, consumer, ID, or option still
            # raises rather than silently claiming nonsense — and
            # ``seen_ms`` is required, since the idle check has
            # no now to measure against without it.
            key = command["key"]
            group = command.get("group")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xclaim group must be a non-empty string")
            consumer = command.get("consumer")
            if not isinstance(consumer, str) or not consumer:
                raise ValueError(
                    "xclaim consumer must be a non-empty string")
            ids = command.get("ids")
            if (not isinstance(ids, list) or not ids
                    or len(ids) > XACK_MAX_IDS
                    or any(not isinstance(i, str) for i in ids)):
                raise ValueError(
                    "xclaim ids must be a non-empty list of at most "
                    f"{XACK_MAX_IDS} stream IDs")
            for entry_id in ids:
                _ms, seq = parse_stream_id(entry_id)
                if seq is None:
                    raise ValueError(
                        "xclaim ids must be explicit \"ms-seq\" IDs")
            min_idle_ms = command.get("min_idle_ms")
            if (not isinstance(min_idle_ms, int)
                    or isinstance(min_idle_ms, bool)
                    or min_idle_ms < 0):
                raise ValueError(
                    "xclaim min_idle_ms must be a non-negative "
                    "integer number of milliseconds")
            retrycount = command.get("retrycount")
            if retrycount is not None and (
                    not isinstance(retrycount, int)
                    or isinstance(retrycount, bool)
                    or retrycount < 0):
                raise ValueError(
                    "xclaim retrycount must be a non-negative "
                    "integer")
            seen_ms = command.get("seen_ms")
            if (not isinstance(seen_ms, int)
                    or isinstance(seen_ms, bool) or seen_ms < 0):
                raise ValueError(
                    "xclaim seen_ms must be a non-negative integer "
                    "number of milliseconds")
            force = bool(command.get("force"))
            justid = bool(command.get("justid"))
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xclaim_failed",
                        "reason": "no_group",
                        "error": "xclaim failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xclaim_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xclaim failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response",
                        "status": "xclaim_failed",
                        "reason": "no_group",
                        "error": "xclaim failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            state = groups[group]
            by_id = {eid: fields for eid, fields in stream["entries"]}
            claimed: list[tuple[str, dict]] = []
            for entry_id in ids:
                info = state["pending"].get(entry_id)
                if info is None:
                    continue
                if entry_id not in by_id:
                    continue
                if not force:
                    delivered_ms = info.get("delivered_ms")
                    idle = (None if delivered_ms is None
                            else max(0, seen_ms - delivered_ms))
                    if idle is None or idle < min_idle_ms:
                        continue
                info["consumer"] = consumer
                if retrycount is not None:
                    info["deliveries"] = retrycount
                else:
                    info["deliveries"] += 1
                info["delivered_ms"] = seen_ms
                claimed.append((entry_id, by_id[entry_id]))
            if claimed:
                _stamp_consumer_delivery(
                    state, consumer, seen_ms,
                    max((eid for eid, _fields in claimed),
                        key=_parse_stored_stream_id))
                self._data[key] = _stream_value(stream)
            if justid:
                return {"type": "client_response", "status": "ok",
                        "ids": [eid for eid, _fields in claimed]}
            return {"type": "client_response", "status": "ok",
                    "entries": [[eid, dict(fields)]
                                for eid, fields in claimed]}
        elif op == "xautoclaim":
            # Claim pending entries in bulk, Redis XAUTOCLAIM:
            # walk the group's pending list in ID order from just
            # after ``start`` ("0-0" or "0" from the very first
            # pending entry, a bare millisecond, or a full
            # "ms-seq"), and take over every entry idle at least
            # ``min_idle_ms`` — the milliseconds since its last
            # delivery, measured against ``seen_ms``, the
            # wall-clock millisecond the leader fixed at log time,
            # like an xclaim — moving it to ``consumer``, raising
            # its delivery count by one, and re-stamping its
            # delivery time to the claim's time. An entry whose
            # deliveries all came from replays of pre-v0.71.0 log
            # entries carries no stamp and no provable idle, so it
            # is skipped, like an xclaim without FORCE — there is
            # no FORCE here, Redis has none for XAUTOCLAIM either.
            # A pending entry whose data is gone — trimmed away
            # under KEEPREF, so there is nothing left to return —
            # is dropped from the pending list and reported under
            # ``deleted``, like Redis's third reply element. COUNT
            # caps how many pending entries one call examines
            # (default 100): a call can return fewer than COUNT
            # claimed entries when the ones it examined were too
            # fresh to take, and the cursor resumes after the last
            # entry examined — that is what bounds the work per
            # call, the point of the cursor. The reply carries
            # ``next`` — the ID to pass as ``start`` on the next
            # call, "0-0" when the scan reached the end of the
            # pending list — plus ``entries`` ([id, {field: value}]
            # pairs in ID order), or ``ids`` with JUSTID, and
            # ``deleted``. A missing key or a missing group fails
            # with ``xautoclaim_failed`` / ``no_group`` — Redis's
            # NOGROUP — and a key holding a plain string fails
            # with WRONGTYPE, like an xadd. The outcome is decided
            # here, at this exact point in the log, like a CAS, so
            # a retry with the same session id returns the first
            # attempt's cursor and entries instead of claiming
            # them twice. A successful claim keeps the key's
            # deadline, like an xadd, stamps the claiming
            # consumer's last-seen time and records the newest
            # entry it claimed as that consumer's last delivery,
            # and a call that claims nothing and deletes nothing
            # writes nothing at all.
            # The group, consumer, start, minimum idle, count,
            # and options were validated on the leader before the
            # entry was logged, so by apply time they are
            # well-formed — a hand-built log entry with a bad
            # group, consumer, start, or option still raises
            # rather than silently claiming nonsense — and
            # ``seen_ms`` is required, since the idle check has
            # no now to measure against without it.
            key = command["key"]
            group = command.get("group")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xautoclaim group must be a non-empty string")
            consumer = command.get("consumer")
            if not isinstance(consumer, str) or not consumer:
                raise ValueError(
                    "xautoclaim consumer must be a non-empty string")
            start = command.get("start")
            if not isinstance(start, str):
                raise ValueError(
                    "xautoclaim start must be a stream ID string")
            start_id = parse_stream_range_bound(start)
            min_idle_ms = command.get("min_idle_ms")
            if (not isinstance(min_idle_ms, int)
                    or isinstance(min_idle_ms, bool)
                    or min_idle_ms < 0):
                raise ValueError(
                    "xautoclaim min_idle_ms must be a non-negative "
                    "integer number of milliseconds")
            count = command.get("count")
            if count is not None and (
                    not isinstance(count, int)
                    or isinstance(count, bool) or count < 0):
                raise ValueError(
                    "xautoclaim count must be a non-negative "
                    "integer")
            if count is None:
                count = XAUTOCLAIM_DEFAULT_COUNT
            seen_ms = command.get("seen_ms")
            if (not isinstance(seen_ms, int)
                    or isinstance(seen_ms, bool) or seen_ms < 0):
                raise ValueError(
                    "xautoclaim seen_ms must be a non-negative "
                    "integer number of milliseconds")
            justid = bool(command.get("justid"))
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xautoclaim_failed",
                        "reason": "no_group",
                        "error": "xautoclaim failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xautoclaim_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xautoclaim failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response",
                        "status": "xautoclaim_failed",
                        "reason": "no_group",
                        "error": "xautoclaim failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            state = groups[group]
            by_id = {eid: fields for eid, fields in stream["entries"]}
            pending_ids = sorted(state["pending"],
                                 key=_parse_stored_stream_id)
            after = [eid for eid in pending_ids
                     if _parse_stored_stream_id(eid) > start_id]
            claimed: list[tuple[str, dict]] = []
            deleted: list[str] = []
            examined = 0
            for entry_id in after:
                if examined >= count:
                    break
                examined += 1
                info = state["pending"][entry_id]
                if entry_id not in by_id:
                    del state["pending"][entry_id]
                    deleted.append(entry_id)
                    continue
                delivered_ms = info.get("delivered_ms")
                idle = (None if delivered_ms is None
                        else max(0, seen_ms - delivered_ms))
                if idle is None or idle < min_idle_ms:
                    continue
                info["consumer"] = consumer
                info["deliveries"] += 1
                info["delivered_ms"] = seen_ms
                claimed.append((entry_id, by_id[entry_id]))
            if claimed or deleted:
                if claimed:
                    _stamp_consumer_delivery(
                        state, consumer, seen_ms,
                        max((eid for eid, _fields in claimed),
                            key=_parse_stored_stream_id))
                self._data[key] = _stream_value(stream)
            if examined >= len(after):
                next_id = "0-0"
            elif examined == 0:
                # COUNT 0 examined nothing, so the cursor does not
                # move: it comes back as the start that was given,
                # normalized to "ms-seq" form.
                next_id = f"{start_id[0]}-{start_id[1]}"
            else:
                next_id = after[examined - 1]
            if justid:
                return {"type": "client_response", "status": "ok",
                        "next": next_id,
                        "ids": [eid for eid, _fields in claimed],
                        "deleted": deleted}
            return {"type": "client_response", "status": "ok",
                    "next": next_id,
                    "entries": [[eid, dict(fields)]
                                for eid, fields in claimed],
                    "deleted": deleted}
        elif op == "xgroup_setid":
            # Set a consumer group's last-delivered ID, Redis
            # XGROUP SETID: ``id`` is "$" (the stream's top, or
            # None on an empty stream, so every later entry
            # counts as new — the same resolution a create with
            # "$" uses) or an explicit "ms-seq" or bare
            # millisecond, which sets last_delivered to exactly
            # that ID — even one past the stream's top, so a
            # later ">" read hands out nothing until entries pass
            # it, like Redis. An optional ``entries_read`` sets
            # the group's entries-read count (Redis's ENTRIESREAD
            # option), a non-negative integer stored alongside
            # the group in the blob, so it survives snapshots and
            # replay like the rest of the group. The pending
            # entries are untouched: a SETID moves the read
            # position, it does not ack or drop anything. The
            # reply carries ``xgroup_setid`` 1, and ``changed`` 1
            # only when the blob actually changed — a SETID to
            # the ID the group already holds, with no new
            # entries-read, writes nothing at all, like an xtrim
            # that removed nothing. A missing key or a missing
            # group fails with ``xgroup_setid_failed`` /
            # ``no_group`` — Redis's NOGROUP — and a key holding
            # a plain string fails with WRONGTYPE, like an xadd.
            # The outcome is decided here, at this exact point in
            # the log, like a CAS, so a retry with the same
            # session id returns the first attempt's result
            # instead of setting the ID twice. A successful setid
            # keeps the key's TTL deadline, like an xadd. The
            # group, ID, and entries-read were validated on the
            # leader before the entry was logged (a non-empty
            # string group; "$", "0", a bare millisecond, or a
            # full "ms-seq" — never "-" or "+", which are range
            # bounds, not IDs — and a non-negative integer
            # entries-read), so by apply time they are
            # well-formed — a hand-built log entry with a bad
            # group, ID, or count still raises rather than
            # silently moving the read position.
            key = command["key"]
            group = command.get("group")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xgroup_setid group must be a non-empty string")
            id_str = command.get("id", "$")
            if not isinstance(id_str, str):
                raise ValueError("xgroup_setid id must be a string")
            if id_str in ("-", "+"):
                raise ValueError(
                    "xgroup_setid id must be \"$\", \"0\", \"ms\", or "
                    "\"ms-seq\"")
            entries_read = command.get("entries_read")
            if entries_read is not None and (
                    not isinstance(entries_read, int)
                    or isinstance(entries_read, bool)
                    or entries_read < 0):
                raise ValueError(
                    "xgroup_setid entries_read must be a "
                    "non-negative integer")
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xgroup_setid_failed",
                        "reason": "no_group",
                        "error": "xgroup_setid failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xgroup_setid_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xgroup_setid failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response",
                        "status": "xgroup_setid_failed",
                        "reason": "no_group",
                        "error": "xgroup_setid failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            state = groups[group]
            if id_str == "$":
                last_delivered = stream["last"]
            else:
                set_ms, set_seq = parse_stream_range_bound(id_str)
                last_delivered = f"{set_ms}-{set_seq}"
            changed = False
            if state["last_delivered"] != last_delivered:
                state["last_delivered"] = last_delivered
                changed = True
            if (entries_read is not None
                    and state.get("entries_read") != entries_read):
                state["entries_read"] = entries_read
                changed = True
            result = {"type": "client_response", "status": "ok",
                      "xgroup_setid": 1}
            if changed:
                self._data[key] = _stream_value(stream)
                result["changed"] = 1
            return result
        elif op == "xgroup_destroy":
            # Destroy a consumer group, Redis XGROUP DESTROY: the
            # group leaves the stream blob entirely, pending
            # entries and all — a consumer holding entries in a
            # destroyed group holds nothing afterwards, since the
            # group itself is gone. The reply carries
            # ``xgroup_destroy`` 1 when the group existed and was
            # destroyed, 0 when the stream exists but the group
            # does not — Redis's answer for a group that is not
            # there, not an error — while a missing key fails
            # with ``xgroup_destroy_failed`` / ``no_group``,
            # Redis's NOGROUP, and a key holding a plain string
            # fails with WRONGTYPE, like an xadd. The outcome is
            # decided here, at this exact point in the log, like
            # a CAS, so a retry with the same session id returns
            # the first attempt's result instead of destroying
            # the group twice — a retry after a successful
            # destroy answers 0, the group already gone. A
            # successful destroy keeps the key's TTL deadline,
            # like an xadd: the stream itself is still there, a
            # group lighter. The group was validated on the
            # leader before the entry was logged (a non-empty
            # string), so by apply time it is well-formed — a
            # hand-built log entry with a bad group still raises
            # rather than silently destroying nonsense.
            key = command["key"]
            group = command.get("group")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xgroup_destroy group must be a non-empty "
                    "string")
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xgroup_destroy_failed",
                        "reason": "no_group",
                        "error": "xgroup_destroy failed: no such key "
                                 f"{key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xgroup_destroy_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xgroup_destroy failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response", "status": "ok",
                        "xgroup_destroy": 0}
            del groups[group]
            self._data[key] = _stream_value(stream)
            return {"type": "client_response", "status": "ok",
                    "xgroup_destroy": 1}
        elif op == "xgroup_delconsumer":
            # Delete one consumer from a consumer group, Redis
            # XGROUP DELCONSUMER: the consumer leaves the group,
            # and every entry it held pending leaves with it —
            # the entries themselves stay in the stream, only
            # their pending record goes, so they are simply no
            # longer pending, like acked ones (a later ">" read
            # still will not re-hand them: the group's last
            # delivery already passed them). The consumer's
            # last-seen record goes too, so XINFO CONSUMERS stops
            # listing it — and the reply carries
            # ``consumer_dropped`` 1 when a record went, so a
            # watcher can tell a delete that only dropped a
            # sighting from one that changed nothing at all. The reply carries
            # ``xgroup_delconsumer`` — how many pending entries
            # the consumer held, 0 when the consumer held none
            # or did not exist at all, like Redis. A missing key
            # or a missing group fails with
            # ``xgroup_delconsumer_failed`` / ``no_group`` —
            # Redis's NOGROUP — and a key holding a plain string
            # fails with WRONGTYPE, like an xadd. The outcome is
            # decided here, at this exact point in the log, like
            # a CAS, so a retry with the same session id returns
            # the first attempt's count instead of deleting the
            # consumer twice — a retry after a successful delete
            # answers 0, the consumer already gone. A successful
            # delete keeps the key's TTL deadline, like an xadd.
            # The group and consumer were validated on the leader
            # before the entry was logged (non-empty strings), so
            # by apply time they are well-formed — a hand-built
            # log entry with a bad name still raises rather than
            # silently deleting nonsense.
            key = command["key"]
            group = command.get("group")
            consumer = command.get("consumer")
            if not isinstance(group, str) or not group:
                raise ValueError(
                    "xgroup_delconsumer group must be a non-empty "
                    "string")
            if not isinstance(consumer, str) or not consumer:
                raise ValueError(
                    "xgroup_delconsumer consumer must be a "
                    "non-empty string")
            current = self._data.get(key)
            if current is None:
                return {"type": "client_response",
                        "status": "xgroup_delconsumer_failed",
                        "reason": "no_group",
                        "error": "xgroup_delconsumer failed: no such "
                                 f"key {key!r} or consumer group "
                                 f"{group!r}"}
            if not is_stream(current):
                return {"type": "client_response",
                        "status": "xgroup_delconsumer_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "xgroup_delconsumer failed: value of "
                                 f"{key!r} is not a stream"}
            stream = _stream_data(current)
            groups = stream.get("groups", {})
            if group not in groups:
                return {"type": "client_response",
                        "status": "xgroup_delconsumer_failed",
                        "reason": "no_group",
                        "error": "xgroup_delconsumer failed: no such "
                                 f"key {key!r} or consumer group "
                                 f"{group!r}"}
            pending = groups[group]["pending"]
            doomed = [eid for eid, info in pending.items()
                      if info["consumer"] == consumer]
            for entry_id in doomed:
                del pending[entry_id]
            consumers = groups[group].get("consumers", {})
            consumer_dropped = consumer in consumers
            if consumer_dropped:
                del consumers[consumer]
                if not consumers:
                    del groups[group]["consumers"]
            if doomed or consumer_dropped:
                self._data[key] = _stream_value(stream)
            result = {"type": "client_response", "status": "ok",
                      "xgroup_delconsumer": len(doomed)}
            if consumer_dropped:
                result["consumer_dropped"] = 1
            return result
        elif op == "setrange":
            # Overwrite part of a string value, Redis SETRANGE:
            # starting at ``offset``, replace as many characters
            # of the current value as ``value`` holds — a missing
            # key counts as an empty string, and when the offset
            # lies past the end the gap is padded with zero bytes
            # ("\x00"), exactly as Redis pads with zero bytes.
            # The reply carries the new value and its length, the
            # length Redis answers with. The outcome is decided
            # here, at this exact point in the log, so a retry
            # with the same session id returns the first
            # attempt's value and length from the dedup table
            # instead of overwriting twice. A value that is not
            # a string fails with WRONGTYPE, like append: every
            # value this store holds is a string, so only a
            # hand-built state can do that, but the check keeps
            # the op honest about what it overwrote. A failed
            # setrange writes nothing at all, like a failed
            # incr. A successful setrange keeps the key's
            # deadline, like a CAS or an append: it is an update
            # of the value, not a fresh set, so it must not
            # resurrect a key that is about to expire. A new key
            # joins the sorted index, like a set of a new key.
            # The offset itself was validated on the leader
            # before the entry was logged (an integer from 0 to
            # SETRANGE_MAX_OFFSET), so by apply time it is
            # always in range — a hand-built log entry with a
            # bad offset still raises rather than silently
            # building a nonsense value.
            key = command["key"]
            offset = command["offset"]
            if (not isinstance(offset, int)
                    or isinstance(offset, bool) or offset < 0):
                raise ValueError(
                    "setrange offset must be a non-negative integer")
            current = self._data.get(key)
            if current is not None and not isinstance(current, str):
                return {"type": "client_response",
                        "status": "setrange_failed",
                        "reason": "wrongtype",
                        "current": current,
                        "error": "setrange failed: value of "
                                 f"{key!r} is not a string"}
            current = current or ""
            if offset > len(current):
                current += "\x00" * (offset - len(current))
            new = (current[:offset] + command["value"]
                   + current[offset + len(command["value"]):])
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = new
            return {"type": "client_response", "status": "ok",
                    "value": new, "length": len(new)}
        else:
            raise ValueError(f"unknown command op: {op!r}")

    def expire_up_to(self, index: int) -> list[str]:
        """Drop every key whose deadline is at or before ``index``
        and return the dropped keys, sorted. The Raft layer calls
        this before applying the entry at ``index`` — every entry
        type, including noop and config entries, since the log
        advancing is what moves time here — so a key is gone
        before any entry at or past its deadline applies, and a
        read served from state applied to ``index`` never sees it.
        The order is sorted so watch events fired for the expiry
        are deterministic across nodes. Pure function of (state,
        index): every node holding the same log computes the same
        expiries at the same positions."""
        expired = sorted(k for k, d in self._expiry.items() if d <= index)
        for key in expired:
            del self._expiry[key]
            if key in self._data:
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
        return expired

    def expiries(self) -> dict[str, int]:
        """The live deadlines, as ``{key: deadline index}`` — what
        a snapshot carries alongside the data, so a node that
        installs the snapshot keeps expiring keys on schedule
        instead of holding them forever."""
        return dict(self._expiry)

    def expire_wall_up_to(self, now: float) -> list[str]:
        """Drop every key whose wall-clock deadline is at or
        before ``now`` (Unix seconds, ``time.time()``) and return
        the dropped keys, sorted. The Raft layer calls this from
        three places: before applying each log entry (so a key
        whose wall deadline passed is gone before the next
        entry's effect lands, like the log-entry purge), from a
        background sweep thread (so a key also dies on a quiet
        cluster, with no log advance at all), and before serving
        a read (so a read never returns a key whose deadline
        passed between sweeps). The order is sorted so watch
        events fired for the expiry are deterministic across
        nodes. Pure function of (state, now) — except for the
        clock itself: two nodes may disagree briefly about
        whether a key is alive, which is the honest limit of a
        wall-clock TTL, and why it is opt-in."""
        expired = sorted(k for k, d in self._wall_expiry.items()
                         if d <= now)
        for key in expired:
            del self._wall_expiry[key]
            if key in self._data:
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
        return expired

    def wall_expiries(self) -> dict[str, float]:
        """The live wall-clock deadlines, as ``{key: deadline}``
        in Unix seconds — what a snapshot carries alongside the
        data and the log-entry deadlines, so a node that installs
        the snapshot keeps expiring wall-clock keys on schedule
        instead of holding them forever."""
        return dict(self._wall_expiry)

    def wall_ttl(self, key: str, now: float) -> tuple[int, float | None]:
        """How many more wall-clock seconds ``key`` has left, as
        of ``now`` — Redis's TTL, in seconds instead of log
        entries: -2 when the key does not exist, -1 when it
        exists with no wall-clock deadline, otherwise the whole
        seconds left until the deadline. The second value is the
        deadline itself — the absolute Unix time at which the key
        is purged — or None without one. A key that exists with
        a wall deadline always has at least zero seconds left:
        the purge runs before the deadline is served, so a live
        key can never sit past its own deadline. Pure read: no
        log entry, no state change."""
        if key not in self._data:
            return -2, None
        deadline = self._wall_expiry.get(key)
        if deadline is None:
            return -1, None
        return max(0, int(deadline - now)), deadline

    def ttl(self, key: str, index: int) -> tuple[int, int | None]:
        """How many more log entries ``key`` has left, as of the
        state applied up to ``index`` — Redis's TTL, in log
        entries instead of seconds: -2 when the key does not
        exist, -1 when it exists with no deadline, otherwise
        ``deadline - index``. The second value is the deadline
        itself — the absolute log index at which the key is
        purged — or None without one. A key that exists with a
        deadline always has at least one entry left: the purge
        runs before the entry at the deadline applies, so a live
        key can never sit at or past its own deadline. Pure
        read: no log entry, no state change."""
        if key not in self._data:
            return -2, None
        deadline = self._expiry.get(key)
        if deadline is None:
            return -1, None
        return deadline - index, deadline

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def getrange(self, key: str, start: int, end: int) -> str:
        """The substring of the string value at ``key`` from
        ``start`` to ``end`` inclusive, Redis GETRANGE. A missing
        key holds the empty string, so any range of it is "". A
        negative index counts back from the end of the string
        (-1 is the last character), and an index past either end
        is clamped to the string — so a range past the end, or
        with start past end, is "" rather than an error. Raises
        TypeError when the value is not a string: every value
        this store holds is a string, so only a hand-built state
        can do that, but the check keeps the read honest about
        what it sliced instead of answering a substring of
        something that has none. Pure read: no log entry, no
        state change."""
        value = self._data.get(key)
        if value is None:
            return ""
        if not isinstance(value, str):
            raise TypeError(
                f"getrange failed: value of {key!r} is not a string")
        n = len(value)
        if start < 0:
            start += n
        if end < 0:
            end += n
        if start < 0:
            start = 0
        if end < 0:
            end = 0
        if end >= n:
            end = n - 1
        if start > end or n == 0:
            return ""
        return value[start:end + 1]

    def pfcount(self, key: str) -> int:
        """The HyperLogLog cardinality estimate at ``key`` — 0 when
        the key does not exist, Redis PFCOUNT. Raises TypeError
        when the value is a plain string: a PFCOUNT counts an HLL,
        not bytes, so a string key is WRONGTYPE, like a getbit on
        a value that is not a byte string. Pure read: no log
        entry, no state change."""
        value = self._data.get(key)
        if value is None:
            return 0
        if not is_hll(value):
            raise TypeError(
                f"pfcount failed: value of {key!r} is not a "
                "hyperloglog")
        return hll_estimate(_hll_registers(value))

    def hget(self, key: str, field: str) -> str | None:
        """The value of ``field`` in the hash at ``key`` — None
        when the key does not exist or the field is not set,
        Redis HGET. Raises TypeError when the value is a plain
        string: an HGET reads a hash, not bytes, so a string key
        is WRONGTYPE, like a pfcount on a plain string. Pure
        read: no log entry, no state change."""
        value = self._data.get(key)
        if value is None:
            return None
        if not is_hash(value):
            raise TypeError(
                f"hget failed: value of {key!r} is not a hash")
        return _hash_fields(value).get(field)

    def hgetall(self, key: str) -> dict[str, str]:
        """Every field and value in the hash at ``key`` — an empty
        dict when the key does not exist, Redis HGETALL. Raises
        TypeError when the value is a plain string: an HGETALL
        reads a hash, not bytes, so a string key is WRONGTYPE,
        like an hget on a plain string. Pure read: no log entry,
        no state change. The dict is a fresh copy of the fields,
        so the caller cannot mutate the stored blob's parse."""
        value = self._data.get(key)
        if value is None:
            return {}
        if not is_hash(value):
            raise TypeError(
                f"hgetall failed: value of {key!r} is not a hash")
        return dict(_hash_fields(value))

    def hlen(self, key: str) -> int:
        """The number of fields in the hash at ``key`` — 0 when
        the key does not exist, Redis HLEN. Raises TypeError
        when the value is a plain string: an HLEN reads a hash,
        not bytes, so a string key is WRONGTYPE, like an hgetall
        on a plain string. Pure read: no log entry, no state
        change."""
        value = self._data.get(key)
        if value is None:
            return 0
        if not is_hash(value):
            raise TypeError(
                f"hlen failed: value of {key!r} is not a hash")
        return len(_hash_fields(value))

    def xlen(self, key: str) -> int:
        """The number of entries in the stream at ``key`` — 0 when
        the key does not exist, Redis XLEN. Raises TypeError
        when the value is a plain string: an XLEN reads a stream,
        not bytes, so a string key is WRONGTYPE, like an hlen on
        a plain string. Pure read: no log entry, no state
        change."""
        value = self._data.get(key)
        if value is None:
            return 0
        if not is_stream(value):
            raise TypeError(
                f"xlen failed: value of {key!r} is not a stream")
        return len(_stream_data(value)["entries"])

    def xrange(self, key: str, start: str, end: str,
               count: int | None = None) -> list[list]:
        """The entries in the stream at ``key`` whose IDs fall in
        [``start``, ``end``], in ID order — Redis XRANGE.
        ``start`` and ``end`` are "-", "+", a bare millisecond
        (that millisecond, sequence 0), or a full "ms-seq"; a
        start past the end answers an empty list rather than an
        error, like a getrange with start past end. ``count``
        caps how many entries one call returns, like Redis's
        COUNT — None means no cap, 0 answers an empty list. A
        missing key answers an empty list. Raises TypeError when
        the value is a plain string: an XRANGE reads a stream,
        not bytes, so a string key is WRONGTYPE, like an xlen on
        a plain string. Raises ValueError for a bad bound or
        count: a bound must be "-", "+", "ms", or "ms-seq" with
        each half at most 2**64 - 1, and a count must be a
        non-negative integer (a bool is not one). The bounds are
        parsed before the key is looked up, so a bad bound is an
        error even on a missing key, like Redis parsing its
        arguments first. Pure read: no log entry, no state
        change. The entries come back as [id, {field: value}]
        pairs, in ID order — the same shape the stream blob
        stores them in — and each field map is a fresh copy, so
        the caller cannot mutate the stored blob's parse."""
        if count is not None and (
                not isinstance(count, int) or isinstance(count, bool)
                or count < 0):
            raise ValueError(
                "xrange count must be a non-negative integer")
        start_id = parse_stream_range_bound(start)
        end_id = parse_stream_range_bound(end)
        value = self._data.get(key)
        if value is None:
            return []
        if not is_stream(value):
            raise TypeError(
                f"xrange failed: value of {key!r} is not a stream")
        entries: list[list] = []
        for entry_id, fields in _stream_data(value)["entries"]:
            entry = parse_stream_id(entry_id)
            if start_id <= entry <= end_id:
                if count is not None and len(entries) >= count:
                    break
                entries.append([entry_id, dict(fields)])
        return entries

    def stream_top(self, key: str) -> str | None:
        """The top ID of the stream at ``key`` — None when the key
        does not exist. Raises TypeError when the value is a plain
        string: a top ID is a stream notion, not a string one,
        like an xlen on a plain string. Pure read: no log entry,
        no state change."""
        value = self._data.get(key)
        if value is None:
            return None
        if not is_stream(value):
            raise TypeError(
                f"stream top failed: value of {key!r} is not a stream")
        return _stream_data(value)["last"]

    def xread_resolve(self, keys: list[str],
                      ids: list[str]) -> list[str]:
        """The concrete ID each xread id names: "$" becomes the
        stream's top ID at this moment — "0-0" when the key does
        not exist, so every future entry counts — and any other
        id passes through unchanged. Raises ValueError when the
        keys are not a non-empty list of non-empty strings, when
        the ids are not one string per key, or when an id is "-",
        "+" (a range bound, not a stream ID), or any other shape
        a range bound cannot parse; raises TypeError when a "$"
        key holds a plain string. Resolving once and reusing the
        list is what makes a blocking XREAD with "$" measure from
        the top the call started with, instead of moving the
        goalpost past the entries that arrived while it waited.
        Pure read: no log entry, no state change."""
        if (not isinstance(keys, list) or not keys
                or not all(isinstance(k, str) and k for k in keys)):
            raise ValueError(
                "xread keys must be a non-empty list of "
                "non-empty strings")
        if (not isinstance(ids, list) or len(ids) != len(keys)
                or not all(isinstance(i, str) for i in ids)):
            raise ValueError(
                "xread ids must be a list of one id per key")
        resolved: list[str] = []
        for key, id_str in zip(keys, ids):
            if id_str == "$":
                value = self._data.get(key)
                if value is None:
                    resolved.append("0-0")
                elif not is_stream(value):
                    raise TypeError(
                        f"xread failed: value of {key!r} is not "
                        "a stream")
                else:
                    last = _stream_data(value)["last"]
                    resolved.append(last if last is not None
                                    else "0-0")
            elif id_str in ("-", "+"):
                raise ValueError(
                    "xread id must be \"$\", \"ms\", or \"ms-seq\"")
            else:
                # A bare millisecond or a full "ms-seq", like an
                # xrange bound — validated here so a bad id is an
                # error before any key is looked up.
                parse_stream_range_bound(id_str)
                resolved.append(id_str)
        return resolved

    def xread(self, keys: list[str], ids: list[str],
              count: int | None = None) -> list[list]:
        """The entries appended to the streams at ``keys`` after
        the matching ID in ``ids``, in ID order — Redis XREAD.
        Each id is "$" (only entries after the stream's top ID
        at this moment), a bare millisecond (that millisecond,
        sequence 0), or a full "ms-seq". The reply is [[key,
        [[id, {field: value}], ...]], ...] — only the streams
        that hold at least one entry after their id ride along,
        in the order the keys were given, so a stream with
        nothing new is simply absent — and ``count`` caps how
        many entries one stream returns, like Redis's COUNT —
        None means no cap, 0 answers nothing for that stream.
        A missing key holds no entries, so it rides along never.
        Raises TypeError when a key holds a plain string: an
        XREAD reads a stream, not bytes, so a string key is
        WRONGTYPE, like an xlen on a plain string. Raises
        ValueError for bad keys, ids, or count: a count must be
        a non-negative integer (a bool is not one). Pure read:
        no log entry, no state change. The entries come back as
        [id, {field: value}] pairs, in ID order — the same shape
        the stream blob stores them in — and each field map is a
        fresh copy, so the caller cannot mutate the stored blob's
        parse."""
        if count is not None and (
                not isinstance(count, int) or isinstance(count, bool)
                or count < 0):
            raise ValueError(
                "xread count must be a non-negative integer")
        resolved = self.xread_resolve(keys, ids)
        results: list[list] = []
        for key, id_str in zip(keys, resolved):
            after = parse_stream_range_bound(id_str)
            value = self._data.get(key)
            if value is None:
                continue
            if not is_stream(value):
                raise TypeError(
                    f"xread failed: value of {key!r} is not a stream")
            entries: list[list] = []
            for entry_id, fields in _stream_data(value)["entries"]:
                entry = parse_stream_id(entry_id)
                if entry > after:
                    if count is not None and len(entries) >= count:
                        break
                    entries.append([entry_id, dict(fields)])
            if entries:
                results.append([key, entries])
        return results

    def xpending(self, key: str, group: str) -> dict:
        """The pending-entries summary for ``group`` on the stream
        at ``key`` — Redis XPENDING, summary form: ``pending``
        the number of entries handed out and not yet acked,
        ``min_id`` / ``max_id`` the smallest / largest pending
        ID (None when nothing is pending), and ``consumers`` the
        [[consumer, count], ...] pairs sorted by consumer name.
        Raises TypeError when the value is a plain string: an
        XPENDING reads a stream, not bytes, so a string key is
        WRONGTYPE, like an xlen on a plain string. Raises
        ValueError when the key does not exist or the group does
        not exist on it: Redis's NOGROUP, a missing group is an
        error, not an empty summary — a typo'd group name must
        not read as "nothing pending". Pure read: no log entry,
        no state change."""
        value = self._data.get(key)
        if value is None:
            raise ValueError(
                f"xpending failed: no such key {key!r} or consumer "
                f"group {group!r}")
        if not is_stream(value):
            raise TypeError(
                f"xpending failed: value of {key!r} is not a stream")
        groups = _stream_data(value).get("groups", {})
        if group not in groups:
            raise ValueError(
                f"xpending failed: no such key {key!r} or consumer "
                f"group {group!r}")
        pending = groups[group]["pending"]
        counts: dict[str, int] = {}
        for info in pending.values():
            consumer = info["consumer"]
            counts[consumer] = counts.get(consumer, 0) + 1
        ids = sorted(pending, key=parse_stream_id)
        return {"pending": len(pending),
                "min_id": ids[0] if ids else None,
                "max_id": ids[-1] if ids else None,
                "consumers": [[c, counts[c]] for c in sorted(counts)]}

    def xpending_entries(self, key: str, group: str, now_ms: int,
                         start: str = "-", end: str = "+",
                         count: int | None = None,
                         consumer: str | None = None,
                         min_idle_ms: int | None = None) -> list[dict]:
        """The individual pending entries of ``group`` on the stream
        at ``key`` — Redis XPENDING, per-entry form: one dict per
        pending entry, in ID order, carrying ``id``, ``consumer``
        — who holds it — ``idle`` — the milliseconds since it was
        last delivered, None when it was never delivered through a
        delivery this version stamped (a replay of a pre-v0.71.0
        log entry carries no stamp) — and ``deliveries`` — how
        many times it has been handed out. ``start`` / ``end``
        bound the IDs, "-" the start of the stream and "+" the
        end, a bare millisecond naming that millisecond with
        sequence 0, like XRANGE; ``count`` caps the list, None
        for no cap; ``consumer`` keeps only the entries one
        consumer holds; ``min_idle_ms`` keeps only the entries
        idle at least that long — an entry with an unknown idle
        cannot prove it has been idle that long, so it is skipped
        when a minimum is given. ``now_ms`` is the wall-clock
        millisecond the idle is measured against, so the store
        itself never reads a clock: the caller passes its own
        now, and the idle is clamped at 0, so a stamp from a
        leader whose clock runs ahead never reads as a negative
        idle. Raises TypeError when the value is a plain string:
        an XPENDING reads a stream, not bytes, so a string key is
        WRONGTYPE, like an xlen on a plain string. Raises
        ValueError when the key does not exist or the group does
        not exist on it: Redis's NOGROUP, a missing group is an
        error, not an empty list — a typo'd group name must not
        read as "nothing pending" — and for a bad bound, count,
        consumer, or minimum idle. Pure read: no log entry, no
        state change."""
        if (not isinstance(start, str)
                or not isinstance(end, str)):
            raise ValueError(
                "xpending start and end must be stream ID bounds")
        start_bound = parse_stream_range_bound(start)
        end_bound = parse_stream_range_bound(end)
        if count is not None and (
                not isinstance(count, int) or isinstance(count, bool)
                or count < 0):
            raise ValueError(
                "xpending count must be a non-negative integer")
        if consumer is not None and (
                not isinstance(consumer, str) or not consumer):
            raise ValueError(
                "xpending consumer must be a non-empty string")
        if min_idle_ms is not None and (
                not isinstance(min_idle_ms, int)
                or isinstance(min_idle_ms, bool) or min_idle_ms < 0):
            raise ValueError(
                "xpending min_idle_ms must be a non-negative "
                "integer")
        value = self._data.get(key)
        if value is None:
            raise ValueError(
                f"xpending failed: no such key {key!r} or consumer "
                f"group {group!r}")
        if not is_stream(value):
            raise TypeError(
                f"xpending failed: value of {key!r} is not a stream")
        groups = _stream_data(value).get("groups", {})
        if group not in groups:
            raise ValueError(
                f"xpending failed: no such key {key!r} or consumer "
                f"group {group!r}")
        pending = groups[group]["pending"]
        entries: list[dict] = []
        for entry_id in sorted(pending, key=parse_stream_id):
            parsed = parse_stream_id(entry_id)
            if parsed < start_bound or parsed > end_bound:
                continue
            info = pending[entry_id]
            if consumer is not None and info["consumer"] != consumer:
                continue
            delivered_ms = info.get("delivered_ms")
            idle = (None if delivered_ms is None
                    else max(0, now_ms - delivered_ms))
            if min_idle_ms is not None and (
                    idle is None or idle < min_idle_ms):
                continue
            if count is not None and len(entries) >= count:
                break
            entries.append({"id": entry_id,
                            "consumer": info["consumer"],
                            "idle": idle,
                            "deliveries": info["deliveries"]})
        return entries

    def xinfo_stream(self, key: str) -> dict:
        """The stream at ``key`` itself — Redis XINFO STREAM:
        ``length`` the number of entries, ``last_generated_id``
        the top ID ("0-0" when the stream holds no entries yet,
        as a group created with MKSTREAM on an empty stream
        does), ``groups`` how many consumer groups the stream
        has, and ``first_entry`` / ``last_entry`` the [id,
        {field: value}] pairs at the ends of the stream, None
        when it holds no entries. Raises TypeError when the value
        is a plain string: an XINFO reads a stream, not bytes, so
        a string key is WRONGTYPE, like an xlen on a plain
        string. Raises ValueError when the key does not exist:
        Redis's "no such key", a missing stream is an error, not
        an empty summary — a typo'd key must not read as an empty
        stream. Pure read: no log entry, no state change. The
        entries come back as fresh copies, so the caller cannot
        mutate the stored blob's parse."""
        value = self._data.get(key)
        if value is None:
            raise ValueError(
                f"xinfo failed: no such key {key!r}")
        if not is_stream(value):
            raise TypeError(
                f"xinfo failed: value of {key!r} is not a stream")
        stream = _stream_data(value)
        entries = stream["entries"]
        return {"length": len(entries),
                "last_generated_id": (stream["last"]
                                      if stream["last"] is not None
                                      else "0-0"),
                "groups": len(stream.get("groups", {})),
                "first_entry": ([entries[0][0], dict(entries[0][1])]
                                if entries else None),
                "last_entry": ([entries[-1][0], dict(entries[-1][1])]
                               if entries else None)}

    def xinfo_groups(self, key: str) -> list[dict]:
        """The consumer groups on the stream at ``key`` — Redis
        XINFO GROUPS: one dict per group, sorted by group name,
        carrying ``name``, ``last_delivered_id`` — the last entry
        the group was handed, None when it was created on an empty
        stream with "$" and nothing has been handed out yet —
        ``entries_read`` — the count an XGROUP SETID ...
        ENTRIESREAD set, None when no SETID ever set it —
        ``lag`` — the entries appended after the group's last
        delivery that it has not read yet, counted from the
        entries still in the stream — and ``pending`` — the
        entries handed out and not yet acked. Raises TypeError
        when the value is a plain string: an XINFO reads a stream,
        not bytes, so a string key is WRONGTYPE, like an xlen on
        a plain string. Raises ValueError when the key does not
        exist: a missing stream is an error, not an empty list.
        Pure read: no log entry, no state change."""
        value = self._data.get(key)
        if value is None:
            raise ValueError(
                f"xinfo failed: no such key {key!r}")
        if not is_stream(value):
            raise TypeError(
                f"xinfo failed: value of {key!r} is not a stream")
        stream = _stream_data(value)
        entries = stream["entries"]
        groups: list[dict] = []
        for name in sorted(stream.get("groups", {})):
            state = stream["groups"][name]
            last = state["last_delivered"]
            after = ((0, 0) if last is None
                     else _parse_stored_stream_id(last))
            lag = sum(1 for eid, _fields in entries
                      if parse_stream_id(eid) > after)
            groups.append({"name": name,
                           "last_delivered_id": last,
                           "entries_read": state.get("entries_read"),
                           "lag": lag,
                           "pending": len(state["pending"])})
        return groups

    def xinfo_consumers(self, key: str, group: str,
                        now_ms: int) -> list[dict]:
        """The consumers of ``group`` on the stream at ``key`` —
        Redis XINFO CONSUMERS: one dict per consumer, sorted by
        consumer name, carrying ``name``, ``pending`` — the
        entries handed out to that consumer and not yet acked —
        ``idle`` — the milliseconds since that consumer was
        last handed an entry, None when the consumer was never
        handed one through a delivery this version stamped (a
        group created before v0.70.0, or a consumer whose entries
        all came from replays of pre-v0.70.0 log entries) — and
        ``lag`` — the entries in the stream past the newest entry
        ever handed to that consumer, the per-consumer analogue
        of the group's lag in XINFO GROUPS, None when the
        consumer was never handed an entry through a delivery
        this version stamped with a last-delivered ID (a
        consumer whose entries all came from replays of
        pre-v0.74.0 log entries). A consumer only appears once
        it has been handed at least one entry: a readgroup that
        hands out nothing writes nothing at all, so a consumer
        that only ever polled an empty group is not listed — and
        a consumer whose pending entries were all acked is still
        listed, with pending 0, until XGROUP DELCONSUMER drops
        it. ``now_ms`` is the wall-clock millisecond the idle is
        measured against, so the store itself never reads a
        clock: the caller passes its own now, and the idle is
        clamped at 0, so a stamp from a leader whose clock runs
        ahead never reads as a negative idle. Raises TypeError
        when the value is a plain string: an XINFO reads a
        stream, not bytes, so a string key is WRONGTYPE, like an
        xlen on a plain string. Raises ValueError when the key
        does not exist or the group does not exist on it:
        Redis's NOGROUP, a missing group is an error, not an
        empty list. Pure read: no log entry, no state change.
        The lag is counted from the entries still in the stream,
        like the group lag, so a trim makes it undercount the
        entries a consumer never read — and entries handed to
        other consumers count as lag for this one, since it has
        not read them: the lag is how far behind the stream head
        the consumer sits, not how many entries it still holds
        pending."""
        value = self._data.get(key)
        if value is None:
            raise ValueError(
                f"xinfo failed: no such key {key!r} or consumer "
                f"group {group!r}")
        if not is_stream(value):
            raise TypeError(
                f"xinfo failed: value of {key!r} is not a stream")
        stream = _stream_data(value)
        entries = stream["entries"]
        groups = stream.get("groups", {})
        if group not in groups:
            raise ValueError(
                f"xinfo failed: no such key {key!r} or consumer "
                f"group {group!r}")
        state = groups[group]
        counts: dict[str, int] = {}
        for info in state["pending"].values():
            consumer = info["consumer"]
            counts[consumer] = counts.get(consumer, 0) + 1
        seen = state.get("consumers", {})
        consumers: list[dict] = []
        for name in sorted(set(counts) | set(seen)):
            record = seen.get(name)
            idle = (None if record is None
                    else max(0, now_ms - record["last_seen_ms"]))
            last = (None if record is None
                    else record.get("last_delivered_id"))
            if last is None:
                lag = None
            else:
                after = _parse_stored_stream_id(last)
                lag = sum(1 for eid, _fields in entries
                          if parse_stream_id(eid) > after)
            consumers.append({"name": name,
                              "pending": counts.get(name, 0),
                              "idle": idle,
                              "lag": lag})
        return consumers

    def xreadgroup_peek(self, key: str, group: str,
                        count: int | None = None) -> list | None:
        """The entries a ``">"`` xreadgroup would hand out right
        now — the entries appended after the group's last delivery,
        in ID order, at most ``count`` of them when a count is
        given — as [id, {field: value}] pairs. None when the key
        does not exist, holds a plain string, or has no such group:
        nothing to wait for, since the write itself answers the
        error at apply time. Raises ValueError for a bad count.
        Pure read: no log entry, no state change — the leader's
        blocking wait uses it to decide whether there is anything
        to hand out yet, and the delivery itself is still decided
        at apply time, so the peek never hands anything out."""
        if count is not None and (
                not isinstance(count, int) or isinstance(count, bool)
                or count < 0):
            raise ValueError(
                "xreadgroup count must be a non-negative integer")
        value = self._data.get(key)
        if value is None or not is_stream(value):
            return None
        stream = _stream_data(value)
        groups = stream.get("groups", {})
        if group not in groups:
            return None
        state = groups[group]
        last = state["last_delivered"]
        after = ((0, 0) if last is None
                 else _parse_stored_stream_id(last))
        chosen = [[eid, dict(fields)]
                  for eid, fields in stream["entries"]
                  if parse_stream_id(eid) > after]
        if count is not None:
            chosen = chosen[:count]
        return chosen

    def hexists(self, key: str, field: str) -> int:
        """1 when ``field`` is set in the hash at ``key``, 0
        otherwise — 0 when the key does not exist or the field
        is not set, Redis HEXISTS. Raises TypeError when the
        value is a plain string: an HEXISTS reads a hash, not
        bytes, so a string key is WRONGTYPE, like an hget on a
        plain string. Pure read: no log entry, no state
        change."""
        value = self._data.get(key)
        if value is None:
            return 0
        if not is_hash(value):
            raise TypeError(
                f"hexists failed: value of {key!r} is not a hash")
        return 1 if field in _hash_fields(value) else 0

    def hkeys(self, key: str) -> list[str]:
        """Every field name in the hash at ``key``, sorted — an
        empty list when the key does not exist, Redis HKEYS.
        Raises TypeError when the value is a plain string: an
        HKEYS reads a hash, not bytes, so a string key is
        WRONGTYPE, like an hgetall on a plain string. Pure
        read: no log entry, no state change. The names come
        back sorted — the blob is dumped with sorted keys, so
        the order is a pure function of the fields, and every
        node applying the same log answers the same order."""
        value = self._data.get(key)
        if value is None:
            return []
        if not is_hash(value):
            raise TypeError(
                f"hkeys failed: value of {key!r} is not a hash")
        return sorted(_hash_fields(value))

    def hvals(self, key: str) -> list[str]:
        """Every field value in the hash at ``key``, in sorted
        field order — an empty list when the key does not exist,
        Redis HVALS. Raises TypeError when the value is a plain
        string: an HVALS reads a hash, not bytes, so a string
        key is WRONGTYPE, like an hkeys on a plain string. Pure
        read: no log entry, no state change. The values are
        aligned with ``hkeys`` — the value at position i is the
        value of the field at position i in the sorted field
        list — so the two calls together rebuild the map."""
        value = self._data.get(key)
        if value is None:
            return []
        if not is_hash(value):
            raise TypeError(
                f"hvals failed: value of {key!r} is not a hash")
        fields = _hash_fields(value)
        return [fields[f] for f in sorted(fields)]

    def hmget(self, key: str, fields: list[str]) -> list[str | None]:
        """The values of ``fields`` in the hash at ``key``,
        aligned with the requested order — None in a position
        whose field is not set, and None in every position when
        the key does not exist, Redis HMGET. Raises TypeError
        when the value is a plain string: an HMGET reads a
        hash, not bytes, so a string key is WRONGTYPE, like an
        hget on a plain string. Pure read: no log entry, no
        state change. A field repeated in one call is answered
        once per occurrence, at its value each time — the reply
        never shifts after a miss or a repeat, like an mget."""
        value = self._data.get(key)
        if value is None:
            return [None] * len(fields)
        if not is_hash(value):
            raise TypeError(
                f"hmget failed: value of {key!r} is not a hash")
        stored = _hash_fields(value)
        return [stored.get(f) for f in fields]

    def hscan(self, key: str, cursor: int = 0,
              match: str | None = None, count: int = 10
              ) -> tuple[int, list[list[str]]]:
        """One page of the fields in the hash at ``key`` — the
        next cursor and the [field, value] pairs on this page,
        Redis HSCAN. The cursor is an offset into the sorted
        field list: start at 0, pass each reply's cursor back,
        and stop when a reply answers cursor 0 — the iteration
        is done. ``count`` is how many fields one call examines
        (a hint, like Redis's COUNT — a page examines at most
        that many fields, matching or not, so a selective MATCH
        can answer a page with no pairs at all and a cursor
        still to go); ``match`` is a glob pattern — ``*``,
        ``?``, ``[abc]`` — applied to field names with
        fnmatch, and None matches everything. A missing key
        answers (0, []), and a cursor past the end finishes
        the scan with (0, []). Raises TypeError when the value
        is a plain string: an HSCAN reads a hash, not bytes, so
        a string key is WRONGTYPE, like an hkeys on a plain
        string. Raises ValueError for a bad cursor or count: a
        cursor must be a non-negative integer (a bool is not
        one) and a count an integer from 1 to 1000 — the same
        cap as an hmget batch, for the same reason, so one
        reply never carries an unbounded page. Pure read: no
        log entry, no state change. The pairs come back in
        sorted field order, so the pages together rebuild the
        map in order — and each page is a read off the applied
        state machine exactly like an hget, but the scan as a
        whole is not a snapshot: a field written or deleted
        between pages can be missed, or appear in a later page
        if it sorts ahead of the cursor, like the keys scan."""
        if (not isinstance(cursor, int) or isinstance(cursor, bool)
                or cursor < 0):
            raise ValueError(
                "hscan cursor must be a non-negative integer")
        if (not isinstance(count, int) or isinstance(count, bool)
                or count < 1 or count > 1000):
            raise ValueError(
                "hscan count must be an integer between 1 and 1000")
        if match is not None and not isinstance(match, str):
            raise ValueError("hscan match must be a string")
        value = self._data.get(key)
        if value is None:
            return 0, []
        if not is_hash(value):
            raise TypeError(
                f"hscan failed: value of {key!r} is not a hash")
        fields = _hash_fields(value)
        names = sorted(fields)
        if cursor >= len(names):
            return 0, []
        pairs: list[list[str]] = []
        i = cursor
        examined = 0
        while i < len(names) and examined < count:
            name = names[i]
            examined += 1
            i += 1
            if match is None or fnmatch.fnmatchcase(name, match):
                pairs.append([name, fields[name]])
        return (0 if i >= len(names) else i), pairs

    def getbit(self, key: str, offset: int) -> int:
        """The bit at ``offset`` in the string value at ``key``,
        Redis GETBIT: bit 0 is the most significant bit of the
        first byte, bit 7 the least significant, bit 8 the most
        significant bit of the second byte, and so on. A missing
        key holds no bits, so any offset of it is 0, and an
        offset past the end of the value is 0 too. Raises
        TypeError when the value is not a byte string: every
        value this store holds is a string, and a bit op reads
        it as bytes — one byte per character, latin-1 — so a
        value holding a character above U+00FF has no byte form
        to read a bit from. Pure read: no log entry, no state
        change."""
        value = self._data.get(key)
        if value is None:
            return 0
        if not isinstance(value, str):
            raise TypeError(
                f"getbit failed: value of {key!r} is not a byte string")
        try:
            raw = value.encode("latin-1")
        except UnicodeEncodeError:
            raise TypeError(
                f"getbit failed: value of {key!r} is not a byte string")
        byte_index = offset // 8
        if byte_index >= len(raw):
            return 0
        return (raw[byte_index] >> (7 - offset % 8)) & 1

    def bitcount(self, key: str, start: int | None = None,
                 end: int | None = None,
                 mode: str = "BYTE") -> int:
        """The number of bits set to 1 in the string value at
        ``key``, Redis BITCOUNT: a missing key holds no bits, so
        it counts 0, and an empty value counts 0 too. With no
        range the whole value is counted; with ``start`` and
        ``end`` only that slice is — in BYTE mode the indexes
        name bytes, in BIT mode they name bits, bit 0 the most
        significant bit of the first byte, exactly like getbit's
        numbering. A negative index counts back from the end of
        the value (or of its bits), and an index past either end
        is clamped, like a getrange — so a range with start past
        end counts 0 rather than raising. Raises TypeError when
        the value is not a byte string, like getbit, and
        ValueError for a bad ``mode`` or a non-integer bound:
        a bool is not an integer, and only BYTE and BIT are
        modes. Pure read: no log entry, no state change."""
        if mode not in ("BYTE", "BIT"):
            raise ValueError(
                "bitcount mode must be BYTE or BIT")
        for name, bound in (("start", start), ("end", end)):
            if (bound is not None
                    and (not isinstance(bound, int)
                         or isinstance(bound, bool))):
                raise ValueError(
                    f"bitcount {name} must be an integer")
        value = self._data.get(key)
        if value is None:
            return 0
        if not isinstance(value, str):
            raise TypeError(
                f"bitcount failed: value of {key!r} is not a byte "
                "string")
        try:
            raw = value.encode("latin-1")
        except UnicodeEncodeError:
            raise TypeError(
                f"bitcount failed: value of {key!r} is not a byte "
                "string")
        # Redis's getRange normalization, in bytes or in bits:
        # a negative bound counts back from the end, a bound
        # still past either end after that is clamped to the
        # edge, and a start past the end counts 0.
        total = len(raw) if mode == "BYTE" else len(raw) * 8
        if start is None:
            start = 0
        if end is None:
            end = total - 1
        if start < 0:
            start += total
        if end < 0:
            end += total
        if start < 0:
            start = 0
        if end < 0:
            end = 0
        if end >= total:
            end = total - 1
        if start > end or total == 0:
            return 0
        if mode == "BYTE":
            return sum(b.bit_count() for b in raw[start:end + 1])
        # BIT mode: count the bits from ``start`` to ``end``
        # inclusive — bit 0 the most significant bit of the
        # first byte — so a range inside one byte masks just
        # those bits, and a range across bytes counts the two
        # partial edges plus every whole byte between them.
        count = 0
        first = start // 8
        last = end // 8
        for i in range(first, last + 1):
            low = start % 8 if i == first else 0
            high = end % 8 if i == last else 7
            mask = ((0xFF >> low) & (0xFF << (7 - high))) & 0xFF
            count += (raw[i] & mask).bit_count()
        return count

    def geodist(self, key: str, member1: str, member2: str,
                unit: str = "m") -> float | None:
        """The distance between two members of the geo set at
        ``key``, Redis GEODIST: None when the key does not exist
        or either member is not in the set, like Redis answering
        nil — and the distance itself in ``unit`` (``m`` by
        default, or ``km``, ``mi``, ``ft``), the haversine over
        the stored coordinates divided by the unit's metres, so
        every unit agrees on the same underlying distance. Raises
        TypeError when the value is not a geo set, like pfcount
        on a plain string, and ValueError for a bad ``unit``: a
        string that is not one of the four, matched
        case-insensitively before it is served, so every node
        measures in the same unit. Pure read: no log entry, no
        state change."""
        if not isinstance(unit, str) or unit.lower() not in GEO_UNIT_METRES:
            raise ValueError(
                "geodist unit must be m, km, mi, or ft")
        value = self._data.get(key)
        if value is None:
            return None
        if not is_geo(value):
            raise TypeError(
                f"geodist failed: value of {key!r} is not a geo set")
        members = _geo_members(value)
        if member1 not in members or member2 not in members:
            return None
        lon1, lat1 = members[member1]
        lon2, lat2 = members[member2]
        metres = geo_distance(lon1, lat1, lon2, lat2)
        return metres / GEO_UNIT_METRES[unit.lower()]

    def strlen(self, key: str) -> int:
        """Length of the string value at ``key`` — 0 when the key
        does not exist, Redis STRLEN. Raises TypeError when the
        value is not a string: every value this store holds is a
        string, so only a hand-built state can do that, but the
        check keeps the read honest about what it measured
        instead of answering a length for something that has
        none. Pure read: no log entry, no state change."""
        value = self._data.get(key)
        if value is None:
            return 0
        if not isinstance(value, str):
            raise TypeError(
                f"strlen failed: value of {key!r} is not a string")
        return len(value)

    def keys(self, prefix: str = "", start_after: str | None = None,
             limit: int = 1000) -> tuple[list[str], bool]:
        """One page of the store's keys, sorted, optionally
        restricted to keys starting with ``prefix`` and to keys
        strictly after ``start_after``. Returns ``(page,
        truncated)``: at most ``limit`` keys, and True when more
        matched. Sorted so a scan's answer is deterministic — the
        same state always enumerates in the same order, which is
        what makes a paged scan (limit + resume from the last key
        returned) well-defined: ``start_after`` is an exclusive
        lower bound on the sorted key order, so resuming with the
        last key of one page starts the next page exactly after
        it, with no repeats and no gaps, as long as the state
        does not change between pages. Pure read: no log entry,
        no state change.

        The page is read off the sorted index, not sorted fresh:
        bisect seeks to the later of the prefix start and the
        cursor (both O(log n)), then the walk takes at most
        ``limit`` + 1 keys — one extra to learn whether more
        matched — stopping at the first key outside the prefix,
        since keys under one prefix are contiguous in sorted
        order. A page therefore costs O(page + log n), not a
        sort of the whole matching keyspace. Honest cost: the
        index is a plain Python list, so the bisect is O(log n)
        but inserting or removing a key shifts the tail of the
        list — O(n) memmove on a write of a new key or a delete
        of an existing one — and the index holds one pointer per
        key on top of the dict."""
        start = 0
        if prefix:
            start = bisect.bisect_left(self._keys, prefix)
        if start_after is not None:
            start = max(start, bisect.bisect_right(self._keys,
                                                    start_after))
        page: list[str] = []
        i = start
        while i < len(self._keys) and len(page) <= limit:
            key = self._keys[i]
            if prefix and not key.startswith(prefix):
                break
            page.append(key)
            i += 1
        truncated = len(page) > limit
        return page[:limit], truncated

    def load(self, state: dict[str, str],
             expiry: dict[str, int] | None = None,
             wall_expiry: dict[str, float] | None = None) -> None:
        """Replace the whole state with ``state`` (used when installing
        a snapshot: the snapshot *is* the state as of its last included
        index, so anything the store held before is discarded). The
        sorted index is rebuilt from the snapshot state, so a node
        that installs a snapshot scans exactly like one that
        replayed the log to the same state. ``expiry`` restores the
        deadlines the snapshot carries (``None`` for snapshots
        written before TTL existed: those hold no deadlines, so
        nothing expires — exactly as the old nodes behaved).
        ``wall_expiry`` restores the wall-clock deadlines the same
        way (``None`` for snapshots written before wall-clock TTL
        existed). Both maps are filtered to keys the snapshot
        holds: a deadline for a key that is not in the state is
        stale, not a key to resurrect."""
        self._data = dict(state)
        self._keys = sorted(state)
        self._expiry = {k: int(v) for k, v in (expiry or {}).items()
                        if k in self._data}
        self._wall_expiry = {k: float(v)
                             for k, v in (wall_expiry or {}).items()
                             if k in self._data}

    def items(self) -> dict[str, str]:
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)
