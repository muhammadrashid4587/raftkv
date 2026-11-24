"""The key-value state machine that sits on top of Raft.

Raft guarantees every node applies the same committed log entries in the
same order, so this state machine is intentionally trivial and
deterministic: applying the same command log always produces the same
state. All it needs to be is correct and idempotent under replay — a node
re-applies its committed prefix after a restart, so ``apply`` must be a
pure function of (current state, command).
"""

import bisect
import hashlib
import math

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


def _is_integer_string(s: str) -> bool:
    """Whether ``s`` is a Redis-style integer: an optional sign
    followed by digits only. ``int()`` alone is too lenient — it
    accepts whitespace and underscores ("1_0", " 1"), which Redis
    rejects — so the shape is checked first and ``int()`` only
    parses what already looks like an integer."""
    if s.startswith(("+", "-")):
        s = s[1:]
    return bool(s) and s.isdigit()


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
        (unit tests, mostly) pass no TTL. A plain ``set`` without
        a TTL clears any deadline the key had — like Redis, a
        fresh SET drops the old TTL — and so do an ``mset``
        without a TTL and ``del``. An ``mset`` carrying
        ``ttl_entries`` sets the same deadline (its entry's
        index + the TTL) on every pair it touches, so the whole
        batch expires together. A successful CAS keeps the key's
        deadline, and so does a successful incr and a getset:
        all three are updates of the value, not fresh sets, so
        none must resurrect a key that is about to expire.

        Returns a result dict for commands whose outcome depends on the
        state at apply time — currently "cas", "expire", "incr",
        "setnx", "getset", "append", "getdel", "setrange", "getex",
        "setbit", and "pfadd" —
        and None for set/del, whose outcome is always ok. The result is
        what the leader hands back to the waiting client and what the
        dedup table caches for retries, so it must be a pure function
        of (state before, command): every node applying the same log
        computes the same result.
        """
        op = command.get("op")
        if op == "set":
            key = command["key"]
            if key not in self._data:
                bisect.insort(self._keys, key)
            self._data[key] = command["value"]
            ttl = command.get("ttl_entries")
            if ttl is not None:
                if index is None:
                    raise ValueError(
                        "ttl_entries needs the log index of the "
                        "entry being applied")
                self._expiry[key] = index + ttl
            else:
                self._expiry.pop(key, None)
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
            return None
        elif op == "del":
            key = command["key"]
            if key in self._data:
                del self._data[key]
                idx = bisect.bisect_left(self._keys, key)
                if idx < len(self._keys) and self._keys[idx] == key:
                    del self._keys[idx]
            self._expiry.pop(key, None)
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
             expiry: dict[str, int] | None = None) -> None:
        """Replace the whole state with ``state`` (used when installing
        a snapshot: the snapshot *is* the state as of its last included
        index, so anything the store held before is discarded). The
        sorted index is rebuilt from the snapshot state, so a node
        that installs a snapshot scans exactly like one that
        replayed the log to the same state. ``expiry`` restores the
        deadlines the snapshot carries (``None`` for snapshots
        written before TTL existed: those hold no deadlines, so
        nothing expires — exactly as the old nodes behaved)."""
        self._data = dict(state)
        self._keys = sorted(state)
        self._expiry = {k: int(v) for k, v in (expiry or {}).items()
                        if k in self._data}

    def items(self) -> dict[str, str]:
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)
