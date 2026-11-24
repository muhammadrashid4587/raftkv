"""The key-value state machine that sits on top of Raft.

Raft guarantees every node applies the same committed log entries in the
same order, so this state machine is intentionally trivial and
deterministic: applying the same command log always produces the same
state. All it needs to be is correct and idempotent under replay — a node
re-applies its committed prefix after a restart, so ``apply`` must be a
pure function of (current state, command).
"""

import bisect

# Signed 64-bit bounds, the range Redis's INCR/INCRBY works in: a
# value outside it is not an integer Redis can count, and a result
# outside it is an overflow, not a bigger number.
_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1


def _is_integer_string(s: str) -> bool:
    """Whether ``s`` is a Redis-style integer: an optional sign
    followed by digits only. ``int()`` alone is too lenient — it
    accepts whitespace and underscores ("1_0", " 1"), which Redis
    rejects — so the shape is checked first and ``int()`` only
    parses what already looks like an integer."""
    if s.startswith(("+", "-")):
        s = s[1:]
    return bool(s) and s.isdigit()


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
        "setnx", "getset", "append", and "getdel" —
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
