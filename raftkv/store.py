"""The key-value state machine that sits on top of Raft.

Raft guarantees every node applies the same committed log entries in the
same order, so this state machine is intentionally trivial and
deterministic: applying the same command log always produces the same
state. All it needs to be is correct and idempotent under replay — a node
re-applies its committed prefix after a restart, so ``apply`` must be a
pure function of (current state, command).
"""

import bisect


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
        fresh SET drops the old TTL — and so do ``mset`` and
        ``del``. A successful CAS keeps the key's deadline: it
        is a conditional update of the value, not a fresh set,
        so it must not resurrect a key that is about to expire.

        Returns a result dict for commands whose outcome depends on the
        state at apply time — currently only "cas" — and None for
        set/del, whose outcome is always ok. The result is what the
        leader hands back to the waiting client and what the dedup
        table caches for retries, so it must be a pure function of
        (state before, command): every node applying the same log
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
            # No TTL on an mset (a batch carries no single
            # deadline), so every pair it touches loses any
            # deadline it had, exactly like a plain set.
            for key, value in command["pairs"]:
                if key not in self._data:
                    bisect.insort(self._keys, key)
                self._data[key] = value
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
