"""The key-value state machine that sits on top of Raft.

Raft guarantees every node applies the same committed log entries in the
same order, so this state machine is intentionally trivial and
deterministic: applying the same command log always produces the same
state. All it needs to be is correct and idempotent under replay — a node
re-applies its committed prefix after a restart, so ``apply`` must be a
pure function of (current state, command).
"""


class KVStore:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    # Sentinel for "key absent": distinct from any value a client can
    # store, so a CAS against a missing key is distinguishable from a
    # CAS against a stored value (including an empty string).
    _MISSING = object()

    def apply(self, command: dict) -> dict | None:
        """Apply one committed log command. Commands are dicts like
        {"op": "set", "key": "k", "value": "v"} or {"op": "del", "key": "k"}.

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
            self._data[command["key"]] = command["value"]
            return None
        elif op == "del":
            self._data.pop(command["key"], None)
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
            self._data[key] = command["value"]
            return {"type": "client_response", "status": "ok",
                    "cas": "success"}
        else:
            raise ValueError(f"unknown command op: {op!r}")

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def keys(self, prefix: str = "", start_after: str | None = None) -> list[str]:
        """Every key in the store, sorted, optionally restricted to
        keys starting with ``prefix`` and to keys strictly after
        ``start_after``. Sorted so a scan's answer is deterministic
        — the same state always enumerates in the same order, which
        is what makes a paged scan (limit + resume from the last key
        returned) well-defined: ``start_after`` is an exclusive
        lower bound on the sorted key order, so resuming with the
        last key of one page starts the next page exactly after it,
        with no repeats and no gaps, as long as the state does not
        change between pages. Pure read: no log entry, no state
        change."""
        matching = (k for k in self._data if k.startswith(prefix)) \
            if prefix else iter(self._data)
        if start_after is not None:
            matching = (k for k in matching if k > start_after)
        return sorted(matching)

    def load(self, state: dict[str, str]) -> None:
        """Replace the whole state with ``state`` (used when installing
        a snapshot: the snapshot *is* the state as of its last included
        index, so anything the store held before is discarded)."""
        self._data = dict(state)

    def items(self) -> dict[str, str]:
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)
