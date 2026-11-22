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

    def apply(self, command: dict) -> None:
        """Apply one committed log command. Commands are dicts like
        {"op": "set", "key": "k", "value": "v"} or {"op": "del", "key": "k"}."""
        op = command.get("op")
        if op == "set":
            self._data[command["key"]] = command["value"]
        elif op == "del":
            self._data.pop(command["key"], None)
        else:
            raise ValueError(f"unknown command op: {op!r}")

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def load(self, state: dict[str, str]) -> None:
        """Replace the whole state with ``state`` (used when installing
        a snapshot: the snapshot *is* the state as of its last included
        index, so anything the store held before is discarded)."""
        self._data = dict(state)

    def items(self) -> dict[str, str]:
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)
