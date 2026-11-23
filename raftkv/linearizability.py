"""Linearizability checker for recorded client histories.

A Raft cluster is *supposed* to be linearizable: every operation must
appear to take effect at a single instant between its invocation and
its response, and reads must see a state consistent with that order.
The consensus tests prove the mechanism; this module checks the
*observable* property instead — record what real clients actually saw
during a run (chaos included) and verify no read ever returned a value
that no linearization could produce.

An ``Op`` is one client operation with its invocation and response
times (``time.monotonic``), the client/session that issued it, and the
value it observed. A ``History`` collects ops (thread-safe, so writer
and reader threads in the chaos harness can share one) and ``check()``
verifies them key by key.

The check, per key:

* A **write** is a successful ``set`` / ``del`` / ``cas``. A delete
  writes ``None``. A failed CAS changes nothing, but its reply carries
  the current value, so it counts as a **read** of that value.
* A **read** is a successful ``get`` (or a failed CAS). Let ``latest``
  be the write that completed most recently before the read was
  invoked, and let ``concurrent`` be the writes that overlap the read
  (invoked before the read responded, completed after it was invoked).
  In any linearization every write completed before the read was
  invoked precedes the read, and any write linearized between
  ``latest`` and the read must overlap the read — so the read must
  return ``latest``'s value, or the value of a concurrent write, or
  ``None`` if no write had completed yet. Anything else is a stale
  read, and the checker reports exactly which write the read should
  have seen instead.

That argument needs one assumption: each key is written by a single
client, in program order (so "most recently completed" is also the
latest write in program order). The chaos harness satisfies it — each
writer thread owns its own keys. If a key *is* written by several
clients, their writes can overlap in real time and be linearized in
either order, so the strict rule could flag a legal read; for those
keys the checker falls back to a weaker rule — the read must return
``None`` or the value of *some* write invoked before the read
responded (a value that was never written, or written only after the
read finished, is still impossible). Multi-writer keys are reported
in the result so a run that leans on the weak check does not look
stricter than it is.

Ops that errored (timeout, redirect loop, node down) carry no
observation and are skipped — a client that heard nothing cannot
contradict a linearization. Retried writes are recorded once, with
the *first* attempt's invocation time and the successful response
time: the write may have committed any time after its first
invocation, and recording the retry's later invocation would make a
read that legitimately saw the write look stale.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Op:
    """One observed client operation.

    ``invoke`` / ``respond`` are ``time.monotonic()`` readings around
    the client request (the first attempt's invocation for a retried
    write). ``value`` is the value a write sets (``None`` for a
    delete); ``returned`` is the value a read observed (``None`` for
    a missing key). ``status`` is the client response status
    (``"ok"``, ``"cas_failed"``, ``"error"``).
    """

    client: str
    seq: int
    kind: str  # "set" | "get" | "del" | "cas"
    key: str
    value: str | None = None
    expected: str | None = None
    invoke: float = 0.0
    respond: float = 0.0
    status: str = "ok"
    returned: str | None = None


@dataclass
class CheckResult:
    """What ``History.check()`` found."""

    violations: list[str] = field(default_factory=list)
    ops_checked: int = 0
    reads_checked: int = 0
    keys_checked: int = 0
    multi_writer_keys: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def __str__(self) -> str:
        if self.ok:
            return (f"linearizability ok: {self.reads_checked} reads "
                    f"over {self.keys_checked} keys checked "
                    f"({self.ops_checked} ops)")
        return (f"linearizability FAILED: {len(self.violations)} "
                f"violation(s) in {self.reads_checked} reads:\n"
                + "\n".join(f"  - {v}" for v in self.violations))


# Ops that change state when they succeed.
_WRITE_KINDS = ("set", "del", "cas")


class History:
    """A thread-safe record of observed client operations."""

    def __init__(self) -> None:
        self._ops: list[Op] = []
        self._lock = threading.Lock()

    def record(self, op: Op) -> None:
        with self._lock:
            self._ops.append(op)

    def __len__(self) -> int:
        with self._lock:
            return len(self._ops)

    def ops(self) -> list[Op]:
        with self._lock:
            return list(self._ops)

    def check(self) -> CheckResult:
        """Verify the recorded history is linearizable, key by key."""
        result = CheckResult()
        ops = self.ops()
        result.ops_checked = len(ops)
        by_key: dict[str, list[Op]] = {}
        for op in ops:
            by_key.setdefault(op.key, []).append(op)
        result.keys_checked = len(by_key)
        for key in sorted(by_key):
            self._check_key(key, by_key[key], result)
        return result

    def _check_key(self, key: str, ops: list[Op],
                   result: CheckResult) -> None:
        # Writes: successful set/del/cas. A delete writes None.
        writes = [op for op in ops
                  if op.kind in _WRITE_KINDS and op.status == "ok"]
        # Reads: successful gets, plus failed CAS (its reply carries
        # the current value, so it observed the state like a read).
        reads = [op for op in ops
                 if (op.kind == "get" and op.status == "ok")
                 or (op.kind == "cas" and op.status == "cas_failed")]
        result.reads_checked += len(reads)
        if not reads:
            return
        writers = {op.client for op in writes}
        if len(writers) > 1:
            result.multi_writer_keys.append(key)
            self._check_key_weak(key, writes, reads, result)
        else:
            self._check_key_strict(key, writes, reads, result)

    def _check_key_strict(self, key: str, writes: list[Op],
                          reads: list[Op], result: CheckResult) -> None:
        """Single-writer rule: a read must see the latest write that
        completed before it was invoked, or a concurrent write."""
        for read in reads:
            completed = [w for w in writes if w.respond <= read.invoke]
            concurrent = [w for w in writes
                          if w.invoke < read.respond
                          and w.respond > read.invoke]
            valid = {w.value for w in concurrent}
            if completed:
                latest = max(completed, key=lambda w: w.respond)
                valid.add(latest.value)
            else:
                valid.add(None)
            if read.returned not in valid:
                if completed:
                    latest = max(completed, key=lambda w: w.respond)
                    detail = (f"latest completed write was "
                              f"{latest.value!r} by {latest.client} "
                              f"(responded at {latest.respond:.3f}, read "
                              f"invoked at {read.invoke:.3f})")
                else:
                    detail = "no write had completed when the read began"
                result.violations.append(
                    f"stale read of {key!r} by {read.client}: returned "
                    f"{read.returned!r}, but {detail}; concurrent writes "
                    f"were {[w.value for w in concurrent]}")

    def _check_key_weak(self, key: str, writes: list[Op],
                        reads: list[Op], result: CheckResult) -> None:
        """Multi-writer rule: a read must return None or the value of
        some write invoked before the read responded. Weaker — it
        cannot catch staleness between overlapping writers — but it
        still catches values that were never written or not yet
        written when the read finished."""
        for read in reads:
            possible = {w.value for w in writes
                        if w.invoke < read.respond}
            if read.returned is not None and read.returned not in possible:
                result.violations.append(
                    f"impossible read of {key!r} by {read.client}: "
                    f"returned {read.returned!r}, but no write of that "
                    f"value was invoked before the read responded "
                    f"(writes seen: {sorted(possible, key=str)})")


def record_client_op(history: History, client: str, seq: int,
                     msg: dict, resp: dict,
                     invoke: float, respond: float) -> None:
    """Turn one client request/response pair into an ``Op`` and record
    it. Errored responses carry no observation and are skipped; a
    failed CAS is recorded as a read of its reported current value."""
    status = resp.get("status")
    if status in (None, "error"):
        return
    kind = msg.get("op")
    if kind not in ("set", "get", "del", "cas"):
        return
    if kind == "cas" and status == "cas_failed":
        history.record(Op(client=client, seq=seq, kind=kind,
                          key=msg["key"], value=msg.get("value"),
                          expected=msg.get("expected"),
                          invoke=invoke, respond=respond, status=status,
                          returned=resp.get("current")))
        return
    history.record(Op(client=client, seq=seq, kind=kind,
                      key=msg["key"], value=msg.get("value"),
                      expected=msg.get("expected"),
                      invoke=invoke, respond=respond, status=status,
                      returned=resp.get("value") if kind == "get" else None))
