"""Watch/subscribe primitive for raftkv.

A client (in-process for now: an embedding application, a test, or a
future watch RPC) registers a callback on a key — or on a key prefix —
and the callback fires with the new value whenever a *committed* log
entry changes that key. Because notification happens in
``RaftNode._apply_committed``, after the state machine has applied the
entry, a watch fires exactly once per committed change, in log order,
on every node that applies the entry — leader and followers alike.

Deliberate limits of this slice (v0.11.0):

- Watches are local to one node and live in memory only: they are not
  replicated, not persisted, and do not survive a restart. A client
  that wants durability re-registers after reconnecting.
- Callbacks run synchronously on the thread that applies the entry,
  while the node's lock is held. Keep them short and non-blocking:
  record the event, enqueue it, return. A callback that blocks stalls
  the apply path; one that raises is caught and dropped so a broken
  watcher cannot break replication.
- A watch fires only on a change that actually lands: a ``set`` fires
  with the new value, a ``del`` fires with ``value=None``, a successful
  CAS fires like a set, and a failed CAS (or a ``del`` of a key that
  was already absent — still a committed log entry) fires with the
  resulting value, which for a no-op delete is ``None``. There is no
  initial-value delivery: a watch sees changes *after* registration,
  never the current value at registration time.
- Since v0.17.0 a watch is also reachable over the wire: a client
  sends ``{"type": "watch", "key": ..., "prefix": ..., "timeout": ...}``
  and the node long-polls — it registers a callback, waits up to
  ``timeout`` seconds for the first event, and replies with every
  event that arrived (``{"type": "watch_response", "status": "ok" |
  "timeout", "events": [...]}``). Any node can serve a watch, leader
  or follower, because the apply path fires on every node. Each event
  carries the log ``index`` of the entry that caused it, so a client
  re-registering after a disconnect can order events and spot a gap.
  The watch is still local and in-memory: it lives only for the
  duration of the one RPC, and the client re-issues it to keep
  watching.
"""

from __future__ import annotations

import threading


class WatchManager:
    """Registry of key/prefix watches, fired by the apply path."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 1
        # watch_id -> (key_or_prefix, is_prefix, callback)
        self._watches: dict[int, tuple[str, bool, object]] = {}

    def watch(self, key: str, callback, prefix: bool = False) -> int:
        """Register ``callback`` for changes to ``key`` (exact match),
        or to every key starting with ``key`` when ``prefix=True``.

        The callback receives one event dict per change:
        ``{"key": ..., "value": ... | None, "op": "set" | "del"}`` —
        plus ``"index"`` (the log index of the entry that caused it)
        when the event comes from the apply path; direct ``notify``
        calls without an index get the three-field event.
        Returns a watch id for ``cancel``.
        """
        with self._lock:
            watch_id = self._next_id
            self._next_id += 1
            self._watches[watch_id] = (key, prefix, callback)
            return watch_id

    def cancel(self, watch_id: int) -> bool:
        """Remove a watch. Returns True if it existed."""
        with self._lock:
            return self._watches.pop(watch_id, None) is not None

    def __len__(self) -> int:
        with self._lock:
            return len(self._watches)

    def notify(self, key: str, value: str | None, op: str,
               index: int | None = None) -> None:
        """Fire every watch matching ``key`` with this change.

        Called by the apply path after a committed entry changed the
        key. Matching callbacks are collected under the lock, then
        invoked outside it, so a callback registering or cancelling a
        watch cannot deadlock against the registry. A callback that
        raises is swallowed: one broken watcher must not break the
        apply path or the other watchers. ``index`` is the log index
        of the entry that caused the change; when given it rides in
        the event dict, so a remote watcher can order events and spot
        a gap after reconnecting. Local callers that omit it get the
        original three-field event.
        """
        event = {"key": key, "value": value, "op": op}
        if index is not None:
            event["index"] = index
        with self._lock:
            callbacks = [cb for (k, is_prefix, cb) in self._watches.values()
                         if (key.startswith(k) if is_prefix else key == k)]
        for cb in callbacks:
            try:
                cb(event)
            except Exception:
                continue
