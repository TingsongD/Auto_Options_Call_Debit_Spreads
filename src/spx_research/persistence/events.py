"""Event store: append-only, hash-chained ledger authority.

The in-memory implementation is used by unit/golden tests and replay; the
PostgreSQL implementation (same protocol) is the durable store. The chain
link is the prior event's ``event_hash`` — a digest over the full envelope
(run_id, seq, sim_time_utc, phase, type) plus ``payload_hash`` and
``previous_hash`` — so sequence and content tampering are both detectable.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from threading import RLock
from typing import Any, Protocol

from spx_research.domain.state import Event


class LedgerError(ValueError):
    pass


def payload_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


class EventStore(Protocol):
    def append(self, event: Event, expected_seq: int) -> Event: ...
    def events(self, run_id: str) -> list[Event]: ...
    def tip(self, run_id: str) -> tuple[int, str]: ...

    def append_batch(self, events: list[Event], expected_seq: int) -> list[Event]: ...


class InMemoryEventStore:
    """Deterministic single-writer event log for tests and replay."""

    def __init__(self) -> None:
        self._events: dict[str, list[Event]] = {}
        self._lock = RLock()

    def append(self, event: Event, expected_seq: int) -> Event:
        return self.append_batch([event], expected_seq)[0]

    def append_batch(self, events: list[Event], expected_seq: int) -> list[Event]:
        """Validate the whole batch before exposing any financial effects."""
        if not events:
            return []
        with self._lock:
            run_id = events[0].run_id
            seq, prev_hash = self.tip(run_id)
            if expected_seq != seq:
                raise LedgerError("SEQUENCE_MISMATCH")
            committed: list[Event] = []
            for event in events:
                if event.run_id != run_id or event.seq != seq + 1:
                    raise LedgerError("SEQUENCE_MISMATCH")
                e = deepcopy(event).with_hashes(payload_hash(event.payload), prev_hash)
                committed.append(e)
                seq, prev_hash = e.seq, e.event_hash
            self._events.setdefault(run_id, []).extend(committed)
            return deepcopy(committed)

    def events(self, run_id: str) -> list[Event]:
        return deepcopy(self._events.get(run_id, []))

    def tip(self, run_id: str) -> tuple[int, str]:
        events = self._events.get(run_id, [])
        if not events:
            return 0, "genesis"
        last = events[-1]
        return last.seq, last.event_hash
