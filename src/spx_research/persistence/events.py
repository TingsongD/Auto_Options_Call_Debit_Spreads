"""Event store: append-only, hash-chained ledger authority.

The in-memory implementation is used by unit/golden tests and replay; the
PostgreSQL implementation (same protocol) arrives with the persistence
milestone. Sequence and hash integrity make a corrupted prefix detectable.
"""

from __future__ import annotations

import hashlib
import json
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


class InMemoryEventStore:
    """Deterministic single-writer event log for tests and replay."""

    def __init__(self) -> None:
        self._events: dict[str, list[Event]] = {}

    def append(self, event: Event, expected_seq: int) -> Event:
        events = self._events.setdefault(event.run_id, [])
        seq, prev_hash = self.tip(event.run_id)
        if expected_seq != seq:
            raise LedgerError("SEQUENCE_MISMATCH")
        e = event.with_hashes(payload_hash(event.payload), prev_hash)
        if e.seq != seq + 1:
            raise LedgerError("SEQUENCE_MISMATCH")
        events.append(e)
        return e

    def events(self, run_id: str) -> list[Event]:
        return list(self._events.get(run_id, []))

    def tip(self, run_id: str) -> tuple[int, str]:
        events = self._events.get(run_id, [])
        if not events:
            return 0, "genesis"
        last = events[-1]
        return last.seq, hashlib.sha256((last.payload_hash + str(last.seq)).encode()).hexdigest()[
            :24
        ]
