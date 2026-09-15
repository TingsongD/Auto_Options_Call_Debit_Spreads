"""Run state aggregates: positions, orders, reservations, agents, events.

The append-only event log is the financial authority; aggregates are
projections rebuilt by folding events. All quantities are Decimal; timestamps
are aware UTC.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from spx_research.domain.types import CreditSpread, Direction


class OrderIntent(Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class OrderStatus(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class Order:
    order_id: str
    agent_id: str
    spread: CreditSpread
    intent: OrderIntent
    limit_points: Decimal
    submitted_at_utc: datetime
    first_eligible_at_utc: datetime
    status: OrderStatus = OrderStatus.PENDING


@dataclass(frozen=True)
class Fill:
    order_id: str
    agent_id: str
    spread: CreditSpread
    intent: OrderIntent
    package_price_points: Decimal
    filled_at_utc: datetime
    fees_usd: Decimal


class PositionStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    SETTLED = "SETTLED"


@dataclass(frozen=True)
class Position:
    position_id: str
    agent_id: str
    spread: CreditSpread
    entry_credit_points: Decimal
    entry_fees_usd: Decimal
    entry_at_utc: datetime
    entry_ny_date: Any  # date, for holding-age math
    reserve_usd: Decimal
    status: PositionStatus = PositionStatus.OPEN


class ReservationStatus(Enum):
    SEEKING_ENTRY = "SEEKING_ENTRY"
    ENTRY_PENDING = "ENTRY_PENDING"
    FILLED = "FILLED"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    agent_id: str
    direction: Direction
    reserve_usd: Decimal
    created_at_utc: datetime
    expires_at_utc: datetime
    status: ReservationStatus = ReservationStatus.SEEKING_ENTRY


class AgentState(Enum):
    CREATED = "CREATED"
    SEEKING_ENTRY = "SEEKING_ENTRY"
    ENTRY_PENDING = "ENTRY_PENDING"
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"
    SETTLED = "SETTLED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    ARCHIVED = "ARCHIVED"


@dataclass(frozen=True)
class Agent:
    agent_id: str
    role: str  # "SPREAD" | "MANAGER"
    direction: Direction | None
    state: AgentState
    created_at_utc: datetime
    next_review_at_utc: datetime
    reservation_id: str | None = None
    position_id: str | None = None


@dataclass(frozen=True)
class Event:
    """One committed ledger event. Hash-chained per run."""

    run_id: str
    seq: int
    sim_time_utc: datetime
    phase: str
    type: str
    payload: dict[str, Any]
    payload_hash: str = ""
    previous_hash: str = ""
    event_hash: str = ""

    def with_hashes(self, payload_hash: str, previous_hash: str) -> Event:
        linked = Event(
            self.run_id,
            self.seq,
            self.sim_time_utc,
            self.phase,
            self.type,
            self.payload,
            payload_hash,
            previous_hash,
        )
        return Event(
            linked.run_id,
            linked.seq,
            linked.sim_time_utc,
            linked.phase,
            linked.type,
            linked.payload,
            linked.payload_hash,
            linked.previous_hash,
            event_hash(linked),
        )


def event_hash(e: Event) -> str:
    """Canonical digest binding the full envelope + payload + chain link.

    Covers every field a tamperer could flip to change replay semantics:
    run_id, seq, sim_time_utc, phase, type, payload_hash and previous_hash.
    """
    blob = json.dumps(
        {
            "run_id": e.run_id,
            "seq": e.seq,
            "sim_time_utc": e.sim_time_utc.isoformat(),
            "phase": e.phase,
            "type": e.type,
            "payload_hash": e.payload_hash,
            "previous_hash": e.previous_hash,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(blob).hexdigest()
