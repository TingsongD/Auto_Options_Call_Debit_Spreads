"""Policy interface: mechanical baseline, recorded tape, and LLM all conform.

``DecisionContext`` is the private engine view (real times, real contract ids).
A model-facing policy implementation is responsible for projecting this through
the temporal knowledge harness before any inference — the engine never hands a
model this object directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol

from spx_research.domain.state import Agent, Position, Reservation
from spx_research.features.candidates import Candidate


class SpreadAction(Enum):
    WAIT = "WAIT"
    OPEN = "OPEN"
    HOLD = "HOLD"
    CLOSE = "CLOSE"


class ManagerAction(Enum):
    ALLOCATE = "ALLOCATE"
    PAUSE_NEW_ALLOCATIONS = "PAUSE_NEW_ALLOCATIONS"
    RESUME_NEW_ALLOCATIONS = "RESUME_NEW_ALLOCATIONS"
    RETIRE_SEARCH_SLOTS = "RETIRE_SEARCH_SLOTS"
    NO_CHANGE = "NO_CHANGE"


@dataclass(frozen=True)
class LimitTemplate:
    """Engine-approved price-limit choice offered on an action menu."""

    template_id: str
    kind: str  # "NATURAL" | "MID" | "NATURAL_PLUS_TICK" ...
    limit_points: Decimal


@dataclass(frozen=True)
class SpreadView:
    """Private per-agent decision view."""

    agent: Agent
    position: Position | None
    current_close_debit_points: Decimal | None
    profit_fraction: Decimal | None  # net est. liquidation P&L / gross credit
    days_held: int
    candidates: tuple[Candidate, ...]
    entry_limit_templates: tuple[LimitTemplate, ...]
    exit_limit_templates: tuple[LimitTemplate, ...]


@dataclass(frozen=True)
class ManagerView:
    """Private manager decision view."""

    as_of_utc: datetime
    active_bullish: int
    active_bearish: int
    reserved_bullish: int
    reserved_bearish: int
    capacity: int
    bullish_target: int
    bearish_target: int
    paused: bool
    available_usd: Decimal
    reservations: tuple[Reservation, ...]
    macro_facts: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class DecisionContext:
    """What one actor may decide from at one simulated decision barrier."""

    run_id: str
    branch_id: str
    actor_id: str
    role: str  # "SPREAD" | "MANAGER"
    as_of_utc: datetime
    session_index: int
    minute_from_open: int
    spread_view: SpreadView | None = None
    manager_view: ManagerView | None = None


@dataclass(frozen=True)
class Proposal:
    """A policy's proposed internal action (resolved against real contracts)."""

    kind: str  # SpreadAction/ManagerAction value
    candidate_id: str | None = None
    limit_template_id: str | None = None
    position_id: str | None = None
    allocation: dict[str, int] | None = None  # {"bullish": n, "bearish": n}
    retire_reservation_ids: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    uncertainty_codes: tuple[str, ...] = ("NONE_IDENTIFIED",)


class Policy(Protocol):
    def decide(self, ctx: DecisionContext) -> Proposal: ...


class PolicyError(ValueError):
    """Decision barrier could not be satisfied — caller pauses, not invents."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass
class Rejection(Exception):
    code: str


def validate_spread_proposal(ctx: DecisionContext, p: Proposal) -> None:
    """Rule check on a resolved internal proposal (dictionary §8 semantics)."""
    view = ctx.spread_view
    if view is None:
        raise Rejection("NO_SPREAD_VIEW")
    state = view.agent.state.name
    if p.kind == "WAIT":
        if state != "SEEKING_ENTRY" or p.candidate_id or p.position_id:
            raise Rejection("WAIT_FIELDS")
    elif p.kind == "OPEN":
        if state != "SEEKING_ENTRY":
            raise Rejection("NOT_ENTRY_STATE")
        if not p.candidate_id or p.position_id or not p.limit_template_id:
            raise Rejection("OPEN_FIELDS")
        if p.candidate_id not in {c.candidate_id for c in view.candidates}:
            raise Rejection("UNKNOWN_CANDIDATE")
        if p.limit_template_id not in {t.template_id for t in view.entry_limit_templates}:
            raise Rejection("UNKNOWN_LIMIT_TEMPLATE")
    elif p.kind == "HOLD":
        if state not in ("OPEN", "EXIT_PENDING") or not p.position_id or p.candidate_id:
            raise Rejection("HOLD_FIELDS")
        if view.position is None or view.position.position_id != p.position_id:
            raise Rejection("NOT_OWN_POSITION")
    elif p.kind == "CLOSE":
        if state != "OPEN" or not p.position_id or p.candidate_id:
            raise Rejection("CLOSE_FIELDS")
        if not p.limit_template_id:
            raise Rejection("CLOSE_FIELDS")
        if view.position is None or view.position.position_id != p.position_id:
            raise Rejection("NOT_OWN_POSITION")
        if p.limit_template_id not in {t.template_id for t in view.exit_limit_templates}:
            raise Rejection("UNKNOWN_LIMIT_TEMPLATE")
    else:
        raise Rejection("UNKNOWN_ACTION")


def validate_manager_proposal(ctx: DecisionContext, p: Proposal) -> None:
    view = ctx.manager_view
    if view is None:
        raise Rejection("NO_MANAGER_VIEW")
    if p.kind == "ALLOCATE":
        if not p.allocation or set(p.allocation) - {"bullish", "bearish"}:
            raise Rejection("ALLOCATE_FIELDS")
        if any(not isinstance(v, int) or v < 0 for v in p.allocation.values()):
            raise Rejection("ALLOCATE_FIELDS")
        if view.paused:
            raise Rejection("ALLOCATIONS_PAUSED")
        committed = (
            view.active_bullish + view.reserved_bullish + p.allocation.get("bullish", 0)
        ) + (view.active_bearish + view.reserved_bearish + p.allocation.get("bearish", 0))
        if committed > view.capacity:
            raise Rejection("CAPACITY_EXCEEDED")
    elif p.kind == "RETIRE_SEARCH_SLOTS":
        if p.allocation:
            raise Rejection("RETIRE_FIELDS")
        live = {r.reservation_id for r in view.reservations if r.status.name == "SEEKING_ENTRY"}
        if not set(p.retire_reservation_ids) <= live:
            raise Rejection("UNKNOWN_RESERVATION")
    elif p.kind in ("PAUSE_NEW_ALLOCATIONS", "RESUME_NEW_ALLOCATIONS", "NO_CHANGE"):
        if p.allocation or p.retire_reservation_ids or p.candidate_id:
            raise Rejection("FIELDS_NOT_EMPTY")
    else:
        raise Rejection("UNKNOWN_ACTION")
