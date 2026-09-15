"""Typed evidence/observation/knowledge records for the temporal knowledge boundary.

Ported from spx_ai_handover_v2/reference/temporal_harness.py (TKH spec §3).
Facts are written only by trusted reducers; model proposals are validated
against these types and the registered vocabulary below.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


class HarnessError(ValueError):
    """Fixed error code; never echo untrusted leaked prose into retry context."""


def aware_check(t: datetime) -> datetime:
    if t.tzinfo is None or t.utcoffset() is None:
        raise HarnessError("NAIVE_TIME")
    return t


# Assertion kinds distinguish realized observations from schedules, forecasts,
# derivations and approved rules (TKH spec §3.1).
KINDS = {"OBSERVATION", "ANNOUNCEMENT", "SCHEDULE", "SOURCE_FORECAST", "DERIVED", "APPROVED_RULE"}

# Registered evidence vocabulary: metric -> (unit, allowed values or None for
# finite Decimal values). A metric's unit is part of its registration; enum
# metrics list every permitted value explicitly.
VOCAB: dict[str, tuple[str, frozenset[str] | None]] = {
    "policy_rate_bps": ("basis_points", None),
    "policy_delta_bps": ("basis_points", None),
    "expected_rate_bps": ("basis_points", None),
    "profit_fraction": ("fraction_of_initial_credit", None),
    "candidate_max_risk": ("fraction_of_equity", None),
    "minutes_to_meeting": ("minutes", None),
    "available_slots": ("slots", None),
    "bull_deficit": ("slots", None),
    "bear_deficit": ("slots", None),
    "direction_target": ("bullish_per_bearish", None),
    "advisory_rule": ("policy", frozenset({"DISCRETIONARY", "MANDATORY", "UNKNOWN"})),
    "direction_mandate": ("policy", frozenset({"BULL_PUT_CREDIT", "BEAR_CALL_CREDIT", "UNKNOWN"})),
}

CONFIDENCE = {"LOW", "MEDIUM", "HIGH", "UNASSESSABLE"}
TOPICS = {
    "POLICY_DIRECTION": {"TIGHTENING_RECENTLY", "EASING_RECENTLY", "NO_CLEAR_CHANGE"},
    "RATE_OUTLOOK": {"CUTS_PLAUSIBLE", "HIKES_PLAUSIBLE", "UNCERTAIN"},
    "RISK_OUTLOOK": {"ELEVATED_RISK", "ORDINARY_RISK", "UNCERTAIN"},
    "MANAGEMENT_OUTLOOK": {"MAINTAIN", "RECONSIDER", "UNCERTAIN"},
}
REASONS = {
    "POLICY_UNCERTAINTY",
    "PROFIT_BAND",
    "LOSS_BAND",
    "RISK_BUDGET",
    "ALLOCATION_DEFICIT",
    "ENTRY_CRITERIA",
    "QUOTE_QUALITY",
    "MAINTAIN_THESIS",
    "INSUFFICIENT_EVIDENCE",
    "LIFECYCLE_RESTRICTION",
}
UNKNOWNS = {
    "UNKNOWN_FUTURE_POLICY_PATH",
    "UNKNOWN_FUTURE_PRICE_PATH",
    "UNVERIFIED_PROBABILITY",
    "AMBIGUOUS_MACRO",
    "QUOTE_LIMITATION",
    "DISCRETIONARY_LOSS_LIMIT",
    "NONE_IDENTIFIED",
}
ACTIONS = {
    "SPREAD": {"WAIT", "OPEN", "HOLD", "CLOSE"},
    "MANAGER": {
        "ALLOCATE",
        "PAUSE_NEW_ALLOCATIONS",
        "RESUME_NEW_ALLOCATIONS",
        "RETIRE_SEARCH_SLOTS",
        "NO_CHANGE",
    },
}


@dataclass(frozen=True)
class Atom:
    atom_id: str
    metric: str
    value: str
    unit: str
    kind: str
    published_at: datetime
    available_at: datetime
    subject_at: datetime
    source_checked: bool = True
    recipients: tuple[str, ...] = ("PUBLIC",)
    dependencies: tuple[str, ...] = ()
    transform: str | None = None


@dataclass(frozen=True)
class Delivery:
    run_id: str
    branch_id: str
    actor_id: str
    atom_id: str
    delivered_at: datetime


@dataclass(frozen=True)
class Context:
    run_id: str
    branch_id: str
    actor_id: str
    actor_role: str
    as_of: datetime
    session_index: int
    minute_from_open: int
    alias_namespace: str
    private_manifest_id: str
    prior_visible_belief_hash: str = "empty"


@dataclass(frozen=True)
class MenuChoice:
    internal_id: str
    kind: str
    required_atoms: tuple[str, ...]
    target_internal_id: str | None = None
    limit_internal_id: str | None = None


@dataclass(frozen=True)
class Compiled:
    public: dict[str, Any]
    context: Context
    premise_map: dict[str, Atom]
    action_map: dict[str, MenuChoice]
