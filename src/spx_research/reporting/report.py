"""Run manifest and summary reporting (M3-05).

The manifest binds a run to its inputs: profile id, dataset manifest content id,
event count, and the hash-chained tip of the committed log. The summary is
deterministic — it folds the same event log the engine wrote.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from decimal import Decimal
from typing import Any

from spx_research.config import Profile
from spx_research.domain.state import Event
from spx_research.engine.ledger import EngineState, replay
from spx_research.engine.scheduler import RunResult


def event_log_digest(events: list[Event]) -> str:
    blob = json.dumps([asdict(e) for e in events], sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()


def run_manifest(
    result: RunResult,
    profile: Profile,
    dataset_manifest_id: str | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **(extra or {}),
        "run_id": result.run_id,
        "profile_id": profile.profile_id,
        "profile_mode": profile.mode,
        "dataset_manifest_id": dataset_manifest_id,
        "event_count": len(result.events),
        "event_log_sha256": event_log_digest(result.events),
        "decision_count": len(result.decisions),
        "final_cash_usd": str(result.final_state.account.cash),
        "initial_cash_usd": (
            str(profile.portfolio.initial_capital_usd)
            if profile.portfolio and profile.portfolio.initial_capital_usd is not None
            else None
        ),
        "open_positions": len(result.final_state.open_positions()),
        "study_label": (profile.harness.study_label if profile.harness else None),
    }


def summarize(result: RunResult) -> dict[str, Any]:
    events = result.events
    by_type: dict[str, int] = {}
    for e in events:
        by_type[e.type] = by_type.get(e.type, 0) + 1
    st = result.final_state
    closed = [e for e in events if e.type == "POSITION_CLOSED"]
    settled = [e for e in events if e.type == "POSITION_SETTLED"]
    return {
        "schema_version": "2.1",
        "run_id": result.run_id,
        "status": result.status,
        "pause": result.pause,
        "research_validity": result.research_validity,
        "classification": "HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED",
        "application_temporal_gate": "NOT_RUN",
        "behavioral_leakage_diagnostics": "NOT_RUN",
        "model_temporal_provenance": "UNKNOWN",
        "parametric_future_knowledge_excluded": False,
        "study_label": "HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED",
        "parametric_ignorance_proven": False,
        "events": len(events),
        "event_types": by_type,
        "agents_total": len(st.agents),
        "positions_total": len(st.positions),
        "closed": len(closed),
        "settled": len(settled),
        "final_cash_usd": str(st.account.cash),
        "reserved_usd": str(st.account.reserved),
        "fees_paid_usd": str(st.account.fees_paid),
        "trading_fees_usd": str(st.account.fees_paid),
        "open_positions": len(st.open_positions()),
        "valuations": result.valuations,
        "final_valuation": result.valuations[-1] if result.valuations else None,
        "scored_end_valuation": result.scored_end_valuation,
        "runoff": result.runoff_summary,
        "coverage": {
            "status": "UNAVAILABLE"
            if result.pause and result.pause.get("category") == "DATA"
            else "OBSERVED_ONLY",
            "events": [e.payload for e in events if "COVERAGE" in e.type or "DATA_GAP" in e.type],
        },
        "decisions": len(result.decisions),
        "rejections": sum(1 for d in result.decisions if "rejected" in d),
    }


def replay_summary(
    run_id: str, initial_cash: Decimal, events: list[Event]
) -> tuple[EngineState, str]:
    """Fold the committed log; returns state + log digest (no model calls)."""
    st = replay(run_id, initial_cash, events)
    return st, event_log_digest(events)


def attempt_summary(journal: list[dict[str, Any]]) -> dict[str, Any]:
    """Account for failed and unresolved calls as well as accepted proposals."""
    attempts: dict[str, dict[str, Any]] = {}
    for row in journal:
        payload = row["payload"]
        if row["kind"] == "ATTEMPT_RESERVED":
            attempts[payload["attempt_id"]] = {"outcome": "DISPATCH_RESERVED", **payload}
        elif row["kind"] == "ATTEMPT_COMPLETED":
            attempts.setdefault(payload["attempt_id"], {}).update(payload)
        elif row["kind"] == "ATTEMPT_RECONCILED":
            attempts.setdefault(payload["attempt_id"], {}).update(
                outcome="RECONCILED",
                actual_usd=payload.get("actual_usd"),
            )
    counts: dict[str, int] = {}
    tokens = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
    unknown_usage = 0
    for item in attempts.values():
        outcome = str(item["outcome"])
        counts[outcome] = counts.get(outcome, 0) + 1
        response = (item.get("response") or {}).get("model_response")
        if response is None or response.get("billing_uncertain"):
            unknown_usage += 1
        if response:
            for key in tokens:
                tokens[key] += int(response.get(key, 0))
    return {
        "attempt_count": len(attempts),
        "outcomes": counts,
        "reported_usage": tokens,
        "unknown_usage_attempts": unknown_usage,
    }
