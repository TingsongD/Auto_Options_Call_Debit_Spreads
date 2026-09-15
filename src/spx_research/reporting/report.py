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
    result: RunResult, profile: Profile, dataset_manifest_id: str | None
) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "profile_id": profile.profile_id,
        "profile_mode": profile.mode,
        "dataset_manifest_id": dataset_manifest_id,
        "event_count": len(result.events),
        "event_log_sha256": event_log_digest(result.events),
        "decision_count": len(result.decisions),
        "final_cash_usd": str(result.final_state.account.cash),
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
        "run_id": result.run_id,
        "events": len(events),
        "event_types": by_type,
        "agents_total": len(st.agents),
        "positions_total": len(st.positions),
        "closed": len(closed),
        "settled": len(settled),
        "final_cash_usd": str(st.account.cash),
        "reserved_usd": str(st.account.reserved),
        "fees_paid_usd": str(st.account.fees_paid),
        "decisions": len(result.decisions),
        "rejections": sum(1 for d in result.decisions if "rejected" in d),
    }


def replay_summary(
    run_id: str, initial_cash: Decimal, events: list[Event]
) -> tuple[EngineState, str]:
    """Fold the committed log; returns state + log digest (no model calls)."""
    st = replay(run_id, initial_cash, events)
    return st, event_log_digest(events)
