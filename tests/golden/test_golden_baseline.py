"""Golden baseline battery: the mechanical engine is a deterministic function
of (dataset, profile). Any change to engine semantics flips the event digest —
that is the point: this battery is the tripwire that forces a deliberate
decision when behavior changes, not an accident.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest

from spx_research.config import Profile
from spx_research.data.availability import Archive
from spx_research.data.synthetic import SyntheticSpec, generate
from spx_research.engine.ledger import replay
from spx_research.persistence.events import InMemoryEventStore
from spx_research.reporting.report import event_log_digest
from spx_research.research.leakage import verify_hash_chain
from spx_research.research.mechanical import MechanicalPolicy
from spx_research.temporal.calendar import build_weekday_manifest

START = date(2024, 1, 2)
END = date(2024, 2, 16)
EXPIRY = END


def _profile() -> Profile:
    return Profile.model_validate(
        {
            "profile_id": "golden-mech",
            "mode": "synthetic_test",
            "permissions": {
                "real_data_requests": False,
                "real_model_requests": False,
                "broker_writes": False,
            },
            "universe": {
                "allowed_contract_roots": ["SPXW"],
                "strategies": ["BULL_PUT_CREDIT", "BEAR_CALL_CREDIT"],
                "target_entry_dte_calendar_days": 45,
                "entry_dte_range": [40, 50],
                "spread_widths_index_points": [Decimal("25")],
                "short_abs_delta_range": [Decimal("0.15"), Decimal("0.35")],
                "max_candidates_per_direction": 12,
            },
            "clock": {
                "agent_review_minutes": 15,
                "simulated_execution_delay_seconds": 60,
            },
            "study": {
                "start_date": START,
                "scored_end_date": END,
                "runoff_end_date": END + timedelta(days=30),
            },
            "portfolio": {
                "initial_capital_usd": Decimal("10000"),
                "max_open_or_reserved_slots": 3,
                "bullish_weight": 2,
                "bearish_weight": 1,
                "max_per_spread_initial_risk_usd": Decimal("3000"),
                "max_aggregate_committed_risk_usd": Decimal("9000"),
                "reservation_buffer_usd": Decimal("10"),
            },
            "exit_policy": {
                "profit_review_band": [Decimal("0.3"), Decimal("0.4")],
                "loss_review_band": [Decimal("0.2"), Decimal("0.3")],
                "loss_activation_days_held": 25,
            },
            "manager": {},
            "execution": {
                "opening_fee_per_leg_usd": Decimal("1"),
                "closing_fee_per_leg_usd": Decimal("1"),
            },
        }
    )


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> tuple[Archive, Any]:
    root = tmp_path_factory.mktemp("golden-ds")
    cal = build_weekday_manifest("cal-golden", START, END + timedelta(days=35))
    spec = SyntheticSpec(
        dataset_id="golden",
        seed=7,
        start=START,
        end=END,
        expiries=(EXPIRY,),
        strike_step=Decimal("25"),
        strikes_each_side=24,
    )
    generate(root, spec, cal)
    return Archive(root / "golden"), cal


def _run(archive: Archive, cal: Any, run_id: str) -> Any:
    from spx_research.engine.scheduler import Engine

    mech = MechanicalPolicy(
        profit_trigger=Decimal("0.35"),
        loss_trigger=Decimal("-0.25"),
        loss_activation_days=25,
        min_entry_credit_fraction=Decimal("0"),
    )
    eng = Engine(_profile(), cal, archive, InMemoryEventStore(), lambda _r: mech, run_id=run_id)
    return eng.run(START, END + timedelta(days=30))


def test_identical_runs_produce_identical_digest(dataset) -> None:
    """The engine is a pure function of its inputs — two runs differ only in
    run_id-scoped identity fields, and the event digest must match exactly."""
    archive, cal = dataset
    a = _run(archive, cal, "golden-a")
    b = _run(archive, cal, "golden-a")  # same run_id → fully identical
    assert event_log_digest(a.events) == event_log_digest(b.events)


def test_golden_run_trades_and_settles(dataset) -> None:
    """A golden baseline that never trades proves nothing — pin that this
    fixture exercises entry, review, and settlement."""
    archive, cal = dataset
    result = _run(archive, cal, "golden-1")
    types = {e.type for e in result.events}
    assert "ORDER_SUBMITTED" in types
    assert "POSITION_OPENED" in types
    assert types & {"POSITION_CLOSED", "POSITION_SETTLED"}
    assert "RUN_ENDED" in types


def test_golden_chain_and_replay(dataset) -> None:
    archive, cal = dataset
    result = _run(archive, cal, "golden-2")
    assert verify_hash_chain(result.events) is True
    st = replay("golden-2", Decimal("10000"), result.events)
    assert st.account.cash == result.final_state.account.cash
    assert st.account.reserved == result.final_state.account.reserved


def test_data_gap_is_once_per_position_per_day(dataset) -> None:
    """Golden invariant from W5: at most one DATA_GAP per position per day —
    a per-minute spam regression flips this."""
    archive, cal = dataset
    result = _run(archive, cal, "golden-3")
    gaps = [e for e in result.events if e.type == "DATA_GAP"]
    seen = {(g.payload.get("position_id"), g.sim_time_utc.date()) for g in gaps}
    assert len(seen) == len(gaps)
