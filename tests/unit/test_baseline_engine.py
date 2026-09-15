"""M3 baseline: candidates, mechanical policy, scheduler, ledger, replay.

Smoke tests run the engine over a small synthetic dataset whose only expiry
settles inside the run window — exercising entry → review → settlement and
ledger reconciliation end to end (T45/T47 replay equivalence, T04 listing
visibility, T06 next-minute fills).
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest

from spx_research.config import Profile
from spx_research.data.availability import Archive
from spx_research.data.synthetic import SyntheticSpec, generate
from spx_research.domain.state import (
    Agent,
    AgentState,
    PositionStatus,
)
from spx_research.domain.types import Direction
from spx_research.engine.ledger import replay
from spx_research.engine.policy import (
    DecisionContext,
    LimitTemplate,
    ManagerView,
    SpreadView,
)
from spx_research.engine.scheduler import Engine
from spx_research.features.candidates import build_candidates
from spx_research.reporting.report import event_log_digest
from spx_research.research.mechanical import MechanicalPolicy
from spx_research.temporal.calendar import build_weekday_manifest

START = date(2024, 1, 2)
END = date(2024, 2, 16)  # Friday; also the single contract expiry (DTE 45)
EXPIRY = END


def _profile_dict() -> dict[str, Any]:
    return {
        "profile_id": "syn-smoke",
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
            "runoff_end_date": END,
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
        "fixture": {
            "description": "t",
            "multiplier": 100,
            "width_index_points": Decimal("25"),
            "initial_credit_index_points": Decimal("2"),
            "final_debit_index_points": Decimal("1"),
            "opening_fee_per_leg_usd": Decimal("1"),
            "closing_fee_per_leg_usd": Decimal("1"),
            "expected_gross_max_profit_usd": Decimal("200"),
            "expected_gross_max_expiration_loss_usd": Decimal("2300"),
            "expected_net_closed_pnl_usd": Decimal("96"),
            "expected_final_cash_usd": Decimal("10096"),
            "expected_profit_fraction_of_gross_credit": Decimal("0.5"),
        },
    }


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> tuple[Archive, Any]:
    root = tmp_path_factory.mktemp("ds")
    cal = build_weekday_manifest("cal-test", START, END + timedelta(days=5))
    spec = SyntheticSpec(
        dataset_id="smoke",
        seed=7,
        start=START,
        end=END,
        expiries=(EXPIRY,),
        strike_step=Decimal("25"),
        strikes_each_side=24,
    )
    generate(root, spec, cal)
    return Archive(root / "smoke"), cal


def _engine(archive: Archive, cal: Any) -> Engine:
    profile = Profile.model_validate(_profile_dict())
    mech = MechanicalPolicy(
        profit_trigger=Decimal("0.35"),
        loss_trigger=Decimal("-0.25"),
        loss_activation_days=25,
        min_entry_credit_fraction=Decimal("0"),
    )
    from spx_research.persistence.events import InMemoryEventStore

    return Engine(profile, cal, archive, InMemoryEventStore(), lambda _r: mech)


def _first_review(archive: Archive, cal: Any) -> Any:
    sess = cal.session(START)
    assert sess is not None
    return sess.open_utc() + timedelta(minutes=15)


def test_candidates_filtered_deterministic(dataset: tuple[Archive, Any]) -> None:
    archive, cal = dataset
    t = _first_review(archive, cal)
    args = dict(
        dte_range=(40, 50),
        widths=[Decimal("25")],
        delta_range=(Decimal("0.15"), Decimal("0.35")),
        reserve_per_spread_usd=Decimal("2510"),
        max_risk_usd=Decimal("3000"),
        max_candidates=12,
    )
    a = build_candidates(archive, t, START, Direction.BULL_PUT_CREDIT, **args)
    b = build_candidates(archive, t, START, Direction.BULL_PUT_CREDIT, **args)
    assert [c.candidate_id for c in a] == [c.candidate_id for c in b]
    assert len(a) <= 12
    for c in a:
        assert 40 <= c.dte <= 50
        assert c.credit_points > 0
        assert c.spread.direction is Direction.BULL_PUT_CREDIT
        assert c.short_delta is not None
        assert Decimal("0.15") <= abs(c.short_delta) <= Decimal("0.35")


def test_candidates_respect_availability(dataset: tuple[Archive, Any]) -> None:
    archive, cal = dataset
    before_open = cal.session(START).open_utc() - timedelta(hours=2)
    out = build_candidates(
        archive,
        before_open,
        START,
        Direction.BULL_PUT_CREDIT,
        dte_range=(40, 50),
        widths=[Decimal("25")],
        delta_range=(Decimal("0.15"), Decimal("0.35")),
        reserve_per_spread_usd=Decimal("2510"),
        max_risk_usd=Decimal("3000"),
        max_candidates=12,
    )
    assert out == []  # no same-session quotes before the open (T04/T31)


def _mgr_ctx(counts: dict[str, int]) -> DecisionContext:
    view = ManagerView(
        as_of_utc=None,  # type: ignore[arg-type]
        active_bullish=counts.get("ab", 0),
        active_bearish=counts.get("aa", 0),
        reserved_bullish=counts.get("rb", 0),
        reserved_bearish=counts.get("ra", 0),
        capacity=3,
        bullish_target=2,
        bearish_target=1,
        paused=False,
        available_usd=Decimal("10000"),
        reservations=(),
    )
    return DecisionContext(
        "r",
        "main",
        "manager-1",
        "MANAGER",
        None,
        0,
        0,  # type: ignore[arg-type]
        manager_view=view,
    )


def test_mechanical_manager_allocates_toward_2to1() -> None:
    mech = MechanicalPolicy()
    p = mech.decide(_mgr_ctx({}))
    assert p.kind == "ALLOCATE"
    assert p.allocation == {"bullish": 2, "bearish": 1}
    p2 = mech.decide(_mgr_ctx({"ab": 2, "aa": 1}))
    assert p2.kind == "NO_CHANGE"


def _mini_spread() -> Any:
    from spx_research.domain.types import Contract, CreditSpread, PricePoints, Right

    def leg(right: Right, strike: int) -> Contract:
        return Contract(
            contract_id=f"c{right.value[0]}{strike}",
            root="SPXW",
            right=right,
            strike_points=PricePoints(Decimal(strike)),
            expiration_local_date=EXPIRY,
            exercise_style="EUROPEAN",
            settlement_style="PM",
            multiplier=100,
            price_increment=Decimal("0.05"),
        )

    return CreditSpread(leg(Right.PUT, 4700), leg(Right.PUT, 4675), Direction.BULL_PUT_CREDIT)


def _spread_ctx(state: AgentState, frac: Decimal | None, days: int) -> DecisionContext:
    from datetime import UTC, datetime

    from spx_research.domain.state import Position

    now = datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
    pos = None
    pos_id = "p1" if state is AgentState.OPEN else None
    if pos_id:
        pos = Position(
            pos_id,
            "a1",
            _mini_spread(),
            Decimal("2"),
            Decimal("2"),
            now,
            START,
            Decimal("2510"),
            PositionStatus.OPEN,
        )
    agent = Agent("a1", "SPREAD", Direction.BULL_PUT_CREDIT, state, now, now, position_id=pos_id)
    view = SpreadView(
        agent,
        pos,
        Decimal("1.0"),
        frac,
        days,
        (),
        (LimitTemplate("entry:x", "NATURAL", Decimal("2")),),
        (LimitTemplate("exit:x", "NATURAL", Decimal("1")),),
    )
    return DecisionContext("r", "main", "a1", "SPREAD", now, 0, 0, spread_view=view)


def test_mechanical_spread_profit_and_loss_bands() -> None:
    mech = MechanicalPolicy(
        profit_trigger=Decimal("0.35"), loss_trigger=Decimal("-0.25"), loss_activation_days=25
    )
    # profit trigger fires regardless of age
    p = mech.decide(_spread_ctx(AgentState.OPEN, Decimal("0.40"), 3))
    assert p.kind == "CLOSE"
    # young loss does not fire
    p2 = mech.decide(_spread_ctx(AgentState.OPEN, Decimal("-0.30"), 10))
    assert p2.kind == "HOLD"
    # aged loss fires
    p3 = mech.decide(_spread_ctx(AgentState.OPEN, Decimal("-0.30"), 30))
    assert p3.kind == "CLOSE"


def test_smoke_run_reconciles(dataset: tuple[Archive, Any]) -> None:
    archive, cal = dataset
    engine = _engine(archive, cal)
    result = engine.run(START, END)
    ev_types = [e.type for e in result.events]
    assert "POSITION_OPENED" in ev_types  # mechanical found + filled entries
    # every opened position eventually closed or settled
    opened = [e for e in result.events if e.type == "POSITION_OPENED"]
    done = {
        e.payload["position_id"]
        for e in result.events
        if e.type in ("POSITION_CLOSED", "POSITION_SETTLED")
    }
    for e in opened:
        assert e.payload["position"]["position_id"] in done
    # entry orders fill the minute after submission (T06)
    for e in result.events:
        if e.type == "ORDER_SUBMITTED":
            assert e.payload["first_eligible_at_utc"] > e.payload["submitted_at_utc"]
    # no capital left encumbered after the run; reservations released
    st = result.final_state
    assert st.account.reserved == 0
    assert not st.open_positions()
    assert all(a.state is AgentState.ARCHIVED for a in st.agents.values())


def test_replay_matches_engine_state(dataset: tuple[Archive, Any]) -> None:
    archive, cal = dataset
    result = _engine(archive, cal).run(START, END)
    st = replay(result.run_id, Decimal("10000"), result.events)
    assert st.account.cash == result.final_state.account.cash
    assert st.account.reserved == result.final_state.account.reserved
    assert st.account.fees_paid == result.final_state.account.fees_paid
    assert set(st.positions) == set(result.final_state.positions)


def test_run_is_deterministic(dataset: tuple[Archive, Any]) -> None:
    archive, cal = dataset
    r1 = _engine(archive, cal).run(START, END)
    r2 = _engine(archive, cal).run(START, END)
    assert event_log_digest(r1.events) == event_log_digest(r2.events)


def test_future_suffix_does_not_change_prefix(
    dataset: tuple[Archive, Any],
    tmp_path: Any,
) -> None:
    """TK12 at the application layer: mutate everything after a cutoff —
    post-cutoff sessions, later Greeks vintages, settlements, macro — and the
    engine's prefix event log must remain byte-identical."""
    import shutil
    from datetime import UTC, datetime

    import polars as pl

    archive_a, cal = dataset
    cutoff = date(2024, 1, 3)
    cutoff_ts = datetime(2024, 1, 4, 0, 0, tzinfo=UTC)

    root_b = tmp_path / "dsB"
    shutil.copytree(archive_a.root, root_b)
    for qf in (root_b / "quotes").glob("session=*.parquet"):
        day = date.fromisoformat(qf.stem.split("=")[1])
        if day > cutoff:
            df = pl.read_parquet(qf)
            df.with_columns(
                (pl.col("bid_points") * 2).alias("bid_points"),
                (pl.col("ask_points") * 2).alias("ask_points"),
            ).write_parquet(qf)
    gp = root_b / "meta" / "greeks.parquet"
    pl.read_parquet(gp).with_columns(
        pl.when(pl.col("asof_utc") > cutoff_ts)
        .then(pl.col("delta") * 0.5)
        .otherwise(pl.col("delta"))
        .alias("delta")
    ).write_parquet(gp)
    sp = root_b / "meta" / "settlements.parquet"
    pl.read_parquet(sp).with_columns(
        (pl.col("value_index_points") * 1.1).alias("value_index_points")
    ).write_parquet(sp)
    mp = root_b / "macro" / "vintages.parquet"
    mdf = pl.read_parquet(mp)
    if "value" in mdf.columns:
        mdf.with_columns(
            pl.when(pl.col("simulated_available_at_utc") > cutoff_ts)
            .then(pl.col("value") + "-MUT")
            .otherwise(pl.col("value"))
            .alias("value")
        ).write_parquet(mp)

    r_a = _engine(archive_a, cal).run(START, cutoff)
    r_b = _engine(Archive(root_b), cal).run(START, cutoff)
    assert event_log_digest(r_a.events) == event_log_digest(r_b.events)


def test_reservation_lifecycle_visible_in_events(
    dataset: tuple[Archive, Any],
) -> None:
    archive, cal = dataset
    result = _engine(archive, cal).run(START, END)
    held = [e for e in result.events if e.type == "RESERVATION_HELD"]
    released = [
        e.payload["reservation_id"] for e in result.events if e.type == "RESERVATION_RELEASED"
    ]
    # reservations either converted to fills or were released exactly once
    for e in held:
        rid = e.payload["reservation_id"]
        statuses = [
            ev.payload["status"]
            for ev in result.events
            if ev.type == "RESERVATION_STATUS" and ev.payload["reservation_id"] == rid
        ]
        assert "FILLED" in statuses or rid in released


def _engine_with(archive: Archive, cal: Any, **profile_overrides: Any) -> Engine:
    """Engine over the smoke dataset with a fully-hold policy (never exits)."""
    import copy

    from spx_research.persistence.events import InMemoryEventStore

    d = copy.deepcopy(_profile_dict())
    for section, kv in profile_overrides.items():
        d[section].update(kv)
    profile = Profile.model_validate(d)
    hold = MechanicalPolicy(
        profit_trigger=Decimal("999"),
        loss_trigger=Decimal("-999"),
        loss_activation_days=9999,
        min_entry_credit_fraction=Decimal("0"),
    )
    return Engine(profile, cal, archive, InMemoryEventStore(), lambda _r: hold)


def test_held_to_expiry_settles(dataset: tuple[Archive, Any]) -> None:
    """T36/B1: a position still open on expiry day must settle when the PM
    value publishes (close+30m) — not strand OPEN with reserve encumbered."""
    archive, cal = dataset
    result = _engine_with(archive, cal).run(START, END)
    opened = [e for e in result.events if e.type == "POSITION_OPENED"]
    assert opened, "fixture should open at least one position"
    settled = {e.payload["position_id"] for e in result.events if e.type == "POSITION_SETTLED"}
    for e in opened:
        pid = e.payload["position"]["position_id"]
        assert pid in settled, f"{pid} never settled"
    st = result.final_state
    assert not st.open_positions()
    assert st.account.reserved == 0
    gaps = [
        e
        for e in result.events
        if e.type == "DATA_GAP" and e.payload.get("kind") == "settlement_unavailable"
    ]
    # Expiry-day gaps may occur intraday (value not yet published) but every
    # position must eventually settle — no permanent stranding.
    stranded = {g.payload["position_id"] for g in gaps} - settled
    assert not stranded


def test_close_fill_uses_closing_fee(dataset: tuple[Archive, Any]) -> None:
    """H2: CLOSE fills must book closing_fee_per_leg_usd, not the opening fee."""
    archive, cal = dataset
    engine = _engine_with(
        archive,
        cal,
        exit_policy={
            "profit_review_band": [Decimal("0.3"), Decimal("0.4")],
            "loss_review_band": [Decimal("0.2"), Decimal("0.3")],
            "loss_activation_days_held": 25,
        },
        execution={
            "opening_fee_per_leg_usd": Decimal("1"),
            "closing_fee_per_leg_usd": Decimal("3"),
        },
    )
    # restore a normal-profit policy so closes actually happen
    import copy

    from spx_research.persistence.events import InMemoryEventStore

    d = copy.deepcopy(_profile_dict())
    d["execution"] = {
        "opening_fee_per_leg_usd": Decimal("1"),
        "closing_fee_per_leg_usd": Decimal("3"),
    }
    profile = Profile.model_validate(d)
    mech = MechanicalPolicy(
        profit_trigger=Decimal("0.35"),
        loss_trigger=Decimal("-0.25"),
        loss_activation_days=25,
        min_entry_credit_fraction=Decimal("0"),
    )
    engine = Engine(profile, cal, archive, InMemoryEventStore(), lambda _r: mech)
    result = engine.run(START, END)
    order_intent = {
        e.payload["order_id"]: e.payload["intent"]
        for e in result.events
        if e.type == "ORDER_SUBMITTED"
    }
    close_fills = [
        e
        for e in result.events
        if e.type == "ORDER_RESOLVED"
        and e.payload.get("status") == "FILLED"
        and order_intent.get(e.payload["order_id"]) == "CLOSE"
    ]
    assert close_fills, "fixture should produce at least one close fill"
    for e in close_fills:
        assert e.payload["fees_usd"] == "6.00"  # 2 legs x $3 closing fee


def test_aggregate_reserve_cap_rejects_whole_allocation(
    dataset: tuple[Archive, Any],
) -> None:
    """H3: committed + new must respect the aggregate cap; over-cap is a
    Rejection (DECISION_REJECTED), never a crash."""
    archive, cal = dataset
    engine = _engine_with(
        archive,
        cal,
        portfolio={
            "max_aggregate_committed_risk_usd": Decimal("2600"),  # < 2 reserves
        },
    )
    result = engine.run(START, END)
    rejected = [
        e
        for e in result.events
        if e.type == "DECISION_REJECTED" and e.payload.get("code") == "RESERVE_EXCEEDS_RISK_LIMIT"
    ]
    assert rejected, "over-cap allocation should be rejected, not crash"
    held = [e for e in result.events if e.type == "RESERVATION_HELD"]
    assert len(held) <= 1  # at most one reserve fits under the cap
