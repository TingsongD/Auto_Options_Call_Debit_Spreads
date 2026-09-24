"""Regression and crash tests for minute barriers, data quality, runoff and equity."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from spx_research.domain.types import Direction, DomainError, Right
from spx_research.engine.policy import PolicyError, Proposal, Rejection, validate_spread_proposal
from spx_research.engine.scheduler import Engine
from spx_research.features.candidates import build_candidates
from spx_research.persistence.events import InMemoryEventStore, LedgerError
from spx_research.persistence.runtime import InMemoryRunStore
from spx_research.research.mechanical import MechanicalPolicy
from tests.engine_support import EXPIRY, START, TinyArchive, tiny_calendar, tiny_profile


def make_engine(archive=None, *, store=None, policy=None, expiry=False, run_id="safety"):
    calendar = tiny_calendar(expiry=expiry)
    archive = archive or TinyArchive(calendar)
    policy = policy or MechanicalPolicy(profit_trigger=Decimal("0.30"))
    return Engine(
        tiny_profile(runoff_end=EXPIRY if expiry else START),
        calendar,
        archive,
        store or InMemoryEventStore(),
        lambda _: policy,
        run_id=run_id,
    )


class FailOncePolicy:
    def __init__(self):
        self.failed = False
        self.spread_calls = 0
        self.delegate = MechanicalPolicy(profit_trigger=Decimal("0.30"))

    def decide(self, ctx):
        if ctx.role == "SPREAD" and not self.failed:
            self.spread_calls += 1
            if self.spread_calls == 2:
                self.failed = True
                raise PolicyError("SYNTHETIC_MODEL_FAILURE")
        return self.delegate.decide(ctx)


@pytest.mark.parametrize(
    "code", ["INVALID_USAGE", "BILLING_UNCERTAIN", "BUDGET_OVERRUN", "BUDGET_EXCEEDED"]
)
def test_unresolved_billing_pause_never_redispatches_on_resume(code):
    class BillingFailure:
        calls = 0

        def decide(self, ctx):
            self.calls += 1
            raise PolicyError(code)

    policy = BillingFailure()
    store = InMemoryRunStore()
    first = make_engine(store=store, policy=policy).run(START, START)
    assert first.status == "PAUSED"
    assert first.pause["category"] == "BUDGET" and first.pause["code"] == code
    resumed = make_engine(store=store, policy=policy).run(START, START)
    assert resumed.status == "PAUSED" and resumed.pause == first.pause
    assert policy.calls == 1


def test_failed_actor_rolls_back_whole_decision_barrier_and_resumes_exactly():
    store = InMemoryRunStore()
    policy = FailOncePolicy()
    engine = make_engine(store=store, policy=policy)
    paused = engine.run(START, START)
    assert paused.status == "PAUSED"
    assert paused.pause["category"] == "MODEL"
    assert not [e for e in paused.events if e.type in ("ORDER_SUBMITTED", "RUN_ENDED")]
    assert paused.final_state.account.reserved == Decimal("3000")
    assert policy.spread_calls == 2  # third actor never called after the failure
    assert store.load_run("safety").cursor["next_phase"] == "DECISION"
    resumed = make_engine(store=store, policy=policy).run(START, START)
    reference = make_engine(store=InMemoryRunStore()).run(START, START)
    assert resumed.status == "COMPLETED"
    assert [e.event_hash for e in resumed.events] == [e.event_hash for e in reference.events]
    assert sum(e.type == "RUN_STARTED" for e in resumed.events) == 1


def test_missing_second_fill_quote_commits_no_partial_market_effects():
    class PartialArchive(TinyArchive):
        failed = True

        def quote_at(self, cid, at, **kwargs):
            if (
                self.failed
                and cid == "pl"
                and at == self.calendar.sessions[0].open_utc() + timedelta(minutes=31)
            ):
                return None
            return super().quote_at(cid, at, **kwargs)

    archive = PartialArchive(tiny_calendar())
    store = InMemoryRunStore()
    paused = make_engine(archive, store=store).run(START, START)
    assert paused.status == "PAUSED" and paused.pause["category"] == "DATA"
    assert paused.final_state.account.cash == Decimal("10000")
    assert not paused.final_state.open_positions()
    assert not [e for e in paused.events if e.type == "ORDER_RESOLVED"]
    archive.failed = False  # restore access to the same synthetic values
    resumed = make_engine(archive, store=store).run(START, START)
    reference = make_engine(store=InMemoryRunStore()).run(START, START)
    assert [e.event_hash for e in resumed.events] == [e.event_hash for e in reference.events]


def test_held_quote_outage_retains_position_and_unknown_equity():
    archive = TinyArchive(tiny_calendar())
    archive.outage_at = archive.calendar.sessions[0].open_utc() + timedelta(minutes=32)
    result = make_engine(archive).run(START, START)
    assert result.status == "PAUSED"
    assert len(result.final_state.open_positions()) == 3
    assert result.valuations[-1]["net_liquidation_equity_usd"] is None
    assert len(result.valuations[-1]["positions"]) == 3
    assert result.events[-1].sim_time_utc < archive.outage_at
    assert not any(e.type == "RUN_ENDED" for e in result.events)


def test_provider_outage_differs_from_healthy_empty_candidate_set():
    archive = TinyArchive(tiny_calendar())
    archive.coverage = "MISSING_SESSION"
    result = make_engine(archive).run(START, START)
    assert result.status == "PAUSED" and result.pause["code"] == "MISSING_SESSION"
    archive.coverage, archive.crossed = "HEALTHY", True
    result = make_engine(archive).run(START, START)
    assert result.status == "COMPLETED"
    assert not result.final_state.positions
    assert any(e.type == "DATA_QUALITY_FINDING" for e in result.events)
    assert any(d["proposal"] == "WAIT" for d in result.decisions)


@pytest.mark.parametrize("flag", ["crossed", "known_stale", "mismatched"])
def test_bad_quotes_never_enter_candidate_menu(flag):
    archive = TinyArchive(tiny_calendar())
    setattr(archive, flag, True)
    at = archive.calendar.sessions[0].open_utc() + timedelta(minutes=30)
    found = build_candidates(
        archive,
        at,
        START,
        Direction.BULL_PUT_CREDIT,
        (40, 50),
        [Decimal("10")],
        (Decimal(".15"), Decimal(".35")),
        Decimal("1000"),
        Decimal("1000"),
        12,
        max_event_age_seconds=60,
    )
    assert found == []


def test_next_review_after_fill_is_next_anchored_grid():
    result = make_engine().run(START, START)
    opened = next(e for e in result.events if e.type == "POSITION_OPENED")
    aid = opened.payload["position"]["agent_id"]
    reviews = [d["at"] for d in result.decisions if d["actor"] == aid]
    next_review = opened.sim_time_utc + timedelta(minutes=14)
    assert next_review.isoformat() in reviews


def test_runoff_only_manages_existing_positions_and_settles_at_publication():
    archive = TinyArchive(tiny_calendar(expiry=True), take_profit=False)
    hold = MechanicalPolicy(profit_trigger=Decimal("999"), loss_activation_days=999)
    result = make_engine(archive, expiry=True, policy=hold).run(START, EXPIRY)
    assert result.status == "COMPLETED"
    assert (
        result.scored_end_valuation["as_of"] == archive.calendar.sessions[0].close_utc().isoformat()
    )
    assert not [
        e for e in result.events if e.type == "RESERVATION_HELD" and e.sim_time_utc.date() > START
    ]
    settlements = [e for e in result.events if e.type == "POSITION_SETTLED"]
    published = archive.calendar.sessions[-1].close_utc() + timedelta(minutes=30)
    assert len(settlements) == 3 and all(e.sim_time_utc == published for e in settlements)
    assert result.events[-1].sim_time_utc == published
    assert result.final_state.account.cash == Decimal("10088")
    assert result.final_state.account.reserved == 0
    assert all(
        a.sim_time_utc <= b.sim_time_utc
        for a, b in zip(result.events, result.events[1:], strict=False)
    )


def test_missing_settlement_pauses_without_fabricated_value():
    archive = TinyArchive(tiny_calendar(expiry=True), take_profit=False)
    archive.settlement_missing = True
    hold = MechanicalPolicy(profit_trigger=Decimal("999"), loss_activation_days=999)
    result = make_engine(archive, expiry=True, policy=hold).run(START, EXPIRY)
    assert result.status == "PAUSED" and result.pause["code"] == "MISSING_SETTLEMENT_VALUE"
    assert len(result.final_state.open_positions()) == 3
    assert not any(e.type == "POSITION_SETTLED" for e in result.events)


def test_mixed_right_and_cross_candidate_limits_rejected():
    engine = make_engine()
    spread = build_candidates(
        engine.archive,
        engine.calendar.sessions[0].open_utc(),
        START,
        Direction.BULL_PUT_CREDIT,
        (40, 50),
        [Decimal("10")],
        (Decimal(".15"), Decimal(".35")),
        Decimal("1000"),
        Decimal("1000"),
        12,
    )[0].spread
    with pytest.raises(DomainError, match="LEG_MISMATCH"):
        replace(spread, long=replace(spread.long, right=Right.CALL))

    class CheckLimits:
        def decide(self, ctx):
            if ctx.role == "SPREAD" and ctx.spread_view.candidates:
                candidate = ctx.spread_view.candidates[0]
                view = replace(
                    ctx.spread_view,
                    entry_limit_templates=(
                        replace(
                            ctx.spread_view.entry_limit_templates[0], template_id="entry:other"
                        ),
                    ),
                )
                with pytest.raises(Rejection, match="CANDIDATE_LIMIT_MISMATCH"):
                    validate_spread_proposal(
                        replace(ctx, spread_view=view),
                        Proposal(
                            "OPEN",
                            candidate_id=candidate.candidate_id,
                            limit_template_id="entry:other",
                        ),
                    )
            return MechanicalPolicy(profit_trigger=Decimal(".30")).decide(ctx)

    assert make_engine(policy=CheckLimits()).run(START, START).status == "COMPLETED"


def test_resume_rejects_event_tampering_and_cursor_mismatch():
    store = InMemoryRunStore()
    make_engine(store=store, policy=FailOncePolicy()).run(START, START)
    row = store._event_store._events["safety"][0]
    store._event_store._events["safety"][0] = replace(
        row, payload={**row.payload, "mode": "tampered"}
    )
    with pytest.raises(DomainError, match="RESUME_EVENT_CHAIN_MISMATCH"):
        make_engine(store=store).run(START, START)


def test_commit_acknowledgement_loss_resumes_from_durable_fill_cursor():
    class LostAcknowledgement(InMemoryRunStore):
        fail = True

        def commit_barrier(self, *args, **kwargs):
            committed = super().commit_barrier(*args, **kwargs)
            if self.fail and any(e.type == "POSITION_OPENED" for e in committed):
                self.fail = False
                raise RuntimeError("synthetic crash after commit")
            return committed

    store = LostAcknowledgement()
    with pytest.raises(RuntimeError, match="after commit"):
        make_engine(store=store).run(START, START)
    assert len([e for e in store.events("safety") if e.type == "POSITION_OPENED"]) == 3
    resumed = make_engine(store=store).run(START, START)
    reference = make_engine(store=InMemoryRunStore()).run(START, START)
    assert [e.event_hash for e in resumed.events] == [e.event_hash for e in reference.events]


def test_resume_rejects_cursor_that_does_not_match_authoritative_tip():
    store = InMemoryRunStore()
    make_engine(store=store, policy=FailOncePolicy()).run(START, START)
    record = store.load_run("safety")
    with pytest.raises(LedgerError, match="STALE_CURSOR_WRITE"):
        store.persist_cursor("safety", {**record.cursor, "ledger_seq": 999}, "PAUSED", record.pause)
    # Bypass the safe setter to verify the engine independently rejects a
    # corrupted durable record during recovery.
    store._rows["runtime_runs"][0]["cursor"]["ledger_seq"] = 999
    with pytest.raises(DomainError, match="RESUME_CURSOR_LINEAGE_MISMATCH"):
        make_engine(store=store).run(START, START)


def test_engine_preserves_above_width_liquidation_and_fill():
    class WideClose(TinyArchive):
        def quote_at(self, cid, at, **kwargs):
            row = super().quote_at(cid, at, **kwargs)
            if at >= self.calendar.sessions[0].open_utc() + timedelta(minutes=45):
                row["bid_points"], row["ask_points"] = (
                    (Decimal("11.90"), Decimal("12.00"))
                    if cid.endswith("s")
                    else (Decimal("0"), Decimal(".10"))
                )
            return row

    class CloseAtReview:
        def decide(self, ctx):
            if ctx.role == "SPREAD" and ctx.spread_view.position:
                return Proposal(
                    "CLOSE",
                    position_id=ctx.spread_view.position.position_id,
                    limit_template_id="exit-natural",
                )
            return MechanicalPolicy().decide(ctx)

    result = make_engine(WideClose(tiny_calendar()), policy=CloseAtReview()).run(START, START)
    assert result.status == "COMPLETED"
    assert result.final_state.account.cash == Decimal("6988")
    assert any(
        "LIQUIDATION_ABOVE_WIDTH" in p.get("anomalies", [])
        for v in result.valuations
        for p in v["positions"]
    )
    assert [
        e.payload["close_debit_points"] for e in result.events if e.type == "POSITION_CLOSED"
    ] == ["12.00"] * 3


def test_weekend_scored_boundary_uses_last_prior_session_close():
    from datetime import date, time

    from spx_research.temporal.calendar import CalendarManifest, SessionDay

    friday, saturday = date(2024, 1, 5), date(2024, 1, 6)
    calendar = CalendarManifest(
        "weekend-synthetic",
        "test",
        (
            SessionDay(START, time(9, 30), time(10, 17)),
            SessionDay(friday, time(9, 30), time(10, 17)),
        ),
    )

    class NoAllocations:
        def decide(self, ctx):
            return Proposal("NO_CHANGE")

    profile = tiny_profile(scored_end=saturday, runoff_end=saturday)
    result = Engine(
        profile, calendar, TinyArchive(calendar), InMemoryEventStore(), lambda _: NoAllocations()
    ).run(START, saturday)
    assert result.status == "COMPLETED"
    assert result.scored_end_valuation["as_of"] == calendar.sessions[-1].close_utc().isoformat()
    assert result.scored_end_valuation["net_liquidation_equity_usd"] == "10000.00"


@pytest.mark.parametrize(
    "equity", [None, Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")]
)
def test_impaired_equity_prevents_allocations_and_new_entries(equity, monkeypatch):
    engine = make_engine(policy=FailOncePolicy())
    result = engine.run(START, START)
    assert result.status == "PAUSED"
    monkeypatch.setattr(engine, "_equity", lambda: equity)
    session = engine.calendar.sessions[0]
    at = session.open_utc() + timedelta(minutes=30)
    context = engine._manager_context(at, 30, 0)
    assert context.manager_view.paused
    with pytest.raises(Rejection, match="EQUITY_NOT_POSITIVE"):
        engine._check_manager_capacity(Proposal("ALLOCATE", allocation={"bullish": 1}), at)
    engine._check_manager_capacity(Proposal("NO_CHANGE"), at)
    for agent in engine.state.live_agents():
        if agent.role == "SPREAD":
            view = engine._spread_view(agent, session, at)
            assert not view.candidates and not view.entry_limit_templates


def test_partial_postclose_settlement_updates_cash_with_remaining_unknown_mark():
    from datetime import date, time

    from spx_research.temporal.calendar import CalendarManifest, SessionDay

    later_expiry = date(2024, 2, 20)
    base = tiny_calendar(expiry=True)
    calendar = CalendarManifest(
        base.calendar_id,
        base.version,
        (*base.sessions, SessionDay(later_expiry, time(9, 30), time(10, 17))),
    )

    class SplitExpiry(TinyArchive):
        def contracts_visible_at(self, at):
            rows = super().contracts_visible_at(at)
            for row in rows:
                if row["right"] == "CALL":
                    row["expiration_local_date"] = later_expiry
                    row["last_trading_at_utc"] = calendar.session(later_expiry).close_utc()
                    row["settlement_event_at_utc"] = row["last_trading_at_utc"]
            return rows

        def quote_at(self, cid, at, **kwargs):
            session = calendar.session(at.date())
            if session and at > session.close_utc():
                return None
            return super().quote_at(cid, at, **kwargs)

    archive = SplitExpiry(calendar, take_profit=False)
    hold = MechanicalPolicy(profit_trigger=Decimal("999"), loss_activation_days=999)
    result = Engine(
        tiny_profile(runoff_end=EXPIRY), calendar, archive, InMemoryEventStore(), lambda _: hold
    ).run(START, EXPIRY)
    assert result.pause["code"] == "RUNOFF_INCOMPLETE"
    assert sum(e.type == "POSITION_SETTLED" for e in result.events) == 2
    assert result.final_state.account.cash == Decimal("10590.00")
    assert result.final_state.account.reserved == Decimal("1000.00")
    latest = result.valuations[-1]
    assert latest["cash_usd"] == "10590.00"
    assert latest["net_liquidation_equity_usd"] is None
    assert latest["quality"] == "UNPRICEABLE"
    assert len(latest["positions"]) == 1
