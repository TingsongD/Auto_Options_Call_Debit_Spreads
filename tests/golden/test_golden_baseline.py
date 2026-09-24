"""Frozen synthetic T20 portfolio: three independent one-lot spreads.

Each opens at 2 points, closes at 1.30 points, and pays $1 per leg each
way. Hand reconciliation: cash 10000 + 3 * (200 - 130 - 4) = 10198;
fees $12, zero remaining reserve. This fixture has no paid/provider data.
"""

from decimal import Decimal

from spx_research.engine.ledger import replay
from spx_research.engine.scheduler import Engine
from spx_research.persistence.events import InMemoryEventStore
from spx_research.reporting.report import event_log_digest
from spx_research.research.leakage import verify_hash_chain
from spx_research.research.mechanical import MechanicalPolicy
from tests.engine_support import START, TinyArchive, tiny_calendar, tiny_profile


def run_golden():
    calendar = tiny_calendar()
    policy = MechanicalPolicy(profit_trigger=Decimal("0.30"))
    return Engine(
        tiny_profile(),
        calendar,
        TinyArchive(calendar),
        InMemoryEventStore(),
        lambda _: policy,
        run_id="golden-hand-v2",
    ).run(START, START)


def test_hand_reconciled_cash_equity_and_frozen_digest():
    result = run_golden()
    assert result.status == "COMPLETED"
    assert result.final_state.account.cash == Decimal("10198.00")
    assert result.final_state.account.reserved == 0
    assert result.final_state.account.fees_paid == Decimal("12.00")
    assert len([e for e in result.events if e.type == "POSITION_CLOSED"]) == 3
    assert result.valuations[-1]["net_liquidation_equity_usd"] == "10198.00"
    # Fixed after checking each cash movement and valuation above, not calculated by the test.
    assert (
        event_log_digest(result.events)
        == "9ab344f7e32d2463dd45d14e6940e1e7fd2108f50f4c0bbcd1e4c9894faf7a2c"
    )


def test_entry_credit_is_offset_by_liability_and_not_reserve():
    result = run_golden()
    mark = next(v for v in result.valuations if len(v["positions"]) == 3)
    assert mark["cash_usd"] == "10594.00"  # three credits of 200, opening fees 6
    assert mark["reserved_usd"] == "3000.00"
    assert mark["mid_liability_usd"] == "630.00"
    assert mark["liquidation_liability_usd"] == "660.00"
    assert mark["mid_equity_usd"] == "9964.00"
    assert mark["net_liquidation_equity_usd"] == "9928.00"  # deduct estimated closing fees 6


def test_frozen_chain_and_replay():
    result = run_golden()
    assert verify_hash_chain(result.events)
    restored = replay(result.run_id, Decimal("10000"), result.events)
    assert restored.account == result.final_state.account
    assert restored.positions == result.final_state.positions
    assert all(
        a.sim_time_utc <= b.sim_time_utc
        for a, b in zip(result.events, result.events[1:], strict=False)
    )
