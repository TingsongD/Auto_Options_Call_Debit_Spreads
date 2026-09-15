"""Property tests: payoff bounds and ledger conservation (ACCEPTANCE_TESTS §property)."""

from datetime import UTC, date, datetime
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from spx_research.domain.types import (
    Contract,
    CreditSpread,
    Direction,
    PricePoints,
    Right,
)
from spx_research.engine.accounting import (
    gross_max_expiration_loss_usd,
    gross_max_profit_usd,
    realized_pnl_usd,
)
from spx_research.engine.settlement import expiration_liability_points

strikes = st.decimals(min_value="100", max_value="9000", places=2)
widths = st.decimals(min_value="5", max_value="50", places=2)
settle_vals = st.decimals(min_value="1", max_value="12000", places=2)
credits = st.decimals(min_value="0.01", max_value="9.99", places=2)


def spread(direction: Direction, k_short: Decimal, width: Decimal) -> CreditSpread:
    def leg(cid: str, right: Right, k: Decimal) -> Contract:
        return Contract(
            contract_id=cid,
            root="SPXW",
            right=right,
            strike_points=PricePoints(k),
            expiration_local_date=date(2019, 2, 15),
            exercise_style="EUROPEAN",
            settlement_style="PM",
            multiplier=100,
            price_increment=Decimal("0.05"),
            listed_at_utc=datetime(2019, 1, 2, 13, 0, tzinfo=UTC),
        )

    if direction is Direction.BULL_PUT_CREDIT:
        return CreditSpread(
            leg("s", Right.PUT, k_short), leg("l", Right.PUT, k_short - width), direction
        )
    return CreditSpread(
        leg("s", Right.CALL, k_short), leg("l", Right.CALL, k_short + width), direction
    )


@given(direction=st.sampled_from(Direction), k=strikes, w=widths, s=settle_vals)
@settings(max_examples=200)
def test_expiration_liability_bounded(direction, k, w, s):
    """Expiration liability stays within [0, width] for valid structures."""
    sp = spread(direction, k, w)
    liability = expiration_liability_points(sp, s)
    assert Decimal(0) <= liability <= w


@given(c=credits)
@settings(max_examples=100)
def test_profit_loss_formulas(c):
    assert gross_max_profit_usd(c, 100) == c * 100
    assert gross_max_expiration_loss_usd(Decimal("10"), c, 100) == (Decimal("10") - c) * 100


@given(c=credits, d=st.decimals(min_value="0", max_value="9.99", places=2))
@settings(max_examples=100)
def test_realized_pnl_conserves(c, d):
    pnl = realized_pnl_usd(c, d, 100, Decimal("2"), Decimal("2"))
    assert pnl == (c - d) * 100 - 4
