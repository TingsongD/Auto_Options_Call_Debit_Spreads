"""M1-01 structural validation: contracts and one-lot credit spreads."""

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from spx_research.domain.types import (
    Contract,
    CreditSpread,
    Direction,
    DomainError,
    PricePoints,
    Right,
    ny_calendar_days,
)

LISTED = datetime(2019, 1, 2, 13, 0, tzinfo=UTC)
EXPIRY = date(2019, 2, 15)


def leg(cid: str, right: Right, strike: str, **kw) -> Contract:
    kw.setdefault("root", "SPXW")
    kw.setdefault("expiry", EXPIRY)
    kw.setdefault("multiplier", 100)
    return Contract(
        contract_id=cid,
        root=kw.pop("root"),
        right=right,
        strike_points=PricePoints(Decimal(strike)),
        expiration_local_date=kw.pop("expiry"),
        exercise_style="EUROPEAN",
        settlement_style="PM",
        multiplier=kw.pop("multiplier"),
        price_increment=Decimal("0.05"),
        listed_at_utc=LISTED,
        **kw,
    )


def bull_put() -> CreditSpread:
    return CreditSpread(
        short=leg("s", Right.PUT, "5000"),
        long=leg("l", Right.PUT, "4990"),
        direction=Direction.BULL_PUT_CREDIT,
    )


def bear_call() -> CreditSpread:
    return CreditSpread(
        short=leg("s", Right.CALL, "5000"),
        long=leg("l", Right.CALL, "5010"),
        direction=Direction.BEAR_CALL_CREDIT,
    )


def test_valid_structures_and_width():
    assert bull_put().width_points == 10
    assert bear_call().width_points == 10
    assert bull_put().multiplier == 100


def test_reversed_protective_ordering_rejected():  # T02
    with pytest.raises(DomainError):
        CreditSpread(
            short=leg("s", Right.PUT, "4990"),
            long=leg("l", Right.PUT, "5000"),
            direction=Direction.BULL_PUT_CREDIT,
        )
    with pytest.raises(DomainError):
        CreditSpread(
            short=leg("s", Right.CALL, "5010"),
            long=leg("l", Right.CALL, "5000"),
            direction=Direction.BEAR_CALL_CREDIT,
        )


def test_wrong_right_rejected():  # T01/T02
    with pytest.raises(DomainError):
        CreditSpread(
            short=leg("s", Right.CALL, "5000"),
            long=leg("l", Right.CALL, "4990"),
            direction=Direction.BULL_PUT_CREDIT,
        )


def test_mismatched_expiry_or_multiplier_rejected():  # T03
    with pytest.raises(DomainError):
        CreditSpread(
            short=leg("s", Right.PUT, "5000"),
            long=leg("l", Right.PUT, "4990", expiry=date(2019, 3, 15)),
            direction=Direction.BULL_PUT_CREDIT,
        )
    with pytest.raises(DomainError):
        CreditSpread(
            short=leg("s", Right.PUT, "5000"),
            long=leg("l", Right.PUT, "4990", multiplier=10),
            direction=Direction.BULL_PUT_CREDIT,
        )


def test_non_spx_root_rejected():  # T01
    with pytest.raises(DomainError):
        leg("x", Right.PUT, "5000", root="SPY")


def test_listing_visibility():  # T04
    c = leg("x", Right.PUT, "5000")
    assert not c.available_at(datetime(2019, 1, 2, 12, 0, tzinfo=UTC))
    assert c.available_at(LISTED)


def test_dte_calendar_days():
    assert bull_put().dte_calendar_days(date(2019, 1, 1)) == 45
    assert ny_calendar_days(date(2019, 1, 1), date(2019, 1, 26)) == 25


def test_naive_time_rejected():
    with pytest.raises(DomainError):
        replace(leg("x", Right.PUT, "5000"), listed_at_utc=datetime(2019, 1, 2, 13, 0))
