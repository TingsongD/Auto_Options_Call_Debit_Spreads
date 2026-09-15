"""M1-05 settlement payoffs (T34/T35) and missing-value gate (T36)."""

from decimal import Decimal

import pytest

from spx_research.domain.types import DomainError
from spx_research.engine.settlement import (
    expiration_liability_points,
    expiration_liability_usd,
    require_settlement_value,
)
from tests.unit.test_domain import bear_call, bull_put


@pytest.mark.parametrize("settle,expected", [("5005", 0), ("4995", 5), ("4985", 10)])
def test_bull_put_payoffs(settle, expected):  # T34
    assert expiration_liability_points(bull_put(), Decimal(settle)) == Decimal(expected)


@pytest.mark.parametrize("settle,expected", [("4995", 0), ("5005", 5), ("5015", 10)])
def test_bear_call_payoffs(settle, expected):  # T35
    assert expiration_liability_points(bear_call(), Decimal(settle)) == Decimal(expected)


def test_liability_usd_uses_multiplier():
    assert expiration_liability_usd(bull_put(), Decimal("4985")) == Decimal("1000")


def test_missing_settlement_value_blocks():  # T36
    with pytest.raises(DomainError):
        require_settlement_value(None)
    with pytest.raises(DomainError):
        require_settlement_value(Decimal("-1"))
