"""PM settlement liabilities and final-position handling (M1-05).

Expiration liability in index points (spec §9.3):
  bull put:  max(K_short - S_settle, 0) - max(K_long - S_settle, 0)
  bear call: max(S_settle - K_short, 0) - max(S_settle - K_long, 0)
Cash liability is that value times the contract multiplier. A missing verified
settlement value blocks validation of hold-to-expiry runs (T36).
"""

from __future__ import annotations

from decimal import Decimal

from spx_research.domain.types import CreditSpread, Direction, DomainError


def expiration_liability_points(spread: CreditSpread, settlement_value: Decimal) -> Decimal:
    """What the short spread owes at expiration, in index points."""
    s = settlement_value
    ks = spread.short.strike_points
    kl = spread.long.strike_points
    if spread.direction is Direction.BULL_PUT_CREDIT:
        return max(ks - s, Decimal(0)) - max(kl - s, Decimal(0))
    return max(s - ks, Decimal(0)) - max(s - kl, Decimal(0))


def expiration_liability_usd(spread: CreditSpread, settlement_value: Decimal) -> Decimal:
    return expiration_liability_points(spread, settlement_value) * spread.multiplier


def max_expiration_liability_points(spread: CreditSpread) -> Decimal:
    return spread.width_points


def require_settlement_value(value: Decimal | None) -> Decimal:
    """T36: hold-to-expiry cannot be validated without a verified value."""
    if value is None:
        raise DomainError("MISSING_SETTLEMENT_VALUE")
    if value < 0:
        raise DomainError("NEGATIVE_SETTLEMENT_VALUE")
    return value
