"""Immutable domain types: units, contracts, spreads (M1-01).

Money is ``Decimal`` USD; option prices are ``Decimal`` index points converted
through the contract multiplier. All timestamps are timezone-aware UTC.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import NewType

PricePoints = NewType("PricePoints", Decimal)
MoneyUSD = NewType("MoneyUSD", Decimal)


class Right(Enum):
    PUT = "PUT"
    CALL = "CALL"


class Direction(Enum):
    BULL_PUT_CREDIT = "BULL_PUT_CREDIT"
    BEAR_CALL_CREDIT = "BEAR_CALL_CREDIT"


class DomainError(ValueError):
    """Fixed-code structural violation; never carries untrusted text."""


def require_aware(t: datetime) -> datetime:
    if t.tzinfo is None or t.utcoffset() is None:
        raise DomainError("NAIVE_TIME")
    return t.astimezone(UTC)


def ny_calendar_days(start: date, end: date) -> int:
    """Holding-age basis: New York calendar-date difference (D03 proposed)."""
    return (end - start).days


@dataclass(frozen=True)
class Contract:
    """One listed option contract (contract master, data dictionary §3)."""

    contract_id: str
    root: str
    right: Right
    strike_points: PricePoints
    expiration_local_date: date
    exercise_style: str
    settlement_style: str
    multiplier: int
    price_increment: Decimal
    listed_at_utc: datetime | None = None
    first_verified_observation_utc: datetime | None = None
    last_trading_at_utc: datetime | None = None
    settlement_event_at_utc: datetime | None = None
    settlement_value_symbol: str | None = None

    def __post_init__(self) -> None:
        if self.root not in ("SPX", "SPXW"):
            raise DomainError("UNSUPPORTED_ROOT")
        if self.multiplier <= 0:
            raise DomainError("BAD_MULTIPLIER")
        if self.strike_points <= 0 or self.price_increment <= 0:
            raise DomainError("BAD_PRICE")
        if self.listed_at_utc is not None:
            require_aware(self.listed_at_utc)
        if self.settlement_event_at_utc is not None:
            require_aware(self.settlement_event_at_utc)

    def available_at(self, t: datetime) -> bool:
        """Conservative listing visibility (T04): first verified observation."""
        t = require_aware(t)
        if self.first_verified_observation_utc is not None:
            return t >= self.first_verified_observation_utc
        if self.listed_at_utc is not None:
            return t >= self.listed_at_utc
        return False


@dataclass(frozen=True)
class CreditSpread:
    """A one-lot vertical credit spread: exactly one short and one long leg."""

    short: Contract
    long: Contract
    direction: Direction

    def __post_init__(self) -> None:
        s, lo = self.short, self.long
        for attr in (
            "root",
            "expiration_local_date",
            "exercise_style",
            "settlement_style",
            "multiplier",
            "price_increment",
        ):
            if getattr(s, attr) != getattr(lo, attr):
                raise DomainError("LEG_MISMATCH")
        if self.direction is Direction.BULL_PUT_CREDIT:
            if s.right is not Right.PUT or not s.strike_points > lo.strike_points:
                raise DomainError("BAD_BULL_PUT_STRUCTURE")
        else:
            if s.right is not Right.CALL or not s.strike_points < lo.strike_points:
                raise DomainError("BAD_BEAR_CALL_STRUCTURE")

    @property
    def width_points(self) -> Decimal:
        return abs(self.short.strike_points - self.long.strike_points)

    @property
    def multiplier(self) -> int:
        return self.short.multiplier

    @property
    def expiration_local_date(self) -> date:
        return self.short.expiration_local_date

    def dte_calendar_days(self, current_ny_date: date) -> int:
        return (self.short.expiration_local_date - current_ny_date).days

    def listed_and_observable(self, t: datetime) -> bool:
        return self.short.available_at(t) and self.long.available_at(t)


@dataclass(frozen=True)
class Quote:
    """One-minute quote snapshot for a single contract (dictionary §4)."""

    contract_id: str
    snapshot_at_utc: datetime
    bid_points: Decimal
    ask_points: Decimal
    bid_size_contracts: int
    ask_size_contracts: int
    simulated_available_at_utc: datetime
    quote_event_time_known: bool = False
    quality_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.snapshot_at_utc)
        require_aware(self.simulated_available_at_utc)
        if self.bid_points < 0 or self.ask_points < 0:
            raise DomainError("NEGATIVE_PRICE")
        if self.bid_points > self.ask_points:
            raise DomainError("CROSSED_QUOTE")

    def usable_at(self, t: datetime) -> bool:
        return require_aware(t) >= self.simulated_available_at_utc
