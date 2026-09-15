"""Package-limit execution model (M1-04).

A vertical order is one intended package (spec §9.1):
  opening credit = short bid - long ask
  closing debit  = short ask - long bid
Both legs come from the same eligible snapshot; relevant-side displayed size
must cover one contract; fills occur only at the order's single first-eligible
minute (T06). A midpoint touch is not a fill; a missing quote is a data-quality
event, not a failed fill (T29/T31).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from spx_research.domain.types import DomainError, Quote, require_aware


class Intent(Enum):
    OPEN = "OPEN"  # sell the package for a minimum credit
    CLOSE = "CLOSE"  # buy the package back for a maximum debit


@dataclass(frozen=True)
class PackageOrder:
    order_id: str
    intent: Intent
    limit_points: Decimal  # min credit for OPEN, max debit for CLOSE
    submitted_at_utc: datetime
    first_eligible_at_utc: datetime

    def __post_init__(self) -> None:
        require_aware(self.submitted_at_utc)
        require_aware(self.first_eligible_at_utc)
        if self.first_eligible_at_utc <= self.submitted_at_utc:
            raise DomainError("NO_EXECUTION_DELAY")
        if self.limit_points <= 0:
            raise DomainError("NONPOSITIVE_LIMIT")


class FillStatus(Enum):
    FILLED = "FILLED"
    LIMIT_NOT_MET = "LIMIT_NOT_MET"
    QUOTE_MISSING = "QUOTE_MISSING"
    QUOTE_UNUSABLE = "QUOTE_UNUSABLE"
    SIZE_INADEQUATE = "SIZE_INADEQUATE"
    NOT_YET_ELIGIBLE = "NOT_YET_ELIGIBLE"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class FillOutcome:
    status: FillStatus
    package_price_points: Decimal | None = None
    fill_at_utc: datetime | None = None


def package_open_credit(short: Quote, long: Quote) -> Decimal:
    return short.bid_points - long.ask_points


def package_close_debit(short: Quote, long: Quote) -> Decimal:
    return short.ask_points - long.bid_points


def _leg_sizes_ok(order: PackageOrder, short: Quote, long: Quote) -> bool:
    if order.intent is Intent.OPEN:
        return short.bid_size_contracts >= 1 and long.ask_size_contracts >= 1
    return short.ask_size_contracts >= 1 and long.bid_size_contracts >= 1


def try_fill(
    order: PackageOrder,
    short_quote: Quote | None,
    long_quote: Quote | None,
    at_utc: datetime,
) -> FillOutcome:
    """Attempt the package fill at one minute boundary.

    ``at_utc`` before ``first_eligible_at_utc`` cannot fill (T06); the order
    expires after its first eligible minute in the base profile.
    """
    at_utc = require_aware(at_utc)
    if at_utc < order.first_eligible_at_utc:
        return FillOutcome(FillStatus.NOT_YET_ELIGIBLE)
    if at_utc > order.first_eligible_at_utc:
        return FillOutcome(FillStatus.EXPIRED)
    if short_quote is None or long_quote is None:
        return FillOutcome(FillStatus.QUOTE_MISSING)
    if short_quote.contract_id == long_quote.contract_id:
        raise DomainError("SAME_LEG_QUOTES")
    if not (short_quote.usable_at(at_utc) and long_quote.usable_at(at_utc)):
        return FillOutcome(FillStatus.QUOTE_UNUSABLE)
    if short_quote.snapshot_at_utc != long_quote.snapshot_at_utc:
        return FillOutcome(FillStatus.QUOTE_UNUSABLE)
    if not _leg_sizes_ok(order, short_quote, long_quote):
        return FillOutcome(FillStatus.SIZE_INADEQUATE)
    if order.intent is Intent.OPEN:
        price = package_open_credit(short_quote, long_quote)
        if price >= order.limit_points:
            return FillOutcome(FillStatus.FILLED, price, at_utc)
    else:
        price = package_close_debit(short_quote, long_quote)
        if price <= order.limit_points:
            return FillOutcome(FillStatus.FILLED, price, at_utc)
    return FillOutcome(FillStatus.LIMIT_NOT_MET)
