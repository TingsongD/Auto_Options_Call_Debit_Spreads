"""Cash, reserves, liabilities and P&L arithmetic (M1-03).

Conventions (spec §9.2): full strike-width cash encumbrance per open/pending
spread plus a configured buffer; no cross-spread offsets; the credit received
raises cash but the short-spread liability is tracked separately — premium is
not booked as earned profit. All money is ``Decimal``; index points convert via
the contract multiplier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

from spx_research.domain.types import DomainError, MoneyUSD, PricePoints

CENTS = Decimal("0.01")


def usd(v: Decimal | str | int) -> MoneyUSD:
    return MoneyUSD(Decimal(v).quantize(CENTS, rounding=ROUND_HALF_UP))


def points(v: Decimal | str | int) -> PricePoints:
    return PricePoints(Decimal(v))


def gross_max_profit_usd(credit_points: Decimal, multiplier: int) -> MoneyUSD:
    return usd(credit_points * multiplier)


def gross_max_expiration_loss_usd(
    width_points: Decimal, credit_points: Decimal, multiplier: int
) -> MoneyUSD:
    return usd((width_points - credit_points) * multiplier)


def realized_pnl_usd(
    credit_points: Decimal,
    debit_points: Decimal,
    multiplier: int,
    opening_fees_usd: Decimal,
    closing_fees_usd: Decimal,
) -> MoneyUSD:
    return usd((credit_points - debit_points) * multiplier - opening_fees_usd - closing_fees_usd)


def estimated_liquidation_pnl_usd(
    credit_points: Decimal,
    current_close_debit_points: Decimal,
    multiplier: int,
    opening_fees_usd: Decimal,
    estimated_closing_fees_usd: Decimal,
) -> MoneyUSD:
    return realized_pnl_usd(
        credit_points,
        current_close_debit_points,
        multiplier,
        opening_fees_usd,
        estimated_closing_fees_usd,
    )


@dataclass(frozen=True)
class AccountSnapshot:
    """Ledger accounting view at a point in the event stream."""

    cash: MoneyUSD
    reserved: MoneyUSD  # capital encumbered for pending reservations + open positions
    fees_paid: MoneyUSD = field(default_factory=lambda: MoneyUSD(Decimal("0")))

    def available(self) -> MoneyUSD:
        return usd(self.cash - self.reserved)


def reserve_required_usd(
    spread_width_points: Decimal, multiplier: int, buffer_usd: Decimal
) -> MoneyUSD:
    """Full-width encumbrance convention (D12): W*M + buffer. Counted once."""
    return usd(Decimal(spread_width_points) * multiplier + buffer_usd)


def apply_entry_fill(
    snap: AccountSnapshot,
    credit_points: Decimal,
    multiplier: int,
    opening_fees_usd: Decimal,
    reservation_reserve_usd: Decimal,
) -> AccountSnapshot:
    """Entry fill: cash += credit - fees; the pending reservation converts to the
    position reserve (same amount — reserve is established at reservation time and
    does not change size when the fill's credit arrives, since the encumbrance is
    the full width)."""
    cash_delta = Decimal(credit_points) * multiplier - opening_fees_usd
    new_cash = usd(snap.cash + cash_delta)
    fees = usd(snap.fees_paid + opening_fees_usd)
    return AccountSnapshot(new_cash, snap.reserved, fees)


def apply_exit_fill(
    snap: AccountSnapshot,
    debit_points: Decimal,
    multiplier: int,
    closing_fees_usd: Decimal,
    position_reserve_usd: Decimal,
) -> AccountSnapshot:
    """Exit fill: cash -= debit + fees; the position's reserve releases exactly once."""
    if position_reserve_usd > snap.reserved:
        raise DomainError("RESERVE_RELEASE_EXCEEDS_RESERVED")
    cash_delta = Decimal(debit_points) * multiplier + closing_fees_usd
    return AccountSnapshot(
        usd(snap.cash - cash_delta),
        usd(snap.reserved - position_reserve_usd),
        usd(snap.fees_paid + closing_fees_usd),
    )


def apply_settlement(
    snap: AccountSnapshot,
    liability_points: Decimal,
    multiplier: int,
    settlement_fees_usd: Decimal,
    position_reserve_usd: Decimal,
) -> AccountSnapshot:
    """Expiration: cash -= liability*M + fees; reserve releases exactly once."""
    if position_reserve_usd > snap.reserved:
        raise DomainError("RESERVE_RELEASE_EXCEEDS_RESERVED")
    cost = Decimal(liability_points) * multiplier + settlement_fees_usd
    return AccountSnapshot(
        usd(snap.cash - cost),
        usd(snap.reserved - position_reserve_usd),
        usd(snap.fees_paid + settlement_fees_usd),
    )


def hold_reservation(snap: AccountSnapshot, reserve_usd: Decimal) -> AccountSnapshot:
    """Pending search/entry reservation encumbers capital before any fill."""
    if reserve_usd > snap.available():
        raise DomainError("INSUFFICIENT_AVAILABLE_CAPITAL")
    return AccountSnapshot(snap.cash, usd(snap.reserved + reserve_usd), snap.fees_paid)


def release_reservation(snap: AccountSnapshot, reserve_usd: Decimal) -> AccountSnapshot:
    if reserve_usd > snap.reserved:
        raise DomainError("RESERVE_RELEASE_EXCEEDS_RESERVED")
    return AccountSnapshot(snap.cash, usd(snap.reserved - reserve_usd), snap.fees_paid)
