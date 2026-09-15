"""M1-03 accounting: golden T20/T21 fixture and reserve lifecycle (T26/T27)."""

from decimal import Decimal

import pytest

from spx_research.domain.types import DomainError
from spx_research.engine.accounting import (
    AccountSnapshot,
    apply_entry_fill,
    apply_exit_fill,
    apply_settlement,
    estimated_liquidation_pnl_usd,
    gross_max_expiration_loss_usd,
    gross_max_profit_usd,
    hold_reservation,
    realized_pnl_usd,
    release_reservation,
    reserve_required_usd,
)

M = 100
WIDTH = Decimal("10")
CREDIT = Decimal("2")
DEBIT = Decimal("1.30")
FEE = Decimal("1.00")  # per leg


def test_t20_golden_fixture():
    """Width 10, credit 2, x100; fees $1/leg; close debit 1.30 on $10k."""
    assert gross_max_profit_usd(CREDIT, M) == Decimal("200.00")
    assert gross_max_expiration_loss_usd(WIDTH, CREDIT, M) == Decimal("800.00")
    pnl = realized_pnl_usd(CREDIT, DEBIT, M, 2 * FEE, 2 * FEE)
    assert pnl == Decimal("66.00")
    cash = Decimal("10000.00") + CREDIT * M - 2 * FEE - DEBIT * M - 2 * FEE
    assert cash == Decimal("10066.00")


def test_t21_profit_fraction():
    pnl = realized_pnl_usd(CREDIT, DEBIT, M, 2 * FEE, 2 * FEE)
    frac = pnl / (CREDIT * M)
    assert frac == Decimal("0.33")


def test_t22_loss_basis_distinction():
    loss = Decimal("-50")  # 25% of credit vs max-loss basis
    credit_basis = loss / (CREDIT * M)
    maxloss_basis = loss / ((WIDTH - CREDIT) * M)
    assert credit_basis == Decimal("-0.25")
    assert maxloss_basis == Decimal("-0.0625")


def test_reservation_lifecycle_reconciles():  # T26/T27
    snap = AccountSnapshot(cash=Decimal("10000.00"), reserved=Decimal("0"))
    reserve = reserve_required_usd(WIDTH, M, Decimal("10.00"))  # 1010
    assert reserve == Decimal("1010.00")
    snap = hold_reservation(snap, reserve)
    assert snap.available() == Decimal("8990.00")
    # Fill: credit arrives, fees paid, reservation converts to position reserve.
    snap = apply_entry_fill(snap, CREDIT, M, 2 * FEE, reserve)
    assert snap.cash == Decimal("10198.00")  # +200 credit - 2 fees
    assert snap.reserved == Decimal("1010.00")  # width obligation still encumbered once
    # Exit: debit + fees leave cash; reserve releases exactly once.
    snap = apply_exit_fill(snap, DEBIT, M, 2 * FEE, reserve)
    assert snap.cash == Decimal("10066.00")
    assert snap.reserved == Decimal("0")
    with pytest.raises(DomainError):
        release_reservation(snap, reserve)  # second release must fail


def test_no_double_counting_width_and_maxloss():  # T27
    snap = AccountSnapshot(cash=Decimal("10000.00"), reserved=Decimal("0"))
    snap = hold_reservation(snap, reserve_required_usd(WIDTH, M, Decimal("0")))
    # Only the width is encumbered, never width AND (width - credit) separately.
    assert snap.reserved == Decimal("1000.00")
    assert snap.available() == Decimal("9000.00")


def test_insufficient_capital_blocks_reservation():
    snap = AccountSnapshot(cash=Decimal("500.00"), reserved=Decimal("0"))
    with pytest.raises(DomainError):
        hold_reservation(snap, Decimal("1010.00"))


def test_settlement_releases_reserve_and_debits_liability():
    snap = AccountSnapshot(cash=Decimal("10200.00"), reserved=Decimal("1010.00"))
    snap = apply_settlement(snap, Decimal("5"), M, Decimal("2.00"), Decimal("1010.00"))
    assert snap.cash == Decimal("9698.00")  # 10200 - 500 - 2
    assert snap.reserved == Decimal("0")


def test_estimated_liquidation_pnl():
    assert estimated_liquidation_pnl_usd(CREDIT, DEBIT, M, 2 * FEE, 2 * FEE) == Decimal("66.00")
