"""M1-04 package-limit execution (T06, T28-T33)."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from spx_research.domain.types import Quote
from spx_research.engine.execution import (
    FillStatus,
    Intent,
    PackageOrder,
    package_close_debit,
    package_open_credit,
    try_fill,
)

T0 = datetime(2019, 1, 2, 15, 0, tzinfo=UTC)


def q(cid: str, bid: str, ask: str, ts=T0, bsz=5, asz=5, avail=None) -> Quote:
    return Quote(
        contract_id=cid,
        snapshot_at_utc=ts,
        bid_points=Decimal(bid),
        ask_points=Decimal(ask),
        bid_size_contracts=bsz,
        ask_size_contracts=asz,
        simulated_available_at_utc=avail if avail is not None else ts,
    )


def open_order(limit: str) -> PackageOrder:
    return PackageOrder("o1", Intent.OPEN, Decimal(limit), T0, T0 + timedelta(seconds=60))


def close_order(limit: str) -> PackageOrder:
    return PackageOrder("o2", Intent.CLOSE, Decimal(limit), T0, T0 + timedelta(seconds=60))


def test_next_minute_eligibility():  # T06
    o = open_order("1.00")
    out = try_fill(o, q("s", "2.00", "2.10"), q("l", "0.50", "0.60"), T0)
    assert out.status is FillStatus.NOT_YET_ELIGIBLE
    out = try_fill(o, q("s", "2.00", "2.10"), q("l", "0.50", "0.60"), T0 + timedelta(seconds=60))
    assert out.status is FillStatus.FILLED
    assert out.package_price_points == Decimal("1.40")
    out = try_fill(o, q("s", "2.00", "2.10"), q("l", "0.50", "0.60"), T0 + timedelta(seconds=120))
    assert out.status is FillStatus.EXPIRED  # first-eligible-minute-only


def test_open_credit_formula_and_limit():
    s, lg = q("s", "2.00", "2.10"), q("l", "0.50", "0.60")
    assert package_open_credit(s, lg) == Decimal("1.40")
    assert package_close_debit(s, lg) == Decimal("1.60")
    out = try_fill(open_order("1.50"), s, lg, T0 + timedelta(seconds=60))
    assert out.status is FillStatus.LIMIT_NOT_MET
    out = try_fill(open_order("1.40"), s, lg, T0 + timedelta(seconds=60))
    assert out.status is FillStatus.FILLED


def test_close_debit_limit():
    s, lg = q("s", "2.00", "2.10"), q("l", "0.50", "0.60")
    out = try_fill(close_order("1.55"), s, lg, T0 + timedelta(seconds=60))
    assert out.status is FillStatus.LIMIT_NOT_MET
    out = try_fill(close_order("1.60"), s, lg, T0 + timedelta(seconds=60))
    assert out.status is FillStatus.FILLED and out.package_price_points == Decimal("1.60")


def test_missing_or_stale_quotes():  # T29
    t = T0 + timedelta(seconds=60)
    assert try_fill(open_order("1.00"), None, q("l", "0.5", "0.6"), t).status is (
        FillStatus.QUOTE_MISSING
    )
    stale = q("s", "2.0", "2.1", avail=T0 + timedelta(hours=1))
    assert try_fill(open_order("1.00"), stale, q("l", "0.5", "0.6"), t).status is (
        FillStatus.QUOTE_UNUSABLE
    )
    # snapshots from different minutes are not a package
    other = q("s", "2.0", "2.1", ts=T0 + timedelta(seconds=30))
    assert try_fill(open_order("1.00"), other, q("l", "0.5", "0.6"), t).status is (
        FillStatus.QUOTE_UNUSABLE
    )


def test_relevant_side_size():  # T28/T29
    t = T0 + timedelta(seconds=60)
    # OPEN sells the short leg (needs short bid size) and buys the long leg (ask size).
    s, lg = q("s", "2.0", "2.1", bsz=0), q("l", "0.5", "0.6")
    assert try_fill(open_order("1.00"), s, lg, t).status is FillStatus.SIZE_INADEQUATE
    s, lg = q("s", "2.0", "2.1"), q("l", "0.5", "0.6", asz=0)
    assert try_fill(open_order("1.00"), s, lg, t).status is FillStatus.SIZE_INADEQUATE
    # A zero bid on the protective long leg is not itself disqualifying (T28).
    s, lg = q("s", "2.0", "2.1"), q("l", "0.0", "0.6")
    assert try_fill(open_order("1.00"), s, lg, t).status is FillStatus.FILLED


def test_overnight_gap_uses_next_eligible_price():  # T32
    # Order submitted at close fills/expiry at next session's eligible minute only.
    o = PackageOrder("o3", Intent.CLOSE, Decimal("8.00"), T0, T0 + timedelta(days=1))
    out = try_fill(o, q("s", "8.0", "8.1"), q("l", "1.0", "1.1"), T0 + timedelta(days=1))
    assert out.status is FillStatus.FILLED and out.package_price_points == Decimal("7.10")


def test_no_width_clipping():  # T33
    # A natural close debit above the spread width is preserved, not clipped.
    s, lg = q("s", "11.0", "12.0"), q("l", "0.0", "0.5")
    assert package_close_debit(s, lg) == Decimal("12.00")  # > width 10 stays 12.00
    out = try_fill(close_order("12.00"), s, lg, T0 + timedelta(seconds=60))
    assert out.status is FillStatus.FILLED and out.package_price_points == Decimal("12.00")
