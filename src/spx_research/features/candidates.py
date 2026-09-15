"""Deterministic candidate generation (M3-01).

At each entry review, eligible one-lot spreads are built from contracts listed
and quotable at the decision time — never from later listings (T04). Filters:
structure, 40-50 DTE, quote usability/sizes, delta band (validated Greeks only),
configured widths, per-spread and aggregate risk. A stable sort yields a
shortlist of at most ``max_candidates_per_direction``; menu/menu-provenance
versioning is part of the strategy (TK25).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from spx_research.data.availability import Archive
from spx_research.domain.types import (
    Contract,
    CreditSpread,
    Direction,
    DomainError,
    PricePoints,
    Right,
    require_aware,
)

_Points = PricePoints  # NewType alias for strike arithmetic


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    spread: CreditSpread
    direction: Direction
    dte: int
    credit_points: Decimal  # natural quote-side credit
    max_loss_usd: Decimal
    reserve_usd: Decimal
    short_delta: Decimal | None
    sort_key: tuple[Any, ...]


def _contract(row: dict[str, Any]) -> Contract:
    return Contract(
        contract_id=row["contract_id"],
        root=row["root"],
        right=Right(row["right"]),
        strike_points=PricePoints(Decimal(str(row["strike_points"]))),
        expiration_local_date=row["expiration_local_date"],
        exercise_style=row["exercise_style"],
        settlement_style=row["settlement_style"],
        multiplier=int(row["multiplier"]),
        price_increment=Decimal(str(row["price_increment"])),
        listed_at_utc=row.get("listed_at_utc"),
        first_verified_observation_utc=row.get("first_verified_observation_utc"),
    )


def build_candidates(
    archive: Archive,
    as_of: datetime,
    ny_date: date,
    direction: Direction,
    dte_range: tuple[int, int],
    widths: list[Decimal],
    delta_range: tuple[Decimal, Decimal],
    reserve_per_spread_usd: Decimal,
    max_risk_usd: Decimal | None,
    max_candidates: int,
    version: str = "candidates-1",
) -> list[Candidate]:
    """Deterministic eligible shortlist for one direction at one decision time."""
    as_of = require_aware(as_of)
    right = Right.PUT if direction is Direction.BULL_PUT_CREDIT else Right.CALL
    contracts = [
        c for c in (_contract(r) for r in archive.contracts_visible_at(as_of)) if c.right is right
    ]
    eligible: list[Candidate] = []
    for expiry in sorted({c.expiration_local_date for c in contracts}):
        dte = (expiry - ny_date).days
        if not dte_range[0] <= dte <= dte_range[1]:
            continue
        legs = sorted(
            (c for c in contracts if c.expiration_local_date == expiry),
            key=lambda c: c.strike_points,
        )
        by_strike = {c.strike_points: c for c in legs}
        quotes = archive.session_quotes([c.contract_id for c in legs], as_of)
        for c in legs:
            for w in widths:
                if direction is Direction.BULL_PUT_CREDIT:
                    short_c, long_c = c, by_strike.get(_Points(c.strike_points - w))
                else:
                    short_c, long_c = c, by_strike.get(_Points(c.strike_points + w))
                if long_c is None:
                    continue
                try:
                    spread = CreditSpread(short_c, long_c, direction)
                except DomainError:
                    continue
                sq = quotes.get(short_c.contract_id)
                lq = quotes.get(long_c.contract_id)
                if sq is None or lq is None:
                    continue
                if sq["bid_size_contracts"] < 1 or lq["ask_size_contracts"] < 1:
                    continue
                credit = Decimal(str(sq["bid_points"])) - Decimal(str(lq["ask_points"]))
                if credit <= 0:
                    continue
                greek = archive.greeks_at(short_c.contract_id, as_of)
                delta = None if greek is None else Decimal(str(greek["delta"]))
                if delta is None or not delta_range[0] <= abs(delta) <= delta_range[1]:
                    continue  # unvalidated delta is an explicit ineligibility reason
                max_loss = (w - credit) * spread.multiplier
                if max_risk_usd is not None and max_loss > max_risk_usd:
                    continue
                if max_loss > reserve_per_spread_usd:
                    continue  # cannot exceed the encumbered reserve
                sort_key = (
                    abs(dte - 45),  # prefer nearest 45 DTE
                    -float(credit / w),  # then richer credit fraction
                    short_c.strike_points,
                    w,
                    expiry.isoformat(),
                )
                cid = f"cand:{direction.value}:{expiry}:{short_c.strike_points}:{w}"
                eligible.append(
                    Candidate(
                        cid,
                        spread,
                        direction,
                        dte,
                        credit,
                        max_loss,
                        reserve_per_spread_usd,
                        delta,
                        sort_key,
                    )
                )
    eligible.sort(key=lambda c: c.sort_key)
    return eligible[:max_candidates]
