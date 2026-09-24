"""Shared deterministic market-data validation, before menus and execution."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from spx_research.domain.types import DomainError, Quote, require_aware


def quote_from_row(row: dict[str, Any]) -> Quote:
    for name in ("bid_size_contracts", "ask_size_contracts"):
        value = row[name]
        if isinstance(value, bool) or value != int(value):
            raise DomainError("INVALID_QUOTE_SIZE")
    return Quote(
        contract_id=str(row["contract_id"]),
        snapshot_at_utc=row["snapshot_at_utc"],
        bid_points=Decimal(str(row["bid_points"])),
        ask_points=Decimal(str(row["ask_points"])),
        bid_size_contracts=int(row["bid_size_contracts"]),
        ask_size_contracts=int(row["ask_size_contracts"]),
        simulated_available_at_utc=row["simulated_available_at_utc"],
        quote_event_time_known=bool(row.get("quote_event_time_known", False)),
        quality_flags=tuple(row.get("quality_flags") or ()),
        quote_event_at_utc=row.get("quote_event_at_utc"),
    )


def validate_quote(
    quote: Quote,
    as_of: datetime,
    *,
    max_snapshot_age_seconds: int | None = None,
    max_event_age_seconds: int | None = None,
) -> None:
    as_of = require_aware(as_of)
    if quote.snapshot_at_utc > as_of or not quote.usable_at(as_of):
        raise DomainError("QUOTE_NOT_AVAILABLE")
    allowed = {"SYNTHETIC", "UNKNOWN_EVENT_AGE", "SNAPSHOT_ONLY"}
    if set(quote.quality_flags) - allowed:
        raise DomainError("QUOTE_QUALITY_FLAG")
    if (
        max_snapshot_age_seconds is not None
        and (as_of - quote.snapshot_at_utc).total_seconds() > max_snapshot_age_seconds
    ):
        raise DomainError("STALE_SNAPSHOT")
    if (
        quote.quote_event_at_utc is not None
        and max_event_age_seconds is not None
        and (as_of - quote.quote_event_at_utc).total_seconds() > max_event_age_seconds
    ):
        raise DomainError("STALE_QUOTE_EVENT")


def validate_pair(
    short: Quote,
    long: Quote,
    as_of: datetime,
    *,
    max_snapshot_age_seconds: int | None = None,
    max_event_age_seconds: int | None = None,
) -> None:
    for quote in (short, long):
        validate_quote(
            quote,
            as_of,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            max_event_age_seconds=max_event_age_seconds,
        )
    if short.contract_id == long.contract_id:
        raise DomainError("SAME_LEG_QUOTES")
    if short.snapshot_at_utc != long.snapshot_at_utc:
        raise DomainError("MISMATCHED_QUOTE_SNAPSHOT")


def on_increment(value: Decimal, increment: Decimal) -> bool:
    return value.is_finite() and increment.is_finite() and increment > 0 and value % increment == 0
