"""Versioned result vocabulary shared by engine, archive, runtime and reports."""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict

RunStatus = Literal["RUNNING", "PAUSED", "COMPLETED", "FAILED"]
RuntimeRunStatus = RunStatus
PauseCategory = Literal["DATA", "MODEL", "BUDGET", "EPISTEMIC"]
MarketCoverage = Literal["HEALTHY", "MISSING_SESSION", "NO_AVAILABLE_QUOTES", "COVERAGE_OUTAGE"]
ValuationQuality = Literal["OK", "UNKNOWN_EVENT_AGE", "PENDING_SETTLEMENT", "UNPRICEABLE"]


class ValuationRecord(TypedDict):
    as_of: str
    cash_usd: str
    reserved_usd: str
    fees_paid_usd: str
    mid_equity_usd: str | None
    net_liquidation_equity_usd: str | None
    quality: ValuationQuality
    positions: list[dict[str, Any]]
    available_capital_usd: NotRequired[str]
    mid_liability_usd: NotRequired[str | None]
    liquidation_liability_usd: NotRequired[str | None]
    estimated_closing_fees_usd: NotRequired[str]
    reason_code: NotRequired[str]


class PauseReason(TypedDict):
    category: PauseCategory
    code: str
    phase: str
    at: str
    valuation: NotRequired[ValuationRecord]
