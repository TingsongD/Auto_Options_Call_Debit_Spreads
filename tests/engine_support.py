"""Small, hand-reconcilable synthetic engine fixtures. No provider data."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from spx_research.config import Profile
from spx_research.temporal.calendar import CalendarManifest, SessionDay

START = date(2024, 1, 2)
EXPIRY = date(2024, 2, 16)


def tiny_calendar(*, expiry: bool = False) -> CalendarManifest:
    sessions = [SessionDay(START, time(9, 30), time(10, 17))]
    if expiry:
        sessions.append(SessionDay(EXPIRY, time(9, 30), time(10, 17)))
    return CalendarManifest("synthetic-hand-cal", "test", tuple(sessions))


def tiny_profile(*, scored_end: date = START, runoff_end: date = START) -> Profile:
    return Profile.model_validate(
        {
            "profile_id": "synthetic-hand-v2",
            "mode": "synthetic_test",
            "permissions": {},
            "universe": {
                "allowed_contract_roots": ["SPXW"],
                "strategies": ["BULL_PUT_CREDIT", "BEAR_CALL_CREDIT"],
                "target_entry_dte_calendar_days": 45,
                "entry_dte_range": [40, 50],
                "spread_widths_index_points": ["10"],
                "short_abs_delta_range": ["0.15", "0.35"],
                "max_candidates_per_direction": 12,
            },
            "clock": {"agent_review_minutes": 15},
            "study": {
                "start_date": START,
                "scored_end_date": scored_end,
                "runoff_end_date": runoff_end,
            },
            "portfolio": {
                "initial_capital_usd": "10000",
                "max_open_or_reserved_slots": 3,
                "max_per_spread_initial_risk_usd": "1000",
                "max_aggregate_committed_risk_usd": "9000",
                "reservation_buffer_usd": "0",
            },
            "exit_policy": {},
            "manager": {},
            "quality": {},
            "execution": {"opening_fee_per_leg_usd": "1", "closing_fee_per_leg_usd": "1"},
        }
    )


class TinyArchive:
    """Four observable contracts; 2-point entry, 1.30-point later close."""

    def __init__(self, calendar: CalendarManifest, *, take_profit: bool = True) -> None:
        self.calendar = calendar
        self.take_profit = take_profit
        self.manifest = {"manifest_id": "synthetic-hand-v2", "dataset_kind": "synthetic"}
        self.outage_at: datetime | None = None
        self.crossed = False
        self.known_stale = False
        self.mismatched = False
        self.coverage = "HEALTHY"
        self.settlement_missing = False

    def contracts_visible_at(self, at: datetime) -> list[dict[str, Any]]:
        start = self.calendar.sessions[0].open_utc()
        expiry_session = self.calendar.session(EXPIRY)
        cutoff = (
            expiry_session.close_utc()
            if expiry_session
            else datetime.combine(
                EXPIRY,
                time(16),
                tzinfo=start.tzinfo,
            )
        )
        return [
            {
                "contract_id": cid,
                "root": "SPXW",
                "right": right,
                "strike_points": strike,
                "expiration_local_date": EXPIRY,
                "exercise_style": "EUROPEAN",
                "settlement_style": "PM",
                "multiplier": 100,
                "price_increment": "0.05",
                "listed_at_utc": start,
                "first_verified_observation_utc": start,
                "last_trading_at_utc": cutoff,
                "settlement_event_at_utc": cutoff,
            }
            for cid, right, strike in [
                ("ps", "PUT", 5000),
                ("pl", "PUT", 4990),
                ("cs", "CALL", 5000),
                ("cl", "CALL", 5010),
            ]
            if at >= start
        ]

    def quote_at(self, cid: str, at: datetime, **_: Any) -> dict[str, Any] | None:
        if self.outage_at and at >= self.outage_at:
            return None
        close_prices = self.take_profit and (
            at >= self.calendar.sessions[0].open_utc() + timedelta(minutes=45)
        )
        bid, ask = ("2.10", "2.20") if close_prices else ("3.00", "3.10")
        if cid.endswith("l"):
            bid, ask = "0.90", "1.00"
        elif self.crossed:
            bid, ask = "3.00", "2.00"
        snapshot = at - timedelta(minutes=1) if self.mismatched and cid.endswith("l") else at
        return {
            "contract_id": cid,
            "snapshot_at_utc": snapshot,
            "simulated_available_at_utc": snapshot,
            "bid_points": Decimal(bid),
            "ask_points": Decimal(ask),
            "bid_size_contracts": 5,
            "ask_size_contracts": 5,
            "quote_event_time_known": self.known_stale,
            "quote_event_at_utc": snapshot - timedelta(hours=1) if self.known_stale else None,
            "quality_flags": ["SYNTHETIC"],
        }

    def session_quotes(
        self, ids: list[str], at: datetime, **kwargs: Any
    ) -> dict[str, dict[str, Any]]:
        return {cid: row for cid in ids if (row := self.quote_at(cid, at, **kwargs)) is not None}

    def session_health(self, at: datetime, *_: Any) -> str:
        return self.coverage

    def greeks_at(self, cid: str, at: datetime, **_: Any) -> dict[str, Any]:
        return {
            "delta": Decimal("0.25"),
            "methodology_id": "synthetic",
            "quality_flags": ["SYNTHETIC"],
        }

    def macro_visible_at(self, at: datetime) -> list[dict[str, Any]]:
        return []

    def index_at(self, at: datetime) -> dict[str, Any]:
        return {"value_index_points": Decimal("5000"), "observed_at_utc": at}

    def settlement_for(self, expiry: date) -> dict[str, Any] | None:
        session = self.calendar.session(expiry)
        if session is None or self.settlement_missing:
            return None
        close = session.close_utc()
        return {
            "settled_at_utc": close,
            "published_at_utc": close + timedelta(minutes=30),
            "simulated_available_at_utc": close + timedelta(minutes=30),
            "value_index_points": Decimal("5005"),
        }
