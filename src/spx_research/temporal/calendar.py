"""Versioned New York exchange calendar (M1-02).

Sessions are explicit data, not assumptions: a ``CalendarManifest`` lists every
trading day with its local open/close times (half days have an early close).
UTC instants are derived through the America/New_York timezone so DST and half
days are correct without host-timezone dependence (T07).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC_TZ = UTC


class CalendarError(ValueError):
    pass


@dataclass(frozen=True)
class SessionDay:
    day: date  # New York calendar date
    open_local: time
    close_local: time
    half_day: bool = False

    def open_utc(self) -> datetime:
        return datetime.combine(self.day, self.open_local, tzinfo=NY).astimezone(UTC)

    def close_utc(self) -> datetime:
        return datetime.combine(self.day, self.close_local, tzinfo=NY).astimezone(UTC)

    def minute_offsets(self) -> range:
        """Tradable minute offsets from open: [0, minutes) excludes the close print."""
        minutes = int((self.close_utc() - self.open_utc()).total_seconds() // 60)
        return range(0, minutes)


@dataclass(frozen=True)
class CalendarManifest:
    """A versioned set of session days; referenced by calendar_manifest_id."""

    calendar_id: str
    version: str
    sessions: tuple[SessionDay, ...]

    def __post_init__(self) -> None:
        days = [s.day for s in self.sessions]
        if len(set(days)) != len(days):
            raise CalendarError("DUPLICATE_SESSION_DATE")
        if days != sorted(days):
            raise CalendarError("SESSIONS_NOT_SORTED")

    def session(self, day: date) -> SessionDay | None:
        for s in self.sessions:
            if s.day == day:
                return s
        return None

    def is_session(self, day: date) -> bool:
        return self.session(day) is not None

    def session_days(self, start: date, end: date) -> list[SessionDay]:
        return [s for s in self.sessions if start <= s.day <= end]

    def review_times(self, day: date, interval_minutes: int) -> list[datetime]:
        """Review grid: open+interval, then every interval, strictly before close."""
        s = self.session(day)
        if s is None:
            return []
        times = []
        t = s.open_utc() + timedelta(minutes=interval_minutes)
        while t < s.close_utc():
            times.append(t)
            t += timedelta(minutes=interval_minutes)
        return times

    def utc_minute(self, day: date, offset: int) -> datetime:
        s = self.session(day)
        if s is None or offset not in s.minute_offsets():
            raise CalendarError("NOT_A_TRADING_MINUTE")
        return s.open_utc() + timedelta(minutes=offset)

    def ny_date(self, t: datetime) -> date:
        return t.astimezone(NY).date()


def _parse_time(v: str) -> time:
    hh, mm = v.split(":")
    return time(int(hh), int(mm))


def load_manifest(path: str | Path) -> CalendarManifest:
    """Load a checked-in calendar manifest (JSON)."""
    raw = json.loads(Path(path).read_text())
    sessions = tuple(
        SessionDay(
            day=date.fromisoformat(s["date"]),
            open_local=_parse_time(s.get("open", "09:30")),
            close_local=_parse_time(s.get("close", "16:00")),
            half_day=bool(s.get("half_day", False)),
        )
        for s in raw["sessions"]
    )
    return CalendarManifest(raw["calendar_id"], raw["version"], sessions)


def build_weekday_manifest(
    calendar_id: str,
    start: date,
    end: date,
    holidays: frozenset[date] = frozenset(),
    half_days: frozenset[date] = frozenset(),
) -> CalendarManifest:
    """Deterministic weekday calendar for synthetic fixtures and tests."""
    sessions: list[SessionDay] = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in holidays:
            sessions.append(
                SessionDay(
                    d, time(9, 30), time(13, 0) if d in half_days else time(16, 0), d in half_days
                )
            )
        d += timedelta(days=1)
    return CalendarManifest(calendar_id, "test", tuple(sessions))
