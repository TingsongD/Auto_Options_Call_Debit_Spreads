"""Coverage and quality report (M2-04).

Distinguishes missing sessions, absent contracts, crossed/negative quotes,
unknown quote-event ages and provider outages — an outage is a data-quality
event, never a market with no opportunities (T31).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from spx_research.data.availability import Archive
from spx_research.domain.types import require_aware
from spx_research.temporal.calendar import CalendarManifest


@dataclass(frozen=True)
class CoverageFinding:
    code: str
    detail: str


@dataclass(frozen=True)
class CoverageReport:
    dataset_root: str
    expected_sessions: int
    sessions_present: int
    missing_sessions: tuple[date, ...]
    findings: tuple[CoverageFinding, ...] = field(default_factory=tuple)


def coverage_report(
    dataset_root: str | Path, cal: CalendarManifest, start: date, end: date
) -> CoverageReport:
    root = Path(dataset_root)
    con = duckdb.connect(database=":memory:")
    expected = {s.day for s in cal.session_days(start, end)}
    present: set[date] = set()
    findings: list[CoverageFinding] = []
    for f in sorted((root / "quotes").glob("session=*.parquet")):
        try:
            present.add(date.fromisoformat(f.stem.split("=", 1)[1]))
        except (ValueError, IndexError):
            findings.append(CoverageFinding("BAD_PARTITION_NAME", f.name))
    missing = sorted(expected - present)
    for d in missing:
        findings.append(CoverageFinding("MISSING_SESSION", d.isoformat()))
    files = sorted((root / "quotes").glob("session=*.parquet"))
    if not files:
        con.close()
        return CoverageReport(str(root), len(expected), 0, tuple(missing), tuple(findings))
    paths = [str(p) for p in files]
    rows = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [paths]).fetchone()
    if rows is not None and rows[0] == 0 and present:
        findings.append(CoverageFinding("EMPTY_QUOTES", "all session files empty"))
    bad = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?) "
        "WHERE bid_points > ask_points OR bid_points < 0 OR ask_points < 0 "
        "OR bid_size_contracts < 0 OR ask_size_contracts < 0",
        [paths],
    ).fetchone()
    if bad and bad[0]:
        findings.append(CoverageFinding("INVALID_QUOTES", f"{bad[0]} crossed/negative rows"))
    unknown_age = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?) WHERE NOT quote_event_time_known",
        [paths],
    ).fetchone()
    if unknown_age and unknown_age[0]:
        findings.append(
            CoverageFinding("UNKNOWN_EVENT_AGE", f"{unknown_age[0]} snapshot-only rows")
        )
    for session in cal.session_days(start, end):
        path = root / "quotes" / f"session={session.day.isoformat()}.parquet"
        if not path.is_file():
            continue
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT snapshot_at_utc FROM read_parquet(?)",
                [str(path)],
            ).fetchall()
        }
        expected_minutes = {
            cal.utc_minute(session.day, offset) for offset in session.minute_offsets()
        }
        missing_count = len(expected_minutes - observed)
        if missing_count:
            findings.append(
                CoverageFinding(
                    "MISSING_MINUTES",
                    f"{session.day.isoformat()}: {missing_count} absent snapshots",
                )
            )
    con.close()
    return CoverageReport(
        str(root), len(expected), len(present & expected), tuple(missing), tuple(findings)
    )


def expected_quote_coverage(
    dataset_root: str | Path, contract_ids: list[str], as_of: datetime
) -> dict[str, bool]:
    """Which contracts have a usable quote at as_of (for outage detection)."""
    require_aware(as_of)
    archive = Archive(dataset_root)
    try:
        quotes = archive.session_quotes(contract_ids, as_of, max_age_seconds=300)
        return {cid: cid in quotes for cid in contract_ids}
    finally:
        archive.close()


def summary_dict(report: CoverageReport) -> dict[str, Any]:
    return {
        "dataset_root": report.dataset_root,
        "expected_sessions": report.expected_sessions,
        "sessions_present": report.sessions_present,
        "missing_sessions": [d.isoformat() for d in report.missing_sessions],
        "findings": [{"code": f.code, "detail": f.detail} for f in report.findings],
    }
