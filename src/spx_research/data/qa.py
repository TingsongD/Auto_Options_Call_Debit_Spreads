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
    rows = con.execute(f"SELECT COUNT(*) FROM '{root}/quotes/session=*.parquet'").fetchone()
    if rows is not None and rows[0] == 0 and present:
        findings.append(CoverageFinding("EMPTY_QUOTES", "all session files empty"))
    bad = con.execute(
        f"SELECT COUNT(*) FROM '{root}/quotes/session=*.parquet' "
        "WHERE bid_points > ask_points OR bid_points < 0 OR ask_points < 0"
    ).fetchone()
    if bad and bad[0]:
        findings.append(CoverageFinding("INVALID_QUOTES", f"{bad[0]} crossed/negative rows"))
    unknown_age = con.execute(
        f"SELECT COUNT(*) FROM '{root}/quotes/session=*.parquet' WHERE NOT quote_event_time_known"
    ).fetchone()
    if unknown_age and unknown_age[0]:
        findings.append(
            CoverageFinding("UNKNOWN_EVENT_AGE", f"{unknown_age[0]} snapshot-only rows")
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
    root = Path(dataset_root)
    con = duckdb.connect(database=":memory:")
    out: dict[str, bool] = {}
    for cid in contract_ids:
        row = con.execute(
            f"SELECT COUNT(*) FROM '{root}/quotes/session=*.parquet' "
            "WHERE contract_id = ? AND simulated_available_at_utc <= ?",
            [cid, as_of],
        ).fetchone()
        out[cid] = bool(row and row[0])
    con.close()
    return out


def summary_dict(report: CoverageReport) -> dict[str, Any]:
    return {
        "dataset_root": report.dataset_root,
        "expected_sessions": report.expected_sessions,
        "sessions_present": report.sessions_present,
        "missing_sessions": [d.isoformat() for d in report.missing_sessions],
        "findings": [{"code": f.code, "detail": f.detail} for f in report.findings],
    }
