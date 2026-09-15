"""Deterministic synthetic dataset generator (M2 development fixture).

Produces partitioned Parquet matching the data-dictionary field contracts so
the availability gateway, QA, and engine can be exercised end-to-end without
provider data. Output is byte-deterministic given the same seed and calendar.
Prices come from a toy intrinsic-plus-time-value model — clearly synthetic,
never strategy evidence.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from spx_research.data.manifests import (
    DataManifest,
    FileRecord,
    file_record,
    utcnow,
    write_manifest,
)
from spx_research.temporal.calendar import CalendarManifest, SessionDay

NY = ZoneInfo("America/New_York")
Q = Decimal("0.05")


@dataclass(frozen=True)
class SyntheticSpec:
    dataset_id: str
    seed: int
    start: date
    end: date
    spot_start: Decimal = Decimal("5000")
    expiries: tuple[date, ...] = ()
    strike_step: Decimal = Decimal("5")
    strikes_each_side: int = 8
    availability_delay_seconds: int = 0


def _toy_premium(right: str, strike: Decimal, spot: Decimal, dte: int, rng_bias: float) -> Decimal:
    intrinsic = max(strike - spot, Decimal(0)) if right == "PUT" else max(spot - strike, Decimal(0))
    time_value = (
        Decimal("0.4") * Decimal(max(dte, 0)) ** Decimal("0.5") * Decimal(str(1.0 + rng_bias))
    )
    moneyness = abs(spot - strike) / spot
    decay = max(Decimal("0.02"), Decimal(1) - moneyness * Decimal("4"))
    # Convex-in-strike skew so OTM verticals carry realistic nonzero credit
    # that decays as the pair moves further from spot (take-profit works).
    if right == "PUT":
        wing = max(Decimal(0), strike - spot * Decimal("0.9"))
    else:
        wing = max(Decimal(0), spot * Decimal("1.1") - strike)
    skew = Decimal("0.000075") * wing * wing
    return max(Decimal("0.05"), (intrinsic + time_value * decay + skew).quantize(Q))


def _listings(spec: SyntheticSpec, cal: CalendarManifest) -> tuple[dict[str, Any], ...]:
    """SPXW contract master rows: both rights over a strike grid per expiry."""
    rows: list[dict[str, Any]] = []
    first_seen = datetime.combine(spec.start, time(13, 30), tzinfo=UTC) - timedelta(days=30)
    for expiry in spec.expiries:
        base = spec.spot_start
        lo = int(base - spec.strike_step * spec.strikes_each_side)
        hi = int(base + spec.strike_step * spec.strikes_each_side)
        for strike in range(lo, hi + int(spec.strike_step), int(spec.strike_step)):
            for right in ("PUT", "CALL"):
                cid = f"SPXW-{expiry.isoformat()}-{right[0]}{strike}"
                settle_dt = datetime.combine(expiry, time(20, 0), tzinfo=UTC)
                rows.append(
                    {
                        "contract_id": cid,
                        "root": "SPXW",
                        "right": right,
                        "strike_points": float(strike),
                        "expiration_local_date": expiry,
                        "exercise_style": "EUROPEAN",
                        "settlement_style": "PM",
                        "multiplier": 100,
                        "price_increment": 0.05,
                        "listed_at_utc": first_seen,
                        "first_verified_observation_utc": first_seen,
                        "last_trading_at_utc": settle_dt,
                        "settlement_event_at_utc": settle_dt,
                        "settlement_value_symbol": "SPXW_SETTLE",
                    }
                )
    return tuple(rows)


def generate(root: Path, spec: SyntheticSpec, cal: CalendarManifest) -> DataManifest:
    """Write a deterministic synthetic dataset and return its manifest."""
    rng = random.Random(spec.seed)
    ds = root / spec.dataset_id
    (ds / "quotes").mkdir(parents=True, exist_ok=True)
    (ds / "macro").mkdir(parents=True, exist_ok=True)
    (ds / "meta").mkdir(parents=True, exist_ok=True)

    contracts = _listings(spec, cal)
    files: list[FileRecord] = []

    contracts_path = ds / "meta" / "contracts.parquet"
    pl.DataFrame(contracts).write_parquet(contracts_path)
    files.append(file_record(ds, contracts_path, len(contracts)))

    spot = spec.spot_start
    sessions = cal.session_days(spec.start, spec.end)
    expiry_list = list(spec.expiries)
    greek_rows: list[dict[str, Any]] = []

    for sd in sessions:
        idx_rows: list[dict[str, Any]] = []
        quote_rows: list[dict[str, Any]] = []
        for off in sd.minute_offsets():
            ts = sd.open_utc() + timedelta(minutes=off)
            avail = ts + timedelta(seconds=spec.availability_delay_seconds)
            spot = max(Decimal("100"), spot + Decimal(str(rng.gauss(0, 1.1))))
            idx_rows.append(
                {
                    "symbol": "SPX",
                    "observed_at_utc": ts,
                    "simulated_available_at_utc": avail,
                    "value_index_points": float(spot),
                    "quality_flags": [],
                }
            )
            for expiry in expiry_list:
                dte = (expiry - sd.day).days
                if dte < 0:
                    continue
                for c in contracts:
                    if c["expiration_local_date"] != expiry:
                        continue
                    mid = _toy_premium(
                        c["right"], Decimal(str(c["strike_points"])), spot, dte, rng.gauss(0, 0.02)
                    )
                    # Tight synthetic spread: ~2% of typical OTM premium so that
                    # far-OTM verticals still carry small positive package credit.
                    half = Decimal("0.02")
                    quote_rows.append(
                        {
                            "contract_id": c["contract_id"],
                            "snapshot_at_utc": ts,
                            "quote_event_at_utc": None,
                            "quote_event_time_known": False,
                            "simulated_available_at_utc": avail,
                            "bid_points": float(max(Decimal("0"), mid - half)),
                            "ask_points": float(mid + half),
                            "bid_size_contracts": 10,
                            "ask_size_contracts": 10,
                            "bid_exchange": "SYN",
                            "ask_exchange": "SYN",
                            "source_sequence": None,
                            "quality_flags": ["SYNTHETIC"],
                        }
                    )
                    if off == 0:  # one Greek observation per contract per session
                        m = (spot - Decimal(str(c["strike_points"]))) / spot
                        raw = (
                            Decimal("0.5") - m * Decimal("5")
                            if c["right"] == "PUT"
                            else (Decimal("0.5") + m * Decimal("5"))
                        )
                        delta = min(max(raw, Decimal("0.01")), Decimal("0.99"))
                        greek_rows.append(
                            {
                                "contract_id": c["contract_id"],
                                "asof_utc": ts,
                                "simulated_available_at_utc": avail,
                                "delta": float(delta),
                                "gamma": None,
                                "vega": None,
                                "theta": None,
                                "implied_volatility": None,
                                "methodology_id": "synthetic-moneyness-1",
                                "input_snapshot_ids": None,
                                "quality_flags": ["SYNTHETIC"],
                            }
                        )
        qpath = ds / "quotes" / f"session={sd.day.isoformat()}.parquet"
        pl.DataFrame(quote_rows).write_parquet(qpath)
        files.append(file_record(ds, qpath, len(quote_rows)))
        ipath = ds / "quotes" / f"index={sd.day.isoformat()}.parquet"
        pl.DataFrame(idx_rows).write_parquet(ipath)
        files.append(file_record(ds, ipath, len(idx_rows)))

    if greek_rows:
        gpath = ds / "meta" / "greeks.parquet"
        pl.DataFrame(greek_rows).write_parquet(gpath)
        files.append(file_record(ds, gpath, len(greek_rows)))

    macro_rows = _macro_rows(spec, cal, sessions)
    if macro_rows:
        mpath = ds / "macro" / "vintages.parquet"
        pl.DataFrame(macro_rows).write_parquet(mpath)
        files.append(file_record(ds, mpath, len(macro_rows)))

    # Deterministic settlement value per expiry: last session's close on/before.
    settle_rows = []
    for e in expiry_list:
        prior = [s for s in sessions if s.day <= e]
        if not prior:
            continue
        last = prior[-1]
        close_ts = last.close_utc()
        value = spec.spot_start + Decimal(str(rng.gauss(0, 5)))
        settle_rows.append(
            {
                "symbol": "SPXW_SETTLE",
                "expiry_local_date": e,
                "settled_at_utc": close_ts,
                "value_index_points": float(value),
                "published_at_utc": close_ts + timedelta(minutes=30),
                "simulated_available_at_utc": close_ts + timedelta(minutes=30),
            }
        )
    if settle_rows:
        spath = ds / "meta" / "settlements.parquet"
        pl.DataFrame(settle_rows).write_parquet(spath)
        files.append(file_record(ds, spath, len(settle_rows)))

    manifest = DataManifest(
        manifest_id="",  # filled below: content-addressed, not the spec name
        dataset_kind="synthetic",
        provider="synthetic",
        adapter_version="synthetic-1",
        schema_version="2.0",
        created_at_utc=utcnow(),
        query_parameters={"seed": str(spec.seed)},
        historical_start=spec.start,
        historical_end=spec.end,
        normalized_files=tuple(files),
        calendar_manifest_id=cal.calendar_id,
    )
    # Content-addressed: two datasets with the same spec name but different
    # seeds/content get different manifest ids (registry dedupe key).
    manifest = replace(manifest, manifest_id=f"syn-{manifest.content_id()[4:]}")
    write_manifest(ds, manifest)
    return manifest


def _macro_rows(
    spec: SyntheticSpec, cal: CalendarManifest, sessions: list[SessionDay]
) -> list[dict[str, Any]]:
    """Typed macro vintages: daily DGS10 plus one scheduled Fed decision.

    DGS10 publishes 16:15 ET — after the research close — so morning agents
    see the prior day (T10). The Fed decision is announced 14:00 ET during
    the session with a distinct effective date (TK19). A second meeting is
    scheduled but its outcome is never written (TK03/TK12).
    """
    import hashlib

    rows: list[dict[str, Any]] = []
    rng = random.Random(spec.seed + 7)
    dgs = Decimal("4.25")
    for sd in sessions:
        pub = datetime.combine(sd.day, time(16, 15), tzinfo=NY).astimezone(UTC)
        dgs += Decimal(str(round(rng.gauss(0, 0.02), 3)))
        rows.append(
            {
                "series_id": "DGS10",
                "observation_period_start": sd.day,
                "observation_period_end": sd.day,
                "value": str(dgs.quantize(Decimal("0.001"))),
                "unit": "percent",
                "vintage_date": sd.day,
                "public_release_at_utc": pub,
                "rate_effective_at_utc": None,
                "simulated_available_at_utc": pub,
                "availability_policy_id": "h15_release_time",
                "source_uri": "synthetic://fred/DGS10",
                "source_document_id": None,
                "content_hash": hashlib.sha256(f"DGS10:{sd.day}:{dgs}".encode()).hexdigest(),
                "source_manifest_id": f"syn-{spec.dataset_id}",
            }
        )
    mid = sessions[len(sessions) // 2] if sessions else None
    if mid is not None:
        announced = datetime.combine(mid.day, time(14, 0), tzinfo=NY).astimezone(UTC)
        effective = datetime.combine(mid.day + timedelta(days=1), time(0, 0), tzinfo=NY)
        effective = effective.astimezone(UTC)
        rows.append(
            {
                "series_id": "FED_TARGET_UPPER_BPS",
                "observation_period_start": mid.day,
                "observation_period_end": mid.day,
                "value": "550",
                "unit": "basis_points",
                "vintage_date": mid.day,
                "public_release_at_utc": announced,
                "rate_effective_at_utc": effective,
                "simulated_available_at_utc": announced,
                "availability_policy_id": "fomc_statement_release",
                "source_uri": "synthetic://fed/fomc-statement",
                "source_document_id": f"fomc-{mid.day.isoformat()}",
                "content_hash": hashlib.sha256(b"fomc-synthetic").hexdigest(),
                "source_manifest_id": f"syn-{spec.dataset_id}",
            }
        )
        rows.append(
            {
                "series_id": "FED_MEETING_SCHEDULE",
                "observation_period_start": mid.day,
                "observation_period_end": mid.day,
                "value": (mid.day + timedelta(days=45)).isoformat(),
                "unit": "scheduled_date",
                "vintage_date": mid.day,
                "public_release_at_utc": announced,
                "rate_effective_at_utc": None,
                "simulated_available_at_utc": announced,
                "availability_policy_id": "fomc_schedule_publication",
                "source_uri": "synthetic://fed/schedule",
                "source_document_id": f"schedule-{mid.day.isoformat()}",
                "content_hash": hashlib.sha256(b"schedule-synthetic").hexdigest(),
                "source_manifest_id": f"syn-{spec.dataset_id}",
            }
        )
    return rows
