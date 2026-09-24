"""Availability gateway (M2-05): point-in-time reads over the archive.

Every access filters on ``simulated_available_at_utc <= as_of`` — the modeled
historical availability, never ingestion time or wall-clock now. Revisions are
separate rows; the gateway returns what was visible, preserving earlier values.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb

from spx_research.domain.results import MarketCoverage
from spx_research.domain.types import require_aware

NY = ZoneInfo("America/New_York")


class AvailabilityError(ValueError):
    pass


def validate_archive(
    dataset_root: str | Path,
    *,
    expected_kind: str | None = None,
    expected_manifest_id: str | None = None,
    expected_calendar_id: str | None = None,
) -> dict[str, Any]:
    """Verify the exact immutable files that Archive can read.

    Type checks include synthetic provenance in the actual quote rows; changing
    a manifest label cannot turn provider data into a synthetic fixture.
    """
    root = Path(dataset_root).resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text())
    except (OSError, ValueError) as exc:
        raise AvailabilityError("INVALID_DATA_MANIFEST") from exc
    if not isinstance(manifest, dict):
        raise AvailabilityError("INVALID_DATA_MANIFEST")
    kind = manifest.get("dataset_kind")
    if kind not in ("synthetic", "provider") or (expected_kind and kind != expected_kind):
        raise AvailabilityError("DATASET_KIND_MISMATCH")
    if expected_manifest_id and manifest.get("manifest_id") != expected_manifest_id:
        raise AvailabilityError("DATA_MANIFEST_MISMATCH")
    if expected_calendar_id and manifest.get("calendar_manifest_id") != expected_calendar_id:
        raise AvailabilityError("CALENDAR_MANIFEST_MISMATCH")
    if not isinstance(manifest.get("manifest_id"), str):
        raise AvailabilityError("INVALID_DATA_MANIFEST")
    listed: set[Path] = set()
    con = duckdb.connect(database=":memory:")
    try:
        for rec in [*manifest.get("raw_files", []), *manifest.get("normalized_files", [])]:
            if type(rec.get("rows")) is not int or rec["rows"] < 0:
                raise AvailabilityError("INVALID_MANIFEST_ROW_COUNT")
            path = (root / rec["path"]).resolve()
            if not path.is_relative_to(root) or path in listed or not path.is_file():
                raise AvailabilityError("MANIFEST_FILE_INVALID")
            listed.add(path)
            digest = hashlib.sha256()
            with path.open("rb") as fh:
                for block in iter(lambda: fh.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != rec.get("sha256") or path.stat().st_size != rec.get("bytes"):
                raise AvailabilityError("MANIFEST_CHECKSUM_MISMATCH")
            if path.suffix == ".parquet":
                result = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()
                if result is None or result[0] != rec.get("rows"):
                    raise AvailabilityError("MANIFEST_ROW_COUNT_MISMATCH")
        actual = {p.resolve() for p in root.rglob("*.parquet")}
        if not actual or actual - listed or root / "meta/contracts.parquet" not in listed:
            raise AvailabilityError("UNMANIFESTED_OR_MISSING_DATA")
        quote_files = sorted(root.glob("quotes/session=*.parquet"))
        if not quote_files:
            raise AvailabilityError("MISSING_QUOTE_PARTITIONS")
        if kind == "synthetic":
            if manifest.get("provider") != "synthetic":
                raise AvailabilityError("SYNTHETIC_PROVENANCE_MISMATCH")
            for path in quote_files:
                bad = con.execute(
                    "SELECT COUNT(*) FROM read_parquet(?) "
                    "WHERE quality_flags IS NULL OR NOT list_contains(quality_flags, 'SYNTHETIC')",
                    [str(path)],
                ).fetchone()
                if bad and bad[0]:
                    raise AvailabilityError("SYNTHETIC_PROVENANCE_MISMATCH")
        elif manifest.get("provider") == "synthetic":
            raise AvailabilityError("DATASET_KIND_MISMATCH")
        date.fromisoformat(manifest["historical_start"])
        date.fromisoformat(manifest["historical_end"])
    except (KeyError, TypeError, ValueError, duckdb.Error) as exc:
        if isinstance(exc, AvailabilityError):
            raise
        raise AvailabilityError("INVALID_DATA_MANIFEST") from exc
    finally:
        con.close()
    return manifest


class Archive:
    """Read-side view of one immutable dataset root."""

    def __init__(self, dataset_root: str | Path, *, verify: bool = True) -> None:
        self.root = Path(dataset_root)
        if not (self.root / "manifest.json").is_file():
            raise AvailabilityError(f"no manifest.json under {self.root}")
        self.manifest = (
            validate_archive(self.root)
            if verify
            else json.loads((self.root / "manifest.json").read_text())
        )
        self._con = duckdb.connect(database=":memory:")
        self._quotes_key: str | None = None
        self._static: set[str] = set()

    def _ensure_session_quotes(self, as_of: datetime) -> bool:
        """Materialize the NY session's quote partition into an in-memory table.

        One load per simulated session avoids repeated parquet scans and a
        DuckDB row-group-skip limitation on nested columns (PlainSkip).
        """
        path = self._session_quotes_path(as_of)
        if not path.is_file():
            if self._quotes_key is not None:
                self._con.execute("DROP TABLE IF EXISTS _quotes")
                self._quotes_key = None
            return False
        if self._quotes_key != path.name:
            self._con.execute("DROP TABLE IF EXISTS _quotes")
            self._con.execute("CREATE TABLE _quotes AS SELECT * FROM read_parquet(?)", [str(path)])
            self._quotes_key = path.name
        return True

    def _ensure_static(self, name: str, glob: str) -> bool:
        """Materialize a small parquet file/glob into ``_<name>`` once.

        Filtered direct parquet scans hit the same DuckDB PlainSkip row-group
        limitation as quotes once files grow past one row group; the small
        reference tables (contracts, greeks, macro, settlements, index) are
        cheap to materialize in full.
        """
        if name in self._static:
            return True
        if not list(self.root.glob(glob)):
            return False
        self._con.execute(
            f"CREATE TABLE _{name} AS SELECT * FROM read_parquet(?)", [str(self.root / glob)]
        )
        self._static.add(name)
        return True

    def _q(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        cur = self._con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    def contracts(self) -> list[dict[str, Any]]:
        if not self._ensure_static("contracts", "meta/contracts.parquet"):
            return []
        return self._q("SELECT * FROM _contracts")

    def contracts_visible_at(self, as_of: datetime) -> list[dict[str, Any]]:
        """Contracts whose first verified observation exists by as_of (T04)."""
        as_of = require_aware(as_of)
        if not self._ensure_static("contracts", "meta/contracts.parquet"):
            return []
        return self._q(
            "SELECT * FROM _contracts "
            "WHERE coalesce(first_verified_observation_utc, listed_at_utc) <= ? "
            "ORDER BY contract_id",
            [as_of],
        )

    def session_health(self, as_of: datetime, max_age_seconds: int = 300) -> MarketCoverage:
        """Coverage is separate from the candidate filter, including an empty partition."""
        as_of = require_aware(as_of)
        if not self._ensure_session_quotes(as_of):
            return "MISSING_SESSION"
        rows = self._q(
            "SELECT max(snapshot_at_utc) AS latest FROM _quotes "
            "WHERE simulated_available_at_utc <= ?",
            [as_of],
        )
        latest = rows[0]["latest"]
        if latest is None:
            return "NO_AVAILABLE_QUOTES"
        if (as_of - latest).total_seconds() > max_age_seconds:
            return "COVERAGE_OUTAGE"
        return "HEALTHY"

    def _session_quotes_path(self, as_of: datetime) -> Path:
        """Partition for the NY session containing ``as_of`` (no overnight fill)."""
        ny_day = as_of.astimezone(NY).date()
        return self.root / "quotes" / f"session={ny_day.isoformat()}.parquet"

    def quote_at(
        self, contract_id: str, as_of: datetime, max_age_seconds: int | None = None
    ) -> dict[str, Any] | None:
        """Latest usable quote from as_of's own session, or None (T29/T31).

        ``max_age_seconds`` bounds snapshot staleness — a quote older than
        the bound is treated as absent rather than silently traded on."""
        as_of = require_aware(as_of)
        if not self._ensure_session_quotes(as_of):
            return None
        rows = self._q(
            "SELECT * FROM _quotes "
            "WHERE contract_id = ? AND simulated_available_at_utc <= ? "
            "AND snapshot_at_utc <= simulated_available_at_utc "
            "ORDER BY snapshot_at_utc DESC, simulated_available_at_utc DESC LIMIT 1",
            [contract_id, as_of],
        )
        row = rows[0] if rows else None
        if (
            row is not None
            and max_age_seconds is not None
            and as_of - row["snapshot_at_utc"] > timedelta(seconds=max_age_seconds)
        ):
            return None
        return row

    def session_quotes(
        self,
        contract_ids: list[str],
        as_of: datetime,
        max_age_seconds: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Latest usable same-session quote per contract, batched."""
        as_of = require_aware(as_of)
        if not self._ensure_session_quotes(as_of) or not contract_ids:
            return {}
        marks = ",".join("?" for _ in contract_ids)
        rows = self._q(
            f"SELECT * FROM _quotes WHERE contract_id IN ({marks}) "
            "AND simulated_available_at_utc <= ? "
            "AND snapshot_at_utc <= simulated_available_at_utc "
            "QUALIFY row_number() OVER (PARTITION BY contract_id "
            "ORDER BY snapshot_at_utc DESC, simulated_available_at_utc DESC) = 1",
            [*contract_ids, as_of],
        )
        if max_age_seconds is not None:
            cutoff = as_of - timedelta(seconds=max_age_seconds)
            rows = [r for r in rows if r["snapshot_at_utc"] >= cutoff]
        return {r["contract_id"]: r for r in rows}

    def quotes_at(self, contract_ids: list[str], as_of: datetime) -> dict[str, dict[str, Any]]:
        return self.session_quotes(contract_ids, as_of)

    def index_at(self, as_of: datetime) -> dict[str, Any] | None:
        as_of = require_aware(as_of)
        if not self._ensure_static("index", "quotes/index=*.parquet"):
            return None
        rows = self._q(
            "SELECT * FROM _index "
            "WHERE simulated_available_at_utc <= ? "
            "AND observed_at_utc <= simulated_available_at_utc "
            "ORDER BY observed_at_utc DESC, simulated_available_at_utc DESC LIMIT 1",
            [as_of],
        )
        return rows[0] if rows else None

    def greeks_at(
        self, contract_id: str, as_of: datetime, max_age_seconds: int | None = None
    ) -> dict[str, Any] | None:
        """Latest greek row visible at as_of; ``max_age_seconds`` bounds the
        observation's age — a stale greek reads as absent."""
        as_of = require_aware(as_of)
        if not self._ensure_static("greeks", "meta/greeks.parquet"):
            return None
        rows = self._q(
            "SELECT * FROM _greeks WHERE contract_id = ? "
            "AND simulated_available_at_utc <= ? "
            "AND asof_utc <= simulated_available_at_utc "
            "ORDER BY asof_utc DESC, simulated_available_at_utc DESC LIMIT 1",
            [contract_id, as_of],
        )
        row = rows[0] if rows else None
        if (
            row is not None
            and max_age_seconds is not None
            and as_of - row["asof_utc"] > timedelta(seconds=max_age_seconds)
        ):
            return None
        return row

    def macro_visible_at(
        self, as_of: datetime, series_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Macro rows whose availability time has passed (T08-T12, TK02/TK05)."""
        as_of = require_aware(as_of)
        if not self._ensure_static("macro", "macro/vintages.parquet"):
            return []
        sql = (
            "SELECT * FROM _macro WHERE simulated_available_at_utc <= ?"
            " AND (public_release_at_utc IS NULL "
            "OR public_release_at_utc <= simulated_available_at_utc)"
            + (" AND series_id = ?" if series_id else "")
            + " ORDER BY simulated_available_at_utc"
        )
        params = [as_of, series_id] if series_id else [as_of]
        return self._q(sql, params)

    def settlement_for(self, expiry: Any) -> dict[str, Any] | None:
        if not self._ensure_static("settlements", "meta/settlements.parquet"):
            return None
        rows = self._q("SELECT * FROM _settlements WHERE expiry_local_date = ? LIMIT 1", [expiry])
        return rows[0] if rows else None

    def close(self) -> None:
        self._con.close()
