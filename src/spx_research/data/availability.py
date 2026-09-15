"""Availability gateway (M2-05): point-in-time reads over the archive.

Every access filters on ``simulated_available_at_utc <= as_of`` — the modeled
historical availability, never ingestion time or wall-clock now. Revisions are
separate rows; the gateway returns what was visible, preserving earlier values.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from spx_research.domain.types import require_aware


class AvailabilityError(ValueError):
    pass


class Archive:
    """Read-side view of one immutable dataset root."""

    def __init__(self, dataset_root: str | Path) -> None:
        self.root = Path(dataset_root)
        if not (self.root / "manifest.json").is_file():
            raise AvailabilityError(f"no manifest.json under {self.root}")
        self._con = duckdb.connect(database=":memory:")

    def _q(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        cur = self._con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    def contracts(self) -> list[dict[str, Any]]:
        return self._q(f"SELECT * FROM '{self.root}/meta/contracts.parquet'")

    def contracts_visible_at(self, as_of: datetime) -> list[dict[str, Any]]:
        """Contracts whose first verified observation exists by as_of (T04)."""
        as_of = require_aware(as_of)
        return self._q(
            f"SELECT * FROM '{self.root}/meta/contracts.parquet' "
            "WHERE first_verified_observation_utc <= ?",
            [as_of],
        )

    def quote_at(self, contract_id: str, as_of: datetime) -> dict[str, Any] | None:
        """Latest quote snapshot usable at as_of, or None (T29/T31)."""
        as_of = require_aware(as_of)
        rows = self._q(
            f"SELECT * FROM '{self.root}/quotes/session=*.parquet' "
            "WHERE contract_id = ? AND simulated_available_at_utc <= ? "
            "ORDER BY snapshot_at_utc DESC LIMIT 1",
            [contract_id, as_of],
        )
        return rows[0] if rows else None

    def quotes_at(self, contract_ids: list[str], as_of: datetime) -> dict[str, dict[str, Any]]:
        return {cid: q for cid in contract_ids if (q := self.quote_at(cid, as_of)) is not None}

    def index_at(self, as_of: datetime) -> dict[str, Any] | None:
        as_of = require_aware(as_of)
        rows = self._q(
            f"SELECT * FROM '{self.root}/quotes/index=*.parquet' "
            "WHERE simulated_available_at_utc <= ? ORDER BY observed_at_utc DESC LIMIT 1",
            [as_of],
        )
        return rows[0] if rows else None

    def greeks_at(self, contract_id: str, as_of: datetime) -> dict[str, Any] | None:
        as_of = require_aware(as_of)
        gpath = self.root / "meta" / "greeks.parquet"
        if not gpath.is_file():
            return None
        rows = self._q(
            f"SELECT * FROM '{gpath}' WHERE contract_id = ? "
            "AND simulated_available_at_utc <= ? ORDER BY asof_utc DESC LIMIT 1",
            [contract_id, as_of],
        )
        return rows[0] if rows else None

    def macro_visible_at(
        self, as_of: datetime, series_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Macro rows whose availability time has passed (T08-T12, TK02/TK05)."""
        as_of = require_aware(as_of)
        mpath = self.root / "macro" / "vintages.parquet"
        if not mpath.is_file():
            return []
        sql = (
            f"SELECT * FROM '{mpath}' WHERE simulated_available_at_utc <= ?"
            + (" AND series_id = ?" if series_id else "")
            + " ORDER BY simulated_available_at_utc"
        )
        params = [as_of, series_id] if series_id else [as_of]
        return self._q(sql, params)

    def settlement_for(self, expiry: Any) -> dict[str, Any] | None:
        spath = self.root / "meta" / "settlements.parquet"
        if not spath.is_file():
            return None
        rows = self._q(f"SELECT * FROM '{spath}' WHERE expiry_local_date = ? LIMIT 1", [expiry])
        return rows[0] if rows else None

    def close(self) -> None:
        self._con.close()
