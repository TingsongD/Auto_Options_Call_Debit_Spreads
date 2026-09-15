"""Macro vintage ingestion adapter (M2-03) — gated stub.

FRED/ALFRED vintages and original Fed releases become typed rows with
``simulated_available_at_utc`` set from documented release times or an approved
conservative bound — never from download time. Announcement vs. effective time
is preserved (TK19); schedules are separate rows so a listed future meeting
carries no outcome (TK03).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from spx_research.config import Profile
from spx_research.data.theta_adapter import ProviderPermissionError


class MacroAdapter:
    def __init__(self, profile: Profile, out_root: Path) -> None:
        self._profile = profile
        self._out = out_root

    def _gate(self) -> None:
        if not self._profile.permissions.real_data_requests:
            raise ProviderPermissionError("real_data_requests disabled")

    def fetch_alfred_vintages(self, series_id: str, start: date, end: date) -> Path:
        self._gate()
        raise NotImplementedError("ALFRED integration pending licence/timing review")

    def fetch_fed_documents(self, start: date, end: date) -> Path:
        self._gate()
        raise NotImplementedError("Fed document ingestion pending timing review")
