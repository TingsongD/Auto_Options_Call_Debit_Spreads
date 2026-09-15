"""ThetaData provider adapter (M2-01) — interface + gated stub.

Only this module may import the provider SDK. Real requests additionally
require ``permissions.real_data_requests`` and the D15/M0-02 licence probe;
until then every call raises ``ProviderPermissionError``. Normalization maps
provider rows into the data-dictionary field contracts with original timestamp
provenance preserved.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from spx_research.config import Profile


class ProviderPermissionError(PermissionError):
    pass


@dataclass(frozen=True)
class FetchWindow:
    start: date
    end: date
    roots: tuple[str, ...] = ("SPXW",)
    interval: str = "1min"


class ThetaAdapter:
    """Boundary between provider SDK and normalized Parquet ingestion."""

    def __init__(self, profile: Profile, out_root: Path) -> None:
        self._profile = profile
        self._out = out_root

    def _gate(self) -> None:
        if not self._profile.permissions.real_data_requests:
            raise ProviderPermissionError(
                "real_data_requests disabled; complete M0-02 licence probe first"
            )
        appr = self._profile.approvals
        if appr is None or not appr.data_rights_approved:
            raise ProviderPermissionError("data_rights_approved is false")

    def fetch_option_quotes(self, window: FetchWindow) -> Path:
        self._gate()
        raise NotImplementedError("provider SDK integration pending licence approval")

    def fetch_contract_listings(self, window: FetchWindow) -> Path:
        self._gate()
        raise NotImplementedError("provider SDK integration pending licence approval")

    def fetch_index(self, window: FetchWindow) -> Path:
        self._gate()
        raise NotImplementedError("provider SDK integration pending licence approval")
