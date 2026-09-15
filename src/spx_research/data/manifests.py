"""Data manifest types (data dictionary §2).

A manifest is an immutable, checksummed description of one dataset version.
``created_at_utc`` is real ingestion time — never the historical time agents
may see the data. A repaired dataset gets a new ``manifest_id`` and a new
experiment identity.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path


@dataclass(frozen=True)
class FileRecord:
    path: str
    sha256: str
    bytes: int
    rows: int


@dataclass(frozen=True)
class DataManifest:
    manifest_id: str
    dataset_kind: str  # "synthetic" | "provider"
    provider: str
    adapter_version: str
    schema_version: str
    created_at_utc: datetime
    query_parameters: dict[str, str]
    historical_start: date
    historical_end: date
    raw_files: tuple[FileRecord, ...] = ()
    normalized_files: tuple[FileRecord, ...] = ()
    calendar_manifest_id: str | None = None
    quality_report_id: str | None = None
    correction_policy: str = "none"
    supersedes_manifest_id: str | None = None
    licence_evidence_id: str | None = None

    def __post_init__(self) -> None:
        if self.created_at_utc.tzinfo is None:
            raise ValueError("manifest created_at must be timezone-aware UTC")

    def content_id(self) -> str:
        """Deterministic manifest content hash over files and parameters."""
        payload = asdict(self)
        payload.pop("created_at_utc", None)  # operational timestamp excluded
        blob = json.dumps(payload, sort_keys=True, default=str).encode()
        return "mft_" + hashlib.sha256(blob).hexdigest()[:24]


def file_record(root: Path, path: Path, rows: int) -> FileRecord:
    blob = path.read_bytes()
    return FileRecord(
        path=str(path.relative_to(root)),
        sha256=hashlib.sha256(blob).hexdigest(),
        bytes=len(blob),
        rows=rows,
    )


def write_manifest(root: Path, manifest: DataManifest) -> Path:
    out = root / "manifest.json"
    out.write_text(json.dumps(asdict(manifest), indent=2, sort_keys=True, default=str))
    return out


def utcnow() -> datetime:
    return datetime.now(UTC)
