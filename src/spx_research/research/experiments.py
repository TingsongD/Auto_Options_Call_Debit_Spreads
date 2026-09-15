"""Experiment registry (M6): content-addressed run lineage.

An experiment's identity is a hash of its full lineage — profile content,
dataset manifest, model id, decision tape, and code version — so two runs
with identical inputs share an experiment id, and any silent drift in inputs
produces a different experiment. The registry is an append-only JSONL file;
registration is idempotent.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STUDY_LABEL = "HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED"


def _sha_file(path: Path | None) -> str | None:
    if path is None or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha_obj(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


@dataclass(frozen=True)
class Experiment:
    experiment_id: str
    run_id: str
    profile_sha256: str | None
    dataset_manifest_id: str | None
    model_id: str | None
    tape_sha256: str | None
    code_version: str | None
    study_label: str
    registered_at_utc: str


def experiment_id(
    *,
    profile_sha256: str | None,
    dataset_manifest_id: str | None,
    model_id: str | None,
    tape_sha256: str | None,
    code_version: str | None,
) -> str:
    lineage = {
        "profile_sha256": profile_sha256,
        "dataset_manifest_id": dataset_manifest_id,
        "model_id": model_id,
        "tape_sha256": tape_sha256,
        "code_version": code_version,
    }
    return "exp-" + _sha_obj(lineage)[:24]


class ExperimentRegistry:
    """Append-only JSONL registry; keyed by experiment_id."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._records: dict[str, Experiment] = {}
        if self.path.is_file():
            for line in self.path.read_text().splitlines():
                if line.strip():
                    e = Experiment(**json.loads(line))
                    self._records[e.experiment_id] = e

    def register(
        self,
        run_id: str,
        *,
        profile_path: Path | None = None,
        dataset_manifest_id: str | None = None,
        model_id: str | None = None,
        tape_path: Path | None = None,
        code_version: str | None = None,
    ) -> Experiment:
        eid = experiment_id(
            profile_sha256=_sha_file(profile_path),
            dataset_manifest_id=dataset_manifest_id,
            model_id=model_id,
            tape_sha256=_sha_file(tape_path),
            code_version=code_version,
        )
        if eid in self._records:
            return self._records[eid]
        rec = Experiment(
            experiment_id=eid,
            run_id=run_id,
            profile_sha256=_sha_file(profile_path),
            dataset_manifest_id=dataset_manifest_id,
            model_id=model_id,
            tape_sha256=_sha_file(tape_path),
            code_version=code_version,
            study_label=STUDY_LABEL,
            registered_at_utc=datetime.now(UTC).isoformat(),
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a") as fh:
            fh.write(json.dumps(asdict(rec), sort_keys=True) + "\n")
        self._records[eid] = rec
        return rec

    def list(self) -> list[Experiment]:
        return sorted(self._records.values(), key=lambda e: e.registered_at_utc)

    def get(self, experiment_id: str) -> Experiment | None:
        return self._records.get(experiment_id)
