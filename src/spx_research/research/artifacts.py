"""Immutable execution inputs and relocatable output artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from spx_research.config import Profile
from spx_research.contracts import bundle_manifest

RUN_FORMAT_VERSION = 2


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
        ).encode()
    ).hexdigest()


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("w") as fh:
        json.dump(value, fh, indent=2, sort_keys=True, default=str, allow_nan=False)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(temp, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def code_identity() -> dict[str, str | None]:
    package = Path(__file__).resolve().parents[1]
    source = {
        str(p.relative_to(package)): file_digest(p)
        for p in sorted(package.rglob("*"))
        if p.is_file() and p.suffix in {".py", ".json", ".md", ".yaml"}
    }
    root = package.parents[1]
    lock = root / "uv.lock"
    if not lock.is_file():
        lock = package / "resources" / "dependency.lock"
    commit = None
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        commit = proc.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return {
        "git_commit": commit,
        "source_sha256": digest(source),
        "dependency_lock_sha256": file_digest(lock) if lock.is_file() else None,
    }


def frozen_manifest(
    *,
    run_id: str,
    profile: Profile,
    dataset_root: Path,
    dataset: dict[str, Any],
    start: str,
    end: str,
    policy: str,
    store: str,
    policy_meta: dict[str, Any],
    alias_key_id: str | None = None,
) -> dict[str, Any]:
    """Inputs are frozen before dispatch; output digests are stored separately."""
    inputs = {
        "profile": profile.model_dump(mode="json"),
        "start_date": start,
        "end_date": end,
        "dataset_manifest_id": dataset["manifest_id"],
        "dataset_manifest_sha256": file_digest(dataset_root / "manifest.json"),
        "calendar_sha256": file_digest(dataset_root / "calendar.json"),
        "contracts": bundle_manifest(),
        "code": code_identity(),
        "policy": policy,
        "policy_meta": policy_meta,
        "alias_key_id": alias_key_id,
    }
    return {
        "format_version": RUN_FORMAT_VERSION,
        "run_id": run_id,
        "branch_id": "main",
        "profile_id": profile.profile_id,
        "profile_mode": profile.mode,
        "policy": policy,
        "store": store,
        "resumable": store == "postgres",
        "inputs": inputs,
        "input_sha256": digest(inputs),
        "dataset_manifest_id": dataset["manifest_id"],
        "private_manifest_id": dataset["manifest_id"],
        "initial_cash_usd": str(profile.portfolio.initial_capital_usd)
        if profile.portfolio
        else None,
        "tape_path": "decision_tape.jsonl" if policy != "mechanical" else None,
        "policy_meta": policy_meta,
        "study_label": "HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED",
    }


def validate_frozen_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("format_version") != RUN_FORMAT_VERSION:
        raise ValueError("LEGACY_RUN_REPLAY_ONLY")
    if not isinstance(manifest.get("inputs"), dict):
        raise ValueError("MISSING_RUN_INPUTS")
    if digest(manifest["inputs"]) != manifest.get("input_sha256"):
        raise ValueError("MANIFEST_HASH_MISMATCH")
    inputs = manifest["inputs"]
    required = {
        "profile",
        "start_date",
        "end_date",
        "dataset_manifest_id",
        "dataset_manifest_sha256",
        "calendar_sha256",
        "contracts",
        "code",
        "policy",
        "policy_meta",
        "alias_key_id",
    }
    if not required <= inputs.keys():
        raise ValueError("MISSING_FROZEN_INPUTS")
    profile = Profile.model_validate(inputs["profile"])
    for key in ("dataset_manifest_sha256", "calendar_sha256"):
        value = inputs[key]
        if not isinstance(value, str) or len(value) != 64:
            raise ValueError("INVALID_FROZEN_DIGEST")
    if not isinstance(inputs["code"], dict) or any(
        not inputs["code"].get(key) for key in ("source_sha256", "dependency_lock_sha256")
    ):
        raise ValueError("MISSING_CODE_OR_DEPENDENCY_IDENTITY")
    expected = {
        "policy": inputs["policy"],
        "policy_meta": inputs["policy_meta"],
        "dataset_manifest_id": inputs["dataset_manifest_id"],
        "private_manifest_id": inputs["dataset_manifest_id"],
        "profile_id": profile.profile_id,
        "profile_mode": profile.mode,
        "initial_cash_usd": str(profile.portfolio.initial_capital_usd)
        if profile.portfolio
        else None,
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("MANIFEST_DUPLICATED_INPUT_MISMATCH")
    if manifest.get("store") not in ("memory", "postgres") or manifest.get("resumable") != (
        manifest.get("store") == "postgres"
    ):
        raise ValueError("INVALID_FROZEN_STORE")


def artifact_path(run_dir: Path, declared: str) -> Path:
    """Only relocatable run-owned artifacts are trusted in new manifests."""
    rel = Path(declared)
    if rel.is_absolute() or not (run_dir / rel).resolve().is_relative_to(run_dir.resolve()):
        raise ValueError("ARTIFACT_PATH_ESCAPE")
    return run_dir / rel
