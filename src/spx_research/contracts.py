"""Versioned runtime contracts installed with the application.

The handover remains the governing specification. Runtime schemas and prompts
are a checksum-pinned derivative so a wheel is independent of checkout layout.
An explicit development override must contain the same declared bundle.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

SPEC_DIR_ENV = "SPX_SPEC_DIR"
BUNDLE_VERSION = "2.1"
_BUNDLED = Path(__file__).parent / "resources" / "contracts"


class SpecNotFoundError(FileNotFoundError):
    """Runtime contract resources are missing or fail integrity validation."""


def bundle_manifest() -> dict[str, Any]:
    try:
        result: dict[str, Any] = json.loads((_BUNDLED / "bundle.json").read_text())
        return result
    except (OSError, ValueError) as exc:
        raise SpecNotFoundError("CONTRACT_BUNDLE_MISSING") from exc


def spec_dir() -> Path:
    root = Path(os.environ[SPEC_DIR_ENV]) if os.environ.get(SPEC_DIR_ENV) else _BUNDLED
    expected = bundle_manifest()
    try:
        declared = json.loads((root / "bundle.json").read_text())
        if declared != expected:
            raise SpecNotFoundError("CONTRACT_BUNDLE_MISMATCH")
        for relative, sha in expected["files"].items():
            p = (root / relative).resolve()
            if not p.is_relative_to(root.resolve()):
                raise SpecNotFoundError("CONTRACT_PATH_ESCAPE")
            if hashlib.sha256(p.read_bytes()).hexdigest() != sha:
                raise SpecNotFoundError(f"CONTRACT_CHECKSUM_MISMATCH:{relative}")
    except (OSError, ValueError) as exc:
        raise SpecNotFoundError("CONTRACT_BUNDLE_INVALID") from exc
    return root


def _read(relative: str) -> Path:
    if relative not in bundle_manifest()["files"]:
        raise SpecNotFoundError("UNREGISTERED_CONTRACT")
    return spec_dir() / relative


def load_schema(name: str) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(_read(f"schemas/{name}.schema.json").read_text())
    return result


def load_prompt(role: str) -> str:
    return _read(f"prompts/{role}.md").read_text()


def load_example(name: str) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(_read(f"examples/{name}.json").read_text())
    return result


def example_config_path(name: str) -> Path:
    return _read(f"config/{name}.example.yaml")
