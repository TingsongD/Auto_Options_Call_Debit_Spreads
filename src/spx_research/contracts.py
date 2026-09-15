"""Locate and load the governing specification's contract files.

The spec package (`spx_ai_handover_v2/`) remains the single source of truth for
JSON schemas, prompts, example packets and example profiles. This module
resolves that directory — `SPX_SPEC_DIR` env var, then the sibling of the app
repo — without duplicating the files into this package.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

SPEC_DIR_ENV = "SPX_SPEC_DIR"
_SPEC_DIRNAME = "spx_ai_handover_v2"


class SpecNotFoundError(FileNotFoundError):
    """The governing spec package could not be located."""


def spec_dir() -> Path:
    """Return the spec package directory or raise SpecNotFoundError."""
    env = os.environ.get(SPEC_DIR_ENV)
    if env:
        p = Path(env)
        if (p / "schemas").is_dir():
            return p
        raise SpecNotFoundError(f"{SPEC_DIR_ENV}={env} has no schemas/ directory")
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / _SPEC_DIRNAME
        if (candidate / "schemas").is_dir():
            return candidate
        candidate = parent / "app" / ".." / _SPEC_DIRNAME
    # app repo layout: <root>/app/src/spx_research/contracts.py -> <root>/<spec>
    candidate = here.parents[4] / _SPEC_DIRNAME
    if (candidate / "schemas").is_dir():
        return candidate
    raise SpecNotFoundError(
        f"could not find {_SPEC_DIRNAME}/schemas above {here}; set {SPEC_DIR_ENV}"
    )


def _read(relative: str) -> Path:
    path = spec_dir() / relative
    if not path.is_file():
        raise SpecNotFoundError(f"missing spec file: {path}")
    return path


def load_schema(name: str) -> dict[str, Any]:
    """Load a JSON schema by file name, e.g. 'model_visible_packet'."""
    with _read(f"schemas/{name}.schema.json").open() as f:
        result: dict[str, Any] = json.load(f)
        return result


def load_prompt(role: str) -> str:
    """Load a prompt template ('manager' or 'spread_agent')."""
    return _read(f"prompts/{role}.md").read_text()


def load_example(name: str) -> dict[str, Any]:
    """Load an example JSON document, e.g. 'spread_packet'."""
    with _read(f"examples/{name}.json").open() as f:
        result: dict[str, Any] = json.load(f)
        return result


def example_config_path(name: str) -> Path:
    """Path to an example profile ('research' or 'synthetic')."""
    return _read(f"config/{name}.example.yaml")
