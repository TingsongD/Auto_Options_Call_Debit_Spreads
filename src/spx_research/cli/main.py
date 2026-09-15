"""Typer CLI entry point (spec §16 surfaces are built up per milestone)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from spx_research.config import load_profile
from spx_research.preflight import blocking, check

app = typer.Typer(help="SPX credit-spread research platform", no_args_is_help=True)


@app.callback()
def _root() -> None:
    """Research platform CLI; see individual commands."""


@app.command()
def validate_config(
    path: Annotated[Path, typer.Argument(help="YAML profile to validate")],
) -> None:
    """Parse a profile and run the mode-aware preflight gate (M0-03 / T49)."""
    try:
        profile = load_profile(path)
    except (OSError, ValueError, ValidationError) as exc:
        typer.secho(f"INVALID: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    findings = check(profile)
    for f in findings:
        color = typer.colors.RED if f.severity == "BLOCK" else typer.colors.YELLOW
        typer.secho(f"{f.severity} {f.code} {f.path}: {f.detail}", fg=color)
    if blocking(findings):
        typer.secho(f"{len(blocking(findings))} blocking finding(s)", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)
    typer.secho(
        f"OK: {profile.profile_id} ({profile.mode}) preflight passed", fg=typer.colors.GREEN
    )
