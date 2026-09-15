"""Typer CLI entry point (spec §16 surfaces are built up per milestone)."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
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


@app.command()
def generate_synthetic(
    out: Annotated[Path, typer.Option(help="dataset root to write")],
    start: Annotated[str, typer.Option(help="first NY session YYYY-MM-DD")],
    end: Annotated[str, typer.Option(help="last NY session YYYY-MM-DD")],
    dataset_id: Annotated[str, typer.Option()] = "syn-dev",
    seed: Annotated[int, typer.Option()] = 7,
    expiry_days_ahead: Annotated[int, typer.Option()] = 45,
) -> None:
    """Build a deterministic synthetic dataset + calendar manifest (M2)."""
    from datetime import timedelta

    from spx_research.data.synthetic import SyntheticSpec, generate
    from spx_research.temporal.calendar import build_weekday_manifest

    s, e = date.fromisoformat(start), date.fromisoformat(end)
    cal = build_weekday_manifest("syn-cal-1", s, e + timedelta(days=90))
    expiry = e + timedelta(days=expiry_days_ahead)
    spec = SyntheticSpec(
        dataset_id=dataset_id,
        seed=seed,
        start=s,
        end=e,
        expiries=(expiry,),
    )
    out.mkdir(parents=True, exist_ok=True)
    manifest = generate(out, spec, cal)
    cal_path = out / dataset_id / "calendar.json"
    cal_path.write_text(
        json.dumps(
            {
                "calendar_id": cal.calendar_id,
                "version": cal.version,
                "sessions": [
                    {
                        "date": sd.day.isoformat(),
                        "open": sd.open_local.isoformat()[:5],
                        "close": sd.close_local.isoformat()[:5],
                        "half_day": sd.half_day,
                    }
                    for sd in cal.sessions
                ],
            },
            indent=2,
        )
    )
    typer.secho(
        f"OK: dataset {manifest.content_id()} at {out} ({len(manifest.normalized_files)} files)",
        fg=typer.colors.GREEN,
    )


@app.command()
def run(
    profile_path: Annotated[Path, typer.Argument(help="YAML run profile")],
    dataset_root: Annotated[Path, typer.Option(help="dataset root with manifest.json")],
    out: Annotated[Path, typer.Option(help="run output directory")],
    start: Annotated[str | None, typer.Option()] = None,
    end: Annotated[str | None, typer.Option()] = None,
    run_id: Annotated[str, typer.Option()] = "run-0001",
    store: Annotated[str, typer.Option(help="memory|postgres (dsn from SPX_DB_DSN)")] = "memory",
) -> None:
    """Execute the deterministic baseline engine over a dataset (M3)."""
    from spx_research.data.availability import Archive
    from spx_research.engine.scheduler import Engine
    from spx_research.persistence.events import InMemoryEventStore
    from spx_research.persistence.postgres import PostgresEventStore, create_engine
    from spx_research.reporting.report import run_manifest, summarize
    from spx_research.research.mechanical import MechanicalPolicy
    from spx_research.temporal.calendar import load_manifest

    profile = load_profile(profile_path)
    findings = blocking(check(profile))
    if findings:
        for f in findings:
            typer.secho(f"BLOCK {f.code} {f.path}: {f.detail}", fg=typer.colors.RED)
        raise typer.Exit(1)
    cal = load_manifest(dataset_root / "calendar.json")
    archive = Archive(dataset_root)
    assert profile.study is not None and profile.study.start_date is not None
    s = date.fromisoformat(start) if start else profile.study.start_date
    e = (
        date.fromisoformat(end)
        if end
        else (profile.study.runoff_end_date or profile.study.scored_end_date)
    )
    assert e is not None, "no end date in profile or --end"

    assert profile.exit_policy is not None
    pb, lb = profile.exit_policy.profit_review_band, profile.exit_policy.loss_review_band
    mech = MechanicalPolicy(
        profit_trigger=(pb[0] + pb[1]) / 2,
        loss_trigger=-(lb[0] + lb[1]) / 2,
        loss_activation_days=profile.exit_policy.loss_activation_days_held,
    )
    if store == "postgres":
        event_store: InMemoryEventStore | PostgresEventStore = PostgresEventStore(
            create_engine(os.environ["SPX_DB_DSN"])
        )
    elif store == "memory":
        event_store = InMemoryEventStore()
    else:
        typer.secho(f"unknown store {store!r}", fg=typer.colors.RED)
        raise typer.Exit(2)
    engine = Engine(profile, cal, archive, event_store, lambda _role: mech, run_id=run_id)
    result = engine.run(s, e)

    mft = json.loads((dataset_root / "manifest.json").read_text())
    out.mkdir(parents=True, exist_ok=True)
    with (out / "events.jsonl").open("w") as fh:
        for ev in result.events:
            fh.write(json.dumps(asdict(ev), sort_keys=True, default=str) + "\n")
    (out / "run_manifest.json").write_text(
        json.dumps(run_manifest(result, profile, mft.get("manifest_id")), indent=2)
    )
    (out / "report.json").write_text(json.dumps(summarize(result), indent=2))
    typer.secho(
        f"OK: {len(result.events)} events, cash={result.final_state.account.cash} -> {out}",
        fg=typer.colors.GREEN,
    )


@app.command()
def migrate() -> None:
    """Apply Alembic migrations to SPX_DB_DSN (M5-01)."""
    from alembic.config import Config

    from alembic import command

    cfg = Config(str(Path(__file__).resolve().parents[3] / "alembic.ini"))
    command.upgrade(cfg, "head")
    typer.secho("OK: migrations applied", fg=typer.colors.GREEN)


@app.command()
def replay(
    events_path: Annotated[Path, typer.Argument(help="events.jsonl from a run")],
    initial_cash: Annotated[str, typer.Option()] = "10000",
) -> None:
    """Fold a committed event log; verifies hash chain. No model calls (M3-05)."""
    from spx_research.domain.state import Event
    from spx_research.reporting.report import replay_summary

    events = [
        Event(
            e["run_id"],
            e["seq"],
            _parse_dt(e["sim_time_utc"]),
            e["phase"],
            e["type"],
            e["payload"],
            e["payload_hash"],
            e["previous_hash"],
        )
        for e in (json.loads(line) for line in events_path.read_text().splitlines() if line.strip())
    ]
    run_id = events[0].run_id if events else "unknown"
    st, digest = replay_summary(run_id, Decimal(initial_cash), events)
    typer.secho(
        f"OK: {len(events)} events folded; cash={st.account.cash}; log={digest[:16]}…",
        fg=typer.colors.GREEN,
    )


def _parse_dt(v: str) -> datetime:
    from datetime import datetime

    return datetime.fromisoformat(v)


@app.command()
def leakage_eval(
    run_dir: Annotated[Path, typer.Argument(help="run output directory with events.jsonl")],
    control_dir: Annotated[
        Path | None, typer.Option(help="second run dir for invariance comparison")
    ] = None,
    tape: Annotated[Path | None, typer.Option(help="decision tape JSONL")] = None,
    initial_cash: Annotated[str, typer.Option()] = "10000",
    out: Annotated[Path | None, typer.Option(help="report output path")] = None,
) -> None:
    """Fixed-classification leakage evaluation for a run (M6)."""
    from spx_research.research.leakage import compare_runs, evaluate_run

    report = evaluate_run(run_dir, tape_path=tape, initial_cash=Decimal(initial_cash))
    if control_dir is not None:
        report["invariance"] = compare_runs(run_dir, control_dir)
    out_path = out or (run_dir / "leakage_report.json")
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True))
    ok = report["checks"]["hash_chain_ok"] and report["checks"]["replay_ok"]
    n_egress = len(report["checks"]["egress_violations"])
    typer.secho(
        f"{'OK' if ok and not n_egress else 'FAIL'}: hash_chain={report['checks']['hash_chain_ok']}"
        f" replay={report['checks']['replay_ok']} egress_violations={n_egress}"
        f" -> {out_path}",
        fg=typer.colors.GREEN if ok and not n_egress else typer.colors.RED,
    )
    if not (ok and not n_egress):
        raise typer.Exit(1)


@app.command()
def register_run(
    run_dir: Annotated[Path, typer.Argument(help="run output directory")],
    registry: Annotated[Path, typer.Option(help="registry JSONL path")],
    profile: Annotated[Path | None, typer.Option(help="profile YAML used for the run")] = None,
    tape: Annotated[Path | None, typer.Option(help="decision tape JSONL")] = None,
    model_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Register a run's lineage in the experiment registry (M6)."""
    import subprocess

    from spx_research.research.experiments import ExperimentRegistry

    manifest_path = run_dir / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    import contextlib

    code_version = None
    with contextlib.suppress(Exception):
        code_version = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    rec = ExperimentRegistry(registry).register(
        manifest.get("run_id", run_dir.name),
        profile_path=profile,
        dataset_manifest_id=manifest.get("dataset_manifest_id"),
        model_id=model_id,
        tape_path=tape,
        code_version=code_version,
    )
    typer.secho(f"OK: {rec.experiment_id} (run {rec.run_id}) -> {registry}", fg=typer.colors.GREEN)
