"""Typer CLI entry point (spec §16 surfaces are built up per milestone)."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any, Literal

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
    dataset_root: Annotated[Path, typer.Option(help="verified dataset root")],
    out: Annotated[Path, typer.Option(help="new run output directory")],
    start: Annotated[str | None, typer.Option()] = None,
    end: Annotated[str | None, typer.Option(help="effective runoff end date")] = None,
    run_id: Annotated[str | None, typer.Option()] = None,
    store: Annotated[str, typer.Option(help="memory|postgres")] = "memory",
    policy: Annotated[str, typer.Option(help="mechanical|llm-mock|llm-replay|llm")] = "mechanical",
    tape: Annotated[
        Path | None, typer.Option(help="sealed version 2 input required by llm-replay")
    ] = None,
    model: Annotated[str | None, typer.Option()] = None,
    budget_usd: Annotated[str | None, typer.Option()] = None,
    price_in_per_mtok: Annotated[str | None, typer.Option()] = None,
    price_out_per_mtok: Annotated[str | None, typer.Option()] = None,
    price_cached_per_mtok: Annotated[str | None, typer.Option()] = None,
    model_context_limit: Annotated[int | None, typer.Option()] = None,
    transport: Annotated[str, typer.Option(help="in-process mock|docker")] = "in-process",
    worker_image: Annotated[str, typer.Option()] = "spx-inference:2.1",
    gateway_volume: Annotated[str, typer.Option()] = "spx_mock_gateway_socket",
    alias_namespace: Annotated[
        str, typer.Option(help="shared visible scope for controls")
    ] = "spx-comparison-v2.1",
) -> None:
    """Freeze inputs and run; PostgreSQL is required for restart or paid inference."""
    from uuid import uuid4

    from spx_research.cli.execution import execute

    try:
        if tape is not None and policy != "llm-replay":
            raise ValueError("TAPE_INPUT_REQUIRES_LLM_REPLAY_POLICY")
        if policy == "llm-replay" and tape is None:
            raise ValueError("REPLAY_REQUIRES_TAPE_INPUT")
        profile = load_profile(profile_path)
        if profile.study is None:
            raise ValueError("MISSING_STUDY")
        s = date.fromisoformat(start) if start else profile.study.start_date
        e = (
            date.fromisoformat(end)
            if end
            else (profile.study.runoff_end_date or profile.study.scored_end_date)
        )
        if s is None or e is None:
            raise ValueError("MISSING_STUDY_BOUNDARIES")
        profile.study.start_date, profile.study.runoff_end_date = s, e
        result = execute(
            profile=profile,
            dataset_root=dataset_root,
            out=out,
            start=s,
            end=e,
            run_id=run_id or f"run-{uuid4().hex}",
            store_kind=store,
            policy=policy,
            policy_options={
                "model_id": model,
                "budget_usd": Decimal(budget_usd) if budget_usd else None,
                "price_in": Decimal(price_in_per_mtok) if price_in_per_mtok else None,
                "price_out": Decimal(price_out_per_mtok) if price_out_per_mtok else None,
                "price_cached": Decimal(price_cached_per_mtok) if price_cached_per_mtok else None,
                "context_limit": model_context_limit,
                "transport": transport,
                "worker_image": worker_image,
                "gateway_volume": gateway_volume,
                "alias_namespace": alias_namespace,
                "replay_source": tape,
            },
        )
    except (OSError, ValueError, InvalidOperation) as exc:
        typer.secho(f"BLOCK: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    _show_run(result, out)


def _show_run(result: Any, out: Path) -> None:
    typer.secho(
        f"{result.status}: {len(result.events)} events, "
        f"cash={result.final_state.account.cash} -> {out}",
        fg=typer.colors.GREEN if result.status == "COMPLETED" else typer.colors.YELLOW,
    )
    if result.status != "COMPLETED":
        typer.secho(f"Pause: {result.pause}", err=True)
        raise typer.Exit(1)


@app.command()
def resume(
    run_dir: Annotated[Path, typer.Argument(help="existing new-format PostgreSQL run")],
    dataset_root: Annotated[Path | None, typer.Option(help="relocated identical dataset")] = None,
) -> None:
    """Verify frozen inputs and continue from the authoritative committed phase."""
    from spx_research.cli.execution import resume_run

    try:
        result = resume_run(run_dir, dataset_root)
    except (OSError, ValueError) as exc:
        typer.secho(f"BLOCK: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    _show_run(result, run_dir)


def _llm_policy_provider(**kwargs: Any) -> Any:
    """Compatibility entry for isolated policy tests; CLI uses execution module."""
    from spx_research.cli.execution import policy_provider

    try:
        return policy_provider(**kwargs)
    except (OSError, ValueError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


@app.command()
def migrate() -> None:
    """Apply Alembic migrations to SPX_DB_DSN (M5-01)."""
    from alembic.config import Config

    from alembic import command

    ini = next(
        (
            p / "alembic.ini"
            for p in (Path.cwd(), *Path.cwd().parents)
            if (p / "alembic.ini").exists()
        ),
        None,
    )
    if "SPX_DB_DSN" not in os.environ:
        typer.secho("SPX_DB_DSN is not set", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    cfg = Config(str(ini)) if ini else Config()
    migrations = (
        ini.parent / "alembic"
        if ini
        else (Path(__file__).resolve().parents[1] / "resources" / "migrations")
    )
    cfg.set_main_option("script_location", str(migrations))
    command.upgrade(cfg, "head")
    typer.secho("OK: migrations applied", fg=typer.colors.GREEN)


@app.command()
def replay(
    events_path: Annotated[Path, typer.Argument(help="events.jsonl from a run")],
    initial_cash: Annotated[
        str | None, typer.Option(help="override; defaults to RUN_STARTED payload")
    ] = None,
) -> None:
    """Fold a committed event log; verifies the envelope hash chain first (M3-05)."""
    from spx_research.reporting.report import replay_summary
    from spx_research.research.leakage import load_events, verify_hash_chain

    events = load_events(events_path)
    if not events:
        typer.secho("no events in log", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    if not verify_hash_chain(events):
        typer.secho(
            "FAIL: hash chain verification failed — log is corrupted or tampered",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)
    if initial_cash is None:
        started = next((e for e in events if e.type == "RUN_STARTED"), None)
        initial_cash = str(started.payload["initial_cash_usd"]) if started else "10000"
    st, digest = replay_summary(events[0].run_id, Decimal(initial_cash), events)
    typer.secho(
        f"OK: {len(events)} events folded; cash={st.account.cash}; log={digest[:16]}…",
        fg=typer.colors.GREEN,
    )


@app.command()
def leakage_eval(
    run_dir: Annotated[Path, typer.Argument(help="run output directory with events.jsonl")],
    control_dir: Annotated[
        Path | None, typer.Option(help="second run dir for invariance comparison")
    ] = None,
    tape: Annotated[Path | None, typer.Option(help="decision tape JSONL")] = None,
    initial_cash: Annotated[str, typer.Option()] = "10000",
    out: Annotated[Path | None, typer.Option(help="report output path")] = None,
    cutoff: Annotated[
        str | None, typer.Option(help="inclusive aware ISO timestamp for comparison")
    ] = None,
    expect: Annotated[str, typer.Option(help="invariant|changed")] = "invariant",
) -> None:
    """Fixed-classification leakage evaluation for a run (M6)."""
    from spx_research.research.leakage import compare_runs, evaluate_run

    if expect not in {"invariant", "changed"}:
        raise typer.BadParameter("expect must be invariant or changed")
    comparison_expect: Literal["invariant", "changed"] = (
        "changed" if expect == "changed" else "invariant"
    )
    report = evaluate_run(run_dir, tape_path=tape, initial_cash=Decimal(initial_cash))
    if control_dir is not None:
        report["invariance"] = compare_runs(
            run_dir,
            control_dir,
            cutoff=datetime.fromisoformat(cutoff) if cutoff else None,
            expect=comparison_expect,
        )
    out_path = out or (run_dir / "leakage_report.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True))
    checks = report["checks"]
    ok = report.get("status") == "PASS" or report.get("audit_status") == "PASS"
    if control_dir is not None:
        ok = ok and report["invariance"].get("success") is True
    n_egress = len(checks["egress_violations"])
    typer.secho(
        f"{'OK' if ok and not n_egress else 'FAIL'}: hash_chain={checks['hash_chain_ok']}"
        f" replay={checks['replay_ok']} log_hash_match={checks.get('log_hash_match')}"
        f" egress_violations={n_egress} -> {out_path}",
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
    """Register frozen execution inputs, preserving every distinct run."""

    from spx_research.research.experiments import ExperimentRegistry

    manifest_path = run_dir / "run_manifest.json"
    try:
        if profile or tape or model_id:
            raise ValueError("REGISTRATION_USES_FROZEN_INPUTS_ONLY")
        manifest = json.loads(manifest_path.read_text())
        rec = ExperimentRegistry(registry).register_manifest(manifest)
    except (OSError, ValueError) as exc:
        typer.secho(f"BLOCK: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    typer.secho(f"OK: {rec.experiment_id} (run {rec.run_id}) -> {registry}", fg=typer.colors.GREEN)
