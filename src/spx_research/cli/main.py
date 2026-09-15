"""Typer CLI entry point (spec §16 surfaces are built up per milestone)."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any, NoReturn

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
    policy: Annotated[
        str, typer.Option(help="mechanical|llm-mock|llm — decision policy")
    ] = "mechanical",
    tape: Annotated[Path | None, typer.Option(help="decision tape path (llm policies)")] = None,
    model: Annotated[str | None, typer.Option(help="model id for --policy llm")] = None,
    budget_usd: Annotated[str | None, typer.Option(help="API spend cap for --policy llm")] = None,
    price_in_per_mtok: Annotated[
        str | None, typer.Option(help="USD per million input tokens")
    ] = None,
    price_out_per_mtok: Annotated[
        str | None, typer.Option(help="USD per million output tokens")
    ] = None,
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
    mft = json.loads((dataset_root / "manifest.json").read_text())
    tape_path = tape or (out / "decision_tape.jsonl")
    if policy == "mechanical":
        policy_provider = lambda _role: mech  # noqa: E731
    elif policy in ("llm-mock", "llm"):
        try:
            policy_provider = _llm_policy_provider(
                policy=policy,
                profile=profile,
                run_id=run_id,
                tape_path=tape_path,
                model_id=model,
                budget_usd=Decimal(budget_usd) if budget_usd else None,
                price_in=Decimal(price_in_per_mtok) if price_in_per_mtok else None,
                price_out=Decimal(price_out_per_mtok) if price_out_per_mtok else None,
                manifest_id=str(mft.get("manifest_id", "local")),
            )
        except InvalidOperation:
            typer.secho("invalid decimal in --budget-usd/--price-*", fg=typer.colors.RED, err=True)
            raise typer.Exit(2) from None
    else:
        typer.secho(f"unknown policy {policy!r}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    if store == "postgres":
        dsn = os.environ.get("SPX_DB_DSN")
        if not dsn:
            typer.secho("SPX_DB_DSN is not set", fg=typer.colors.RED, err=True)
            raise typer.Exit(2)
        event_store: InMemoryEventStore | PostgresEventStore = PostgresEventStore(
            create_engine(dsn)
        )
    elif store == "memory":
        event_store = InMemoryEventStore()
    else:
        typer.secho(f"unknown store {store!r}", fg=typer.colors.RED)
        raise typer.Exit(2)
    engine = Engine(profile, cal, archive, event_store, policy_provider, run_id=run_id)
    result = engine.run(s, e)

    out.mkdir(parents=True, exist_ok=True)
    with (out / "events.jsonl").open("w") as fh:
        for ev in result.events:
            fh.write(json.dumps(asdict(ev), sort_keys=True, default=str) + "\n")
    (out / "run_manifest.json").write_text(
        json.dumps(
            run_manifest(
                result,
                profile,
                mft.get("manifest_id"),
                extra={
                    "branch_id": engine.branch_id,
                    "policy": policy,
                    "private_manifest_id": (
                        str(mft.get("manifest_id", "local")) if policy != "mechanical" else ""
                    ),
                },
            ),
            indent=2,
        )
    )
    (out / "report.json").write_text(json.dumps(summarize(result), indent=2))
    typer.secho(
        f"OK: {len(result.events)} events, cash={result.final_state.account.cash} -> {out}",
        fg=typer.colors.GREEN,
    )


def _llm_policy_provider(
    *,
    policy: str,
    profile: Any,
    run_id: str,
    tape_path: Path,
    model_id: str | None,
    budget_usd: Decimal | None,
    price_in: Decimal | None,
    price_out: Decimal | None,
    manifest_id: str,
) -> Any:
    """Build a per-role LLMPolicy factory over one shared pipeline (B2).

    ``llm-mock`` is fully offline; ``llm`` fails closed unless the profile
    permits real model requests, OPENAI_API_KEY is set, a model id is known,
    and a bounded budget with explicit price rates is configured.
    """
    import hashlib

    from spx_research.agents.graphs import PolicyDeps
    from spx_research.agents.llm_policy import LLMPolicy
    from spx_research.contracts import SpecNotFoundError, load_prompt, load_schema
    from spx_research.epistemics.harness import Harness
    from spx_research.epistemics.store import InMemoryObservationLedger
    from spx_research.llm.tape import DecisionTape

    def _fail(msg: str) -> NoReturn:
        typer.secho(msg, fg=typer.colors.RED, err=True)
        raise typer.Exit(2)

    try:
        system_prompts = {
            "manager": load_prompt("manager"),
            "spread": load_prompt("spread_agent"),
        }
        schemas = {
            "manager_decision": load_schema("manager_decision"),
            "spread_decision": load_schema("spread_decision"),
        }
    except SpecNotFoundError as e:
        _fail(f"spec contracts unavailable: {e}")

    models = profile.models
    if policy == "llm-mock":
        from spx_research.llm.gateway import MockGateway

        gateway: Any = MockGateway()
        budget = None
        mid = "mock-1"
    else:
        from spx_research.llm.budget import Budget, PriceSheet
        from spx_research.llm.openai_gateway import OpenAIGateway

        if not profile.permissions.real_model_requests:
            _fail("--policy llm requires profile.permissions.real_model_requests: true")
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            _fail("OPENAI_API_KEY is not set")
        candidate = model_id or (
            (models.manager_role_candidate or models.spread_role_candidate) if models else None
        )
        if not candidate:
            _fail("no model id: pass --model or set models.*_role_candidate in the profile")
        mid = candidate
        cap = budget_usd or (models.experiment_api_budget_usd if models else None)
        if cap is None or price_in is None or price_out is None:
            _fail(
                "--policy llm requires a bounded budget: --budget-usd "
                "(or models.experiment_api_budget_usd) plus "
                "--price-in-per-mtok and --price-out-per-mtok"
            )
        import openai

        sheet = PriceSheet("cli", mid, price_in, price_out)
        gateway = OpenAIGateway(openai.OpenAI(api_key=api_key), mid, sheet, allow_real_calls=True)
        budget = Budget(cap, sheet)

    tape_path.parent.mkdir(parents=True, exist_ok=True)
    alias_key = hashlib.sha256(f"spx-alias:{manifest_id}:{run_id}".encode()).digest()
    deps = PolicyDeps(
        harness=Harness(alias_key),
        ledger=InMemoryObservationLedger(),
        gateway=gateway,
        tape=DecisionTape(tape_path),
        profile=profile,
        budget=budget,
        max_retries=models.retry_attempts_after_initial if models else 2,
        max_output_tokens=models.max_output_tokens_per_call if models else 800,
        model_id=mid,
        private_manifest_id=manifest_id,
        system_prompts=system_prompts,
        schemas=schemas,
    )
    pol = LLMPolicy(deps)
    return lambda _role: pol


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
    if ini is None:
        typer.secho("alembic.ini not found (run from the repo root)", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    if "SPX_DB_DSN" not in os.environ:
        typer.secho("SPX_DB_DSN is not set", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    cfg = Config(str(ini))
    cfg.set_main_option("script_location", str(ini.parent / "alembic"))
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
