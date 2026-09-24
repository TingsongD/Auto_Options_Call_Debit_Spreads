"""Run orchestration: freeze inputs, recover journal, atomically export results."""

from __future__ import annotations

import hmac
import json
import os
import tempfile
from dataclasses import asdict
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from spx_research.config import Profile
from spx_research.contracts import bundle_manifest, load_prompt, load_schema
from spx_research.persistence.runtime import InMemoryRunStore, PostgresRunStore, RunStore
from spx_research.research.artifacts import (
    atomic_json,
    code_identity,
    file_digest,
    frozen_manifest,
    validate_frozen_manifest,
)


def run_store(kind: str) -> RunStore:
    if kind == "memory":
        return InMemoryRunStore()
    if kind != "postgres":
        raise ValueError("UNKNOWN_STORE")
    from spx_research.persistence.postgres import create_engine

    dsn = os.environ.get("SPX_DB_DSN")
    if not dsn:
        raise ValueError("SPX_DB_DSN_REQUIRED")
    return PostgresRunStore(create_engine(dsn))


def alias_key_id() -> str:
    secret = os.environ.get("SPX_ALIAS_KEY", "")
    if len(secret) < 16:
        raise ValueError("SPX_ALIAS_KEY_REQUIRED_MINIMUM_16_CHARACTERS")
    return hmac.new(secret.encode(), b"spx-alias-key-identifier-v2", "sha256").hexdigest()


def model_settings(
    profile: Profile,
    policy: str,
    *,
    model_id: str | None,
    budget_usd: Decimal | None,
    price_in: Decimal | None,
    price_out: Decimal | None,
    price_cached: Decimal | None,
    context_limit: int | None,
    transport: str,
    worker_image: str,
    gateway_volume: str,
    alias_namespace: str | None,
) -> dict[str, Any]:
    models = profile.models
    role_ids: dict[str, str] = {}
    if models and models.resolved_model_ids_manifest:
        raw = json.loads(Path(models.resolved_model_ids_manifest).read_text())
        role_ids = {
            role: str(raw[key])
            for key, role in (("manager_role", "manager"), ("spread_role", "spread"))
            if raw.get(key)
        }
    if model_id:
        role_ids = {"manager": model_id, "spread": model_id}
    for role in ("manager", "spread"):
        candidate = getattr(models, f"{role}_role_candidate", None) if models else None
        role_ids.setdefault(role, candidate or ("mock-1" if policy == "llm-mock" else ""))
    prices: dict[str, Any] = {}
    if models and models.price_sheet_id:
        sheet = json.loads(Path(models.price_sheet_id).read_text())
        for row in sheet.get("models", [sheet]):
            prices[row["model_id"]] = row
    if any(x is not None for x in (price_in, price_out, price_cached)):
        if any(x is None for x in (price_in, price_out, price_cached)):
            raise ValueError("ALL_THREE_PRICE_RATES_REQUIRED")
        if len(set(role_ids.values())) != 1:
            raise ValueError("MULTIPLE_MODELS_REQUIRE_MODEL_SPECIFIC_PRICE_SHEET")
        mid = next(iter(role_ids.values()))
        prices[mid] = {
            "model_id": mid,
            "sheet_id": "cli-frozen",
            "input_per_mtok": str(price_in),
            "output_per_mtok": str(price_out),
            "cached_input_per_mtok": str(price_cached),
            "model_context_limit": context_limit,
        }
    for mid, row in prices.items():
        if not row.get("model_context_limit"):
            row["model_context_limit"] = context_limit or (
                models.model_context_limits.get(mid) if models else None
            )
    cap = (
        budget_usd
        if budget_usd is not None
        else (models.experiment_api_budget_usd if models else None)
    )
    return {
        "resolved_model_ids": role_ids,
        "prices": prices,
        "budget_usd": str(cap) if cap is not None else None,
        "max_output_tokens": models.max_output_tokens_per_call if models else 800,
        "max_retries": models.retry_attempts_after_initial if models else 2,
        "transport": transport,
        "worker_image": worker_image,
        "gateway_volume": gateway_volume,
        "alias_namespace": alias_namespace,
    }


def policy_provider(
    *,
    policy: str,
    profile: Profile,
    run_id: str,
    branch_id: str,
    tape_path: Path,
    model_id: str | None,
    budget_usd: Decimal | None,
    price_in: Decimal | None,
    price_out: Decimal | None,
    manifest_id: str,
    ledger: Any,
    runtime: RunStore | None = None,
    price_cached: Decimal | None = None,
    context_limit: int | None = None,
    transport: str = "in-process",
    worker_image: str = "spx-inference:2.1",
    gateway_volume: str = "spx_mock_gateway_socket",
    alias_namespace: str | None = None,
    frozen_settings: dict[str, Any] | None = None,
    replay_source: Path | None = None,
) -> Any:
    from spx_research.agents.graphs import PolicyDeps
    from spx_research.agents.llm_policy import LLMPolicy
    from spx_research.epistemics.harness import Harness
    from spx_research.llm.budget import Budget, PriceSheet
    from spx_research.llm.gateway import MockGateway, RecordedDecisionGateway
    from spx_research.llm.tape import DecisionTape

    if (
        policy == "llm"
        and profile.models is not None
        and (profile.models.provider != "openai" or profile.models.interface != "responses")
    ):
        raise ValueError("UNSUPPORTED_PROVIDER_INTERFACE")
    replay_gateway = None
    if policy == "llm-replay":
        if replay_source is None:
            raise ValueError("REPLAY_REQUIRES_TAPE_INPUT")
        if (
            any(
                value is not None
                for value in (
                    model_id,
                    budget_usd,
                    price_in,
                    price_out,
                    price_cached,
                    context_limit,
                )
            )
            or transport != "in-process"
        ):
            raise ValueError("REPLAY_USES_SOURCE_MODEL_SETTINGS_ONLY")
        replay_gateway = RecordedDecisionGateway(
            DecisionTape(replay_source), file_digest(replay_source)
        )
        replay_models = dict(replay_gateway.role_ids)
        for role in ("manager", "spread"):
            replay_models.setdefault(role, next(iter(replay_models.values())))
        replay_settings: dict[str, Any] = {
            "resolved_model_ids": replay_models,
            "prices": {},
            "budget_usd": None,
            "max_output_tokens": replay_gateway.max_output_tokens,
            "max_retries": 0,
            "transport": "sealed-tape",
            "alias_namespace": replay_gateway.alias_namespace,
            "replay_source": {"path": "replay_input.jsonl", "sha256": file_digest(replay_source)},
        }
        if frozen_settings is not None and frozen_settings != replay_settings:
            raise ValueError("FROZEN_REPLAY_INPUT_CHANGED")
        settings = replay_settings
    else:
        settings = frozen_settings or model_settings(
            profile,
            policy,
            model_id=model_id,
            budget_usd=budget_usd,
            price_in=price_in,
            price_out=price_out,
            price_cached=price_cached,
            context_limit=context_limit,
            transport=transport,
            worker_image=worker_image,
            gateway_volume=gateway_volume,
            alias_namespace=alias_namespace,
        )
    role_ids = settings["resolved_model_ids"]
    sheets: dict[str, PriceSheet] = {}
    budget = None
    if policy == "llm":
        if not profile.permissions.real_model_requests:
            raise ValueError("REAL_MODEL_PERMISSION_REQUIRED")
        if not isinstance(runtime, PostgresRunStore):
            raise ValueError("PAID_RUN_REQUIRES_POSTGRES")
        if settings["transport"] != "docker":
            raise ValueError("PAID_RUN_REQUIRES_DOCKER_ISOLATION")
        for mid in set(role_ids.values()):
            raw = settings["prices"].get(mid)
            if not mid or not raw or raw.get("model_id") != mid:
                raise ValueError("MISSING_MODEL_SPECIFIC_PRICE_SHEET")
            if raw.get("cached_input_per_mtok") is None or not raw.get("model_context_limit"):
                raise ValueError("CACHED_PRICING_AND_CONTEXT_LIMIT_REQUIRED")
            sheets[mid] = PriceSheet(
                str(raw.get("sheet_id", "frozen")),
                mid,
                Decimal(str(raw["input_per_mtok"])),
                Decimal(str(raw["output_per_mtok"])),
                cached_input_per_million=Decimal(str(raw["cached_input_per_mtok"])),
                model_context_limit=int(raw["model_context_limit"]),
            )
        if settings["budget_usd"] is None:
            raise ValueError("BOUNDED_BUDGET_REQUIRED")
        budget = Budget(Decimal(settings["budget_usd"]), next(iter(sheets.values())), sheets)
    if replay_gateway is not None:
        gateway: Any = replay_gateway
    elif settings["transport"] == "docker":
        from spx_research.isolation.client import DockerGateway

        gateway = DockerGateway(
            image=settings["worker_image"],
            socket_volume=settings["gateway_volume"],
            sheets=sheets,
            mock=policy == "llm-mock",
        )
        settings = {**settings, "worker_image": gateway.image}
    elif settings["transport"] == "in-process" and policy == "llm-mock":
        gateway = MockGateway()
    else:
        raise ValueError("UNSUPPORTED_INFERENCE_TRANSPORT")
    alias_key_id()
    namespace = settings["alias_namespace"] or f"{run_id}:{branch_id}"
    key = hmac.new(os.environ["SPX_ALIAS_KEY"].encode(), namespace.encode(), "sha256").digest()
    deps = PolicyDeps(
        harness=Harness(key),
        ledger=ledger,
        gateway=gateway,
        tape=DecisionTape(tape_path),
        profile=profile,
        budget=budget,
        model_id=role_ids["manager"],
        model_ids=role_ids,
        max_retries=int(settings["max_retries"]),
        max_output_tokens=int(settings["max_output_tokens"]),
        private_manifest_id=manifest_id,
        system_prompts={"manager": load_prompt("manager"), "spread": load_prompt("spread_agent")},
        schemas={name: load_schema(name) for name in ("manager_decision", "spread_decision")},
        runtime=runtime,
        public_alias_namespace=settings["alias_namespace"],
    )
    pol = LLMPolicy(deps)

    def provider(_role: str) -> Any:
        return pol

    provider.meta = settings  # type: ignore[attr-defined]
    return provider


def validate_inputs(profile: Profile, root: Path, start: date, end: date) -> dict[str, Any]:
    from spx_research.data.availability import validate_archive
    from spx_research.preflight import blocking, check
    from spx_research.temporal.calendar import load_manifest

    findings = blocking(check(profile))
    if findings:
        raise ValueError("PREFLIGHT_BLOCKED:" + ",".join(f"{f.code}:{f.path}" for f in findings))
    for section in (
        "universe",
        "clock",
        "portfolio",
        "exit_policy",
        "manager",
        "execution",
        "study",
    ):
        if getattr(profile, section) is None:
            raise ValueError("MISSING_CONFIG_SECTION:" + section)
    assert profile.study and profile.clock
    scored = profile.study.scored_end_date
    if scored is None or not start <= scored <= end:
        raise ValueError("INVALID_EFFECTIVE_STUDY_BOUNDARIES")
    # Inspect type, permission and dates before any archive contents are opened.
    raw = json.loads((root / "manifest.json").read_text())
    kind = "synthetic" if profile.mode == "synthetic_test" else "provider"
    if raw.get("dataset_kind") != kind:
        raise ValueError("DATASET_KIND_MISMATCH")
    if kind == "provider" and not profile.permissions.real_data_requests:
        raise ValueError("REAL_DATA_PERMISSION_REQUIRED")
    if (
        date.fromisoformat(raw["historical_start"]) > start
        or date.fromisoformat(raw["historical_end"]) < end
    ):
        raise ValueError("DATASET_DOES_NOT_COVER_EFFECTIVE_STUDY")
    cal = load_manifest(root / "calendar.json")
    if not cal.session_days(start, end):
        raise ValueError("NO_CALENDAR_SESSIONS_IN_STUDY")
    if profile.clock.calendar_manifest_id not in (None, cal.calendar_id):
        raise ValueError("CALENDAR_MANIFEST_MISMATCH")
    return validate_archive(
        root,
        expected_kind=kind,
        expected_manifest_id=profile.study.data_manifest_id,
        expected_calendar_id=cal.calendar_id,
    )


def export_tape(store: RunStore, run_id: str, path: Path) -> None:
    """Rebuild accepted-decision export exclusively from authoritative records."""
    from spx_research.llm.tape import DecisionTape
    from spx_research.llm.types import ModelRequest, response_from_dict

    temp = path.with_name(".journal-export.jsonl")
    temp.unlink(missing_ok=True)
    tape = DecisionTape(temp)
    for entry in store.journal(run_id):
        if entry["kind"] != "DECISION_ACCEPTED":
            continue
        rec = store.load_decision(run_id, entry["payload"]["decision_id"])
        if rec is None or rec.result is None:
            raise ValueError("JOURNAL_DECISION_MISSING")
        result = rec.result
        tape.append(
            ModelRequest(**result["request"]),
            response_from_dict(result["response"]),
            rec.request["compiled"]["context"]["as_of"],
            decision_id=rec.decision_id,
            prepared=rec.request,
            result=result,
        )
    tape.export()
    os.replace(temp, path)


def execute(
    *,
    profile: Profile,
    dataset_root: Path,
    out: Path,
    start: date,
    end: date,
    run_id: str,
    store_kind: str,
    policy: str,
    policy_options: dict[str, Any],
    resume_manifest: dict[str, Any] | None = None,
) -> Any:
    from spx_research.data.availability import Archive
    from spx_research.engine.scheduler import Engine
    from spx_research.reporting.report import attempt_summary, event_log_digest, summarize
    from spx_research.research.mechanical import MechanicalPolicy
    from spx_research.temporal.calendar import load_manifest

    if policy not in ("mechanical", "llm-mock", "llm-replay", "llm"):
        raise ValueError("UNKNOWN_POLICY")
    replay_source = policy_options.get("replay_source")
    if replay_source is not None and policy != "llm-replay":
        raise ValueError("TAPE_INPUT_REQUIRES_LLM_REPLAY_POLICY")
    if policy == "llm-replay" and replay_source is None and resume_manifest is None:
        raise ValueError("REPLAY_REQUIRES_TAPE_INPUT")
    dataset = validate_inputs(profile, dataset_root, start, end)
    runtime = run_store(store_kind)
    if resume_manifest is None and runtime.load_run(run_id) is not None:
        raise ValueError("RUN_ID_EXISTS_USE_RESUME")
    if resume_manifest is None and out.exists() and any(out.iterdir()):
        raise ValueError("OUTPUT_DIRECTORY_NOT_EMPTY")
    if resume_manifest is not None:
        validate_frozen_manifest(resume_manifest)
        frozen = resume_manifest["inputs"]
        if (
            frozen["code"] != code_identity()
            or frozen["contracts"] != bundle_manifest()
            or frozen["dataset_manifest_sha256"] != file_digest(dataset_root / "manifest.json")
            or frozen["calendar_sha256"] != file_digest(dataset_root / "calendar.json")
            or frozen["profile"] != profile.model_dump(mode="json")
        ):
            raise ValueError("FROZEN_INPUTS_CHANGED")
        if policy != "mechanical" and frozen["alias_key_id"] != alias_key_id():
            raise ValueError("ALIAS_KEY_CHANGED")
        record = runtime.load_run(run_id)
        if record is None or record.manifest != resume_manifest:
            raise ValueError("AUTHORITATIVE_RUN_MANIFEST_MISMATCH")
        if record.status == "FAILED":
            raise ValueError("FAILED_RUN_REQUIRES_SEPARATE_EXPERIMENT")
    out.mkdir(parents=True, exist_ok=True)
    if policy == "llm-replay":
        frozen_source = out / "replay_input.jsonl"
        if resume_manifest is None:
            # Retain a relocatable exact source snapshot; never mutate the input tape.
            assert replay_source is not None
            source_bytes = Path(replay_source).read_bytes()
            fd, tmp = tempfile.mkstemp(prefix=".replay-input-", dir=out)
            try:
                with os.fdopen(fd, "wb") as source_fh:
                    source_fh.write(source_bytes)
                    source_fh.flush()
                    os.fsync(source_fh.fileno())
                os.replace(tmp, frozen_source)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
        else:
            expected = resume_manifest["inputs"]["policy_meta"]["replay_source"]
            if (
                expected["path"] != frozen_source.name
                or file_digest(frozen_source) != expected["sha256"]
            ):
                raise ValueError("FROZEN_REPLAY_INPUT_CHANGED")
        policy_options = {**policy_options, "replay_source": frozen_source}
    if policy == "mechanical":
        assert profile.exit_policy
        exit_policy = profile.exit_policy
        mech = MechanicalPolicy(
            profit_trigger=sum(exit_policy.profit_review_band, Decimal(0)) / 2,
            loss_trigger=-sum(exit_policy.loss_review_band, Decimal(0)) / 2,
            loss_activation_days=exit_policy.loss_activation_days_held,
        )

        def provider(_role: str) -> Any:
            return mech
    else:
        if resume_manifest:
            # Files are outputs; DB recovery never consumes a partial export.
            export_tape(runtime, run_id, out / "decision_tape.jsonl")
        provider = policy_provider(
            policy=policy,
            profile=profile,
            run_id=run_id,
            branch_id="main",
            tape_path=out / "decision_tape.jsonl",
            manifest_id=dataset["manifest_id"],
            ledger=runtime.observation_ledger,
            runtime=runtime,
            frozen_settings=resume_manifest["policy_meta"] if resume_manifest else None,
            **policy_options,
        )
    manifest = resume_manifest or frozen_manifest(
        run_id=run_id,
        profile=profile,
        dataset_root=dataset_root,
        dataset=dataset,
        start=start.isoformat(),
        end=end.isoformat(),
        policy=policy,
        store=store_kind,
        policy_meta=getattr(provider, "meta", {}),
        alias_key_id=alias_key_id() if policy != "mechanical" else None,
    )
    if resume_manifest is None:
        atomic_json(out / "run_manifest.json", manifest)
        atomic_json(out / "run_locations.json", {"dataset_root": str(dataset_root.resolve())})
    runtime.begin_run(run_id, manifest)
    archive = Archive(dataset_root, verify=False)  # Already verified, before run registration.
    engine = Engine(
        profile,
        load_manifest(dataset_root / "calendar.json"),
        archive,
        runtime,
        provider,
        run_id=run_id,
    )
    result = engine.run(start, end)
    events_temp = out / ".events.jsonl.tmp"
    with events_temp.open("w") as fh:
        for event in result.events:
            fh.write(json.dumps(asdict(event), sort_keys=True, default=str) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(events_temp, out / "events.jsonl")
    if policy != "mechanical":
        export_tape(runtime, run_id, out / "decision_tape.jsonl")
    report = summarize(result)
    report["model_costs"] = {k: str(v) for k, v in runtime.budget_totals(run_id).items()}
    report["incidents"] = [
        {"incident_id": i.incident_id, "code": i.code, "at": i.at.isoformat()}
        for i in runtime.observation_ledger.incidents(run_id)
    ]
    report["model_provenance"] = manifest["policy_meta"]
    journal = runtime.journal(run_id)
    report["model_attempts"] = attempt_summary(journal)
    report["pause_history"] = [
        entry["payload"]["pause"]
        for entry in journal
        if entry["kind"] == "CURSOR" and entry["payload"].get("pause")
    ]
    atomic_json(out / "report.json", report)
    atomic_json(
        out / "journal.json",
        {"format_version": 2, "run_id": run_id, "entries": journal},
    )
    files = ["events.jsonl", "report.json", "journal.json"]
    if policy != "mechanical":
        files.append("decision_tape.jsonl")
    if policy == "llm-replay":
        files.append("replay_input.jsonl")
    atomic_json(
        out / "run_result.json",
        {
            "format_version": 2,
            "run_id": run_id,
            "input_sha256": manifest["input_sha256"],
            "status": result.status,
            "pause": result.pause,
            "event_log_sha256": event_log_digest(result.events),
            "artifacts": {name: file_digest(out / name) for name in files},
        },
    )
    return result


def resume_run(out: Path, dataset_root: Path | None = None) -> Any:
    manifest = json.loads((out / "run_manifest.json").read_text())
    validate_frozen_manifest(manifest)
    if manifest.get("store") != "postgres" or not manifest.get("resumable"):
        raise ValueError("RESUME_REQUIRES_AUTHORITATIVE_POSTGRES")
    inputs = manifest["inputs"]
    root = dataset_root or Path(
        json.loads((out / "run_locations.json").read_text())["dataset_root"]
    )
    return execute(
        profile=Profile.model_validate(inputs["profile"]),
        dataset_root=root,
        out=out,
        start=date.fromisoformat(inputs["start_date"]),
        end=date.fromisoformat(inputs["end_date"]),
        run_id=manifest["run_id"],
        store_kind="postgres",
        policy=manifest["policy"],
        policy_options={"model_id": None, "budget_usd": None, "price_in": None, "price_out": None},
        resume_manifest=manifest,
    )
