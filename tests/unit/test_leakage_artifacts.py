"""Audits need complete evidence and compare complete attempted requests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from spx_research.config import Profile
from spx_research.domain.state import Event
from spx_research.epistemics.harness import digest
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import ModelRequest, ModelResponse
from spx_research.persistence.runtime import InMemoryRunStore, json_value
from spx_research.reporting.report import event_log_digest
from spx_research.research.artifacts import file_digest
from spx_research.research.leakage import compare_runs, evaluate_run
from tests.unit.test_baseline_engine import _profile_dict

AT = datetime(2024, 1, 2, 15, tzinfo=UTC)


def write_run(root: Path, *, run_id="run-a", prompt="Frozen policy", value="1", reject_prompt=None):
    root.mkdir()
    profile = Profile.model_validate(_profile_dict()).model_dump(mode="json")
    schema = {"type": "object"}
    inputs = {
        "profile": profile,
        "start_date": "2024-01-02",
        "end_date": "2024-01-02",
        "dataset_manifest_id": "synthetic-fixture",
        "dataset_manifest_sha256": "a" * 64,
        "calendar_sha256": "b" * 64,
        "code": {"source_sha256": "c" * 64, "dependency_lock_sha256": "d" * 64},
        "alias_key_id": "fixed-alias-key",
        "policy": "llm-mock",
        "policy_meta": {
            "resolved_model_ids": {"manager": "mock", "spread": "mock"},
            "max_output_tokens": 800,
        },
        "contracts": {
            "bundle_version": "2.1",
            "files": {"prompts/manager.md": hashlib.sha256(prompt.encode()).hexdigest()},
            "schema_canonical_sha256": {"schemas/manager_decision.schema.json": digest(schema)},
        },
    }
    manifest = {
        "format_version": 2,
        "run_id": run_id,
        "inputs": inputs,
        "input_sha256": digest(inputs),
        "policy": "llm-mock",
        "policy_meta": inputs["policy_meta"],
        "profile_id": profile["profile_id"],
        "profile_mode": profile["mode"],
        "store": "memory",
        "resumable": False,
        "dataset_manifest_id": "synthetic-fixture",
        "private_manifest_id": "synthetic-fixture",
        "branch_id": "main",
        "initial_cash_usd": "10000",
        "tape_path": "decision_tape.jsonl",
    }
    (root / "run_manifest.json").write_text(json.dumps(manifest))
    runtime = InMemoryRunStore()
    runtime.begin_run(run_id, manifest)
    req = ModelRequest(
        "manager",
        {"episode_token": "ep_" + "a" * 24, "facts": [{"value": value}]},
        "manager_decision",
        "mock",
        system_text=prompt,
        output_schema=schema,
        system_prompt_hash=hashlib.sha256(prompt.encode()).hexdigest(),
        schema_hash=digest(schema),
    )
    context = {
        "as_of": AT.isoformat(),
        "actor_role": "MANAGER",
        "actor_id": "actor-one",
        "run_id": run_id,
    }
    prepared = {"compiled": {"context": context}, "model_request": asdict(req)}
    decision = "decision-one"
    runtime.prepare_decision(run_id, decision, prepared)
    if reject_prompt:
        failed = ModelRequest(
            **{
                **asdict(req),
                "retry_error_code": reject_prompt,
            }
        )
        runtime.start_attempt(
            run_id, decision, "attempt-zero", Decimal(0), Decimal(0), request=asdict(failed)
        )
        runtime.complete_attempt(
            run_id, "attempt-zero", response={}, actual_usd=Decimal(0), outcome="SCHEMA"
        )
    runtime.start_attempt(
        run_id, decision, "attempt-one", Decimal(0), Decimal(0), request=asdict(req)
    )
    response = ModelResponse(req.request_hash(), "{}", {}, "mock", 0, 0, Decimal(0))
    runtime.complete_attempt(
        run_id, "attempt-one", response={}, actual_usd=Decimal(0), outcome="SUCCESS"
    )
    witness = {
        "actor_id": "actor-one",
        "private_decision_id": decision,
        "request_hash": req.request_hash(),
        "packet_hash": digest(req.packet),
    }
    result = {"witness": witness}
    runtime.accept_decision(run_id, decision, result=result, attempt_id="attempt-one")
    tape = DecisionTape(root / "decision_tape.jsonl")
    tape.append(
        req, response, AT.isoformat(), decision_id=decision, prepared=prepared, result=result
    )
    events = [
        Event(run_id, 1, AT, "RUN", "RUN_STARTED", {"initial_cash_usd": "10000"}),
        Event(run_id, 2, AT, "DECISION", "DECISION_WITNESS", witness),
        Event(run_id, 3, AT, "RUN", "RUN_ENDED", {}),
    ]
    committed = runtime.append_batch(events, 0)
    (root / "events.jsonl").write_text(
        "".join(json.dumps(asdict(e), default=str) + "\n" for e in committed)
    )
    (root / "report.json").write_text(json.dumps({"final_cash_usd": "10000"}))
    (root / "journal.json").write_text(
        json.dumps(
            {"format_version": 2, "run_id": run_id, "entries": json_value(runtime.journal(run_id))}
        )
    )
    output = {
        "format_version": 2,
        "run_id": run_id,
        "input_sha256": manifest["input_sha256"],
        "status": "COMPLETED",
        "event_log_sha256": event_log_digest(committed),
        "artifacts": {
            name: file_digest(root / name)
            for name in ("events.jsonl", "report.json", "journal.json", "decision_tape.jsonl")
        },
    }
    (root / "run_result.json").write_text(json.dumps(output))


def test_complete_mock_artifacts_pass_only_application_checks(tmp_path):
    root = tmp_path / "complete"
    write_run(root)
    report = evaluate_run(root)
    assert report["errors"] == []
    assert report["success"] is True
    assert report["audit_status"] == "PASS"
    assert report["application_temporal_gate"] == "NOT_RUN"
    assert "recorded_request_egress" in report["scoped_checks"]
    assert report["behavioral_leakage_diagnostics"] == "NOT_RUN"
    assert report["parametric_future_knowledge_excluded"] is False


@pytest.mark.parametrize(
    "missing",
    [
        "run_manifest.json",
        "run_result.json",
        "report.json",
        "events.jsonl",
        "journal.json",
        "decision_tape.jsonl",
    ],
)
def test_missing_required_artifact_never_passes(tmp_path, missing):
    root = tmp_path / "missing"
    write_run(root)
    (root / missing).unlink()
    assert evaluate_run(root)["success"] is False


def test_truncated_last_tape_record_is_not_a_clean_scan(tmp_path):
    root = tmp_path / "torn"
    write_run(root)
    path = root / "decision_tape.jsonl"
    path.write_text(path.read_text()[:-15])
    report = evaluate_run(root)
    assert report["success"] is False
    assert any(v["code"] == "TAPE_TORN" for v in report["checks"]["egress_violations"])


def test_exact_requests_require_cutoff_and_respect_positive_control(tmp_path):
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    write_run(a)
    write_run(b, run_id="run-b")
    write_run(c, run_id="run-c", value="2")
    assert compare_runs(a, b)["success"] is False
    assert compare_runs(a, b, cutoff=AT)["success"] is True
    assert compare_runs(a, c, cutoff=AT)["success"] is False
    assert compare_runs(a, c, cutoff=AT, expect="changed")["success"] is True
    assert compare_runs(a, b, cutoff=AT, expect="changed")["success"] is False


def test_prompt_drift_cannot_hide_behind_identical_action(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    write_run(a)
    write_run(b, run_id="run-b", prompt="Changed system prompt")
    comparison = compare_runs(a, b, cutoff=AT)
    assert comparison["invariant"] is False
    assert comparison["success"] is False


def test_rejected_dispatches_are_part_of_prefix_comparison(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    write_run(a, reject_prompt="SCHEMA")
    write_run(b, run_id="run-b", reject_prompt="PROVIDER_FAILED")
    comparison = compare_runs(a, b, cutoff=AT)
    assert comparison["request_count_a"] == 2
    assert comparison["invariant"] is False


def test_replay_source_is_required_even_if_result_is_resealed(tmp_path):
    root = tmp_path / "replay"
    write_run(root)
    source = root / "replay_input.jsonl"
    source.write_bytes((root / "decision_tape.jsonl").read_bytes())
    manifest_path = root / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["policy"] = manifest["inputs"]["policy"] = "llm-replay"
    manifest["inputs"]["policy_meta"]["replay_source"] = {
        "path": source.name,
        "sha256": file_digest(source),
    }
    manifest["policy_meta"] = manifest["inputs"]["policy_meta"]
    manifest["input_sha256"] = digest(manifest["inputs"])
    manifest_path.write_text(json.dumps(manifest))
    result_path = root / "run_result.json"
    result = json.loads(result_path.read_text())
    result["input_sha256"] = manifest["input_sha256"]
    result["artifacts"][source.name] = file_digest(source)
    result_path.write_text(json.dumps(result))
    assert evaluate_run(root)["success"] is True
    source.unlink()
    del result["artifacts"][source.name]
    result_path.write_text(json.dumps(result))
    report = evaluate_run(root)
    assert report["success"] is False
    assert "REPLAY_SOURCE_MISSING_OR_INVALID" in report["errors"]


@pytest.mark.parametrize("tamper", ["prompt", "schema", "unterminated"])
def test_unaccepted_attempts_require_frozen_contracts_and_completed_accounting(tmp_path, tamper):
    root = tmp_path / tamper
    write_run(root)
    journal_path = root / "journal.json"
    journal = json.loads(journal_path.read_text())
    original = next(e for e in journal["entries"] if e["kind"] == "ATTEMPT_RESERVED")
    injected = json.loads(json.dumps(original))
    injected["payload"]["attempt_id"] = "hidden-attempt"
    request = injected["payload"]["request"]
    if tamper == "prompt":
        request["system_text"] += " Hidden future information 2035-01-01"
    if tamper == "schema":
        request["output_schema"] = {"type": "string"}
    journal["entries"].append(injected)
    journal_path.write_text(json.dumps(journal))
    result_path = root / "run_result.json"
    result = json.loads(result_path.read_text())
    result["artifacts"][journal_path.name] = file_digest(journal_path)
    result_path.write_text(json.dumps(result))
    report = evaluate_run(root)
    assert report["success"] is False
    assert "ATTEMPT_ACCOUNTING_UNRESOLVED" in report["errors"]
    if tamper == "prompt":
        assert "PROMPT_NOT_BOUND_TO_RUN" in report["errors"]
    if tamper == "schema":
        assert "SCHEMA_NOT_BOUND_TO_RUN" in report["errors"]
