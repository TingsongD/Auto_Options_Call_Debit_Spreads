"""Explicit sealed decision inputs replay exact public requests without billing."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from spx_research.agents.graphs import logical_decision_id, run_decision
from spx_research.cli.execution import execute
from spx_research.cli.main import app
from spx_research.engine.policy import PolicyError
from spx_research.epistemics.harness import Harness
from spx_research.llm.budget import Budget
from spx_research.llm.gateway import RecordedDecisionGateway
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import ModelError
from spx_research.research.artifacts import file_digest
from tests.unit import test_cli_recovery
from tests.unit.test_cli_recovery import run_fixture
from tests.unit.test_llm_pipeline import KEY, _spread_ctx
from tests.unit.test_runtime_decisions import RateLimitThenGood, _runtime_deps, _sheet


@pytest.fixture()
def cli_dataset(tmp_path):
    return test_cli_recovery.cli_dataset.__wrapped__(tmp_path)


def _execute_replay(fixture: Any, out: Path, source: Path, **overrides: Any) -> Any:
    profile, root, day = fixture
    return execute(
        profile=profile,
        dataset_root=root,
        out=out,
        start=day,
        end=day,
        run_id="distinct-replayed-run",
        store_kind="memory",
        policy="llm-replay",
        policy_options={
            "model_id": None,
            "budget_usd": None,
            "price_in": None,
            "price_out": None,
            "replay_source": source,
            **overrides,
        },
    )


def test_replay_frozen_source_distinct_run_and_zero_cost(cli_dataset, tmp_path, monkeypatch):
    monkeypatch.setenv("SPX_ALIAS_KEY", "synthetic-only-test-key")
    source_dir = tmp_path / "original"
    run_fixture(cli_dataset, source_dir, "original-run", "llm-mock")
    source = source_dir / "decision_tape.jsonl"
    source_bytes = source.read_bytes()
    out = tmp_path / "replayed"
    result = _execute_replay(cli_dataset, out, source)
    assert result.status == "COMPLETED"
    manifest = json.loads((out / "run_manifest.json").read_text())
    frozen = manifest["inputs"]["policy_meta"]
    assert frozen["transport"] == "sealed-tape"
    assert frozen["alias_namespace"] == "test-shared"
    assert frozen["replay_source"] == {"path": "replay_input.jsonl", "sha256": file_digest(source)}
    assert (out / "replay_input.jsonl").read_bytes() == source_bytes == source.read_bytes()
    original = DecisionTape(source).records()
    replayed = DecisionTape(out / "decision_tape.jsonl").records()
    assert len(original) == len(replayed) > 0
    for before, after in zip(original, replayed, strict=True):
        assert before.request == after.request
        assert before.response.parsed == after.response.parsed
        assert before.decision_id != after.decision_id
        assert after.response.outcome == "REPLAYED"
        assert after.response.provider_metadata["replay_source_sha256"] == file_digest(source)
    report = json.loads((out / "report.json").read_text())
    assert Decimal(report["model_costs"]["committed"]) == 0
    journal = json.loads((out / "journal.json").read_text())["entries"]
    outcomes = [row for row in journal if row["kind"] == "ATTEMPT_COMPLETED"]
    assert outcomes
    assert all(row["payload"]["outcome"] == "REPLAYED" for row in outcomes)


def test_replay_cli_requires_explicit_source_and_forbids_live_settings(
    cli_dataset, tmp_path, monkeypatch
):
    monkeypatch.setenv("SPX_ALIAS_KEY", "synthetic-only-test-key")
    runner = CliRunner()
    args = ["run", "unused.yaml", "--dataset-root", str(tmp_path), "--out", str(tmp_path)]
    missing = runner.invoke(app, [*args, "--policy", "llm-replay"])
    assert missing.exit_code == 2 and "REPLAY_REQUIRES_TAPE_INPUT" in missing.output
    invalid = runner.invoke(app, [*args, "--tape", "unused.jsonl"])
    assert invalid.exit_code == 2 and "TAPE_INPUT_REQUIRES_LLM_REPLAY_POLICY" in invalid.output
    source_dir = tmp_path / "source"
    run_fixture(cli_dataset, source_dir, "source", "llm-mock")
    with pytest.raises(ValueError, match="REPLAY_USES_SOURCE_MODEL_SETTINGS_ONLY"):
        _execute_replay(
            cli_dataset, tmp_path / "invalid", source_dir / "decision_tape.jsonl", model_id="live"
        )


class ChargedRetry(RateLimitThenGood):
    def complete(self, *args, **kwargs):
        return replace(
            super().complete(*args, **kwargs),
            cost_usd=Decimal(".0123"),
            input_tokens=20,
            output_tokens=10,
            cached_input_tokens=5,
        )


def _retry_replay(tmp_path):
    original = _runtime_deps(tmp_path / "source", ChargedRetry(), Budget(Decimal(1), _sheet()))
    original.public_alias_namespace = "replay-test"
    ctx = _spread_ctx()
    expected = run_decision(original, ctx)[0]
    source = original.tape.path
    gateway = RecordedDecisionGateway(DecisionTape(source), file_digest(source))
    destination = _runtime_deps(tmp_path / "destination", gateway)
    destination.public_alias_namespace = "replay-test"
    destination.runtime.begin_run("run-2", {"format_version": 2})
    return original, destination, replace(ctx, run_id="run-2"), expected


@pytest.mark.parametrize("crash_phase", [None, "reserved", "response"])
def test_retry_replay_restores_exact_request_and_source_usage_without_charge(tmp_path, crash_phase):
    original, deps, ctx, expected = _retry_replay(tmp_path)
    if crash_phase == "reserved":
        complete = deps.runtime.complete_attempt
        deps.runtime.complete_attempt = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("crash"))
    elif crash_phase == "response":
        deps.harness.validate = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("crash"))
    if crash_phase:
        with pytest.raises(RuntimeError, match="crash"):
            run_decision(deps, ctx)
        if crash_phase == "reserved":
            deps.runtime.complete_attempt = complete
        deps.harness = Harness(KEY)
        deps.prepared_cache = {}
    assert run_decision(deps, ctx)[0] == expected
    records = deps.tape.records()
    assert len(records) == 1
    source_record = original.tape.records()[0]
    response = records[0].response
    assert records[0].request == source_record.request
    assert response.cost_usd == 0 and response.input_tokens == response.output_tokens == 0
    assert response.provider_metadata["source_usage"] == {
        "cost_usd": "0.0123",
        "input_tokens": 20,
        "output_tokens": 10,
        "cached_input_tokens": 5,
    }
    attempts = deps.runtime.list_attempts("run-2", logical_decision_id(deps, ctx))
    assert len(attempts) == 1 and attempts[0].outcome == "REPLAYED"
    assert attempts[0].request["retry_error_code"] == "RATE_LIMIT"
    assert deps.runtime.budget_totals("run-2")["committed"] == 0


def test_replay_miss_never_imports_source_context_or_falls_back(tmp_path):
    _original, deps, ctx, _expected = _retry_replay(tmp_path)
    changed = replace(ctx, spread_view=replace(ctx.spread_view, days_held=7))
    with pytest.raises(PolicyError, match="TAPE_MISS"):
        run_decision(deps, changed)
    assert deps.runtime.list_attempts("run-2", logical_decision_id(deps, changed)) == []
    assert deps.ledger.deliveries("run-2", "main", ctx.actor_id) == []


def test_replay_rejects_torn_empty_legacy_inputs(tmp_path):
    empty = DecisionTape(tmp_path / "empty.jsonl")
    empty.export()
    with pytest.raises(ModelError, match="NONEMPTY_SEALED"):
        RecordedDecisionGateway(empty, "input-hash")
    original, _deps, _ctx, _expected = _retry_replay(tmp_path)
    source = original.tape.path
    legacy = tmp_path / "legacy.jsonl"
    legacy_row = json.loads(source.read_text().splitlines()[0])
    legacy_row.pop("format_version")
    legacy.write_text(json.dumps(legacy_row) + "\n")
    legacy_bytes = legacy.read_bytes()
    with pytest.raises(ModelError, match="NONEMPTY_SEALED"):
        RecordedDecisionGateway(DecisionTape(legacy), "input-hash")
    assert legacy.read_bytes() == legacy_bytes
    source.write_text(source.read_text().splitlines()[0] + "\n")
    with pytest.raises(ModelError, match="NONEMPTY_SEALED"):
        RecordedDecisionGateway(DecisionTape(source), "input-hash")


def test_report_keeps_rejected_payload_private(cli_dataset, tmp_path, monkeypatch):
    from spx_research.llm.gateway import MockGateway

    monkeypatch.setenv("SPX_ALIAS_KEY", "synthetic-only-test-key")
    original = MockGateway.complete

    def rejected(self, *args, **kwargs):
        response = original(self, *args, **kwargs)
        return replace(response, parsed={**response.parsed, "private_prose": "private-sentinel"})

    monkeypatch.setattr(MockGateway, "complete", rejected)
    out = tmp_path / "rejected"
    result = run_fixture(cli_dataset, out, "rejected-run", "llm-mock")
    assert result.status == "PAUSED"
    report_text = (out / "report.json").read_text()
    assert "private-sentinel" not in report_text
    incidents = json.loads(report_text)["incidents"]
    assert incidents and set(incidents[0]) == {"incident_id", "code", "at"}
