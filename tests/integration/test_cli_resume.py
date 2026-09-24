"""The real CLI orchestration resumes authoritative PostgreSQL phase commits."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from spx_research.cli.execution import execute, resume_run
from spx_research.persistence.runtime import PostgresRunStore
from spx_research.research.leakage import evaluate_run
from tests.unit import test_cli_recovery as fixtures

cli_dataset = fixtures.cli_dataset


def test_resume_after_commit_ack_loss_keeps_events_attempts_and_frozen_inputs(
    pg, cli_dataset, tmp_path: Path, monkeypatch
):
    profile, root, day = cli_dataset
    monkeypatch.setenv("SPX_ALIAS_KEY", "offline-crash-resume-key")
    out = tmp_path / "recover"
    original = PostgresRunStore.commit_barrier
    fault = {"armed": True}

    def commit_and_lose_ack(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        if fault["armed"] and any(e.type == "DECISION_MADE" for e in result):
            fault["armed"] = False
            raise RuntimeError("simulated commit acknowledgement loss")
        return result

    monkeypatch.setattr(PostgresRunStore, "commit_barrier", commit_and_lose_ack)
    with pytest.raises(RuntimeError, match="acknowledgement loss"):
        execute(
            profile=profile,
            dataset_root=root,
            out=out,
            start=day,
            end=day,
            run_id="cli-crash",
            store_kind="postgres",
            policy="llm-mock",
            policy_options={
                "model_id": None,
                "budget_usd": None,
                "price_in": None,
                "price_out": None,
                "alias_namespace": "offline-resume",
            },
        )
    store = PostgresRunStore(pg)
    before = store.events("cli-crash")
    reservations = [x for x in store.journal("cli-crash") if x["kind"] == "ATTEMPT_RESERVED"]
    manifest_bytes = (out / "run_manifest.json").read_bytes()
    result = resume_run(out)
    assert result.status == "COMPLETED"
    assert store.events("cli-crash")[: len(before)] == before
    assert [
        x for x in store.journal("cli-crash") if x["kind"] == "ATTEMPT_RESERVED"
    ] == reservations
    assert (out / "run_manifest.json").read_bytes() == manifest_bytes
    assert evaluate_run(out)["success"] is True
    seal = json.loads((out / "run_result.json").read_text())
    again = resume_run(out)
    assert again.events == result.events
    assert (
        json.loads((out / "run_result.json").read_text())["event_log_sha256"]
        == seal["event_log_sha256"]
    )
    calendar = root / "calendar.json"
    calendar.write_text(calendar.read_text() + "\n")
    with pytest.raises(ValueError, match="FROZEN_INPUTS_CHANGED"):
        resume_run(out)
