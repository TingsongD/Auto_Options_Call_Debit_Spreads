"""Frozen input, report and packaging acceptance at the command boundary."""

from __future__ import annotations

import json
from datetime import date, time, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from spx_research.cli.execution import execute, resume_run, validate_inputs
from spx_research.cli.main import app
from spx_research.config import Profile
from spx_research.data.synthetic import SyntheticSpec, generate
from spx_research.research.artifacts import file_digest, validate_frozen_manifest
from spx_research.research.experiments import ExperimentRegistry
from spx_research.temporal.calendar import CalendarManifest, SessionDay
from tests.unit.test_baseline_engine import _profile_dict


@pytest.fixture()
def cli_dataset(tmp_path: Path):
    day = date(2024, 1, 2)
    cal = CalendarManifest("tiny-cli", "1", (SessionDay(day, time(9, 30), time(10), True),))
    generate(
        tmp_path,
        SyntheticSpec(
            dataset_id="tiny", seed=7, start=day, end=day, expiries=(day + timedelta(days=45),)
        ),
        cal,
    )
    root = tmp_path / "tiny"
    (root / "calendar.json").write_text(
        json.dumps(
            {
                "calendar_id": cal.calendar_id,
                "version": cal.version,
                "sessions": [
                    {"date": day.isoformat(), "open": "09:30", "close": "10:00", "half_day": True}
                ],
            }
        )
    )
    raw = _profile_dict()
    raw["study"] = {"start_date": day, "scored_end_date": day, "runoff_end_date": day}
    return Profile.model_validate(raw), root, day


def run_fixture(cli_dataset, out: Path, run_id: str, policy: str = "mechanical"):
    profile, root, day = cli_dataset
    return execute(
        profile=profile,
        dataset_root=root,
        out=out,
        start=day,
        end=day,
        run_id=run_id,
        store_kind="memory",
        policy=policy,
        policy_options={
            "model_id": None,
            "budget_usd": None,
            "price_in": None,
            "price_out": None,
            "alias_namespace": "test-shared",
        },
    )


def test_run_freezes_inputs_and_seals_independent_outputs(cli_dataset, tmp_path):
    out = tmp_path / "run"
    result = run_fixture(cli_dataset, out, "r1")
    assert result.status == "COMPLETED"
    manifest = json.loads((out / "run_manifest.json").read_text())
    validate_frozen_manifest(manifest)
    assert manifest["format_version"] == 2
    assert "event_log_sha256" not in manifest
    seal = json.loads((out / "run_result.json").read_text())
    assert all(file_digest(out / n) == h for n, h in seal["artifacts"].items())
    report = json.loads((out / "report.json").read_text())
    assert report["scored_end_valuation"]["quality"] == "OK"
    assert report["parametric_future_knowledge_excluded"] is False
    with pytest.raises(ValueError, match="OUTPUT_DIRECTORY_NOT_EMPTY"):
        run_fixture(cli_dataset, out, "r2")
    with pytest.raises(ValueError, match="RESUME_REQUIRES_AUTHORITATIVE_POSTGRES"):
        resume_run(out)


def test_distinct_runs_share_experiment_but_remain_registered(cli_dataset, tmp_path):
    registry = ExperimentRegistry(tmp_path / "registry.jsonl")
    for run_id in ("one", "two"):
        out = tmp_path / run_id
        run_fixture(cli_dataset, out, run_id)
        manifest = json.loads((out / "run_manifest.json").read_text())
        registry.register_manifest(manifest)
        registry.register_manifest(manifest)
    records = registry.list()
    assert len(records) == 2
    assert records[0].experiment_id == records[1].experiment_id


def test_mock_tape_is_journal_export_and_exact_requests_match(cli_dataset, tmp_path, monkeypatch):
    from spx_research.llm.tape import DecisionTape

    monkeypatch.setenv("SPX_ALIAS_KEY", "synthetic-only-test-key")
    out = tmp_path / "mock"
    result = run_fixture(cli_dataset, out, "mock-run", "llm-mock")
    assert result.status == "COMPLETED"
    tape = DecisionTape(out / "decision_tape.jsonl")
    assert not tape.legacy and not tape.incomplete
    journal = json.loads((out / "journal.json").read_text())["entries"]
    accepted = [e for e in journal if e["kind"] == "DECISION_ACCEPTED"]
    assert accepted
    assert all(tape.decision(e["payload"]["decision_id"]) for e in accepted)


def test_type_and_boundaries_rejected_before_archive_access(cli_dataset, monkeypatch):
    profile, root, day = cli_dataset
    import spx_research.data.availability as availability

    monkeypatch.setattr(
        availability, "validate_archive", lambda *a, **k: pytest.fail("opened archive")
    )
    raw = json.loads((root / "manifest.json").read_text())
    raw["dataset_kind"] = "provider"
    (root / "manifest.json").write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="DATASET_KIND_MISMATCH"):
        validate_inputs(profile, root, day, day)


def test_resume_legacy_exits_closed(tmp_path):
    (tmp_path / "run_manifest.json").write_text('{"run_id":"legacy"}')
    result = CliRunner().invoke(app, ["resume", str(tmp_path)])
    assert result.exit_code == 2
    assert "LEGACY_RUN_REPLAY_ONLY" in result.output
