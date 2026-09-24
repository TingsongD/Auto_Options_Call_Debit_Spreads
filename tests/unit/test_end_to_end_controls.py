"""Real synthetic archives exercise CLI, engine, journal, tape and audit controls."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path

import polars as pl
import yaml
from typer.testing import CliRunner

from spx_research.cli.main import app
from spx_research.config import Profile
from spx_research.data.availability import validate_archive
from spx_research.data.manifests import DataManifest, file_record, write_manifest
from spx_research.data.synthetic import SyntheticSpec, generate
from spx_research.epistemics.harness import canonical
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import ModelRequest
from spx_research.research.leakage import compare_runs, evaluate_run
from spx_research.temporal.calendar import CalendarManifest, SessionDay
from tests.unit.test_baseline_engine import _profile_dict


def _freeze_dataset(root: Path, original: DataManifest) -> DataManifest:
    files = tuple(
        file_record(root, path, pl.read_parquet(path).height)
        for path in sorted(root.rglob("*.parquet"))
    )
    manifest = replace(original, manifest_id="", normalized_files=files)
    manifest = replace(manifest, manifest_id=f"syn-{manifest.content_id()[4:]}")
    write_manifest(root, manifest)
    validate_archive(root, expected_kind="synthetic")
    return manifest


def _journal_prefix(run_dir: Path, cutoff: datetime) -> list[bytes]:
    entries = json.loads((run_dir / "journal.json").read_text())["entries"]
    prepared = {
        entry["payload"]["decision_id"]: entry["payload"]["request"]
        for entry in entries
        if entry["kind"] == "DECISION_PREPARED"
    }
    bodies = []
    for entry in entries:
        if entry["kind"] != "ATTEMPT_RESERVED":
            continue
        payload = entry["payload"]
        context = prepared[payload["decision_id"]]["compiled"]["context"]
        if datetime.fromisoformat(context["as_of"]) <= cutoff:
            bodies.append(canonical(ModelRequest(**payload["request"]).body()))
    return bodies


def test_actual_archive_suffix_invariance_and_observed_prefix_positive_control(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SPX_ALIAS_KEY", "synthetic-only-control-key")
    first, second = date(2024, 1, 2), date(2024, 1, 3)
    calendar = CalendarManifest(
        "two-session-controls",
        "1",
        tuple(SessionDay(day, time(9, 30), time(10), True) for day in (first, second)),
    )
    cutoff = calendar.sessions[0].close_utc()
    original = generate(
        tmp_path,
        SyntheticSpec(
            dataset_id="base-data",
            seed=7,
            start=first,
            end=second,
            expiries=(first + timedelta(days=45),),
            strikes_each_side=1,
        ),
        calendar,
    )
    base = tmp_path / "base-data"
    (base / "calendar.json").write_text(
        json.dumps(
            {
                "calendar_id": calendar.calendar_id,
                "version": calendar.version,
                "sessions": [
                    {"date": day.isoformat(), "open": "09:30", "close": "10:00", "half_day": True}
                    for day in (first, second)
                ],
            }
        )
    )
    macro_path = base / "macro/vintages.parquet"
    macro = pl.read_parquet(macro_path)
    # One explicitly synthetic, legally visible release precedes the first review.
    visible = dict(next(row for row in macro.to_dicts() if row["series_id"] == "DGS10"))
    published = calendar.sessions[0].open_utc() + timedelta(minutes=1)
    visible.update(
        value="4.125",
        public_release_at_utc=published,
        simulated_available_at_utc=published,
        content_hash=hashlib.sha256(b"synthetic-visible-DGS10:4.125").hexdigest(),
    )
    pl.DataFrame([visible, *macro.to_dicts()], schema=macro.schema).write_parquet(macro_path)
    base_manifest = _freeze_dataset(base, original)

    future, observed = tmp_path / "future-data", tmp_path / "observed-data"
    shutil.copytree(base, future)
    shutil.copytree(base, observed)
    # The future branch shares every first-session input; only unavailable data changes.
    for name, fields in (
        (f"session={second}.parquet", ("bid_points", "ask_points")),
        (f"index={second}.parquet", ("value_index_points",)),
    ):
        path = future / "quotes" / name
        pl.read_parquet(path).with_columns(
            [(pl.col(field) + 100).alias(field) for field in fields]
        ).write_parquet(path)
    for root, change_visible in ((future, False), (observed, True)):
        path = root / "macro/vintages.parquet"
        frame = pl.read_parquet(path)
        rows = frame.to_dicts()
        changed = 0
        for row in rows:
            if row["series_id"] == "DGS10" and (
                (row["simulated_available_at_utc"] <= cutoff) == change_visible
            ):
                row["value"] = "5.125"
                row["content_hash"] = hashlib.sha256(b"synthetic-visible-DGS10:5.125").hexdigest()
                changed += 1
        assert changed > 0
        pl.DataFrame(rows, schema=frame.schema).write_parquet(path)
    future_manifest = _freeze_dataset(future, original)
    observed_manifest = _freeze_dataset(observed, original)
    assert len({m.manifest_id for m in (base_manifest, future_manifest, observed_manifest)}) == 3
    assert (base / f"quotes/session={first}.parquet").read_bytes() == (
        future / f"quotes/session={first}.parquet"
    ).read_bytes()

    raw_profile = _profile_dict()
    raw_profile["study"] = {
        "start_date": first,
        "scored_end_date": second,
        "runoff_end_date": second,
    }
    profile = tmp_path / "profile.yaml"
    profile.write_text(yaml.safe_dump(Profile.model_validate(raw_profile).model_dump(mode="json")))
    runner = CliRunner()
    outputs = []
    for name, dataset in (("base", base), ("future", future), ("observed", observed)):
        out = tmp_path / f"run-{name}"
        result = runner.invoke(
            app,
            [
                "run",
                str(profile),
                "--dataset-root",
                str(dataset),
                "--out",
                str(out),
                "--run-id",
                f"control-{name}",
                "--policy",
                "llm-mock",
                "--alias-namespace",
                "shared-control-scope",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "COMPLETED" in result.output
        tape = DecisionTape(out / "decision_tape.jsonl")
        assert not tape.legacy and not tape.incomplete and len(tape.records()) == 2
        audit = evaluate_run(out)
        assert audit["audit_status"] == "PASS", audit["errors"]
        outputs.append(out)

    base_run, future_run, observed_run = outputs
    prefix = _journal_prefix(base_run, cutoff)
    assert len(prefix) == 1
    assert prefix == _journal_prefix(future_run, cutoff)
    assert prefix != _journal_prefix(observed_run, cutoff)
    assert b"4.125" in prefix[0]
    assert b"5.125" in _journal_prefix(observed_run, cutoff)[0]
    for out in outputs:
        for body in _journal_prefix(out, calendar.sessions[-1].close_utc()):
            for private in (
                base_manifest.manifest_id,
                future_manifest.manifest_id,
                observed_manifest.manifest_id,
                "control-base",
                "control-future",
                "control-observed",
                "synthetic-only-control-key",
            ):
                assert private.encode() not in body
    invariant = compare_runs(base_run, future_run, cutoff=cutoff)
    assert invariant["success"] is True and invariant["invariant"] is True, invariant
    # The changed suffix is actually observed in the second session.
    assert (
        compare_runs(
            base_run, future_run, cutoff=calendar.sessions[-1].close_utc(), expect="changed"
        )["success"]
        is True
    )
    positive = compare_runs(base_run, observed_run, cutoff=cutoff, expect="changed")
    assert positive["success"] is True and positive["invariant"] is False, positive
    assert compare_runs(base_run, observed_run, cutoff=cutoff)["success"] is False
    for control, expect, exit_code in (
        (future_run, "invariant", 0),
        (observed_run, "changed", 0),
        (observed_run, "invariant", 1),
    ):
        result = runner.invoke(
            app,
            [
                "leakage-eval",
                str(base_run),
                "--control-dir",
                str(control),
                "--cutoff",
                cutoff.isoformat(),
                "--expect",
                expect,
                "--out",
                str(tmp_path / f"audit-{control.name}-{expect}.json"),
            ],
        )
        assert result.exit_code == exit_code, result.output
