"""M6 tests: experiment registry lineage + fixed-classification leakage report."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from spx_research.domain.state import Event
from spx_research.persistence.events import InMemoryEventStore
from spx_research.research.experiments import STUDY_LABEL, ExperimentRegistry
from spx_research.research.leakage import (
    compare_runs,
    egress_scan_packets,
    evaluate_run,
    verify_hash_chain,
)


def _write_run(dir_path: Path, run_id: str, n: int = 3, tamper: bool = False) -> None:
    store = InMemoryEventStore()
    for i in range(1, n + 1):
        store.append(
            Event(
                run_id,
                i,
                datetime(2020, 1, 2, 14, 30, tzinfo=UTC),
                "SIM",
                "RUN_STARTED" if i == 1 else ("RUN_ENDED" if i == n else "DECISION_MADE"),
                {"k": i},
            ),
            expected_seq=i - 1,
        )
    events = store.events(run_id)
    if tamper:
        e = events[1]
        events[1] = Event(e.run_id, e.seq, e.sim_time_utc, e.phase, e.type, {"k": 999})
    dir_path.mkdir(parents=True, exist_ok=True)
    with (dir_path / "events.jsonl").open("w") as fh:
        for e in events:
            fh.write(json.dumps(asdict(e), sort_keys=True, default=str) + "\n")
    (dir_path / "run_manifest.json").write_text(
        json.dumps({"run_id": run_id, "initial_cash_usd": "10000"})
    )
    (dir_path / "report.json").write_text(json.dumps({"final_cash_usd": "10000"}))


def test_registry_preserves_runs_sharing_lineage(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    reg = ExperimentRegistry(reg_path)
    profile = tmp_path / "p.yaml"
    profile.write_text("x")
    a = reg.register("run-1", profile_path=profile, model_id="mock-1", code_version="abc")
    b = reg.register("run-2", profile_path=profile, model_id="mock-1", code_version="abc")
    assert a.experiment_id == b.experiment_id  # same lineage -> same experiment
    c = reg.register("run-3", profile_path=profile, model_id="gpt-x", code_version="abc")
    assert c.experiment_id != a.experiment_id  # model drift -> new experiment
    assert len(ExperimentRegistry(reg_path).list()) == 3
    assert a.study_label == STUDY_LABEL


def test_hash_chain_and_replay_report(tmp_path):
    run_dir = tmp_path / "run-ok"
    _write_run(run_dir, "r-1")
    rep = evaluate_run(run_dir)
    assert rep["checks"]["hash_chain_ok"] is True
    assert rep["checks"]["replay_ok"] is True
    assert rep["classification"] == STUDY_LABEL
    assert rep["parametric_ignorance_proven"] is False
    assert rep["application_temporal_gate"] == "NOT_RUN"
    assert rep["success"] is False


def test_tampered_log_detected(tmp_path):
    run_dir = tmp_path / "run-bad"
    _write_run(run_dir, "r-2", tamper=True)
    rep = evaluate_run(run_dir)
    assert rep["checks"]["hash_chain_ok"] is False


def test_compare_runs_invariance(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    _write_run(a, "r-same")  # same run_id + payloads -> identical digest
    _write_run(b, "r-same")
    res = compare_runs(a, b)
    assert res["invariant"] is None  # financial equality cannot establish request invariance
    assert res["success"] is False
    _write_run(b, "r-diff", tamper=True)
    assert compare_runs(a, b)["success"] is False


def test_egress_scan_tape(tmp_path):
    tape = tmp_path / "tape.jsonl"
    clean = {
        "request_hash": "h1",
        "request": {"packet": {"facts": [{"value": "425"}], "menu": []}},
        "response": {},
        "recorded_at_utc": "2020-01-02T14:30:00+00:00",
    }
    leaky = {
        "request_hash": "h2",
        "request": {"packet": {"facts": [{"value": "as of 2020-01-02"}]}},
        "response": {},
        "recorded_at_utc": "2020-01-02T14:30:00+00:00",
    }
    tape.write_text(json.dumps(clean) + "\n" + json.dumps(leaky) + "\n")
    violations = egress_scan_packets(tape, "run-x")
    assert len(violations) == 1
    assert violations[0]["request_hash"] == "h2"
    assert "EGRESS_LEAK" in violations[0]["code"]


def test_verify_chain_order_independent(tmp_path):
    """Events sort by seq before verification — file order can't hide gaps."""
    store = InMemoryEventStore()
    for i in (1, 2):
        store.append(
            Event("r", i, datetime(2020, 1, 2, tzinfo=UTC), "SIM", "RUN_STARTED", {"k": i}),
            expected_seq=i - 1,
        )
    evs = store.events("r")
    assert verify_hash_chain(list(reversed(evs))) is True
