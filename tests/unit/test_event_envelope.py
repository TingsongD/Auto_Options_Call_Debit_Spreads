"""Envelope-bound hash chain: tampering with any event field is detected."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from spx_research.domain.state import Event
from spx_research.persistence.events import InMemoryEventStore
from spx_research.research.leakage import compare_runs, load_events, verify_hash_chain


def _log(run_id: str = "r-1", n: int = 3) -> tuple[InMemoryEventStore, list[Event]]:
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
    return store, store.events(run_id)


def test_chain_verifies_clean_log():
    assert verify_hash_chain(_log()[1]) is True


def test_tamper_type_detected():
    evs = _log()[1]
    e = evs[1]
    evs[1] = Event(
        e.run_id,
        e.seq,
        e.sim_time_utc,
        e.phase,
        "POSITION_CLOSED",
        e.payload,
        e.payload_hash,
        e.previous_hash,
        e.event_hash,
    )
    assert verify_hash_chain(evs) is False


def test_tamper_sim_time_detected():
    evs = _log()[1]
    e = evs[1]
    evs[1] = Event(
        e.run_id,
        e.seq,
        datetime(2031, 1, 1, tzinfo=UTC),
        e.phase,
        e.type,
        e.payload,
        e.payload_hash,
        e.previous_hash,
        e.event_hash,
    )
    assert verify_hash_chain(evs) is False


def test_tamper_phase_and_run_id_detected():
    for field in ("phase", "run_id"):
        evs = _log()[1]
        e = evs[1]
        evs[1] = Event(
            "OTHER" if field == "run_id" else e.run_id,
            e.seq,
            e.sim_time_utc,
            "BOOT" if field == "phase" else e.phase,
            e.type,
            e.payload,
            e.payload_hash,
            e.previous_hash,
            e.event_hash,
        )
        assert verify_hash_chain(evs) is False, field


def test_seq_field_rewrite_detected():
    evs = _log(n=4)[1]
    e = evs[1]
    evs[1] = Event(
        e.run_id,
        9,
        e.sim_time_utc,
        e.phase,
        e.type,
        e.payload,
        e.payload_hash,
        e.previous_hash,
        e.event_hash,
    )
    assert verify_hash_chain(evs) is False


def test_dropped_event_detected():
    evs = _log(n=4)[1]
    assert verify_hash_chain(evs[:1] + evs[2:]) is False


def _write_dir(path: Path, events: list[Event]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with (path / "events.jsonl").open("w") as fh:
        for e in events:
            fh.write(json.dumps(asdict(e), sort_keys=True, default=str) + "\n")


def test_legacy_financial_equality_cannot_establish_request_invariance(tmp_path):
    """Without complete request artifacts, token scrubbing must not claim invariance."""
    store_a, evs_a = _log("run-a")
    store_b, evs_b = _log("run-b")
    # Simulate key-derived material that differs per run: a witness payload.
    keyed_a = Event(
        "run-a",
        4,
        datetime(2020, 1, 2, 15, tzinfo=UTC),
        "SIM",
        "DECISION_WITNESS",
        {
            "actor_id": "manager-1",
            "private_decision_id": "dec_aaaabbbbccccdddd",
            "packet_hash": "1" * 64,
            "prior_belief_hash": "2" * 64,
            "run_id": "run-a",
        },
    )
    keyed_b = Event(
        "run-b",
        4,
        datetime(2020, 1, 2, 15, tzinfo=UTC),
        "SIM",
        "DECISION_WITNESS",
        {
            "actor_id": "manager-1",
            "private_decision_id": "dec_9999888877776666",
            "packet_hash": "3" * 64,
            "prior_belief_hash": "4" * 64,
            "run_id": "run-b",
        },
    )
    evs_a.append(store_a.append(keyed_a, expected_seq=3))
    evs_b.append(store_b.append(keyed_b, expected_seq=3))
    a, b = tmp_path / "a", tmp_path / "b"
    _write_dir(a, evs_a)
    _write_dir(b, evs_b)
    res = compare_runs(a, b)
    assert res["invariant"] is None
    assert res["success"] is False


def test_compare_runs_detects_behavior_difference(tmp_path):
    evs_a = _log("run-a")[1]
    evs_b = _log("run-b")[1]
    e = evs_b[1]
    evs_b[1] = Event(
        e.run_id,
        e.seq,
        e.sim_time_utc,
        e.phase,
        e.type,
        {"k": 999},
        e.payload_hash,
        e.previous_hash,
        e.event_hash,
    )
    a, b = tmp_path / "a", tmp_path / "b"
    _write_dir(a, evs_a)
    _write_dir(b, evs_b)
    assert compare_runs(a, b)["invariant"] is None
    assert compare_runs(a, b)["success"] is False


def test_load_events_roundtrip(tmp_path):
    p = tmp_path / "events.jsonl"
    _write_dir(tmp_path, _log("rt-1")[1])
    evs = load_events(p)
    assert all(e.event_hash for e in evs)
    assert verify_hash_chain(evs) is True
