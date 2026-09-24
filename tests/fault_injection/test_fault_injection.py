"""Fault-injection battery: corrupt inputs must produce findings, not silence.

Each test breaks a different artifact (event log, decision tape, run manifest)
and asserts the harness detects it — a detector that never fires is worse than
no detector.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pytest

from spx_research.domain.state import Event
from spx_research.epistemics.store import InMemoryObservationLedger
from spx_research.epistemics.types import Atom, HarnessError
from spx_research.persistence.events import InMemoryEventStore
from spx_research.research.leakage import (
    egress_scan_packets,
    evaluate_run,
    verify_hash_chain,
)


def _event(run_id: str, seq: int, typ: str = "DECISION_MADE", payload=None) -> Event:
    return Event(
        run_id,
        seq,
        datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        "SIM",
        typ,
        payload if payload is not None else {"k": seq},
    )


def _log(run_id: str = "r-1", n: int = 4) -> list[Event]:
    store = InMemoryEventStore()
    for i in range(1, n + 1):
        store.append(
            _event(run_id, i, "RUN_STARTED" if i == 1 else "DECISION_MADE"),
            expected_seq=i - 1,
        )
    return store.events(run_id)


def _write_events(run_dir: Path, events: list[Event]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    with (run_dir / "events.jsonl").open("w") as fh:
        for e in events:
            fh.write(json.dumps(asdict(e), sort_keys=True, default=str) + "\n")


# --- event log tampering ---------------------------------------------------


def test_payload_bitflip_breaks_chain() -> None:
    evs = _log()
    e = evs[2]
    evs[2] = Event(
        e.run_id,
        e.seq,
        e.sim_time_utc,
        e.phase,
        e.type,
        {"k": "tampered"},
        e.payload_hash,
        e.previous_hash,
        e.event_hash,
    )
    assert verify_hash_chain(evs) is False


def test_genesis_link_rewrite_detected() -> None:
    evs = _log()
    e = evs[0]
    evs[0] = Event(
        e.run_id,
        e.seq,
        e.sim_time_utc,
        e.phase,
        e.type,
        e.payload,
        e.payload_hash,
        "forged-genesis",
        e.event_hash,
    )
    assert verify_hash_chain(evs) is False


def test_event_log_edit_breaks_log_hash_match(tmp_path: Path) -> None:
    """evaluate_run recomputes the digest over the file on disk — post-hoc
    edits flip log_hash_match to False even if the manifest still claims the
    original digest."""
    run_dir = tmp_path / "run-1"
    evs = _log()
    _write_events(run_dir, evs)
    from spx_research.reporting.report import event_log_digest

    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "r-1",
                "event_log_sha256": event_log_digest(evs),
                "initial_cash_usd": "10000",
            }
        )
    )
    ok = evaluate_run(run_dir)
    assert ok["checks"]["log_hash_match"] is True

    # Tamper: rewrite one payload in the file without touching the manifest.
    lines = (run_dir / "events.jsonl").read_text().splitlines()
    rec = json.loads(lines[1])
    rec["payload"]["k"] = "edited-after-run"
    lines[1] = json.dumps(rec, sort_keys=True)
    (run_dir / "events.jsonl").write_text("\n".join(lines) + "\n")

    bad = evaluate_run(run_dir)
    assert bad["checks"]["log_hash_match"] is False
    assert bad["checks"]["hash_chain_ok"] is False


# --- decision tape faults ---------------------------------------------------


def _tape_rec(i: int) -> dict:
    return {
        "request_hash": f"req-{i:04d}",
        "request": {"packet": {"premises": [], "asof_index": i}},
        "response": {},
    }


def test_torn_tail_is_an_audit_finding(tmp_path: Path) -> None:
    """A readable prefix does not make a torn tape complete audit evidence."""
    tape = tmp_path / "decision_tape.jsonl"
    tape.write_text(
        "\n".join(json.dumps(_tape_rec(i)) for i in range(3)) + '\n{"request_hash": "par'
    )
    violations = egress_scan_packets(tape, "r-1")
    assert any(v["code"] == "TAPE_TORN" for v in violations)


def test_midfile_tear_is_a_finding(tmp_path: Path) -> None:
    """A corrupt line in the MIDDLE of the tape is not a torn tail — it means
    the tape was edited after writing and must surface as TAPE_TORN."""
    tape = tmp_path / "decision_tape.jsonl"
    lines = [json.dumps(_tape_rec(i)) for i in range(3)]
    lines[1] = '{"request_hash": "corrupt'
    tape.write_text("\n".join(lines) + "\n")
    violations = egress_scan_packets(tape, "r-1")
    assert any(v["code"] == "TAPE_TORN" for v in violations)


def test_egress_literal_in_packet_flagged(tmp_path: Path) -> None:
    """A packet containing the raw run_id string is an egress violation."""
    tape = tmp_path / "decision_tape.jsonl"
    rec = _tape_rec(0)
    rec["request"]["packet"]["premises"] = [{"note": "context for run-1 here"}]
    tape.write_text(json.dumps(rec) + "\n")
    violations = egress_scan_packets(tape, "run-1")
    assert any(v["code"] == "EGRESS_LEAK:RUN_ID" for v in violations)


# --- observation ledger fault paths ----------------------------------------


def _atom(atom_id: str = "a-1", recipients: tuple[str, ...] = ("PUBLIC",)) -> Atom:
    t = datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
    return Atom(
        atom_id=atom_id,
        metric="available_slots",
        value="1",
        unit="count",
        kind="view",
        published_at=t,
        available_at=t,
        subject_at=t,
        recipients=recipients,
    )


def test_delivery_before_availability_rejected() -> None:
    led = InMemoryObservationLedger()
    a = _atom()
    led.put_atom(a)
    early = datetime(2024, 1, 2, 14, 0, tzinfo=UTC)  # before available_at
    with pytest.raises(HarnessError, match="DELIVERY_BEFORE_AVAILABILITY"):
        led.deliver("r-1", "main", "manager-1", a.atom_id, early)


def test_wrong_recipient_rejected() -> None:
    led = InMemoryObservationLedger()
    a = _atom(recipients=("manager-1",))
    led.put_atom(a)
    t = datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
    with pytest.raises(HarnessError, match="WRONG_RECIPIENT"):
        led.deliver("r-1", "main", "spread-99", a.atom_id, t)


def test_missing_atom_rejected() -> None:
    led = InMemoryObservationLedger()
    t = datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
    with pytest.raises(HarnessError, match="MISSING_OBSERVATION"):
        led.deliver("r-1", "main", "manager-1", "no-such-atom", t)


def test_redelivery_is_first_wins() -> None:
    """Retry storms must not duplicate deliveries — same key returns the
    original record (Postgres ON CONFLICT parity)."""
    led = InMemoryObservationLedger()
    a = _atom()
    led.put_atom(a)
    t = datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
    d1 = led.deliver("r-1", "main", "manager-1", a.atom_id, t)
    d2 = led.deliver("r-1", "main", "manager-1", a.atom_id, t)
    assert d1 is d2
    assert len(led.deliveries("r-1", "main", "manager-1")) == 1
