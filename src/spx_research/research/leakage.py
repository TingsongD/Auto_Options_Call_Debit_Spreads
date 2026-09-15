"""Leakage evaluation (M6): hash-chain, replay, egress, invariance probes.

Produces the fixed-classification report for a run directory. The label is
always HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED — a clean scan
means the *information boundary* held, not that the model lacks pretrained
historical knowledge (parametric_ignorance_proven is always false).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from spx_research.domain.state import Event
from spx_research.engine.ledger import replay
from spx_research.persistence.events import payload_hash
from spx_research.reporting.report import event_log_digest
from spx_research.research.experiments import STUDY_LABEL


def load_events(path: Path) -> list[Event]:
    out = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        out.append(
            Event(
                e["run_id"],
                e["seq"],
                datetime.fromisoformat(e["sim_time_utc"]),
                e["phase"],
                e["type"],
                e["payload"],
                e.get("payload_hash", ""),
                e.get("previous_hash", ""),
            )
        )
    return out


def verify_hash_chain(events: list[Event]) -> bool:
    """Recompute payload_hash/previous_hash over the committed prefix.

    Chaining rule (both stores): previous_hash(e_n) =
    sha256(payload_hash(e_{n-1}) + str(seq_{n-1}))[:24].
    """
    import hashlib

    prev = "genesis"
    for e in sorted(events, key=lambda x: x.seq):
        if e.payload_hash != payload_hash(e.payload) or e.previous_hash != prev:
            return False
        prev = hashlib.sha256((e.payload_hash + str(e.seq)).encode()).hexdigest()[:24]
    return True


def egress_scan_packets(tape_path: Path, run_id: str) -> list[dict[str, str]]:
    """Run the egress gate over every public packet on a decision tape."""
    from spx_research.epistemics.egress import egress_check
    from spx_research.epistemics.types import Context

    violations: list[dict[str, str]] = []
    if not tape_path.is_file():
        return violations
    ctx = Context(
        run_id=run_id,
        branch_id="",
        actor_id="",
        actor_role="",
        as_of=datetime.now(UTC),
        session_index=0,
        minute_from_open=0,
        alias_namespace="",
        private_manifest_id="",
    )
    for line in tape_path.read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        packet = rec.get("request", {}).get("packet")
        if packet is None:
            continue
        try:
            egress_check(packet, ctx)
        except Exception as e:  # HarnessError: EGRESS_LEAK:*
            violations.append({"request_hash": rec.get("request_hash", "?"), "code": str(e)})
    return violations


def evaluate_run(
    run_dir: Path,
    tape_path: Path | None = None,
    initial_cash: Decimal = Decimal("10000"),
) -> dict[str, Any]:
    """Fixed-classification evaluation for one run directory."""
    events = load_events(run_dir / "events.jsonl")
    report: dict[str, Any] = {}
    rp = run_dir / "report.json"
    if rp.exists():
        report = json.loads(rp.read_text())
    manifest: dict[str, Any] = {}
    mp = run_dir / "run_manifest.json"
    if mp.exists():
        manifest = json.loads(mp.read_text())
    cash = Decimal(str(manifest.get("initial_cash_usd") or initial_cash))
    run_id = events[0].run_id if events else manifest.get("run_id", run_dir.name)

    st = replay(run_id, cash, events) if events else None
    replay_ok = st is not None and (
        not report.get("final_cash_usd")
        or Decimal(str(st.account.cash)) == Decimal(str(report["final_cash_usd"]))
    )
    if tape_path is None:
        candidate = run_dir / "decision_tape.jsonl"
        tape_path = candidate if candidate.exists() else None
    violations = egress_scan_packets(tape_path, run_id) if tape_path else []
    return {
        "schema_version": "1.0",
        "run_id": run_id,
        "event_count": len(events),
        "event_log_sha256": event_log_digest(events) if events else None,
        "checks": {
            "hash_chain_ok": verify_hash_chain(events),
            "replay_ok": replay_ok,
            "egress_violations": violations,
        },
        "classification": STUDY_LABEL,
        "parametric_ignorance_proven": False,
        "residual_risk": (
            "Boundary checks verify prefix-only inputs; they cannot establish "
            "absence of pretrained historical knowledge. Identity probes and "
            "counterfactual continuations are the diagnostics for residual "
            "parametric memory."
        ),
    }


def compare_runs(dir_a: Path, dir_b: Path) -> dict[str, Any]:
    """Future-suffix/prefix invariance probe over two run directories.

    Identical event digests ⇒ the run was invariant to the mutated suffix.
    Differing digests with an intentionally changed prefix is the positive
    control — the probe can tell the difference.
    """
    ev_a = load_events(dir_a / "events.jsonl")
    ev_b = load_events(dir_b / "events.jsonl")
    da = event_log_digest(ev_a)
    db = event_log_digest(ev_b)
    return {
        "run_a": ev_a[0].run_id if ev_a else str(dir_a),
        "run_b": ev_b[0].run_id if ev_b else str(dir_b),
        "digest_a": da,
        "digest_b": db,
        "invariant": da == db,
    }
