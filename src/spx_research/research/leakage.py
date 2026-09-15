"""Leakage evaluation (M6): hash-chain, replay, egress, invariance probes.

Produces the fixed-classification report for a run directory. The label is
always HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED — a clean scan
means the *information boundary* held, not that the model lacks pretrained
historical knowledge (parametric_ignorance_proven is always false).
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from spx_research.domain.state import Event, event_hash
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
                e.get("event_hash", ""),
            )
        )
    return out


def verify_hash_chain(events: list[Event]) -> bool:
    """Recompute payload_hash/event_hash/previous_hash over the committed prefix.

    Chaining rule (both stores): ``previous_hash(e_n)`` is the prior event's
    ``event_hash`` (``"genesis"`` for the first), where ``event_hash`` binds
    run_id, seq, sim_time_utc, phase, type, payload_hash and previous_hash —
    so envelope tampering (type/time/phase) is detected, not just payload
    edits.
    """
    prev = "genesis"
    expected_seq: int | None = None
    for e in sorted(events, key=lambda x: x.seq):
        if expected_seq is not None and e.seq != expected_seq:
            return False
        if (
            e.payload_hash != payload_hash(e.payload)
            or e.previous_hash != prev
            or e.event_hash != event_hash(e)
        ):
            return False
        prev = e.event_hash
        expected_seq = e.seq + 1
    return True


def egress_scan_packets(
    tape_path: Path,
    run_id: str,
    *,
    branch_id: str = "",
    actor_ids: tuple[str, ...] = (),
    private_manifest_id: str = "",
) -> list[dict[str, str]]:
    """Run the egress gate over every public packet on a decision tape.

    Scans against the union of the run's real literals (branch, all actors,
    private manifest, alias namespace) — equivalent to per-actor contexts but
    O(lines) instead of O(lines x actors).
    """
    from spx_research.epistemics.egress import _PATTERNS, _is_numeric, _strings
    from spx_research.epistemics.harness import canonical

    violations: list[dict[str, str]] = []
    if not tape_path.is_file():
        return violations
    literals = [
        (lit, name)
        for lit, name in [
            (run_id, "RUN_ID"),
            (branch_id, "BRANCH_ID"),
            (private_manifest_id, "PRIVATE_MANIFEST"),
            (f"{run_id}:{branch_id}" if branch_id else "", "ALIAS_NAMESPACE"),
            *[(a, "ACTOR_ID") for a in actor_ids],
        ]
        if lit
    ]
    for line in tape_path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue  # torn tail — same tolerance as DecisionTape
        packet = rec.get("request", {}).get("packet")
        if packet is None:
            continue
        hit: str | None = None
        for s in _strings(packet):
            if not _is_numeric(s):
                for name, pat in _PATTERNS:
                    if pat.search(s):
                        hit = name
                        break
            if hit is None:
                for lit, name in literals:
                    if lit in s:
                        hit = name
                        break
            if hit is not None:
                break
        if hit is None:
            blob = canonical(packet)
            for lit, name in literals:
                if lit.encode() in blob:
                    hit = name
                    break
        if hit is not None:
            violations.append(
                {"request_hash": rec.get("request_hash", "?"), "code": f"EGRESS_LEAK:{hit}"}
            )
    return violations


def evaluate_run(
    run_dir: Path,
    tape_path: Path | None = None,
    initial_cash: Decimal = Decimal("10000"),
) -> dict[str, Any]:
    """Fixed-classification evaluation for one run directory."""
    events_path = run_dir / "events.jsonl"
    events = load_events(events_path) if events_path.is_file() else []
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
    replay_ok: bool | None = None
    if events:
        replay_ok = st is not None and (
            not report.get("final_cash_usd")
            or Decimal(str(st.account.cash)) == Decimal(str(report["final_cash_usd"]))
        )
    # The manifest's log digest was computed at write time; recomputing it
    # over the file on disk catches edits made after the run finished.
    log_digest = event_log_digest(events) if events else None
    manifest_digest = manifest.get("event_log_sha256")
    log_hash_match: bool | None = None
    if log_digest is not None and manifest_digest:
        log_hash_match = log_digest == manifest_digest
    if tape_path is None:
        candidate = run_dir / "decision_tape.jsonl"
        tape_path = candidate if candidate.exists() else None
    branch_id = manifest.get("branch_id") or ""
    if not branch_id and events:
        branch_id = next(
            (
                str(e.payload["branch_id"])
                for e in events
                if e.type == "DECISION_WITNESS" and e.payload.get("branch_id")
            ),
            "",
        )
    actor_ids = tuple(
        sorted(
            {
                str(e.payload["actor_id"])
                for e in events
                if e.type in ("DECISION_MADE", "DECISION_WITNESS") and e.payload.get("actor_id")
            }
        )
    )
    violations = (
        egress_scan_packets(
            tape_path,
            run_id,
            branch_id=branch_id,
            actor_ids=actor_ids,
            private_manifest_id=str(manifest.get("private_manifest_id") or ""),
        )
        if tape_path
        else []
    )
    return {
        "schema_version": "1.0",
        "run_id": run_id,
        "event_count": len(events),
        "event_log_sha256": log_digest,
        "checks": {
            "events_present": bool(events),
            "hash_chain_ok": verify_hash_chain(events) if events else None,
            "replay_ok": replay_ok,
            "log_hash_match": log_hash_match,
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
    da = _normalized_digest(ev_a)
    db = _normalized_digest(ev_b)
    return {
        "run_a": ev_a[0].run_id if ev_a else str(dir_a),
        "run_b": ev_b[0].run_id if ev_b else str(dir_b),
        "digest_a": da,
        "digest_b": db,
        "invariant": da == db,
    }


_TOKEN_RE = None  # lazily compiled
_SCRUB_KEYS = frozenset(
    {
        # Key-derived (HMAC) values differ across run ids even when behavior
        # is identical — they are identity material, not behavior.
        "private_decision_id",
        "decision_token",
        "packet_token",
        "prior_belief_token",
        "episode_token",
        "packet_hash",
        "prior_belief_hash",
        "proposal_hash",
        "belief_hash",
    }
)


def _scrub(value: Any) -> Any:
    """Replace key-derived tokens/hashes with placeholders, recursively."""
    global _TOKEN_RE
    if _TOKEN_RE is None:
        import re

        _TOKEN_RE = re.compile(r"^(dec|pkt|ep|as|act|ev|tgt|lim)_[0-9a-f]{12,}$")
    if isinstance(value, dict):
        return {
            k: ("<KEYED>" if k in _SCRUB_KEYS else _scrub(v)) for k, v in value.items()
        }
    if isinstance(value, list):
        return [_scrub(v) for v in value]
    if isinstance(value, str) and _TOKEN_RE.match(value):
        return "<TOK>"
    return value


def _normalized_digest(events: list[Event]) -> str | None:
    """Event digest with run-scoped identity material scrubbed.

    ``run_id`` appears in the envelope and inside payloads (incident ids,
    witnesses); HMAC-derived tokens and the packet/belief/proposal hashes
    that embed them also differ across run ids under identical *behavior*.
    All are normalized so the digest compares behavior only. Hash-chain
    envelope fields are excluded — they are verified separately by
    ``verify_hash_chain``.
    """
    import hashlib

    if not events:
        return None
    h = hashlib.sha256()
    for e in sorted(events, key=lambda x: x.seq):
        blob = json.dumps(
            {
                "seq": e.seq,
                "sim_time_utc": e.sim_time_utc.isoformat(),
                "phase": e.phase,
                "type": e.type,
                "payload": _scrub(e.payload),
            },
            sort_keys=True,
            default=str,
        )
        h.update(blob.replace(e.run_id, "<RUN>").encode() + b"\n")
    return h.hexdigest()
