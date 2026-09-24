"""Fail-closed artifact audit and exact provider-request prefix comparisons.

Legacy artifacts remain inspectable. Missing evidence never becomes a passed
check, and an offline audit makes no claim about hidden model knowledge.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from spx_research.domain.state import Event, event_hash
from spx_research.engine.ledger import replay
from spx_research.persistence.events import payload_hash
from spx_research.reporting.report import event_log_digest
from spx_research.research.artifacts import (
    artifact_path,
    digest,
    file_digest,
    validate_frozen_manifest,
)
from spx_research.research.experiments import STUDY_LABEL


def load_events(path: Path) -> list[Event]:
    events = []
    for line in path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            events.append(
                Event(
                    row["run_id"],
                    row["seq"],
                    datetime.fromisoformat(row["sim_time_utc"]),
                    row["phase"],
                    row["type"],
                    row["payload"],
                    row.get("payload_hash", ""),
                    row.get("previous_hash", ""),
                    row.get("event_hash", ""),
                )
            )
    return events


def verify_hash_chain(events: list[Event]) -> bool:
    if not events:
        return False
    if any(not isinstance(e.seq, int) or not isinstance(e.payload, dict) for e in events):
        return False
    previous = "genesis"
    run_id = events[0].run_id
    for seq, event in enumerate(sorted(events, key=lambda e: e.seq), start=1):
        if (
            event.seq != seq
            or event.run_id != run_id
            or event.sim_time_utc.tzinfo is None
            or event.payload_hash != payload_hash(event.payload)
            or event.previous_hash != previous
            or event.event_hash != event_hash(event)
        ):
            return False
        previous = event.event_hash
    return True


def _inputs(manifest: dict[str, Any]) -> dict[str, Any]:
    value = manifest.get("inputs")
    return value if isinstance(value, dict) else {}


def _json_file(path: Path, errors: list[str], label: str) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text())
        if not isinstance(result, dict):
            raise ValueError("expected object")
        return result
    except (OSError, ValueError):
        errors.append(f"{label}_MISSING_OR_INVALID")
        return {}


def _packet_violation(packet: Any, literals: list[tuple[str, str]]) -> str | None:
    from spx_research.epistemics.egress import _PATTERNS, _is_numeric, _is_opaque_token, _strings
    from spx_research.epistemics.harness import canonical

    strings = _strings(packet)
    for text in strings:
        if _is_opaque_token(text):
            continue
        if not _is_numeric(text):
            for name, pattern in _PATTERNS:
                if pattern.search(text):
                    return name
        for literal, name in literals:
            if literal and literal in text:
                return name
    try:
        serialized = canonical(packet)
    except (ValueError, TypeError):
        return "INVALID_PACKET"
    for text in strings:
        if _is_opaque_token(text):
            serialized = serialized.replace(json.dumps(text).encode(), b'"<OPAQUE>"')
    for literal, name in literals:
        if literal and literal.encode() in serialized:
            return name
    return None


def egress_scan_packets(
    tape_path: Path,
    run_id: str,
    *,
    branch_id: str = "",
    actor_ids: tuple[str, ...] = (),
    private_manifest_id: str = "",
) -> list[dict[str, str]]:
    """Inspect packets even in legacy tapes; report torn tails and missing files."""
    if not tape_path.is_file():
        return [{"request_hash": "?", "code": "TAPE_MISSING"}]
    literals = [
        (run_id, "RUN_ID"),
        (branch_id, "BRANCH_ID"),
        (private_manifest_id, "PRIVATE_MANIFEST"),
        (f"{run_id}:{branch_id}" if branch_id else "", "ALIAS_NAMESPACE"),
        *[(actor, "ACTOR_ID") for actor in actor_ids],
    ]
    violations = []
    try:
        lines = tape_path.read_text().splitlines()
    except (OSError, ValueError):
        return [{"request_hash": "?", "code": "TAPE_UNREADABLE"}]
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("not an object")
        except ValueError:
            violations.append({"request_hash": "?", "code": "TAPE_TORN"})
            continue
        if record.get("type") == "SEAL":
            continue
        request = record.get("request")
        if not isinstance(request, dict) or "packet" not in request:
            violations.append(
                {"request_hash": record.get("request_hash", "?"), "code": "TAPE_REQUEST_MISSING"}
            )
            continue
        hit = _packet_violation(request["packet"], literals)
        if hit:
            violations.append(
                {"request_hash": record.get("request_hash", "?"), "code": f"EGRESS_LEAK:{hit}"}
            )
    return violations


def _read_tape(path: Path, errors: list[str]) -> list[dict[str, Any]]:
    from spx_research.llm.tape import DecisionTape
    from spx_research.llm.types import ModelRequest

    if not path.is_file():
        errors.append("TAPE_MISSING")
        return []
    try:
        tape = DecisionTape(path)
        if tape.legacy or tape.incomplete:
            errors.append("LEGACY_OR_INCOMPLETE_TAPE")
        records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        records = [r for r in records if r.get("type") != "SEAL"]
        for rec in records:
            if rec.get("format_version") != 2:
                continue
            req = ModelRequest(**rec["model_request"])
            if (
                rec["request"].get("body") != req.body()
                or rec["request"].get("packet") != req.packet
            ):
                errors.append("TAPE_WIRE_BODY_MISMATCH")
            if (
                hashlib.sha256(req.system_text.encode()).hexdigest() != req.system_prompt_hash
                or digest(req.output_schema) != req.schema_hash
            ):
                errors.append("TAPE_CONTRACT_HASH_MISMATCH")
            if not rec.get("decision_id") or not isinstance(rec.get("prepared"), dict):
                errors.append("TAPE_DECISION_EVIDENCE_MISSING")
        return records
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        errors.append(f"TAPE_INVALID:{type(exc).__name__}")
        return []


def _artifact_checks(
    run_dir: Path, manifest: dict[str, Any], result: dict[str, Any], errors: list[str]
) -> None:
    try:
        validate_frozen_manifest(manifest)
    except ValueError as exc:
        errors.append(str(exc))
    if result.get("format_version") != 2:
        errors.append("RESULT_VERSION_MISSING")
    if result.get("run_id") != manifest.get("run_id"):
        errors.append("RESULT_RUN_ID_MISMATCH")
    if result.get("input_sha256") != manifest.get("input_sha256"):
        errors.append("RESULT_INPUT_HASH_MISMATCH")
    declared = result.get("artifacts")
    if not isinstance(declared, dict):
        errors.append("ARTIFACT_SEAL_MISSING")
        return
    required = {"events.jsonl", "report.json", "journal.json"}
    if manifest.get("policy", "mechanical") != "mechanical":
        required.add(str(manifest.get("tape_path") or "decision_tape.jsonl"))
    if _inputs(manifest).get("policy") == "llm-replay":
        try:
            source = _inputs(manifest)["policy_meta"]["replay_source"]
            source_name = source["path"]
            if not isinstance(source_name, str) or not isinstance(source["sha256"], str):
                raise ValueError("invalid replay source")
            required.add(source_name)
            source_path = artifact_path(run_dir, source_name)
            if file_digest(source_path) != source["sha256"]:
                errors.append("REPLAY_SOURCE_HASH_MISMATCH")
            _read_tape(source_path, errors)
        except (KeyError, OSError, ValueError, TypeError):
            errors.append("REPLAY_SOURCE_MISSING_OR_INVALID")
    for name in sorted(required - set(declared)):
        errors.append(f"ARTIFACT_NOT_SEALED:{name}")
    for name, expected in declared.items():
        try:
            path = artifact_path(run_dir, name)
            if not isinstance(expected, str) or file_digest(path) != expected:
                errors.append(f"ARTIFACT_HASH_MISMATCH:{name}")
        except (OSError, ValueError, TypeError):
            errors.append(f"ARTIFACT_MISSING_OR_INVALID:{name}")


def _request_contract_checks(
    request: dict[str, Any], context: dict[str, Any], manifest: dict[str, Any], errors: list[str]
) -> None:
    try:
        bundle = _inputs(manifest)["contracts"]
        role = context["actor_role"]
        prompt = "manager" if role == "MANAGER" else "spread_agent"
        expected_prompt = bundle["files"].get(f"prompts/{prompt}.md")
        if (
            not expected_prompt
            or expected_prompt != request["system_prompt_hash"]
            or hashlib.sha256(request["system_text"].encode()).hexdigest() != expected_prompt
        ):
            errors.append("PROMPT_NOT_BOUND_TO_RUN")
        expected_schema = bundle["schema_canonical_sha256"].get(
            f"schemas/{request['schema_name']}.schema.json"
        )
        if (
            not expected_schema
            or expected_schema != request["schema_hash"]
            or digest(request["output_schema"]) != expected_schema
        ):
            errors.append("SCHEMA_NOT_BOUND_TO_RUN")
        settings = _inputs(manifest)["policy_meta"]
        if (
            request["model_id"]
            != settings["resolved_model_ids"].get("manager" if role == "MANAGER" else "spread")
            or request["max_output_tokens"] != settings["max_output_tokens"]
        ):
            errors.append("MODEL_SETTINGS_NOT_BOUND_TO_RUN")
        if request["retry_error_code"] not in {
            "",
            "RATE_LIMIT",
            "TRANSPORT",
            "TIMEOUT",
            "INCOMPLETE",
            "REFUSAL",
            "SCHEMA",
            "PROVIDER_FAILED",
        }:
            errors.append("UNAPPROVED_RETRY_CODE")
        if context["run_id"] != manifest["run_id"]:
            errors.append("TAPE_CONTEXT_RUN_MISMATCH")
    except (KeyError, TypeError, AttributeError, ValueError):
        errors.append("REQUEST_CONTRACT_EVIDENCE_MISSING")


def _tape_contract_checks(
    records: list[dict[str, Any]], manifest: dict[str, Any], events: list[Event], errors: list[str]
) -> None:
    witnesses = {
        str(e.payload.get("private_decision_id")): e.payload
        for e in events
        if e.type == "DECISION_WITNESS"
    }
    for rec in records:
        try:
            request = rec["model_request"]
            context = rec["prepared"]["compiled"]["context"]
            _request_contract_checks(request, context, manifest, errors)
            witness = witnesses.get(rec["decision_id"])
            if witness is None or witness != rec.get("result", {}).get("witness"):
                errors.append("TAPE_WITNESS_MISMATCH")
            elif witness.get("request_hash") != rec["request_hash"]:
                errors.append("WITNESS_REQUEST_MISMATCH")
        except (KeyError, TypeError, AttributeError):
            errors.append("TAPE_CONTEXT_INCOMPLETE")
    if len(witnesses) != len(records):
        errors.append("WITNESS_TAPE_COVERAGE_MISMATCH")


def _journal_requests(
    run_dir: Path,
    manifest: dict[str, Any],
    records: list[dict[str, Any]],
    errors: list[str],
) -> list[dict[str, Any]]:
    """Include failed/rejected dispatches, which the accepted-decision tape omits."""
    from spx_research.llm.types import ModelRequest

    journal = _json_file(run_dir / "journal.json", errors, "JOURNAL")
    if journal.get("format_version") != 2 or journal.get("run_id") != manifest.get("run_id"):
        errors.append("JOURNAL_IDENTITY_INVALID")
    entries = journal.get("entries")
    if not isinstance(entries, list):
        errors.append("JOURNAL_ENTRIES_MISSING")
        return []
    prepared: dict[str, dict[str, Any]] = {}
    reserved: dict[str, dict[str, Any]] = {}
    accepted: dict[str, str | None] = {}
    completed: dict[str, dict[str, Any]] = {}
    requests = []
    try:
        for entry in entries:
            payload = entry["payload"]
            if entry["kind"] == "DECISION_PREPARED":
                if digest(payload["request"]) != payload["request_hash"]:
                    errors.append("JOURNAL_PREPARATION_HASH_MISMATCH")
                prepared[payload["decision_id"]] = payload["request"]
            elif entry["kind"] == "ATTEMPT_RESERVED":
                if payload["attempt_id"] in reserved:
                    errors.append("DUPLICATE_ATTEMPT_RECORD")
                reserved[payload["attempt_id"]] = payload
                request = ModelRequest(**payload["request"])
                context = prepared[payload["decision_id"]]["compiled"]["context"]
                _request_contract_checks(payload["request"], context, manifest, errors)
                original = prepared[payload["decision_id"]]["model_request"]
                if {k: v for k, v in payload["request"].items() if k != "retry_error_code"} != {
                    k: v for k, v in original.items() if k != "retry_error_code"
                }:
                    errors.append("ATTEMPT_PREPARATION_MISMATCH")
                requests.append(
                    {
                        "context": context,
                        "body": request.body(),
                        "packet": request.packet,
                        "attempt_id": payload["attempt_id"],
                    }
                )
            elif entry["kind"] == "DECISION_ACCEPTED":
                accepted[payload["decision_id"]] = payload.get("attempt_id")
            elif entry["kind"] in {"ATTEMPT_COMPLETED", "ATTEMPT_RECONCILED"}:
                completed[payload["attempt_id"]] = payload
        for reserved_id in reserved:
            outcome = completed.get(reserved_id)
            if outcome is None or outcome.get("actual_usd") is None:
                errors.append("ATTEMPT_ACCOUNTING_UNRESOLVED")
        for record in records:
            decision_id = record["decision_id"]
            if record.get("prepared") != prepared.get(decision_id):
                errors.append("TAPE_PREPARATION_MISMATCH")
            if decision_id not in accepted:
                errors.append("TAPE_ACCEPTANCE_NOT_JOURNALED")
                continue
            attempt_id = accepted[decision_id]
            if attempt_id is None:
                if not _inputs(manifest).get("policy_meta", {}).get("replay_source"):
                    errors.append("REPLAY_SOURCE_NOT_BOUND")
            elif (
                ModelRequest(**reserved[attempt_id]["request"]).request_hash()
                != record["request_hash"]
            ):
                errors.append("ACCEPTED_ATTEMPT_REQUEST_MISMATCH")
    except (KeyError, TypeError, ValueError):
        errors.append("JOURNAL_REQUEST_EVIDENCE_MISSING")
    return requests


def evaluate_run(
    run_dir: Path, tape_path: Path | None = None, initial_cash: Decimal = Decimal("10000")
) -> dict[str, Any]:
    errors: list[str] = []
    manifest = _json_file(run_dir / "run_manifest.json", errors, "MANIFEST")
    report = _json_file(run_dir / "report.json", errors, "REPORT")
    modern = manifest.get("format_version") == 2
    result = _json_file(run_dir / "run_result.json", errors, "RESULT") if modern else {}
    if not modern:
        errors.append("LEGACY_RUN_AUDIT_UNAVAILABLE")
    else:
        _artifact_checks(run_dir, manifest, result, errors)
    events: list[Event] = []
    try:
        events = load_events(run_dir / "events.jsonl")
    except (OSError, ValueError, KeyError, TypeError):
        errors.append("EVENTS_MISSING_OR_INVALID")
    chain_ok = verify_hash_chain(events) if events else None
    if chain_ok is not True:
        errors.append("EVENT_CHAIN_UNVERIFIED")
    run_id = str(manifest.get("run_id") or (events[0].run_id if events else run_dir.name))
    if events and any(e.run_id != run_id for e in events):
        errors.append("EVENT_RUN_ID_MISMATCH")
    log_hash = event_log_digest(events) if events else None
    expected_log = result.get("event_log_sha256") if modern else manifest.get("event_log_sha256")
    log_match = log_hash == expected_log if log_hash is not None and expected_log else None
    if modern and log_match is not True:
        errors.append("EVENT_LOG_SEAL_UNVERIFIED")
    replay_ok: bool | None = None
    if chain_ok:
        try:
            cash = Decimal(str(manifest.get("initial_cash_usd") or initial_cash))
            state = replay(run_id, cash, events)
            replay_ok = (
                (Decimal(str(report["final_cash_usd"])) == state.account.cash)
                if "final_cash_usd" in report
                else None
            )
        except (ValueError, KeyError, TypeError, ArithmeticError):
            replay_ok = False
    if replay_ok is not True:
        errors.append("REPLAY_UNVERIFIED")
    policy = manifest.get("policy") or _inputs(manifest).get("policy", "mechanical")
    model_involved = policy != "mechanical" or any(e.type == "DECISION_WITNESS" for e in events)
    selected_tape = tape_path
    if selected_tape is None and (model_involved or (run_dir / "decision_tape.jsonl").exists()):
        try:
            declared = str(manifest.get("tape_path") or "decision_tape.jsonl")
            selected_tape = artifact_path(run_dir, declared) if modern else Path(declared)
            if not modern and not selected_tape.is_absolute():
                selected_tape = run_dir / selected_tape
        except ValueError:
            errors.append("TAPE_PATH_INVALID")
    records: list[dict[str, Any]] = []
    violations: list[dict[str, str]] = []
    if selected_tape is not None:
        if modern:
            try:
                relative = str(selected_tape.resolve().relative_to(run_dir.resolve()))
                declared_artifacts = result.get("artifacts")
                if not isinstance(declared_artifacts, dict) or relative not in declared_artifacts:
                    errors.append("TAPE_NOT_SEALED_IN_RESULT")
            except ValueError:
                errors.append("TAPE_OUTSIDE_RUN_ARTIFACTS")
        records = _read_tape(selected_tape, errors)
        actors = tuple({str(e.payload["actor_id"]) for e in events if e.payload.get("actor_id")})
        violations = egress_scan_packets(
            selected_tape,
            run_id,
            branch_id=str(manifest.get("branch_id") or ""),
            actor_ids=actors,
            private_manifest_id=str(manifest.get("private_manifest_id") or ""),
        )
        if modern:
            _tape_contract_checks(records, manifest, events, errors)
    elif model_involved:
        errors.append("TAPE_MISSING")
    if violations:
        errors.append("EGRESS_VIOLATIONS")
    if modern:
        dispatched = _journal_requests(run_dir, manifest, records, errors)
        for attempt in dispatched:
            context = attempt["context"]
            if not isinstance(context, dict):
                errors.append("JOURNAL_CONTEXT_MISSING")
                continue
            literals = [
                (run_id, "RUN_ID"),
                (str(context.get("actor_id", "")), "ACTOR_ID"),
                (str(manifest.get("private_manifest_id") or ""), "PRIVATE_MANIFEST"),
            ]
            hit = _packet_violation(attempt["packet"], literals)
            if hit:
                violations.append(
                    {"request_hash": attempt["attempt_id"], "code": f"EGRESS_LEAK:{hit}"}
                )
                errors.append("EGRESS_VIOLATIONS")
    status = str(result.get("status") or "UNKNOWN")
    if modern and status != "COMPLETED":
        errors.append("RUN_NOT_COMPLETED")
    if modern and (not events or events[-1].type != "RUN_ENDED"):
        errors.append("RUN_END_MISSING")
    success = modern and not errors
    return {
        "schema_version": "2.0",
        "run_id": run_id,
        "status": status,
        "event_count": len(events),
        "event_log_sha256": log_hash,
        "success": success,
        "audit_status": "PASS" if success else ("FAIL" if modern else "UNAVAILABLE"),
        "errors": sorted(set(errors)),
        "checks": {
            "events_present": bool(events),
            "hash_chain_ok": chain_ok,
            "replay_ok": replay_ok,
            "log_hash_match": log_match,
            "egress_violations": violations,
            "artifacts_complete": success,
        },
        "application_temporal_gate": "NOT_RUN",
        "scoped_checks": [
            "artifact_integrity",
            "event_chain_and_financial_replay",
            "recorded_request_egress",
            "accepted_request_witness_linkage",
        ],
        "temporal_test_evidence": "NOT_ATTACHED",
        "behavioral_leakage_diagnostics": "NOT_RUN",
        "model_temporal_provenance": "UNKNOWN",
        "parametric_future_knowledge_excluded": False,
        "study_label": STUDY_LABEL,
        "classification": STUDY_LABEL,
        "parametric_ignorance_proven": False,
        "residual_risk": (
            "Artifact and request checks cannot exclude pretrained historical knowledge. "
            "The full application temporal gate and behavioral diagnostics were not run."
        ),
    }


def _prefix_requests(
    run_dir: Path, cutoff: datetime, errors: list[str]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = _json_file(run_dir / "run_manifest.json", errors, "MANIFEST")
    result = _json_file(run_dir / "run_result.json", errors, "RESULT")
    _artifact_checks(run_dir, manifest, result, errors)
    errors.extend(evaluate_run(run_dir)["errors"])
    try:
        path = artifact_path(run_dir, str(manifest.get("tape_path") or "decision_tape.jsonl"))
    except ValueError:
        errors.append("TAPE_PATH_INVALID")
        return manifest, []
    records = _read_tape(path, errors)
    rows = _journal_requests(run_dir, manifest, records, errors)
    prefix = []
    contexts: dict[tuple[str, str, str], int] = {}
    for rec in rows:
        try:
            context = rec["context"]
            at = datetime.fromisoformat(context["as_of"])
            if at.tzinfo is None:
                raise ValueError("naive")
            if at > cutoff:
                continue
            base_key = (at.astimezone(UTC).isoformat(), context["actor_role"], context["actor_id"])
            ordinal = contexts.get(base_key, 0)
            contexts[base_key] = ordinal + 1
            prefix.append(
                {
                    "context": (*base_key, ordinal),
                    "episode_token": rec["packet"]["episode_token"],
                    "body": rec["body"],
                }
            )
        except (KeyError, ValueError, TypeError):
            errors.append("PREFIX_CONTEXT_MISSING")
    if not prefix:
        errors.append("EMPTY_REQUEST_PREFIX")
    return manifest, sorted(prefix, key=lambda p: p["context"])


def compare_runs(
    dir_a: Path,
    dir_b: Path,
    cutoff: datetime | None = None,
    expect: Literal["invariant", "changed"] = "invariant",
) -> dict[str, Any]:
    """Compare exact full provider bodies through an explicit historical cutoff.

    Identity-bearing request fields are deliberately never scrubbed. A changed
    prompt, schema, retry code or opaque token is a changed request.
    """
    errors: list[str] = []
    if cutoff is None or cutoff.tzinfo is None:
        return {
            "success": False,
            "invariant": None,
            "expect": expect,
            "errors": ["EXPLICIT_AWARE_CUTOFF_REQUIRED"],
            "digest_a": None,
            "digest_b": None,
        }
    if expect not in ("invariant", "changed"):
        errors.append("INVALID_COMPARISON_EXPECTATION")
    manifest_a, a = _prefix_requests(dir_a, cutoff, errors)
    manifest_b, b = _prefix_requests(dir_b, cutoff, errors)
    alias_a = _inputs(manifest_a).get("alias_key_id")
    alias_b = _inputs(manifest_b).get("alias_key_id")
    if not alias_a or alias_a != alias_b:
        errors.append("ALIAS_CONFIGURATION_MISMATCH")
    if [p["context"] for p in a] != [p["context"] for p in b]:
        errors.append("PREFIX_CONTEXT_ALIGNMENT_MISMATCH")
    if [p["episode_token"] for p in a] != [p["episode_token"] for p in b]:
        errors.append("VISIBLE_ALIAS_MISMATCH")
    da, db = (digest(a) if a else None), (digest(b) if b else None)
    invariant: bool | None = da == db if da is not None and db is not None and not errors else None
    matches = invariant is (expect == "invariant") if invariant is not None else False
    if invariant is not None and not matches:
        errors.append("COMPARISON_EXPECTATION_FAILED")
    return {
        "run_a": manifest_a.get("run_id", str(dir_a)),
        "run_b": manifest_b.get("run_id", str(dir_b)),
        "cutoff": cutoff.astimezone(UTC).isoformat(),
        "expect": expect,
        "digest_a": da,
        "digest_b": db,
        "request_count_a": len(a),
        "request_count_b": len(b),
        "invariant": invariant,
        "success": matches and not errors,
        "errors": sorted(set(errors)),
    }
