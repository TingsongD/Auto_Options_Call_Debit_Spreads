"""Durable runtime-v2 ledger: immutable requests, paid attempts and atomic barriers.

Every mutation is serialized per run. Inference attempts survive a failed
financial barrier; financial events, accepted knowledge and the resume cursor
commit together. Legacy event-only runs remain replayable but cannot resume.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from threading import RLock
from typing import Any, Protocol

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from spx_research.domain.results import RunStatus
from spx_research.domain.state import Event
from spx_research.epistemics.store import InMemoryObservationLedger, ObservationLedger
from spx_research.epistemics.types import HarnessError
from spx_research.persistence import schema as S
from spx_research.persistence.events import InMemoryEventStore, LedgerError
from spx_research.persistence.postgres import PostgresEventStore, PostgresObservationLedger, _scope


def json_value(value: Any) -> Any:
    """Detach runtime values and encode Decimal/time without binary rounding."""
    return json.loads(json.dumps(value, sort_keys=True, default=_json_default, allow_nan=False))


def _json_default(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise LedgerError("NAIVE_RUNTIME_TIMESTAMP")
        return value.isoformat()
    raise TypeError(f"unsupported runtime value: {type(value).__name__}")


def record_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(json_value(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _money(value: Decimal) -> Decimal:
    amount = Decimal(value)
    if not amount.is_finite() or amount < 0:
        raise LedgerError("INVALID_BUDGET_AMOUNT")
    return amount


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    manifest: dict[str, Any]
    manifest_hash: str
    status: RunStatus
    cursor: dict[str, Any]
    pause: dict[str, Any] | None = None
    budget_cap: Decimal | None = None


@dataclass(frozen=True)
class DecisionRecord:
    run_id: str
    decision_id: str
    request_hash: str
    request: dict[str, Any]
    response: dict[str, Any] | None
    status: str
    result: dict[str, Any] | None = None
    accepted_attempt_id: str | None = None


@dataclass(frozen=True)
class AttemptRecord:
    run_id: str
    attempt_id: str
    decision_id: str
    reserved_usd: Decimal
    actual_usd: Decimal | None
    outcome: str
    response: dict[str, Any] | None
    error_code: str = ""
    request: dict[str, Any] | None = None

    @property
    def status(self) -> str:
        return self.outcome


_KEYS: dict[str, tuple[str, ...]] = {
    "runtime_runs": ("run_id",),
    "runtime_decisions": ("run_id", "decision_id"),
    "runtime_attempts": ("run_id", "attempt_id"),
    "runtime_barriers": ("run_id", "barrier_id"),
}


class _Transaction(Protocol):
    def get(self, name: str, **key: Any) -> dict[str, Any] | None: ...
    def rows(self, name: str, **key: Any) -> list[dict[str, Any]]: ...
    def put(self, name: str, row: dict[str, Any]) -> None: ...
    def journal(self, run_id: str, kind: str, payload: dict[str, Any]) -> None: ...
    def events(self, run_id: str) -> list[Event]: ...
    def tip(self, run_id: str) -> tuple[int, str]: ...
    def append(
        self, events: list[Event], expected_seq: int, outbox: list[dict[str, Any]]
    ) -> list[Event]: ...
    def observations(
        self, observations: dict[str, Any], assessments: list[dict[str, Any]]
    ) -> None: ...
    def create_legacy_run(self, run_id: str, manifest: dict[str, Any]) -> None: ...


class RunStore:
    """Shared transition rules; backends differ only in transaction mechanics."""

    observation_ledger: ObservationLedger

    @contextmanager
    def _transaction(self, run_id: str, *, readonly: bool = False) -> Iterator[_Transaction]:
        raise NotImplementedError
        yield  # pragma: no cover

    def begin_run(self, run_id: str, manifest: dict[str, Any]) -> RunRecord:
        if manifest.get("format_version") != 2:
            raise LedgerError("LEGACY_RUN_READ_ONLY")
        frozen = json_value(manifest)
        mh = record_hash(frozen)
        with self._transaction(run_id) as tx:
            existing = tx.get("runtime_runs", run_id=run_id)
            if existing:
                if existing["manifest_hash"] != mh:
                    raise LedgerError("RUN_MANIFEST_MISMATCH")
                return RunRecord(**existing)
            if tx.tip(run_id)[0]:
                raise LedgerError("LEGACY_RUN_READ_ONLY")
            tx.create_legacy_run(run_id, frozen)
            row = dict(
                run_id=run_id,
                manifest=frozen,
                manifest_hash=mh,
                status="RUNNING",
                cursor={},
                pause=None,
                budget_cap=None,
            )
            tx.put("runtime_runs", row)
            tx.journal(run_id, "RUN_PREPARED", {"manifest_hash": mh})
            return RunRecord(**row)

    def load_run(self, run_id: str) -> RunRecord | None:
        with self._transaction(run_id, readonly=True) as tx:
            row = tx.get("runtime_runs", run_id=run_id)
            return RunRecord(**row) if row else None

    @staticmethod
    def _require(tx: _Transaction, run_id: str) -> dict[str, Any]:
        row = tx.get("runtime_runs", run_id=run_id)
        if row is None:
            raise LedgerError("RUN_NOT_PREPARED_OR_LEGACY")
        return row

    def persist_cursor(
        self,
        run_id: str,
        cursor: dict[str, Any],
        status: RunStatus,
        pause: dict[str, Any] | None = None,
    ) -> None:
        if status not in {"RUNNING", "PAUSED", "COMPLETED", "FAILED"}:
            raise LedgerError("INVALID_RUN_STATUS")
        with self._transaction(run_id) as tx:
            row = self._require(tx, run_id)
            frozen = json_value(cursor)
            if ("ledger_seq" in frozen or "ledger_hash" in frozen) and (
                frozen.get("ledger_seq"),
                frozen.get("ledger_hash"),
            ) != tx.tip(run_id):
                raise LedgerError("STALE_CURSOR_WRITE")
            if row["status"] in {"COMPLETED", "FAILED"} and (
                status != row["status"] or frozen != row["cursor"]
            ):
                raise LedgerError("TERMINAL_RUN_IMMUTABLE")
            row.update(cursor=frozen, status=status, pause=json_value(pause))
            tx.put("runtime_runs", row)
            tx.journal(
                run_id, "CURSOR", {"cursor": row["cursor"], "status": status, "pause": row["pause"]}
            )

    def events(self, run_id: str) -> list[Event]:
        with self._transaction(run_id, readonly=True) as tx:
            return tx.events(run_id)

    def tip(self, run_id: str) -> tuple[int, str]:
        with self._transaction(run_id, readonly=True) as tx:
            return tx.tip(run_id)

    def append(self, event: Event, expected_seq: int) -> Event:
        return self.append_batch([event], expected_seq)[0]

    def append_batch(self, events: list[Event], expected_seq: int) -> list[Event]:
        if not events:
            return []
        with self._transaction(events[0].run_id) as tx:
            row = self._require(tx, events[0].run_id)
            committed = tx.append(events, expected_seq, [])
            for event in committed:
                if event.type == "RUN_STARTED":
                    row["status"] = "RUNNING"
                elif event.type == "RUN_ENDED":
                    row["status"] = str(event.payload.get("status", "COMPLETED"))
            tx.put("runtime_runs", row)
            return committed

    def prepare_barrier(
        self,
        run_id: str,
        barrier_id: str,
        expected_seq: int,
        request_hashes: dict[str, str],
        cursor: dict[str, Any],
    ) -> None:
        with self._transaction(run_id) as tx:
            self._require(tx, run_id)
            existing = tx.get("runtime_barriers", run_id=run_id, barrier_id=barrier_id)
            wanted = dict(
                run_id=run_id,
                barrier_id=barrier_id,
                expected_seq=expected_seq,
                request_hashes=json_value(request_hashes),
                cursor=json_value(cursor),
            )
            if existing:
                if any(existing[k] != v for k, v in wanted.items()):
                    raise LedgerError("BARRIER_PREPARATION_MISMATCH")
                return
            if tx.tip(run_id)[0] != expected_seq:
                raise LedgerError("SEQUENCE_MISMATCH")
            tx.put(
                "runtime_barriers",
                {**wanted, "batch_hash": None, "last_seq": None, "status": "PREPARED"},
            )
            tx.journal(
                run_id, "BARRIER_PREPARED", {"barrier_id": barrier_id, "expected_seq": expected_seq}
            )

    def commit_barrier(
        self,
        run_id: str,
        barrier_id: str,
        expected_seq: int,
        events: list[Event],
        cursor: dict[str, Any],
        assessments: list[dict[str, Any]] | None = None,
        outbox: list[dict[str, Any]] | None = None,
        observations: dict[str, Any] | None = None,
    ) -> list[Event]:
        assessment_rows = json_value(assessments or [])
        obs = json_value(observations or {})
        cb = json_value(cursor)
        body_hash = record_hash(
            {
                "events": [asdict(e) for e in events],
                "cursor": cb,
                "assessments": assessment_rows,
                "observations": obs,
                "outbox": outbox or [],
            }
        )
        with self._transaction(run_id) as tx:
            run = self._require(tx, run_id)
            existing = tx.get("runtime_barriers", run_id=run_id, barrier_id=barrier_id)
            if existing and existing["status"] == "COMMITTED":
                if existing["batch_hash"] != body_hash or existing["expected_seq"] != expected_seq:
                    raise LedgerError("BARRIER_COMMIT_MISMATCH")
                return [
                    e for e in tx.events(run_id) if expected_seq < e.seq <= existing["last_seq"]
                ]
            if existing and existing["expected_seq"] != expected_seq:
                raise LedgerError("BARRIER_PREPARATION_MISMATCH")
            if tx.tip(run_id)[0] != expected_seq:
                raise LedgerError("SEQUENCE_MISMATCH")
            if any(e.run_id != run_id for e in events):
                raise LedgerError("CROSS_RUN_BARRIER")
            for item in assessment_rows + obs.get("deliveries", []):
                if item["run_id"] != run_id:
                    raise LedgerError("CROSS_RUN_OBSERVATION")
            committed = tx.append(events, expected_seq, outbox or [])
            tx.observations(obs, assessment_rows)
            tx.put(
                "runtime_barriers",
                dict(
                    run_id=run_id,
                    barrier_id=barrier_id,
                    expected_seq=expected_seq,
                    request_hashes=existing["request_hashes"] if existing else {},
                    cursor=cb,
                    batch_hash=body_hash,
                    last_seq=committed[-1].seq if committed else expected_seq,
                    status="COMMITTED",
                ),
            )
            ended = next((e for e in reversed(committed) if e.type == "RUN_ENDED"), None)
            run.update(
                cursor=cb,
                status=str(ended.payload.get("status", "COMPLETED")) if ended else "RUNNING",
                pause=None,
            )
            tx.put("runtime_runs", run)
            tx.journal(
                run_id, "BARRIER_COMMITTED", {"barrier_id": barrier_id, "batch_hash": body_hash}
            )
            return committed

    def load_decision(self, run_id: str, decision_id: str) -> DecisionRecord | None:
        with self._transaction(run_id, readonly=True) as tx:
            row = tx.get("runtime_decisions", run_id=run_id, decision_id=decision_id)
            return DecisionRecord(**row) if row else None

    def prepare_decision(
        self, run_id: str, decision_id: str, request: dict[str, Any]
    ) -> DecisionRecord:
        frozen = json_value(request)
        request_hash = record_hash(frozen)
        with self._transaction(run_id) as tx:
            self._require(tx, run_id)
            row = tx.get("runtime_decisions", run_id=run_id, decision_id=decision_id)
            if row:
                if row["request_hash"] != request_hash:
                    raise LedgerError("DECISION_REQUEST_MISMATCH")
                return DecisionRecord(**row)
            row = dict(
                run_id=run_id,
                decision_id=decision_id,
                request_hash=request_hash,
                request=frozen,
                response=None,
                status="PREPARED",
                result=None,
                accepted_attempt_id=None,
            )
            tx.put("runtime_decisions", row)
            tx.journal(
                run_id,
                "DECISION_PREPARED",
                {"decision_id": decision_id, "request_hash": request_hash, "request": frozen},
            )
            return DecisionRecord(**row)

    @staticmethod
    def _totals(tx: _Transaction, run_id: str, run: dict[str, Any]) -> dict[str, Decimal]:
        attempts = tx.rows("runtime_attempts", run_id=run_id)
        return {
            "committed": sum(
                (Decimal(a["actual_usd"]) for a in attempts if a["actual_usd"] is not None),
                Decimal(0),
            ),
            "reserved": sum(
                (Decimal(a["reserved_usd"]) for a in attempts if a["actual_usd"] is None),
                Decimal(0),
            ),
            "cap": Decimal(run["budget_cap"]) if run["budget_cap"] is not None else Decimal(0),
        }

    def budget_totals(self, run_id: str) -> dict[str, Decimal]:
        with self._transaction(run_id, readonly=True) as tx:
            return self._totals(tx, run_id, self._require(tx, run_id))

    @staticmethod
    def _has_overrun(tx: _Transaction, run_id: str) -> bool:
        return any(
            a["actual_usd"] is not None and Decimal(a["actual_usd"]) > Decimal(a["reserved_usd"])
            for a in tx.rows("runtime_attempts", run_id=run_id)
        )

    def start_attempt(
        self,
        run_id: str,
        decision_id: str,
        attempt_id: str,
        reserved_usd: Decimal,
        budget_usd: Decimal,
        *,
        request: dict[str, Any] | None = None,
    ) -> AttemptRecord:
        reserve, cap = _money(reserved_usd), _money(budget_usd)
        with self._transaction(run_id) as tx:
            run = self._require(tx, run_id)
            if run["status"] in {"COMPLETED", "FAILED"}:
                raise LedgerError("TERMINAL_RUN_IMMUTABLE")
            if (run.get("pause") or {}).get("category") == "EPISTEMIC":
                raise LedgerError("EPISTEMIC_RUN_PAUSED")
            decision = tx.get("runtime_decisions", run_id=run_id, decision_id=decision_id)
            if decision is None:
                raise LedgerError("DECISION_NOT_PREPARED")
            if run["budget_cap"] is not None and Decimal(run["budget_cap"]) != cap:
                raise LedgerError("RUN_BUDGET_MISMATCH")
            if self._has_overrun(tx, run_id):
                raise LedgerError("BUDGET_OVERRUN")
            prior = tx.get("runtime_attempts", run_id=run_id, attempt_id=attempt_id)
            if prior:
                if (
                    prior["decision_id"] != decision_id
                    or Decimal(prior["reserved_usd"]) != reserve
                    or prior.get("request") != json_value(request)
                ):
                    raise LedgerError("ATTEMPT_ID_MISMATCH")
                # Reserving grants a single dispatch lease. Recovery reads the
                # existing attempt; it never obtains a second dispatch grant.
                raise LedgerError("ATTEMPT_ALREADY_RESERVED")
            if any(
                a["actual_usd"] is None
                for a in tx.rows("runtime_attempts", run_id=run_id, decision_id=decision_id)
            ):
                raise LedgerError("BILLING_UNCERTAIN")
            if decision["status"] == "ACCEPTED":
                raise LedgerError("DECISION_ALREADY_ACCEPTED")
            totals = self._totals(tx, run_id, run)
            if totals["committed"] + totals["reserved"] + reserve > cap:
                raise LedgerError("BUDGET_EXCEEDED")
            run["budget_cap"] = cap
            tx.put("runtime_runs", run)
            row: dict[str, Any] = dict(
                run_id=run_id,
                attempt_id=attempt_id,
                decision_id=decision_id,
                reserved_usd=reserve,
                actual_usd=None,
                outcome="DISPATCH_RESERVED",
                response=None,
                error_code="",
                request=json_value(request),
            )
            tx.put("runtime_attempts", row)
            tx.journal(
                run_id,
                "ATTEMPT_RESERVED",
                {
                    "attempt_id": attempt_id,
                    "decision_id": decision_id,
                    "reserved_usd": str(reserve),
                    "request": json_value(request),
                },
            )
            return AttemptRecord(**row)

    def complete_attempt(
        self,
        run_id: str,
        attempt_id: str,
        *,
        response: dict[str, Any] | None,
        actual_usd: Decimal | None,
        outcome: str,
        error_code: str = "",
    ) -> AttemptRecord:
        actual = _money(actual_usd) if actual_usd is not None else None
        frozen = json_value(response)
        with self._transaction(run_id) as tx:
            run = self._require(tx, run_id)
            row = tx.get("runtime_attempts", run_id=run_id, attempt_id=attempt_id)
            if row is None:
                raise LedgerError("ATTEMPT_NOT_RESERVED")
            wanted = dict(
                response=frozen,
                actual_usd=actual,
                outcome=outcome if actual is not None else "BILLING_UNCERTAIN",
                error_code=error_code,
            )
            if row["outcome"] != "DISPATCH_RESERVED":
                if any(row[k] != v for k, v in wanted.items()):
                    raise LedgerError("ATTEMPT_COMPLETION_MISMATCH")
                return AttemptRecord(**row)
            row.update(wanted)
            tx.put("runtime_attempts", row)
            decision = tx.get("runtime_decisions", run_id=run_id, decision_id=row["decision_id"])
            assert decision is not None
            if frozen is not None:
                decision.update(response=frozen, status="RESPONSE_RECORDED")
                tx.put("runtime_decisions", decision)
            totals = self._totals(tx, run_id, run)
            code = (
                "BILLING_UNCERTAIN"
                if actual is None
                else "BUDGET_EXCEEDED"
                if totals["committed"] + totals["reserved"] > totals["cap"]
                else "BUDGET_OVERRUN"
                if actual > Decimal(row["reserved_usd"])
                else None
            )
            if code is not None:
                run.update(
                    status="PAUSED",
                    pause={
                        "category": "BUDGET",
                        "code": code,
                    },
                )
                tx.put("runtime_runs", run)
            tx.journal(run_id, "ATTEMPT_COMPLETED", {"attempt_id": attempt_id, **wanted})
            return AttemptRecord(**row)

    def reconcile_attempt(
        self,
        run_id: str,
        attempt_id: str,
        *,
        actual_usd: Decimal,
        evidence_ref: str,
    ) -> AttemptRecord:
        """Resolve uncertain billing only with explicit auditable evidence.

        This does not fabricate a response or resume the paused financial
        barrier. The prior reservation and outcome remain in the journal.
        """
        amount = _money(actual_usd)
        if not evidence_ref.strip():
            raise LedgerError("RECONCILIATION_EVIDENCE_REQUIRED")
        with self._transaction(run_id) as tx:
            run = self._require(tx, run_id)
            row = tx.get("runtime_attempts", run_id=run_id, attempt_id=attempt_id)
            if row is None or row["actual_usd"] is not None:
                raise LedgerError("ATTEMPT_NOT_UNCERTAIN")
            previous = json_value(row)
            row.update(actual_usd=amount, outcome="RECONCILED", error_code="")
            tx.put("runtime_attempts", row)
            totals = self._totals(tx, run_id, run)
            if (run.get("pause") or {}).get("category") == "BUDGET":
                code = (
                    "BUDGET_EXCEEDED"
                    if totals["committed"] + totals["reserved"] > totals["cap"]
                    else "BUDGET_OVERRUN"
                    if self._has_overrun(tx, run_id)
                    else "BILLING_UNCERTAIN"
                    if any(
                        a["actual_usd"] is None for a in tx.rows("runtime_attempts", run_id=run_id)
                    )
                    else "BILLING_RECONCILED"
                )
                run.update(status="PAUSED", pause={**run["pause"], "code": code})
                tx.put("runtime_runs", run)
            tx.journal(
                run_id,
                "ATTEMPT_RECONCILED",
                {
                    "attempt_id": attempt_id,
                    "previous": previous,
                    "actual_usd": str(amount),
                    "evidence_ref": evidence_ref,
                },
            )
            return AttemptRecord(**row)

    def list_attempts(self, run_id: str, decision_id: str) -> list[AttemptRecord]:
        with self._transaction(run_id, readonly=True) as tx:
            return [
                AttemptRecord(**r)
                for r in tx.rows("runtime_attempts", run_id=run_id, decision_id=decision_id)
            ]

    def accept_decision(
        self,
        run_id: str,
        decision_id: str,
        *,
        result: dict[str, Any],
        attempt_id: str | None = None,
    ) -> DecisionRecord:
        frozen = json_value(result)
        with self._transaction(run_id) as tx:
            self._require(tx, run_id)
            if self._has_overrun(tx, run_id):
                raise LedgerError("BUDGET_OVERRUN")
            row = tx.get("runtime_decisions", run_id=run_id, decision_id=decision_id)
            if row is None:
                raise LedgerError("DECISION_NOT_PREPARED")
            if row["status"] == "ACCEPTED":
                if row["result"] != frozen or row["accepted_attempt_id"] != attempt_id:
                    raise LedgerError("DECISION_ACCEPTANCE_MISMATCH")
                return DecisionRecord(**row)
            if attempt_id is not None:
                attempt = tx.get("runtime_attempts", run_id=run_id, attempt_id=attempt_id)
                if (
                    attempt is None
                    or attempt["decision_id"] != decision_id
                    or attempt["actual_usd"] is None
                ):
                    raise LedgerError("UNRESOLVED_DECISION_ATTEMPT")
            row.update(status="ACCEPTED", result=frozen, accepted_attempt_id=attempt_id)
            tx.put("runtime_decisions", row)
            tx.journal(
                run_id, "DECISION_ACCEPTED", {"decision_id": decision_id, "attempt_id": attempt_id}
            )
            return DecisionRecord(**row)

    def journal(self, run_id: str) -> list[dict[str, Any]]:
        with self._transaction(run_id, readonly=True) as tx:
            return tx.rows("runtime_journal", run_id=run_id)


class _MemoryTransaction:
    def __init__(self, owner: InMemoryRunStore, readonly: bool = False) -> None:
        self.owner = owner
        self.readonly = readonly
        self.undo: list[Callable[[], Any]] = []

    def _remember(self, action: Callable[[], Any]) -> None:
        if self.readonly:
            raise LedgerError("READ_ONLY_TRANSACTION")
        self.undo.append(action)

    def get(self, name: str, **key: Any) -> dict[str, Any] | None:
        if name in _KEYS and set(key) == set(_KEYS[name]):
            position = self.owner._indexes.get(name, {}).get(tuple(key[k] for k in _KEYS[name]))
            return deepcopy(self.owner._rows[name][position]) if position is not None else None
        rows = self.rows(name, **key)
        return rows[0] if rows else None

    def rows(self, name: str, **key: Any) -> list[dict[str, Any]]:
        return deepcopy(
            [r for r in self.owner._rows.get(name, []) if all(r[k] == v for k, v in key.items())]
        )

    def put(self, name: str, row: dict[str, Any]) -> None:
        rows = self.owner._rows.setdefault(name, [])
        index = self.owner._indexes.setdefault(name, {})
        key = tuple(row[k] for k in _KEYS[name])
        position = index.get(key)
        if position is not None:
            old = rows[position]
            self._remember(lambda: rows.__setitem__(position, old))
            rows[position] = deepcopy(row)
        else:
            self._remember(lambda: (rows.pop(), index.pop(key)))
            index[key] = len(rows)
            rows.append(deepcopy(row))

    def journal(self, run_id: str, kind: str, payload: dict[str, Any]) -> None:
        rows = self.owner._rows.setdefault("runtime_journal", [])
        self._remember(lambda: rows.pop())
        rows.append(
            dict(run_id=run_id, kind=kind, payload=json_value(payload), at_utc=datetime.now(UTC))
        )

    def events(self, run_id: str) -> list[Event]:
        return self.owner._event_store.events(run_id)

    def tip(self, run_id: str) -> tuple[int, str]:
        return self.owner._event_store.tip(run_id)

    def append(
        self, events: list[Event], expected_seq: int, outbox: list[dict[str, Any]]
    ) -> list[Event]:
        if events:
            run_id = events[0].run_id
            rows = self.owner._event_store._events.setdefault(run_id, [])
            length = len(rows)
            self._remember(lambda: rows.__delitem__(slice(length, None)))
        committed = self.owner._event_store.append_batch(events, expected_seq)
        if outbox:
            emitted = self.owner._rows.setdefault("outbox", [])
            count = len(emitted)
            self._remember(lambda: emitted.__delitem__(slice(count, None)))
            emitted.extend(deepcopy(outbox))
        return committed

    def observations(self, observations: dict[str, Any], assessments: list[dict[str, Any]]) -> None:
        from spx_research.epistemics.store import AssessmentRecord
        from spx_research.epistemics.types import Atom

        led = self.owner.observation_ledger
        delivery_count, assessment_count = len(led._deliveries), len(led._assessments)
        if observations.get("deliveries"):
            self._remember(lambda: led._deliveries.__delitem__(slice(delivery_count, None)))
        if assessments:
            self._remember(lambda: led._assessments.__delitem__(slice(assessment_count, None)))
        for raw in observations.get("atoms", []):
            atom = Atom(**_typed_atom(raw))
            previous = led._atoms.get(atom.atom_id)
            if previous is not None and previous != atom:
                raise HarnessError("ATOM_IDENTITY_CONFLICT")
            if previous is None:

                def forget_atom(atom_id: str = atom.atom_id) -> None:
                    led._atoms.pop(atom_id, None)

                self._remember(forget_atom)
            led.put_atom(atom)
        for raw in observations.get("deliveries", []):
            led.deliver(
                raw["run_id"],
                raw["branch_id"],
                raw["actor_id"],
                raw["atom_id"],
                _datetime(raw["delivered_at"]),
            )
        for raw in assessments:
            row = dict(raw)
            row["accepted_at"] = _datetime(row["accepted_at"])
            row["premise_atom_ids"] = tuple(row["premise_atom_ids"])
            led.put_assessment(AssessmentRecord(**row))

    def create_legacy_run(self, run_id: str, manifest: dict[str, Any]) -> None:
        pass


class InMemoryRunStore(RunStore):
    observation_ledger: InMemoryObservationLedger

    def __init__(self) -> None:
        self._lock = RLock()
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._indexes: dict[str, dict[tuple[Any, ...], int]] = {}
        self._event_store = InMemoryEventStore()
        self.observation_ledger = InMemoryObservationLedger()

    @contextmanager
    def _transaction(self, run_id: str, *, readonly: bool = False) -> Iterator[_Transaction]:
        with self._lock:
            transaction = _MemoryTransaction(self, readonly)
            try:
                yield transaction
            except BaseException:
                for undo in reversed(transaction.undo):
                    undo()
                raise


def _datetime(value: Any) -> datetime:
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise LedgerError("NAIVE_RUNTIME_TIMESTAMP")
    return result


def _typed_atom(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    for key in ("published_at", "available_at", "subject_at"):
        row[key] = _datetime(row[key])
    for key in ("recipients", "dependencies"):
        if key in row:
            row[key] = tuple(row[key])
    return row


class _PostgresTransaction:
    def __init__(self, conn: sa.engine.Connection) -> None:
        self.conn = conn

    def rows(self, name: str, **key: Any) -> list[dict[str, Any]]:
        table = S.metadata.tables[name]
        statement = sa.select(table).where(*(table.c[k] == v for k, v in key.items()))
        if "id" in table.c:
            statement = statement.order_by(table.c.id)
        elif "attempt_id" in table.c:
            statement = statement.order_by(table.c.attempt_id)
        return [dict(r) for r in self.conn.execute(statement).mappings()]

    def get(self, name: str, **key: Any) -> dict[str, Any] | None:
        rows = self.rows(name, **key)
        return rows[0] if rows else None

    def put(self, name: str, row: dict[str, Any]) -> None:
        table = S.metadata.tables[name]
        statement = pg_insert(table).values(**row)
        self.conn.execute(
            statement.on_conflict_do_update(
                index_elements=list(_KEYS[name]),
                set_={k: v for k, v in row.items() if k not in _KEYS[name]},
            )
        )

    def journal(self, run_id: str, kind: str, payload: dict[str, Any]) -> None:
        self.conn.execute(
            S.runtime_journal.insert().values(
                run_id=run_id, kind=kind, payload=json_value(payload), at_utc=datetime.now(UTC)
            )
        )

    def events(self, run_id: str) -> list[Event]:
        rows = self.conn.execute(
            sa.select(S.events).where(S.events.c.run_id == run_id).order_by(S.events.c.seq)
        ).mappings()
        return [Event(**dict(r)) for r in rows]

    def tip(self, run_id: str) -> tuple[int, str]:
        row = self.conn.execute(
            sa.select(S.events.c.seq, S.events.c.event_hash)
            .where(S.events.c.run_id == run_id)
            .order_by(S.events.c.seq.desc())
            .limit(1)
        ).first()
        return (row.seq, row.event_hash) if row else (0, "genesis")

    def append(
        self, events: list[Event], expected_seq: int, outbox: list[dict[str, Any]]
    ) -> list[Event]:
        return PostgresEventStore.append_in_transaction(self.conn, events, expected_seq, outbox)

    def create_legacy_run(self, run_id: str, manifest: dict[str, Any]) -> None:
        profile = manifest.get("profile", {})
        profile_id = (
            manifest.get("profile_id")
            or (profile.get("profile_id") if isinstance(profile, dict) else None)
            or "runtime-v2"
        )
        self.conn.execute(
            pg_insert(S.runs)
            .values(
                run_id=run_id,
                profile_id=profile_id,
                manifest=manifest,
                status="RUNNING",
                started_at_utc=datetime.now(UTC),
            )
            .on_conflict_do_nothing()
        )

    def observations(self, observations: dict[str, Any], assessments: list[dict[str, Any]]) -> None:
        for raw in sorted(observations.get("atoms", []), key=lambda a: a["atom_id"]):
            row = _typed_atom(raw)
            self.conn.execute(
                sa.text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                {"key": f"atom:{row['atom_id']}"},
            )
            old = self.get("evidence_atoms", atom_id=row["atom_id"])
            if old is not None:
                if record_hash(old) != record_hash(row):
                    raise HarnessError("ATOM_IDENTITY_CONFLICT")
            else:
                self.conn.execute(S.atoms.insert().values(**row))
        for raw in observations.get("deliveries", []):
            row = dict(raw)
            row["delivered_at"] = _datetime(row["delivered_at"])
            atom = self.get("evidence_atoms", atom_id=row["atom_id"])
            if atom is None or row["delivered_at"] < atom["available_at"]:
                raise LedgerError("DELIVERY_BEFORE_AVAILABILITY")
            if "PUBLIC" not in atom["recipients"] and row["actor_id"] not in atom["recipients"]:
                raise LedgerError("WRONG_RECIPIENT")
            self.conn.execute(
                pg_insert(S.deliveries)
                .values(**row)
                .on_conflict_do_nothing(
                    index_elements=["run_id", "branch_id", "actor_id", "atom_id"]
                )
            )
        for raw in assessments:
            row = dict(raw)
            row["accepted_at"] = _datetime(row["accepted_at"])
            key = {
                k: row[k] for k in ("run_id", "branch_id", "actor_id", "decision_token", "topic")
            }
            old = self.get("assessments", **key)
            if old is not None:
                old.pop("id")
                if record_hash(old) != record_hash(row):
                    raise LedgerError("ASSESSMENT_CONTENT_MISMATCH")
            else:
                self.conn.execute(S.assessments.insert().values(**row))


class PostgresRunStore(RunStore):
    def __init__(self, engine: sa.engine.Engine) -> None:
        self._engine = engine
        self.observation_ledger = PostgresObservationLedger(engine)

    @contextmanager
    def _transaction(self, run_id: str, *, readonly: bool = False) -> Iterator[_Transaction]:
        with self._engine.begin() as conn:
            _scope(conn, run_id)
            conn.execute(sa.text("SELECT pg_advisory_xact_lock(hashtext(:r))"), {"r": run_id})
            yield _PostgresTransaction(conn)
