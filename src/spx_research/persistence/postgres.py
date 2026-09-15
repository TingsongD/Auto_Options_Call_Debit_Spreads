"""PostgreSQL stores (M5): durable event ledger + observation ledger.

Single-writer is enforced by a per-run ``pg_advisory_xact_lock`` inside the
append transaction: a concurrent writer serializes behind the lock and then
fails ``expected_seq`` validation, so only one commit can advance a run's
log. Outbox rows are inserted in the same transaction — consumers observe an
event and its side-effects atomically (H-08, T40/T41).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import sqlalchemy as sa

from spx_research.domain.state import Event
from spx_research.epistemics.types import Atom, Delivery, HarnessError, aware_check
from spx_research.persistence import schema as S
from spx_research.persistence.events import LedgerError, payload_hash


def _scope(conn: sa.engine.Connection, run_id: str) -> None:
    """Set the RLS run scope for this transaction (no-op for table owners)."""
    conn.execute(sa.text("SELECT set_config('app.run_id', :r, true)"), {"r": run_id})


class PostgresEventStore:
    """Hash-chained append-only event log on Postgres."""

    def __init__(self, engine: sa.engine.Engine) -> None:
        self._engine = engine

    def append(
        self,
        event: Event,
        expected_seq: int,
        outbox: list[dict[str, Any]] | None = None,
    ) -> Event:
        with self._engine.begin() as conn:
            _scope(conn, event.run_id)
            # Serialize writers on this run for the duration of the txn.
            conn.execute(
                sa.text("SELECT pg_advisory_xact_lock(hashtext(:r))"),
                {"r": event.run_id},
            )
            row = conn.execute(
                sa.select(S.events.c.seq, S.events.c.event_hash)
                .where(S.events.c.run_id == event.run_id)
                .order_by(S.events.c.seq.desc())
                .limit(1)
            ).first()
            if row:
                seq, prev_hash = row.seq, row.event_hash
            else:
                seq, prev_hash = 0, "genesis"
            if expected_seq != seq:
                raise LedgerError("SEQUENCE_MISMATCH")
            e = event.with_hashes(payload_hash(event.payload), prev_hash)
            if e.seq != seq + 1:
                raise LedgerError("SEQUENCE_MISMATCH")
            try:
                # Auto-register the run row on first append (FK target).
                conn.execute(
                    sa.text(
                        "INSERT INTO runs (run_id, profile_id, status, started_at_utc)"
                        " VALUES (:r, :p, 'RUNNING', :t) ON CONFLICT (run_id) DO NOTHING"
                    ),
                    {
                        "r": event.run_id,
                        "p": str(event.payload.get("profile_id", "unknown")),
                        "t": event.sim_time_utc,
                    },
                )
                conn.execute(
                    S.events.insert().values(
                        run_id=e.run_id,
                        seq=e.seq,
                        sim_time_utc=e.sim_time_utc,
                        phase=e.phase,
                        type=e.type,
                        payload=e.payload,
                        payload_hash=e.payload_hash,
                        previous_hash=e.previous_hash,
                        event_hash=e.event_hash,
                    )
                )
            except sa.exc.IntegrityError as exc:
                raise LedgerError("SEQUENCE_MISMATCH") from exc
            for ob in outbox or []:
                conn.execute(
                    S.outbox.insert().values(
                        run_id=e.run_id,
                        event_seq=e.seq,
                        topic=ob["topic"],
                        payload=ob["payload"],
                    )
                )
            if e.type == "RUN_ENDED":
                # Terminal status: the event is committed in this txn, so the
                # run row's status transitions atomically with the ledger.
                conn.execute(
                    sa.text("UPDATE runs SET status='COMPLETED' WHERE run_id=:r"),
                    {"r": e.run_id},
                )
            return e

    def events(self, run_id: str) -> list[Event]:
        with self._engine.connect() as conn:
            _scope(conn, run_id)
            rows = conn.execute(
                sa.select(S.events).where(S.events.c.run_id == run_id).order_by(S.events.c.seq)
            ).all()
        return [
            Event(
                run_id=r.run_id,
                seq=r.seq,
                sim_time_utc=r.sim_time_utc,
                phase=r.phase,
                type=r.type,
                payload=dict(r.payload),
                payload_hash=r.payload_hash,
                previous_hash=r.previous_hash,
                event_hash=r.event_hash,
            )
            for r in rows
        ]

    def tip(self, run_id: str) -> tuple[int, str]:
        with self._engine.connect() as conn:
            _scope(conn, run_id)
            row = conn.execute(
                sa.select(S.events.c.seq, S.events.c.event_hash)
                .where(S.events.c.run_id == run_id)
                .order_by(S.events.c.seq.desc())
                .limit(1)
            ).first()
        if not row:
            return 0, "genesis"
        return row.seq, row.event_hash


class PostgresObservationLedger:
    """Durable evidence/delivery/assessment/incident store (H-08)."""

    def __init__(self, engine: sa.engine.Engine) -> None:
        self._engine = engine

    def put_atom(self, atom: Atom) -> None:
        for t in (atom.published_at, atom.available_at, atom.subject_at):
            aware_check(t)
        # Atoms are content-addressed: the same atom_id is the same atom, so
        # re-put (e.g. a macro atom delivered at many barriers) is a no-op.
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        with self._engine.begin() as conn:
            conn.execute(
                pg_insert(S.atoms)
                .values(
                    atom_id=atom.atom_id,
                    metric=atom.metric,
                    value=atom.value,
                    unit=atom.unit,
                    kind=atom.kind,
                    published_at=atom.published_at,
                    available_at=atom.available_at,
                    subject_at=atom.subject_at,
                    source_checked=atom.source_checked,
                    recipients=list(atom.recipients),
                    dependencies=list(atom.dependencies),
                    transform=atom.transform,
                )
                .on_conflict_do_nothing(index_elements=["atom_id"])
            )

    def atoms(self) -> dict[str, Atom]:
        with self._engine.connect() as conn:
            rows = conn.execute(sa.select(S.atoms)).all()
        return {
            r.atom_id: Atom(
                atom_id=r.atom_id,
                metric=r.metric,
                value=r.value,
                unit=r.unit,
                kind=r.kind,
                published_at=r.published_at,
                available_at=r.available_at,
                subject_at=r.subject_at,
                source_checked=r.source_checked,
                recipients=tuple(r.recipients),
                dependencies=tuple(r.dependencies),
                transform=r.transform,
            )
            for r in rows
        }

    def deliver(
        self, run_id: str, branch_id: str, actor_id: str, atom_id: str, at: datetime
    ) -> Delivery:
        """First-wins delivery: re-delivering returns the original record.

        All reads/validates happen inside the scoped transaction so RLS is
        active for every query and the check-then-insert is atomic.
        """
        aware_check(at)
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        with self._engine.begin() as conn:
            _scope(conn, run_id)
            arow = conn.execute(
                sa.select(
                    S.atoms.c.available_at,
                    S.atoms.c.recipients,
                ).where(S.atoms.c.atom_id == atom_id)
            ).first()
            if arow is None:
                raise HarnessError("MISSING_OBSERVATION")
            if at < arow.available_at:
                raise HarnessError("DELIVERY_BEFORE_AVAILABILITY")
            if "PUBLIC" not in arow.recipients and actor_id not in arow.recipients:
                raise HarnessError("WRONG_RECIPIENT")
            existing = conn.execute(
                sa.select(S.deliveries.c.delivered_at).where(
                    (S.deliveries.c.run_id == run_id)
                    & (S.deliveries.c.branch_id == branch_id)
                    & (S.deliveries.c.actor_id == actor_id)
                    & (S.deliveries.c.atom_id == atom_id)
                )
            ).first()
            if existing is not None:
                return Delivery(run_id, branch_id, actor_id, atom_id, existing.delivered_at)
            conn.execute(
                pg_insert(S.deliveries)
                .values(
                    run_id=run_id,
                    branch_id=branch_id,
                    actor_id=actor_id,
                    atom_id=atom_id,
                    delivered_at=at,
                )
                .on_conflict_do_nothing(
                    index_elements=["run_id", "branch_id", "actor_id", "atom_id"]
                )
            )
        return Delivery(run_id, branch_id, actor_id, atom_id, at)

    def deliveries(self, run_id: str, branch_id: str, actor_id: str) -> list[Delivery]:
        with self._engine.connect() as conn:
            _scope(conn, run_id)
            rows = conn.execute(
                sa.select(S.deliveries).where(
                    (S.deliveries.c.run_id == run_id)
                    & (S.deliveries.c.branch_id == branch_id)
                    & (S.deliveries.c.actor_id == actor_id)
                )
            ).all()
        return [
            Delivery(r.run_id, r.branch_id, r.actor_id, r.atom_id, r.delivered_at) for r in rows
        ]

    def put_assessment(self, rec: Any) -> None:
        aware_check(rec.accepted_at)
        if not rec.run_id:
            raise LedgerError("ASSESSMENT_RUN_SCOPE_REQUIRED")
        with self._engine.begin() as conn:
            _scope(conn, rec.run_id)
            exists = conn.execute(
                sa.select(S.assessments.c.id).where(
                    (S.assessments.c.run_id == rec.run_id)
                    & (S.assessments.c.branch_id == rec.branch_id)
                    & (S.assessments.c.actor_id == rec.actor_id)
                    & (S.assessments.c.decision_token == rec.decision_token)
                    & (S.assessments.c.topic == rec.topic)
                )
            ).first()
            if exists is not None:
                return  # natural-key dedupe: retries/replays are no-ops
            conn.execute(
                S.assessments.insert().values(
                    run_id=rec.run_id,
                    branch_id=rec.branch_id,
                    actor_id=rec.actor_id,
                    topic=rec.topic,
                    assessment=rec.assessment,
                    confidence_label=rec.confidence_label,
                    premise_atom_ids=list(rec.premise_atom_ids),
                    accepted_at=rec.accepted_at,
                    decision_token=rec.decision_token,
                )
            )

    def assessments(self, run_id: str, branch_id: str, actor_id: str) -> list[Any]:
        from spx_research.epistemics.store import AssessmentRecord

        with self._engine.connect() as conn:
            _scope(conn, run_id)
            rows = conn.execute(
                sa.select(S.assessments).where(
                    (S.assessments.c.run_id == run_id)
                    & (S.assessments.c.branch_id == branch_id)
                    & (S.assessments.c.actor_id == actor_id)
                )
            ).all()
        return [
            AssessmentRecord(
                actor_id=r.actor_id,
                topic=r.topic,
                assessment=r.assessment,
                confidence_label=r.confidence_label,
                premise_atom_ids=tuple(r.premise_atom_ids),
                accepted_at=r.accepted_at,
                decision_token=r.decision_token,
                run_id=r.run_id,
                branch_id=r.branch_id,
            )
            for r in rows
        ]

    def quarantine(self, incident: Any) -> None:
        with self._engine.begin() as conn:
            _scope(conn, incident.run_id)
            conn.execute(
                S.incidents.insert().values(
                    incident_id=incident.incident_id,
                    run_id=incident.run_id,
                    branch_id=incident.branch_id,
                    actor_id=incident.actor_id,
                    code=incident.code,
                    rejected_payload=dict(incident.rejected_payload),
                    at_utc=incident.at,
                )
            )

    def next_incident_id(self, run_id: str) -> str:
        with self._engine.connect() as conn:
            _scope(conn, run_id)
            n = conn.execute(
                sa.select(sa.func.count()).where(S.incidents.c.run_id == run_id)
            ).scalar_one()
        return f"inc-{run_id}-{n + 1:04d}"

    def incidents(self, run_id: str) -> list[Any]:
        from spx_research.epistemics.store import Incident

        with self._engine.connect() as conn:
            _scope(conn, run_id)
            rows = conn.execute(sa.select(S.incidents).where(S.incidents.c.run_id == run_id)).all()
        return [
            Incident(
                incident_id=r.incident_id,
                run_id=r.run_id,
                branch_id=r.branch_id,
                actor_id=r.actor_id,
                code=r.code,
                rejected_payload=dict(r.rejected_payload),
                at=r.at_utc,
            )
            for r in rows
        ]


def create_engine(dsn: str) -> sa.engine.Engine:
    return sa.create_engine(dsn, pool_pre_ping=True)
