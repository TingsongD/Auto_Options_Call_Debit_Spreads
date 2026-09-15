"""Persistent observation ledger (H-01/H-02).

Atoms, deliveries, accepted assessments, and private incidents are stored per
(run, branch, actor). ``deliver`` fails closed on any delivery recorded before
the atom's availability; ``incidents`` hold rejected proposals privately — only
their fixed codes may enter retry context, never the leaked prose.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from spx_research.epistemics.types import Atom, Delivery, HarnessError, aware_check


@dataclass(frozen=True)
class AssessmentRecord:
    """A typed assessment accepted into an actor's belief state."""

    actor_id: str
    topic: str
    assessment: str
    confidence_label: str
    premise_atom_ids: tuple[str, ...]
    accepted_at: datetime
    decision_token: str
    run_id: str = ""
    branch_id: str = ""


@dataclass(frozen=True)
class Incident:
    """Private quarantine record for a rejected proposal (never model-facing)."""

    incident_id: str
    run_id: str
    branch_id: str
    actor_id: str
    code: str
    rejected_payload: dict[str, Any]  # private; must not re-enter prompts
    at: datetime


class ObservationLedger(Protocol):
    def put_atom(self, atom: Atom) -> None: ...
    def atoms(self) -> dict[str, Atom]: ...
    def deliver(
        self, run_id: str, branch_id: str, actor_id: str, atom_id: str, at: datetime
    ) -> Delivery: ...
    def deliveries(self, run_id: str, branch_id: str, actor_id: str) -> list[Delivery]: ...
    def put_assessment(self, rec: AssessmentRecord) -> None: ...
    def assessments(self, run_id: str, branch_id: str, actor_id: str) -> list[AssessmentRecord]: ...
    def quarantine(self, incident: Incident) -> None: ...
    def next_incident_id(self, run_id: str) -> str: ...
    def incidents(self, run_id: str) -> list[Incident]: ...


class InMemoryObservationLedger:
    """Deterministic store used by tests/replay; Postgres adapter lands with M5."""

    def __init__(self) -> None:
        self._atoms: dict[str, Atom] = {}
        self._deliveries: list[Delivery] = []
        self._assessments: list[AssessmentRecord] = []
        self._incidents: list[Incident] = []
        self._incident_seq: dict[str, int] = {}

    def put_atom(self, atom: Atom) -> None:
        for t in (atom.published_at, atom.available_at, atom.subject_at):
            aware_check(t)
        self._atoms[atom.atom_id] = atom

    def atoms(self) -> dict[str, Atom]:
        return dict(self._atoms)

    def deliver(
        self, run_id: str, branch_id: str, actor_id: str, atom_id: str, at: datetime
    ) -> Delivery:
        aware_check(at)
        a = self._atoms.get(atom_id)
        if a is None:
            raise HarnessError("MISSING_OBSERVATION")
        if at < a.available_at:
            raise HarnessError("DELIVERY_BEFORE_AVAILABILITY")  # fail closed
        if "PUBLIC" not in a.recipients and actor_id not in a.recipients:
            raise HarnessError("WRONG_RECIPIENT")
        # First-wins: redelivering the same atom to the same recipient returns
        # the original delivery record (parity with the Postgres ON CONFLICT
        # path); the unique key is (run, branch, actor, atom).
        for d in self._deliveries:
            if (d.run_id, d.branch_id, d.actor_id, d.atom_id) == (
                run_id,
                branch_id,
                actor_id,
                atom_id,
            ):
                return d
        d = Delivery(run_id, branch_id, actor_id, atom_id, at)
        self._deliveries.append(d)
        return d

    def deliveries(self, run_id: str, branch_id: str, actor_id: str) -> list[Delivery]:
        return [
            d
            for d in self._deliveries
            if (d.run_id, d.branch_id, d.actor_id) == (run_id, branch_id, actor_id)
        ]

    def put_assessment(self, rec: AssessmentRecord) -> None:
        aware_check(rec.accepted_at)
        # Idempotent on the natural key — retries/replays re-put the same
        # record without duplicating rows (parity with Postgres).
        key = (rec.run_id, rec.branch_id, rec.actor_id, rec.decision_token, rec.topic)
        if any(
            (r.run_id, r.branch_id, r.actor_id, r.decision_token, r.topic) == key
            for r in self._assessments
        ):
            return
        self._assessments.append(rec)

    def assessments(self, run_id: str, branch_id: str, actor_id: str) -> list[AssessmentRecord]:
        # Same scoping as PostgresObservationLedger: run + branch + actor.
        return [
            r
            for r in self._assessments
            if (r.run_id, r.branch_id, r.actor_id) == (run_id, branch_id, actor_id)
        ]

    def quarantine(self, incident: Incident) -> None:
        self._incident_seq[incident.run_id] = self._incident_seq.get(incident.run_id, 0) + 1
        self._incidents.append(incident)

    def next_incident_id(self, run_id: str) -> str:
        # Per-run sequence, same shape as PostgresObservationLedger.
        return f"inc-{run_id}-{self._incident_seq.get(run_id, 0) + 1:04d}"

    def incidents(self, run_id: str) -> list[Incident]:
        return [i for i in self._incidents if i.run_id == run_id]
