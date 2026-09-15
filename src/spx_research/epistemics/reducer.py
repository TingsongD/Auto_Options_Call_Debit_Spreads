"""Verified knowledge-state reducer (H-02).

Folds an actor's observation deliveries into a ``BeliefState``: verified facts
(atoms that pass availability/dependency/vocabulary checks at ``as_of``),
accepted typed assessments carried forward, and typed unknowns. Prefix-only:
nothing with ``delivered_at > as_of`` or ``available_at > as_of`` enters.
"""

from __future__ import annotations

from dataclasses import dataclass

from spx_research.epistemics.harness import Harness, digest
from spx_research.epistemics.store import AssessmentRecord, ObservationLedger
from spx_research.epistemics.types import Atom, Context

BASE_UNKNOWNS = ("UNKNOWN_FUTURE_POLICY_PATH", "UNKNOWN_FUTURE_PRICE_PATH")


@dataclass(frozen=True)
class BeliefState:
    """What one actor is entitled to believe at a decision barrier."""

    actor_id: str
    facts: tuple[Atom, ...]  # sorted by atom_id
    assessments: tuple[AssessmentRecord, ...]
    unknowns: tuple[str, ...]
    belief_hash: str


def reduce_belief(
    ledger: ObservationLedger,
    harness: Harness,
    ctx: Context,
    extra_unknowns: tuple[str, ...] = (),
) -> BeliefState:
    """Verified prefix-only belief state for one actor at ``ctx.as_of``."""
    atoms = ledger.atoms()
    observed: list[Atom] = []
    for d in ledger.deliveries(ctx.run_id, ctx.branch_id, ctx.actor_id):
        if d.delivered_at > ctx.as_of:
            continue  # prefix-only
        a = atoms.get(d.atom_id)
        if a is None:
            continue
        if d.delivered_at < a.available_at:
            continue  # inconsistent delivery: never observed
        harness.check_atom(a.atom_id, atoms, ctx)  # verifies availability+deps
        observed.append(a)
    observed.sort(key=lambda a: a.atom_id)
    assessments = tuple(
        sorted(
            ledger.assessments(ctx.run_id, ctx.branch_id, ctx.actor_id),
            key=lambda r: (r.accepted_at, r.topic),
        )
    )
    unknowns = tuple(sorted(set(BASE_UNKNOWNS) | set(extra_unknowns)))
    blob = {
        "facts": [a.atom_id for a in observed],
        "assessments": [[r.topic, r.assessment, r.confidence_label] for r in assessments],
        "unknowns": list(unknowns),
    }
    return BeliefState(ctx.actor_id, tuple(observed), assessments, unknowns, digest(blob))
