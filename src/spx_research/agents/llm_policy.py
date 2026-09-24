"""Policy adapter: proposals and observation/assessment writes stay staged."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from spx_research.agents.graphs import PolicyDeps, _atom_restore, logical_decision_id, run_decision
from spx_research.engine.policy import DecisionContext, PolicyError, Proposal
from spx_research.epistemics.store import AssessmentRecord
from spx_research.epistemics.types import Delivery


class LLMPolicy:
    def __init__(self, deps: PolicyDeps) -> None:
        self.deps = deps
        self.last_witness: dict[str, Any] | None = None
        self._pending: dict[str, list[AssessmentRecord]] = {}
        self._observations: dict[str, dict[str, Any]] = {}

    def decision_id(self, ctx: DecisionContext) -> str:
        return logical_decision_id(self.deps, ctx)

    def decide(self, ctx: DecisionContext) -> Proposal:
        proposal, witness, pending = run_decision(self.deps, ctx)
        self.last_witness = witness or None
        key = self.decision_id(ctx)
        self._pending[key] = pending
        prepared = self.deps.prepared_cache.get(key)
        if prepared is None and self.deps.runtime is not None:
            prepared = self.deps.runtime.load_decision(ctx.run_id, key).request
        if prepared is None:
            taped = self.deps.tape.decision(key)
            prepared = taped.prepared if taped is not None else None
        self._observations[key] = prepared["observations"] if prepared else {}
        return proposal

    def staged_assessments(self, ctx: DecisionContext) -> list[AssessmentRecord]:
        return list(self._pending.get(self.decision_id(ctx), []))

    def staged_observations(self, ctx: DecisionContext) -> dict[str, Any]:
        raw = self._observations.get(self.decision_id(ctx), {})
        return {
            "atoms": [_atom_restore(a) for a in raw.get("atoms", [])],
            "deliveries": [
                Delivery(**{**d, "delivered_at": datetime.fromisoformat(d["delivered_at"])})
                for d in raw.get("deliveries", [])
            ],
        }

    def finalize(self, ctx: DecisionContext, *, persisted: bool = True) -> None:
        if not persisted:
            self.commit(ctx)
            return
        self.discard(ctx)

    def commit(self, ctx: DecisionContext) -> None:
        # Compatibility for isolated/offline policy users. The engine's durable
        # barrier uses staged_* and finalize instead of these individual writes.
        observations = self.staged_observations(ctx)
        for atom in observations["atoms"]:
            self.deps.ledger.put_atom(atom)
        for d in observations["deliveries"]:
            self.deps.ledger.deliver(d.run_id, d.branch_id, d.actor_id, d.atom_id, d.delivered_at)
        for rec in self.staged_assessments(ctx):
            self.deps.ledger.put_assessment(rec)
        self.discard(ctx)

    def discard(self, ctx: DecisionContext) -> None:
        key = self.decision_id(ctx)
        self._pending.pop(key, None)
        self._observations.pop(key, None)


__all__ = ["LLMPolicy", "PolicyDeps", "PolicyError"]
