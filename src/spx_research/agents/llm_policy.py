"""LLM policy adapter (M4-05): Policy interface over the TKH pipeline.

``decide`` runs the LangGraph barrier — packet build, gateway call, witness
validation, token resolution — and returns an internal ``Proposal`` that the
engine re-validates. Terminal failure raises ``PolicyError``; the scheduler
pauses the barrier rather than inventing an action.

Assessment writes are staged at ``decide`` and persisted only on ``commit``
— the engine may reject a structurally valid proposal, and a rejected
decision must leave no trace in the actor's belief state.
"""

from __future__ import annotations

from typing import Any

from spx_research.agents.graphs import PolicyDeps, run_decision
from spx_research.engine.policy import DecisionContext, PolicyError, Proposal
from spx_research.epistemics.store import AssessmentRecord


class LLMPolicy:
    def __init__(self, deps: PolicyDeps) -> None:
        self.deps = deps
        self.last_witness: dict[str, Any] | None = None
        self._pending: dict[tuple[str, str], list[AssessmentRecord]] = {}

    @staticmethod
    def _key(ctx: DecisionContext) -> tuple[str, str]:
        return (ctx.actor_id, ctx.as_of_utc.isoformat())

    def decide(self, ctx: DecisionContext) -> Proposal:
        proposal, witness, pending = run_decision(self.deps, ctx)
        self.last_witness = witness or None
        self._pending[self._key(ctx)] = pending
        return proposal

    def commit(self, ctx: DecisionContext) -> None:
        for rec in self._pending.pop(self._key(ctx), []):
            self.deps.ledger.put_assessment(rec)

    def discard(self, ctx: DecisionContext) -> None:
        self._pending.pop(self._key(ctx), None)


__all__ = ["LLMPolicy", "PolicyDeps", "PolicyError"]
