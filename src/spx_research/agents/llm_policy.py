"""LLM policy adapter (M4-05): Policy interface over the TKH pipeline.

``decide`` runs the LangGraph barrier — packet build, gateway call, witness
validation, token resolution — and returns an internal ``Proposal`` that the
engine re-validates. Terminal failure raises ``PolicyError``; the scheduler
pauses the barrier rather than inventing an action.
"""

from __future__ import annotations

from spx_research.agents.graphs import PolicyDeps, run_decision
from spx_research.engine.policy import DecisionContext, PolicyError, Proposal


class LLMPolicy:
    def __init__(self, deps: PolicyDeps) -> None:
        self.deps = deps

    def decide(self, ctx: DecisionContext) -> Proposal:
        proposal, _witness = run_decision(self.deps, ctx)
        return proposal


__all__ = ["LLMPolicy", "PolicyDeps", "PolicyError"]
