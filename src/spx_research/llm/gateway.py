"""Model gateways (M4-01): mock, tape replay, and the protocol both share.

MockGateway emits a deterministic structurally-valid proposal (defaults to the
first WAIT/NO_CHANGE choice, or a scripted override) so the full pipeline is
exercised without a provider. TapeGateway replays recorded responses keyed by
request hash — a miss is an error, never a silent provider call.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

from spx_research.llm.types import ModelError, ModelRequest, ModelResponse

if TYPE_CHECKING:
    from spx_research.llm.tape import DecisionTape


class ModelGateway(Protocol):
    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse: ...


class MockGateway:
    """Deterministic in-process policy for tests and CI (no network)."""

    def __init__(self, prefer_kind: str | None = None) -> None:
        self.prefer_kind = prefer_kind
        self.calls = 0

    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse:
        self.calls += 1
        menu = req.packet["action_menu"]
        choice = next((m for m in menu if m["kind"] == self.prefer_kind), menu[0])
        cited = list(choice["required_premise_tokens"])
        for p in req.packet["premises"]:
            if p["token"] not in cited:
                cited.append(p["token"])
                break  # one supporting premise beyond the required set
        parsed = {
            "schema_version": "2.0",
            "actor_role": req.packet["actor_role"],
            "decision_token": req.packet["decision_token"],
            "packet_token": req.packet["packet_token"],
            "prior_belief_token": req.packet["prior_belief_token"],
            "action_id": choice["action_id"],
            "premise_tokens": cited,
            "assessment_updates": [],
            "reason_codes": ["MAINTAIN_THESIS"],
            "uncertainty_codes": ["NONE_IDENTIFIED"],
            "confidence_label": "LOW",
        }
        return ModelResponse(
            request_hash=req.request_hash(),
            text=json.dumps(parsed, sort_keys=True),
            parsed=parsed,
            model_id="mock-1",
            input_tokens=0,
            output_tokens=0,
            cost_usd=Decimal("0"),
        )


class TapeGateway:
    """Replay-only gateway over a decision tape (M4-02 / T45)."""

    def __init__(self, tape: DecisionTape) -> None:
        self.tape = tape

    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse:
        rec = self.tape.lookup(req.request_hash())
        if rec is None:
            raise ModelError("TAPE_MISS")
        return rec.response
