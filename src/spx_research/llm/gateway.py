"""Model gateways (M4-01): mock, tape replay, and the protocol both share.

MockGateway emits a deterministic structurally-valid proposal (defaults to the
first WAIT/NO_CHANGE choice, or a scripted override) so the full pipeline is
exercised without a provider. TapeGateway replays recorded responses keyed by
request hash — a miss is an error, never a silent provider call.
"""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

from spx_research.llm.types import ModelError, ModelRequest, ModelResponse, response_dict

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
            "schema_version": req.packet["schema_version"],
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
            model_id=req.model_id,
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
        if self.tape.legacy or self.tape.incomplete:
            raise ModelError("LEGACY_OR_INCOMPLETE_TAPE_NOT_RESUMABLE")
        rec = self.tape.lookup(req.request_hash())
        if rec is None:
            raise ModelError("TAPE_MISS")
        return rec.response


class RecordedDecisionGateway:
    """Explicit new-run replay, matched to exact initial public requests.

    Source private context is never restored into the destination run. The
    destination rebuilds and validates its own lawful packet and menu. Only an
    accepted retry's fixed error code may differ from that exact initial request.
    """

    def __init__(self, tape: DecisionTape, source_sha256: str) -> None:
        if not tape.path.is_file() or tape.legacy or tape.incomplete or not len(tape):
            raise ModelError("REPLAY_REQUIRES_NONEMPTY_SEALED_V2_TAPE")
        self.source_sha256 = source_sha256
        self._records: dict[str, tuple[ModelRequest, ModelResponse, str]] = {}
        self.role_ids: dict[str, str] = {}
        namespaces: set[str] = set()
        output_limits: set[int] = set()
        for rec in tape.records():
            if rec.prepared is None or rec.result is None or not rec.decision_id:
                raise ModelError("REPLAY_MISSING_PREPARED_DECISION")
            initial = ModelRequest(**rec.prepared["model_request"])
            accepted = ModelRequest(**rec.result["request"])
            if (
                initial.retry_error_code
                or replace(accepted, retry_error_code="") != initial
                or accepted.request_hash() != rec.request_hash
                or accepted.body() != rec.request.get("body")
                or rec.prepared["compiled"]["public"] != initial.packet
                or rec.result["response"] != response_dict(rec.response)
                or rec.response.model_id != accepted.model_id
                or rec.response.error_code
                or rec.response.billing_uncertain
                or rec.response.outcome not in {"COMPLETED", "REPLAYED"}
            ):
                raise ModelError("REPLAY_DECISION_IDENTITY_MISMATCH")
            role = initial.packet["actor_role"].lower()
            if role not in {"manager", "spread"}:
                raise ModelError("REPLAY_ROLE_INVALID")
            if role in self.role_ids and self.role_ids[role] != initial.model_id:
                raise ModelError("REPLAY_MODEL_CHANGED_WITHIN_ROLE")
            self.role_ids[role] = initial.model_id
            namespaces.add(rec.prepared["compiled"]["context"]["alias_namespace"])
            output_limits.add(initial.max_output_tokens)
            key = initial.request_hash()
            if key in self._records:
                raise ModelError("REPLAY_AMBIGUOUS_INITIAL_REQUEST")
            self._records[key] = (accepted, rec.response, rec.decision_id)
        if len(namespaces) != 1 or not next(iter(namespaces)) or len(output_limits) != 1:
            raise ModelError("REPLAY_INCONSISTENT_REQUEST_SETTINGS")
        self.alias_namespace = next(iter(namespaces))
        self.max_output_tokens = next(iter(output_limits))

    def replay_decision(self, req: ModelRequest) -> tuple[ModelRequest, ModelResponse]:
        found = self._records.get(req.request_hash())
        if found is None:
            raise ModelError("TAPE_MISS")
        accepted, response, decision_id = found
        # No source usage is charged again; retain its exact accounting evidence.
        return accepted, replace(
            response,
            input_tokens=0,
            output_tokens=0,
            cached_input_tokens=0,
            cost_usd=Decimal(0),
            outcome="REPLAYED",
            provider_metadata={
                "replay_source_sha256": self.source_sha256,
                "source_decision_id": decision_id,
                "source_request_hash": accepted.request_hash(),
                "source_usage": {
                    key: value
                    for key, value in response_dict(response).items()
                    if key in {"input_tokens", "output_tokens", "cached_input_tokens", "cost_usd"}
                },
                "source_provider_metadata": response.provider_metadata,
            },
        )

    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse:
        accepted, response = self.replay_decision(req)
        if accepted != req:
            raise ModelError("REPLAY_RETRY_REQUIRES_DECISION_PIPELINE")
        return response
