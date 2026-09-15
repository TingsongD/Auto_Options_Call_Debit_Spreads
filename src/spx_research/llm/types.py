"""Model gateway types (M4-01).

A ``ModelRequest`` is fully determined by (system prompt id, packet, schema,
model id) — its hash is the decision-tape key and the retry/budget identity.
Provider continuation state, opaque reasoning replay, and uninspected
compaction are structurally absent from the request shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from spx_research.epistemics.harness import digest


class ModelError(ValueError):
    """Fixed codes: REFUSAL, TIMEOUT, SCHEMA, TRANSPORT, INCOMPLETE, BUDGET."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ModelRequest:
    system_prompt_id: str  # versioned prompt name from the spec package
    packet: dict[str, Any]  # blinded public packet (already egress-checked)
    schema_name: str  # "spread_decision" | "manager_decision"
    model_id: str
    max_output_tokens: int = 800

    def request_hash(self) -> str:
        return (
            "req_"
            + digest(
                [
                    self.system_prompt_id,
                    self.packet,
                    self.schema_name,
                    self.model_id,
                    self.max_output_tokens,
                ]
            )[:24]
        )


@dataclass(frozen=True)
class ModelResponse:
    request_hash: str
    text: str
    parsed: dict[str, Any]
    model_id: str
    input_tokens: int
    output_tokens: int
    cost_usd: Decimal
    provider_metadata: dict[str, Any] | None = None


def request_body(req: ModelRequest, system_text: str) -> dict[str, Any]:
    """Provider-neutral request body — deliberately no continuation fields."""
    return {
        "system": system_text,
        "packet": req.packet,
        "schema_name": req.schema_name,
        "model": req.model_id,
        "max_output_tokens": req.max_output_tokens,
    }
