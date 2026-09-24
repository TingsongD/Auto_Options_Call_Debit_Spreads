"""Standalone, canonical model requests and usage-bearing outcomes."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Literal

from spx_research.epistemics.harness import canonical, digest

ProviderOutcome = Literal["COMPLETED", "FAILED", "REPLAYED"]


class ModelError(ValueError):
    def __init__(self, code: str, *, billing_uncertain: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.billing_uncertain = billing_uncertain


@dataclass(frozen=True)
class ModelRequest:
    system_prompt_id: str
    packet: dict[str, Any]
    schema_name: str
    model_id: str
    max_output_tokens: int = 800
    system_prompt_hash: str = ""
    schema_hash: str = ""
    retry_error_code: str = ""
    system_text: str = ""
    output_schema: dict[str, Any] | None = None

    def request_hash(self) -> str:
        return "req_" + digest(asdict(self))

    def body(self, system_text: str = "", schema: dict[str, Any] | None = None) -> dict[str, Any]:
        prompt = self.system_text or system_text
        output = self.output_schema if self.output_schema is not None else schema
        content = canonical(self.packet).decode()
        if self.retry_error_code:
            # Only locally generated, bounded fixed codes enter a retry.
            if not all(c.isupper() or c.isdigit() or c in "_:" for c in self.retry_error_code):
                raise ModelError("INVALID_RETRY_CODE")
            content += "\nRETRY_ERROR_CODE=" + self.retry_error_code
        return {
            "model": self.model_id,
            "input": [{"role": "system", "content": prompt}, {"role": "user", "content": content}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": self.schema_name,
                    "schema": output or {},
                    "strict": True,
                }
            },
            "max_output_tokens": self.max_output_tokens,
            "store": False,
        }


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
    outcome: ProviderOutcome = "COMPLETED"
    error_code: str = ""
    billing_uncertain: bool = False
    cached_input_tokens: int = 0


def response_dict(response: ModelResponse) -> dict[str, Any]:
    result = asdict(response)
    result["cost_usd"] = str(response.cost_usd)
    return result


def response_from_dict(raw: dict[str, Any]) -> ModelResponse:
    return ModelResponse(**{**raw, "cost_usd": Decimal(str(raw["cost_usd"]))})


def request_body(req: ModelRequest, system_text: str) -> dict[str, Any]:
    return req.body(system_text)
