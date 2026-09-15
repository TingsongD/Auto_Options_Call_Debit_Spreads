"""OpenAI Responses-API gateway (M4-01).

Hard-gated: constructing with ``allow_real_calls=False`` (the default) raises.
The request deliberately carries no ``store``, no ``previous_response_id``, no
conversation object, and no reasoning replay — each call is reconstructed
locally from approved state. Usage is priced off the configured sheet and
logged onto the decision tape by the caller.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from spx_research.llm.budget import PriceSheet
from spx_research.llm.types import ModelError, ModelRequest, ModelResponse


class OpenAIGateway:
    def __init__(
        self,
        client: Any,
        model_id: str,
        sheet: PriceSheet,
        allow_real_calls: bool = False,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not allow_real_calls:
            raise ModelError("REAL_CALLS_DISABLED")
        self.client = client
        self.model_id = model_id
        self.sheet = sheet
        self.timeout = timeout_seconds

    def complete(
        self, req: ModelRequest, system_text: str, schema: dict[str, Any]
    ) -> ModelResponse:
        import openai

        try:
            resp = self.client.responses.create(
                model=req.model_id,
                input=[
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": json.dumps(req.packet, sort_keys=True)},
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": req.schema_name,
                        "schema": schema,
                        "strict": True,
                    }
                },
                max_output_tokens=req.max_output_tokens,
                store=False,
                timeout=self.timeout,
            )
        except openai.APITimeoutError as e:
            raise ModelError("TIMEOUT") from e
        except openai.RateLimitError as e:
            raise ModelError("RATE_LIMIT") from e
        except openai.APIError as e:
            raise ModelError("TRANSPORT") from e
        status = getattr(resp, "status", "completed")
        if status == "incomplete":
            raise ModelError("INCOMPLETE")
        text = getattr(resp, "output_text", "")
        if not text:
            raise ModelError("REFUSAL")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as e:
            raise ModelError("SCHEMA") from e
        usage = getattr(resp, "usage", None)
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)
        cost = (
            Decimal(in_tok) * self.sheet.input_per_million
            + Decimal(out_tok) * self.sheet.output_per_million
        ) / Decimal(1_000_000)
        return ModelResponse(
            request_hash=req.request_hash(),
            text=text,
            parsed=parsed,
            model_id=req.model_id,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=cost,
            provider_metadata={"status": status, "response_id": getattr(resp, "id", None)},
        )
