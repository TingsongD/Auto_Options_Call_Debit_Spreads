"""Standalone Responses gateway; every returned outcome preserves usage."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from spx_research.llm.budget import PriceSheet
from spx_research.llm.types import ModelError, ModelRequest, ModelResponse
from spx_research.llm.usage import parse_usage


class OpenAIGateway:
    def __init__(
        self,
        client: Any,
        model_id: str,
        sheet: PriceSheet,
        allow_real_calls: bool = False,
        timeout_seconds: float = 30.0,
        sheets: dict[str, PriceSheet] | None = None,
    ) -> None:
        if not allow_real_calls:
            raise ModelError("REAL_CALLS_DISABLED")
        # The application journals retries; SDK retries would hide billed attempts.
        self.client = (
            client.with_options(max_retries=0) if hasattr(client, "with_options") else client
        )
        self.model_id = model_id
        self.sheet = sheet
        self.sheets = sheets or {sheet.model_id: sheet}
        self.timeout = timeout_seconds

    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse:
        import openai

        if req.model_id not in self.sheets:
            raise ModelError("PRICE_MODEL_MISMATCH")
        try:
            resp = self.client.responses.create(
                **req.body(system_text, schema), timeout=self.timeout
            )
        except openai.APITimeoutError as e:
            raise ModelError("TIMEOUT", billing_uncertain=True) from e
        except openai.RateLimitError as e:
            raise ModelError("RATE_LIMIT") from e
        except openai.APIError as e:
            raise ModelError("TRANSPORT", billing_uncertain=True) from e
        status = getattr(resp, "status", "completed")
        text = getattr(resp, "output_text", "") or ""
        usage = parse_usage(getattr(resp, "usage", None))
        cost = (
            self.sheets[req.model_id].cost(
                usage.input_tokens, usage.output_tokens, usage.cached_input_tokens
            )
            if usage.valid
            else Decimal(0)
        )
        code = ""
        parsed: dict[str, Any] = {}
        if status == "incomplete":
            code = "INCOMPLETE"
        elif status != "completed":
            code = "PROVIDER_FAILED"
        elif not text:
            code = "REFUSAL"
        else:
            try:
                value = json.loads(text)
                if not isinstance(value, dict):
                    code = "SCHEMA"
                else:
                    parsed = value
            except json.JSONDecodeError:
                code = "SCHEMA"
        return ModelResponse(
            req.request_hash(),
            text,
            parsed,
            req.model_id,
            usage.input_tokens,
            usage.output_tokens,
            Decimal(cost),
            {
                "status": status,
                "response_id": getattr(resp, "id", None),
                "raw_usage": usage.raw,
                "output_error_code": code,
            },
            outcome="FAILED" if code or not usage.valid else "COMPLETED",
            error_code=code if usage.valid else "INVALID_USAGE",
            billing_uncertain=not usage.valid,
            cached_input_tokens=usage.cached_input_tokens,
        )
