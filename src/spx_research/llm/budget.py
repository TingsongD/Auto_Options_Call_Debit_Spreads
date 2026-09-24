"""Validated model-specific prices and conservative pre-dispatch reservations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from threading import RLock

from spx_research.epistemics.harness import canonical
from spx_research.llm.types import ModelError, ModelRequest


@dataclass(frozen=True)
class PriceSheet:
    sheet_id: str
    model_id: str
    input_per_million: Decimal
    output_per_million: Decimal
    cached_input_per_million: Decimal | None = None
    input_overhead_tokens: int = 1024
    model_context_limit: int | None = None

    def __post_init__(self) -> None:
        for rate in (
            self.input_per_million,
            self.output_per_million,
            self.cached_input_per_million,
        ):
            if rate is not None and (not rate.is_finite() or rate < 0):
                raise ModelError("INVALID_PRICE_SHEET")
        if not self.model_id or self.input_overhead_tokens < 0:
            raise ModelError("INVALID_PRICE_SHEET")

    def cost(self, input_tokens: int, output_tokens: int, cached_tokens: int = 0) -> Decimal:
        if min(input_tokens, output_tokens, cached_tokens) < 0 or cached_tokens > input_tokens:
            raise ModelError("INVALID_USAGE")
        cached_rate = (
            self.cached_input_per_million
            if self.cached_input_per_million is not None
            else self.input_per_million
        )
        return (
            Decimal(input_tokens - cached_tokens) * self.input_per_million
            + Decimal(cached_tokens) * cached_rate
            + Decimal(output_tokens) * self.output_per_million
        ) / Decimal(1_000_000)

    def bound(self, request: ModelRequest) -> Decimal:
        if request.model_id != self.model_id:
            raise ModelError("PRICE_MODEL_MISMATCH")
        if request.max_output_tokens <= 0:
            raise ModelError("INVALID_OUTPUT_LIMIT")
        if self.model_context_limit is None or self.model_context_limit <= 0:
            raise ModelError("MISSING_MODEL_CONTEXT_LIMIT")
        if (
            request.max_output_tokens > self.model_context_limit
            or len(canonical(request.body())) + request.max_output_tokens > self.model_context_limit
        ):
            raise ModelError("REQUEST_CONTEXT_LIMIT")
        # Reserve the frozen model's entire supported input window, rather
        # than guessing provider framing/tokenization costs from characters.
        cached_at_higher_rate = (
            self.model_context_limit
            if self.cached_input_per_million is not None
            and self.cached_input_per_million > self.input_per_million
            else 0
        )
        return self.cost(self.model_context_limit, request.max_output_tokens, cached_at_higher_rate)


class Budget:
    """Memory implementation for tests; RunStore owns durable reservations."""

    def __init__(
        self, cap_usd: Decimal, sheet: PriceSheet, sheets: dict[str, PriceSheet] | None = None
    ) -> None:
        if not cap_usd.is_finite() or cap_usd < 0:
            raise ModelError("INVALID_BUDGET")
        self.cap = cap_usd
        self.sheet = sheet
        self.sheets = sheets or {sheet.model_id: sheet}
        self.committed = Decimal(0)
        self.reserved = Decimal(0)
        self.overrun = False
        self._lock = RLock()

    def price_for(self, model_id: str) -> PriceSheet:
        if model_id not in self.sheets:
            raise ModelError("PRICE_MODEL_MISMATCH")
        return self.sheets[model_id]

    def estimate(self, input_tokens: int, output_tokens: int) -> Decimal:
        return self.sheet.cost(input_tokens, output_tokens)

    def reserve_amount(self, est: Decimal) -> Decimal:
        with self._lock:
            if self.overrun or self.committed + self.reserved + est > self.cap:
                raise ModelError("BUDGET_EXCEEDED")
            self.reserved += est
            return est

    def reserve(self, input_tokens: int, max_output_tokens: int) -> Decimal:
        return self.reserve_amount(self.estimate(input_tokens, max_output_tokens))

    def commit(self, reservation: Decimal, actual: Decimal) -> None:
        with self._lock:
            self.reserved -= reservation
            self.committed += actual
            self.overrun = self.committed + self.reserved > self.cap

    def abort(self, reservation: Decimal) -> None:
        with self._lock:
            self.reserved -= reservation
