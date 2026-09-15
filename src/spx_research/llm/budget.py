"""API budget ledger (M4-03).

Reservations are taken before a call is dispatched (concurrent batches cannot
overspend together — T37); actuals are committed from response usage. Costs use
a configured dated price sheet, never a hard-coded rate.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from spx_research.llm.types import ModelError


@dataclass(frozen=True)
class PriceSheet:
    """Dated USD-per-million-token rates for one provider model."""

    sheet_id: str
    model_id: str
    input_per_million: Decimal
    output_per_million: Decimal


class Budget:
    def __init__(self, cap_usd: Decimal, sheet: PriceSheet) -> None:
        self.cap = cap_usd
        self.sheet = sheet
        self.committed = Decimal(0)
        self.reserved = Decimal(0)

    def estimate(self, input_tokens: int, output_tokens: int) -> Decimal:
        s = self.sheet
        return (
            Decimal(input_tokens) * s.input_per_million
            + Decimal(output_tokens) * s.output_per_million
        ) / Decimal(1_000_000)

    def reserve(self, input_tokens: int, max_output_tokens: int) -> Decimal:
        est = self.estimate(input_tokens, max_output_tokens)
        if self.committed + self.reserved + est > self.cap:
            raise ModelError("BUDGET_EXCEEDED")
        self.reserved += est
        return est

    def commit(self, reservation: Decimal, actual: Decimal) -> None:
        self.reserved -= reservation
        self.committed += actual

    def abort(self, reservation: Decimal) -> None:
        self.reserved -= reservation
