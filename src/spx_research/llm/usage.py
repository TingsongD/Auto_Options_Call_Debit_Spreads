"""Strict provider-usage parsing; missing or malformed counts never mean free."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, TypeGuard


def _snapshot(value: Any) -> Any:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, dict):
        return {str(key): _snapshot(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_snapshot(item) for item in value]
    if hasattr(value, "model_dump"):
        return _snapshot(value.model_dump())
    if hasattr(value, "__dict__"):
        return _snapshot(vars(value))
    return str(value)


def _count(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


@dataclass(frozen=True)
class ProviderUsage:
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    valid: bool
    raw: Any


def parse_usage(value: Any) -> ProviderUsage:
    raw = _snapshot(value)
    usage = raw if isinstance(raw, dict) else {}
    incoming, outgoing = usage.get("input_tokens"), usage.get("output_tokens")
    details = usage.get("input_tokens_details")
    cached = details.get("cached_tokens", 0) if isinstance(details, dict) else 0
    valid = (
        isinstance(raw, dict)
        and _count(incoming)
        and _count(outgoing)
        and (details is None or isinstance(details, dict))
        and _count(cached)
        and cached <= incoming
    )
    # Placeholders cannot be charged unless valid is true. Raw values are kept
    # beside the outcome so an explicit billing receipt can resolve uncertainty.
    return ProviderUsage(
        incoming if _count(incoming) else 0,
        outgoing if _count(outgoing) else 0,
        cached if _count(cached) else 0,
        valid,
        raw,
    )
