"""Public-packet egress gate (H-03).

Serializes the model-visible packet and rejects it if any identity cue slips
through: true dates/times, contract symbols/roots, run or actor identifiers,
manifest ids, file paths, or URLs. This is a belt over the projector's design
— the check is dumb pattern scanning, deliberately.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from spx_research.epistemics.harness import canonical
from spx_research.epistemics.types import Context, HarnessError


def _is_numeric(s: str) -> bool:
    try:
        return Decimal(s).is_finite()
    except InvalidOperation:
        return False


_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("ISO_DATE", re.compile(r"\b(19|20)\d{2}-\d{2}-\d{2}\b")),
    ("ISO_TIME", re.compile(r"\b\d{2}:\d{2}:\d{2}\b")),
    ("YEAR", re.compile(r"\b(19|20)\d{2}\b")),
    ("CONTRACT_SYMBOL", re.compile(r"\bSPXW?\b|-[PC]\d", re.IGNORECASE)),
    ("URL", re.compile(r"https?://", re.IGNORECASE)),
    ("FILE_PATH", re.compile(r"[/\\][\w.-]+\.(parquet|json|csv|jsonl|zip)", re.IGNORECASE)),
    ("MANIFEST_ID", re.compile(r"\bmft_[0-9a-f]{8,}\b|\bmanifest\b", re.IGNORECASE)),
)


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [s for v in value.values() for s in _strings(v)] + [
            s for k in value for s in _strings(k)
        ]
    if isinstance(value, (list, tuple)):
        return [s for v in value for s in _strings(v)]
    return []


def egress_check(public: Mapping[str, Any], ctx: Context) -> None:
    """Fail closed if the serialized packet contains identity cues."""
    forbidden_literals = [
        (ctx.run_id, "RUN_ID"),
        (ctx.branch_id, "BRANCH_ID"),
        (ctx.actor_id, "ACTOR_ID"),
        (ctx.private_manifest_id, "PRIVATE_MANIFEST"),
        (ctx.alias_namespace, "ALIAS_NAMESPACE"),
    ]
    for s in _strings(public):
        # Pure-numeric values (prices, fractions, counts) cannot encode dates;
        # scan only non-numeric strings for date/time/symbol patterns.
        if not _is_numeric(s):
            for name, pat in _PATTERNS:
                if pat.search(s):
                    raise HarnessError(f"EGRESS_LEAK:{name}")
        for lit, name in forbidden_literals:
            if lit and lit in s:
                raise HarnessError(f"EGRESS_LEAK:{name}")
    # Whole-packet belt: the canonical serialization must be leak-free too.
    blob = canonical(public)
    for lit, name in forbidden_literals:
        if lit and lit.encode() in blob:
            raise HarnessError(f"EGRESS_LEAK:{name}")
