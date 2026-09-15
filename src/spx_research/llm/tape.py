"""Decision tape (M4-02): append-only request→response records.

Keyed by request hash. Replay reuses successful persisted responses after a
crash (T41) and forbids drifting re-calls. Records are JSONL; the response is
stored verbatim plus token usage for cost accounting.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from spx_research.llm.types import ModelRequest, ModelResponse


@dataclass(frozen=True)
class TapeRecord:
    request_hash: str
    request: dict[str, Any]
    response: ModelResponse
    recorded_at_utc: str


class DecisionTape:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._records: dict[str, TapeRecord] = {}
        if self.path.is_file():
            for line in self.path.read_text().splitlines():
                if not line.strip():
                    continue
                raw = json.loads(line)
                resp = raw["response"]
                self._records[raw["request_hash"]] = TapeRecord(
                    request_hash=raw["request_hash"],
                    request=raw["request"],
                    response=ModelResponse(
                        request_hash=resp["request_hash"],
                        text=resp["text"],
                        parsed=resp["parsed"],
                        model_id=resp["model_id"],
                        input_tokens=resp["input_tokens"],
                        output_tokens=resp["output_tokens"],
                        cost_usd=Decimal(str(resp["cost_usd"])),
                        provider_metadata=resp.get("provider_metadata"),
                    ),
                    recorded_at_utc=raw["recorded_at_utc"],
                )

    def lookup(self, request_hash: str) -> TapeRecord | None:
        return self._records.get(request_hash)

    def append(self, req: ModelRequest, resp: ModelResponse, recorded_at_utc: str) -> TapeRecord:
        rec = TapeRecord(
            request_hash=req.request_hash(),
            request={
                "packet": req.packet,
                "schema": req.schema_name,
                "model": req.model_id,
                "system": req.system_prompt_id,
            },
            response=resp,
            recorded_at_utc=recorded_at_utc,
        )
        if req.request_hash() in self._records:
            raise ValueError("DUPLICATE_REQUEST_HASH")
        self._records[req.request_hash()] = rec
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a") as f:
            f.write(
                json.dumps(
                    {
                        "request_hash": rec.request_hash,
                        "request": rec.request,
                        "response": {
                            "request_hash": resp.request_hash,
                            "text": resp.text,
                            "parsed": resp.parsed,
                            "model_id": resp.model_id,
                            "input_tokens": resp.input_tokens,
                            "output_tokens": resp.output_tokens,
                            "cost_usd": str(resp.cost_usd),
                            "provider_metadata": resp.provider_metadata,
                        },
                        "recorded_at_utc": rec.recorded_at_utc,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        return rec

    def __len__(self) -> int:
        return len(self._records)
