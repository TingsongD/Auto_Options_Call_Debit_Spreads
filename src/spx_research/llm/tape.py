"""Versioned atomic decision export. Durable attempts live in RunStore.

Legacy files are readable and never rewritten. Truncated exports are explicitly
incomplete and cannot be appended to or used for current-format recovery.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from spx_research.epistemics.harness import digest
from spx_research.llm.types import ModelRequest, ModelResponse, response_dict, response_from_dict


@dataclass(frozen=True)
class TapeRecord:
    request_hash: str
    request: dict[str, Any]
    response: ModelResponse
    recorded_at_utc: str
    decision_id: str = ""
    prepared: dict[str, Any] | None = None
    result: dict[str, Any] | None = None


class DecisionTape:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._records: dict[str, TapeRecord] = {}
        self._decisions: dict[str, TapeRecord] = {}
        self.legacy = False
        self.incomplete = False
        self._lock = RLock()
        seal: dict[str, Any] | None = None
        record_hashes: list[str] = []
        if self.path.is_file():
            lines = self.path.read_text().splitlines()
            for i, line in enumerate(lines):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    if i == len(lines) - 1:
                        self.incomplete = True
                        continue
                    raise ValueError("TAPE_CORRUPT") from None
                if seal is not None:
                    raise ValueError("TAPE_TRAILING_RECORDS")
                if raw.get("type") == "SEAL":
                    seal = raw
                    if (
                        seal.get("complete") is not True
                        or seal.get("record_count") != len(record_hashes)
                        or seal.get("sha256") != digest(record_hashes)
                    ):
                        raise ValueError("TAPE_SEAL_MISMATCH")
                    continue
                version = raw.get("format_version", 1)
                if version == 1:
                    self.legacy = True
                elif version != 2:
                    raise ValueError("TAPE_VERSION_UNSUPPORTED")
                else:
                    expected = raw.get("record_hash")
                    if expected != digest({k: v for k, v in raw.items() if k != "record_hash"}):
                        raise ValueError("TAPE_HASH_MISMATCH")
                    record_hashes.append(raw["record_hash"])
                    model_request = ModelRequest(**raw["model_request"])
                    if model_request.request_hash() != raw["request_hash"]:
                        raise ValueError("TAPE_REQUEST_MISMATCH")
                rec = TapeRecord(
                    raw["request_hash"],
                    raw["request"],
                    response_from_dict(raw["response"]),
                    raw["recorded_at_utc"],
                    raw.get("decision_id", ""),
                    raw.get("prepared"),
                    raw.get("result"),
                )
                if rec.response.request_hash != rec.request_hash:
                    raise ValueError("TAPE_RESPONSE_MISMATCH")
                if rec.request_hash in self._records:
                    raise ValueError("DUPLICATE_REQUEST_HASH")
                self._records[rec.request_hash] = rec
                if rec.decision_id:
                    if rec.decision_id in self._decisions:
                        raise ValueError("DUPLICATE_DECISION_ID")
                    self._decisions[rec.decision_id] = rec
        if self.path.is_file() and not self.legacy and seal is None:
            self.incomplete = True
        self._serialized: list[dict[str, Any]] = []
        if self.path.is_file() and not self.legacy and not self.incomplete:
            self._serialized = [
                json.loads(x)
                for x in self.path.read_text().splitlines()
                if x.strip() and json.loads(x).get("type") != "SEAL"
            ]

    def lookup(self, request_hash: str) -> TapeRecord | None:
        return self._records.get(request_hash)

    def records(self) -> tuple[TapeRecord, ...]:
        """Accepted records in export order; does not expose mutable indexes."""
        return tuple(self._records.values())

    def decision(self, decision_id: str) -> TapeRecord | None:
        if self.legacy or self.incomplete:
            raise ValueError("LEGACY_OR_INCOMPLETE_TAPE_NOT_RESUMABLE")
        return self._decisions.get(decision_id)

    def append(
        self,
        req: ModelRequest,
        resp: ModelResponse,
        recorded_at_utc: str,
        *,
        decision_id: str = "",
        prepared: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
    ) -> TapeRecord:
        with self._lock:
            if self.legacy or self.incomplete:
                raise ValueError("LEGACY_OR_INCOMPLETE_TAPE_READ_ONLY")
            if req.request_hash() in self._records:
                raise ValueError("DUPLICATE_REQUEST_HASH")
            if decision_id and decision_id in self._decisions:
                raise ValueError("DUPLICATE_DECISION_ID")
            if resp.request_hash != req.request_hash():
                raise ValueError("TAPE_RESPONSE_MISMATCH")
            request = {
                "packet": req.packet,
                "schema": req.schema_name,
                "model": req.model_id,
                "system": req.system_prompt_id,
                "body": req.body(),
            }
            rec = TapeRecord(
                req.request_hash(), request, resp, recorded_at_utc, decision_id, prepared, result
            )
            raw = {
                "format_version": 2,
                "request_hash": rec.request_hash,
                "model_request": asdict(req),
                "request": request,
                "response": response_dict(resp),
                "recorded_at_utc": recorded_at_utc,
                "decision_id": decision_id,
                "prepared": prepared,
                "result": result,
            }
            raw["record_hash"] = digest(raw)
            data = [*self._serialized, raw]
            self._write(data)
            self._serialized = data
            self._records[rec.request_hash] = rec
            if decision_id:
                self._decisions[decision_id] = rec
            return rec

    def export(self) -> None:
        """Seal an empty or unchanged export without touching legacy artifacts."""
        with self._lock:
            if self.legacy or self.incomplete:
                raise ValueError("LEGACY_OR_INCOMPLETE_TAPE_READ_ONLY")
            self._write(self._serialized)

    def _write(self, data: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".tape-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as f:
                for row in data:
                    f.write(json.dumps(row, sort_keys=True) + "\n")
                f.write(
                    json.dumps(
                        {
                            "format_version": 2,
                            "type": "SEAL",
                            "complete": True,
                            "record_count": len(data),
                            "sha256": digest([r["record_hash"] for r in data]),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
                f.flush()
                os.fsync(f.fileno())
            os.replace(name, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def __len__(self) -> int:
        return len(self._records)
