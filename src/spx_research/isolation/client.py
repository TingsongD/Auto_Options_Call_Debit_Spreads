"""Trusted-engine adapter for a locked-down Docker worker and fixed gateway."""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import replace
from decimal import Decimal
from typing import Any

from spx_research.isolation.protocol import canonical, sha
from spx_research.llm.budget import PriceSheet
from spx_research.llm.types import ModelError, ModelRequest, ModelResponse
from spx_research.llm.usage import parse_usage


class DockerGateway:
    def __init__(
        self,
        *,
        image: str,
        socket_volume: str,
        sheets: dict[str, PriceSheet] | None = None,
        mock: bool = False,
    ) -> None:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", socket_volume):
            raise ValueError("INVALID_GATEWAY_VOLUME")
        probe = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", image],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        self.image = probe.stdout.strip()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.image):
            raise ValueError("UNPINNED_WORKER_IMAGE")
        self.volume = socket_volume
        self.sheets = sheets or {}
        self.mock = mock

    def command(self) -> list[str]:
        return [
            "docker",
            "run",
            "--rm",
            "-i",
            "--network",
            "none",
            "--read-only",
            "--user",
            "65532:65532",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            "64",
            "--memory",
            "128m",
            "--cpus",
            "1",
            "--mount",
            f"type=volume,src={self.volume},dst=/run/spx,readonly",
            self.image,
        ]

    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse:
        if not req.system_text or req.output_schema is None:
            req = replace(req, system_text=system_text, output_schema=schema)
        raw = {
            "protocol_version": 1,
            "request_hash": req.request_hash(),
            "body_sha256": sha(req.body()),
            "packet": req.packet,
            "prompt_id": req.system_prompt_id,
            "prompt_sha256": req.system_prompt_hash,
            "schema_name": req.schema_name,
            "schema_sha256": req.schema_hash,
            "model_id": req.model_id,
            "max_output_tokens": req.max_output_tokens,
            "retry_error_code": req.retry_error_code,
        }
        try:
            proc = subprocess.run(
                self.command(),
                input=canonical(raw) + b"\n",
                capture_output=True,
                check=True,
                timeout=75,
            )
            result = json.loads(proc.stdout)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise ModelError("ISOLATED_GATEWAY_FAILED", billing_uncertain=True) from exc
        if result.get("request_hash") != req.request_hash():
            raise ModelError("RESPONSE_IDENTITY_MISMATCH", billing_uncertain=True)
        if result.get("error"):
            raise ModelError("TRANSPORT", billing_uncertain=True)
        provider = result["provider"]
        if provider.get("model") not in (None, req.model_id):
            raise ModelError("MODEL_IDENTITY_MISMATCH", billing_uncertain=True)
        usage = parse_usage(provider.get("usage"))
        it, ot, ct = usage.input_tokens, usage.output_tokens, usage.cached_input_tokens
        if self.mock:
            if it or ot or ct or provider.get("id") != "offline-mock":
                raise ModelError("MOCK_GATEWAY_MISMATCH", billing_uncertain=True)
            cost = Decimal(0)
        else:
            if provider.get("id") == "offline-mock":
                raise ModelError("PROVIDER_GATEWAY_MISMATCH")
            if req.model_id not in self.sheets:
                raise ModelError("PRICE_MODEL_MISMATCH", billing_uncertain=True)
            cost = self.sheets[req.model_id].cost(it, ot, ct) if usage.valid else Decimal(0)
        text = "".join(
            c.get("text", "")
            for o in provider.get("output", [])
            for c in o.get("content", [])
            if c.get("type") == "output_text"
        )
        status = provider.get("status", "failed")
        code = (
            "INCOMPLETE"
            if status == "incomplete"
            else ("PROVIDER_FAILED" if status != "completed" else ("REFUSAL" if not text else ""))
        )
        parsed = {}
        if not code:
            try:
                parsed = json.loads(text)
                if not isinstance(parsed, dict):
                    parsed, code = {}, "SCHEMA"
            except ValueError:
                code = "SCHEMA"
        return ModelResponse(
            req.request_hash(),
            text,
            parsed,
            req.model_id,
            it,
            ot,
            cost,
            {
                "response_id": provider.get("id"),
                "status": status,
                "worker_image": self.image,
                "raw_usage": usage.raw,
                "output_error_code": code,
            },
            outcome="FAILED" if code or not usage.valid else "COMPLETED",
            error_code=code if usage.valid else "INVALID_USAGE",
            billing_uncertain=not usage.valid,
            cached_input_tokens=ct,
        )
