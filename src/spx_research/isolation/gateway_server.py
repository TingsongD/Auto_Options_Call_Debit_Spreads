"""Credential-bearing fixed-function gateway, reached only by Unix socket."""

from __future__ import annotations

import argparse
import json
import os
import socket
import socketserver
from pathlib import Path
from typing import Any

try:
    from .protocol import MAX_BYTES, canonical, request_body, validate_request
except ImportError:
    from protocol import (  # type: ignore[no-redef,import-not-found]
        MAX_BYTES,
        canonical,
        request_body,
        validate_request,
    )


def mock_response(raw: dict[str, Any]) -> dict[str, Any]:
    packet = raw["packet"]
    menu = packet["action_menu"]
    choice = next((m for m in menu if m["kind"] in ("WAIT", "NO_CHANGE", "HOLD")), menu[0])
    cited = list(choice["required_premise_tokens"])
    if not cited and packet["premises"]:
        cited.append(packet["premises"][0]["token"])
    parsed = {
        "schema_version": "2.1",
        "actor_role": packet["actor_role"],
        "decision_token": packet["decision_token"],
        "packet_token": packet["packet_token"],
        "prior_belief_token": packet["prior_belief_token"],
        "action_id": choice["action_id"],
        "premise_tokens": cited,
        "assessment_updates": [],
        "reason_codes": ["MAINTAIN_THESIS"],
        "uncertainty_codes": ["NONE_IDENTIFIED"],
        "confidence_label": "LOW",
    }
    return {
        "request_hash": raw["request_hash"],
        "provider": {
            "id": "offline-mock",
            "model": raw["model_id"],
            "status": "completed",
            "output": [{"content": [{"type": "output_text", "text": json.dumps(parsed)}]}],
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "input_tokens_details": {"cached_tokens": 0},
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default="/run/spx/gateway.sock")
    parser.add_argument("--bundle", default="/app/contracts")
    parser.add_argument("--mode", choices=("mock", "provider"), default="mock")
    parser.add_argument("--models", default="mock-1")
    parser.add_argument("--key-file", default="/run/secrets/provider_key")
    parser.add_argument("--proxy-socket", default="/run/egress/provider.sock")
    args = parser.parse_args()
    allowed = set(args.models.split(","))

    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request_hash = ""
            try:
                line = self.rfile.readline(MAX_BYTES + 1)
                if len(line) > MAX_BYTES or not line.endswith(b"\n"):
                    raise ValueError("INVALID_RPC_LENGTH")
                raw, prompt, schema = validate_request(json.loads(line), Path(args.bundle))
                request_hash = raw["request_hash"]
                if raw["model_id"] not in allowed:
                    raise ValueError("MODEL_NOT_ALLOWED")
                if args.mode == "mock":
                    result = mock_response(raw)
                else:
                    envelope = {
                        "request_hash": request_hash,
                        "body": request_body(raw, prompt, schema),
                        "api_key": Path(args.key_file).read_text().strip(),
                    }
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                        connection.settimeout(45)
                        connection.connect(args.proxy_socket)
                        connection.sendall(canonical(envelope) + b"\n")
                        result = json.loads(connection.makefile("rb").readline(MAX_BYTES + 1))
            except (ValueError, OSError, KeyError):
                result = {
                    "request_hash": request_hash,
                    "error": "GATEWAY_UNCERTAIN",
                    "billing_uncertain": True,
                }
            self.wfile.write(canonical(result) + b"\n")

    path = Path(args.socket)
    if path.exists():
        path.unlink()
    with socketserver.UnixStreamServer(str(path), Handler) as server:
        os.chmod(path, 0o666)
        server.serve_forever()


if __name__ == "__main__":
    main()
