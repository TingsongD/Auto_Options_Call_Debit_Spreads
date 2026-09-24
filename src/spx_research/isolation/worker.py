"""No-network, no-credential worker: bounded stdin RPC to one Unix socket."""

from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

try:
    from .protocol import MAX_BYTES, canonical, validate_request
except ImportError:
    from protocol import (  # type: ignore[no-redef,import-not-found]
        MAX_BYTES,
        canonical,
        validate_request,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default="/run/spx/gateway.sock")
    parser.add_argument("--bundle", default="/app/contracts")
    args = parser.parse_args()
    line = sys.stdin.buffer.readline(MAX_BYTES + 1)
    try:
        if len(line) > MAX_BYTES or not line.endswith(b"\n"):
            raise ValueError("INVALID_RPC_LENGTH")
        raw, _, _ = validate_request(json.loads(line), Path(args.bundle))
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(60)
            conn.connect(args.socket)
            conn.sendall(canonical(raw) + b"\n")
            response = conn.makefile("rb").readline(MAX_BYTES + 1)
        if len(response) > MAX_BYTES or not response.endswith(b"\n"):
            raise ValueError("INVALID_GATEWAY_RESPONSE")
        result = json.loads(response)
        if result.get("request_hash") != raw["request_hash"]:
            raise ValueError("RESPONSE_IDENTITY_MISMATCH")
    except (ValueError, OSError) as exc:
        # Never echo requests, provider credentials or rejected prose as errors.
        result = {"error": "ISOLATED_GATEWAY_FAILED", "exception_type": type(exc).__name__}
    sys.stdout.buffer.write(canonical(result) + b"\n")
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
