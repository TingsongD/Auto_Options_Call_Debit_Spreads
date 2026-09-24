"""One-destination TLS proxy. No caller-controlled URL, path, tools or redirects."""

from __future__ import annotations

import argparse
import http.client
import ipaddress
import json
import os
import socket
import socketserver
import ssl
from pathlib import Path

try:
    from .protocol import MAX_BYTES, canonical
except ImportError:
    from protocol import MAX_BYTES, canonical  # type: ignore[no-redef,import-not-found]

PROVIDER_HOST = "api.openai.com"


class ProviderConnection(http.client.HTTPSConnection):
    def connect(self) -> None:
        addresses = socket.getaddrinfo(PROVIDER_HOST, 443, type=socket.SOCK_STREAM)
        if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
            raise ValueError("NONPUBLIC_PROVIDER_ADDRESS")
        # Connect to the checked address; TLS still verifies the fixed hostname.
        sock = socket.create_connection((str(addresses[0][4][0]), 443), timeout=self.timeout)
        self.sock = ssl.create_default_context().wrap_socket(sock, server_hostname=PROVIDER_HOST)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default="/run/egress/provider.sock")
    parser.add_argument("--models", required=True)
    args = parser.parse_args()
    allowed = set(args.models.split(","))

    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request_hash = ""
            try:
                line = self.rfile.readline(MAX_BYTES + 1)
                if len(line) > MAX_BYTES or not line.endswith(b"\n"):
                    raise ValueError("INVALID_PROXY_RPC")
                raw = json.loads(line)
                if set(raw) != {"request_hash", "body", "api_key"}:
                    raise ValueError("INVALID_PROXY_RPC")
                request_hash = raw["request_hash"]
                body = raw["body"]
                if (
                    set(body) != {"model", "input", "text", "max_output_tokens", "store"}
                    or body["model"] not in allowed
                    or body["store"] is not False
                ):
                    raise ValueError("UNAPPROVED_PROVIDER_REQUEST")
                if not raw["api_key"] or any(c in raw["api_key"] for c in "\r\n"):
                    raise ValueError("INVALID_CREDENTIAL")
                conn = ProviderConnection(PROVIDER_HOST, timeout=35)
                try:
                    conn.request(
                        "POST",
                        "/v1/responses",
                        body=canonical(body),
                        headers={
                            "Authorization": "Bearer " + raw["api_key"],
                            "Content-Type": "application/json",
                        },
                    )
                    response = conn.getresponse()
                    content = response.read(MAX_BYTES + 1)
                    if len(content) > MAX_BYTES or 300 <= response.status < 400:
                        raise ValueError("UNAPPROVED_PROVIDER_RESPONSE")
                    result = {
                        "request_hash": request_hash,
                        "provider": json.loads(content),
                        "http_status": response.status,
                    }
                finally:
                    conn.close()
            except (OSError, ValueError, KeyError, http.client.HTTPException):
                result = {
                    "request_hash": request_hash,
                    "error": "PROVIDER_UNCERTAIN",
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
