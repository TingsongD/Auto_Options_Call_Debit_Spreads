"""Offline protocol and fixed-endpoint transport boundary tests."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from spx_research.contracts import load_example, load_prompt, load_schema, spec_dir
from spx_research.isolation import egress_proxy
from spx_research.isolation.protocol import canonical, request_body, sha, validate_request
from spx_research.llm.types import ModelError, ModelRequest


def model_request() -> ModelRequest:
    prompt = load_prompt("spread_agent")
    schema = load_schema("spread_decision")
    return ModelRequest(
        "spread_v2.1",
        load_example("spread_packet"),
        "spread_decision",
        "mock-1",
        system_prompt_hash=hashlib.sha256(prompt.encode()).hexdigest(),
        schema_hash=sha(schema),
        system_text=prompt,
        output_schema=schema,
    )


def wire(req: ModelRequest) -> dict[str, Any]:
    return {
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


def test_wire_reconstructs_exact_provider_request_including_retry() -> None:
    req = replace(model_request(), retry_error_code="RATE_LIMIT")
    raw, prompt, schema = validate_request(wire(req), spec_dir())
    assert canonical(request_body(raw, prompt, schema)) == canonical(req.body())
    assert "RETRY_ERROR_CODE=RATE_LIMIT" in req.body()["input"][1]["content"]
    assert "previous_response_id" not in req.body() and req.body()["store"] is False


def test_provider_policy_cannot_mistake_mock_socket_for_paid_inference(monkeypatch) -> None:
    from spx_research.isolation import client
    from spx_research.isolation.gateway_server import mock_response

    req = model_request()

    def run(args, **kwargs):
        if args[1:3] == ["image", "inspect"]:
            return SimpleNamespace(stdout="sha256:" + "0" * 64)
        return SimpleNamespace(stdout=canonical(mock_response(wire(req))))

    monkeypatch.setattr(client.subprocess, "run", run)
    gateway = client.DockerGateway(image="fixture", socket_volume="mock-socket", mock=False)
    with pytest.raises(ModelError, match="PROVIDER_GATEWAY_MISMATCH") as caught:
        gateway.complete(req)
    assert caught.value.billing_uncertain is False


@pytest.mark.parametrize(
    "field", ["url", "api_key", "previous_response_id", "tools", "conversation"]
)
def test_rpc_rejects_extra_capabilities(field: str) -> None:
    raw = wire(model_request())
    raw[field] = "https://unapproved.invalid/"
    with pytest.raises(ValueError, match="INVALID_RPC"):
        validate_request(raw, spec_dir())


@pytest.mark.parametrize(
    "field,code",
    [
        ("request_hash", "REQUEST_IDENTITY_MISMATCH"),
        ("body_sha256", "REQUEST_BODY_MISMATCH"),
        ("prompt_sha256", "PROMPT_HASH_MISMATCH"),
        ("schema_sha256", "SCHEMA_HASH_MISMATCH"),
    ],
)
def test_rpc_rejects_modified_hashes(field: str, code: str) -> None:
    raw = wire(model_request())
    raw[field] = "0" * 64
    with pytest.raises(ValueError, match=code):
        validate_request(raw, spec_dir())


def test_hashing_added_packet_field_does_not_authorize_it() -> None:
    req = model_request()
    req = replace(req, packet={**req.packet, "archive_path": "/archive/future.parquet"})
    with pytest.raises(ValueError, match="EXTRA_PACKET_FIELD"):
        validate_request(wire(req), spec_dir())


@pytest.mark.parametrize(
    "value", ["https://unapproved.invalid", "file:///repo/private.json", "SPXW", "2024-01-02"]
)
def test_packet_identity_or_url_rejected_even_with_recomputed_hash(value: str) -> None:
    req = model_request()
    packet = json.loads(json.dumps(req.packet))
    packet["premises"][0]["value"] = value
    with pytest.raises(ValueError, match="PACKET_IDENTITY_CUE"):
        validate_request(wire(replace(req, packet=packet)), spec_dir())


def test_role_cannot_choose_another_roles_prompt() -> None:
    req = replace(model_request(), system_prompt_id="manager_v2.1")
    with pytest.raises(ValueError, match="UNAPPROVED_PROMPT"):
        validate_request(wire(req), spec_dir())


def test_bundle_tamper_fails_before_dispatch(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    shutil.copytree(spec_dir(), bundle)
    (bundle / "prompts" / "spread_agent.md").write_text("unapproved prompt")
    with pytest.raises(ValueError, match="CONTRACT_CHECKSUM_MISMATCH"):
        validate_request(wire(model_request()), bundle)


def test_model_field_cannot_override_provider_url() -> None:
    req = replace(model_request(), model_id="https://unapproved.invalid")
    with pytest.raises(ValueError, match="INVALID_MODEL"):
        validate_request(wire(req), spec_dir())


def test_proxy_rejects_private_dns_resolution_before_socket_connect(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        egress_proxy.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("127.0.0.1", 443))]
    )
    called = []
    monkeypatch.setattr(egress_proxy.socket, "create_connection", lambda *a, **k: called.append(a))
    with pytest.raises(ValueError, match="NONPUBLIC_PROVIDER_ADDRESS"):
        egress_proxy.ProviderConnection("api.openai.com", timeout=1).connect()
    assert called == []


def test_proxy_uses_checked_ip_and_fixed_tls_hostname(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        egress_proxy.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("8.8.8.8", 443))]
    )

    def connect(address: Any, **kwargs: Any) -> object:
        captured["address"] = address
        return object()

    class Context:
        def wrap_socket(self, sock: object, *, server_hostname: str) -> object:
            captured["hostname"] = server_hostname
            return sock

    monkeypatch.setattr(egress_proxy.socket, "create_connection", connect)
    monkeypatch.setattr(egress_proxy.ssl, "create_default_context", Context)
    connection = egress_proxy.ProviderConnection("malicious.invalid", timeout=1)
    connection.connect()
    assert captured == {"address": ("8.8.8.8", 443), "hostname": "api.openai.com"}
