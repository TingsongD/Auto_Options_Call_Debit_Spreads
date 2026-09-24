"""Small standard-library-only wire boundary, shared by isolated processes."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

MAX_BYTES = 2 * 1024 * 1024
FIELDS = {
    "protocol_version",
    "request_hash",
    "body_sha256",
    "packet",
    "prompt_id",
    "prompt_sha256",
    "schema_name",
    "schema_sha256",
    "model_id",
    "max_output_tokens",
    "retry_error_code",
}


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode()


def sha(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def validate_request(raw: Any, bundle: Path) -> tuple[dict[str, Any], str, dict[str, Any]]:
    if not isinstance(raw, dict) or set(raw) != FIELDS or raw["protocol_version"] != 1:
        raise ValueError("INVALID_RPC")
    if len(canonical(raw)) > MAX_BYTES:
        raise ValueError("RPC_TOO_LARGE")
    if not isinstance(raw["packet"], dict) or raw["packet"].get("schema_version") != "2.1":
        raise ValueError("INVALID_PACKET_VERSION")
    role = raw["packet"].get("actor_role")
    if role not in ("MANAGER", "SPREAD"):
        raise ValueError("INVALID_ROLE")
    prompt_name = "manager" if role == "MANAGER" else "spread_agent"
    if raw["schema_name"] != ("manager_decision" if role == "MANAGER" else "spread_decision"):
        raise ValueError("SCHEMA_ROLE_MISMATCH")
    expected_prompt = "manager_v2.1" if role == "MANAGER" else "spread_v2.1"
    if raw["prompt_id"] != expected_prompt:
        raise ValueError("UNAPPROVED_PROMPT")
    if not isinstance(raw["model_id"], str) or not re.fullmatch(
        r"[A-Za-z0-9_.:-]{1,160}", raw["model_id"]
    ):
        raise ValueError("INVALID_MODEL")
    if type(raw["max_output_tokens"]) is not int or not 1 <= raw["max_output_tokens"] <= 32768:
        raise ValueError("INVALID_OUTPUT_LIMIT")
    error = raw["retry_error_code"]
    if error not in ("", "SCHEMA", "REFUSAL", "INCOMPLETE", "RATE_LIMIT", "PROVIDER_FAILED"):
        raise ValueError("INVALID_RETRY_CODE")
    manifest = json.loads((bundle / "bundle.json").read_text())
    for rel, expected in manifest["files"].items():
        path = (bundle / rel).resolve()
        if not path.is_relative_to(bundle.resolve()):
            raise ValueError("CONTRACT_PATH_ESCAPE")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("CONTRACT_CHECKSUM_MISMATCH")
    packet_schema = json.loads(
        (bundle / "schemas" / "model_visible_packet.schema.json").read_text()
    )
    validate_shape(raw["packet"], packet_schema)
    screen_packet(raw["packet"])
    prompt = (bundle / "prompts" / f"{prompt_name}.md").read_text()
    schema = json.loads((bundle / "schemas" / f"{raw['schema_name']}.schema.json").read_text())
    if hashlib.sha256(prompt.encode()).hexdigest() != raw["prompt_sha256"]:
        raise ValueError("PROMPT_HASH_MISMATCH")
    if sha(schema) != raw["schema_sha256"]:
        raise ValueError("SCHEMA_HASH_MISMATCH")
    body = request_body(raw, prompt, schema)
    if sha(body) != raw["body_sha256"]:
        raise ValueError("REQUEST_BODY_MISMATCH")
    identity = {
        "system_prompt_id": raw["prompt_id"],
        "packet": raw["packet"],
        "schema_name": raw["schema_name"],
        "model_id": raw["model_id"],
        "max_output_tokens": raw["max_output_tokens"],
        "system_prompt_hash": raw["prompt_sha256"],
        "schema_hash": raw["schema_sha256"],
        "retry_error_code": error,
        "system_text": prompt,
        "output_schema": schema,
    }
    if raw["request_hash"] != "req_" + sha(identity):
        raise ValueError("REQUEST_IDENTITY_MISMATCH")
    return raw, prompt, schema


def validate_shape(value: Any, schema: dict[str, Any]) -> None:
    """The bundled packet schema uses this deliberately small JSON-schema subset.

    No remote references, dynamic code, files named by the request, or network
    schema resolution are supported inside the worker.
    """
    expected = schema.get("type")
    kinds = expected if isinstance(expected, list) else [expected]
    matches = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": type(value) is int,
        "number": type(value) in (int, float),
        "boolean": type(value) is bool,
        "null": value is None,
    }
    if expected and not any(matches.get(k, False) for k in kinds if isinstance(k, str)):
        raise ValueError("INVALID_PACKET_SHAPE")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError("INVALID_PACKET_ENUM")
    if isinstance(value, dict):
        properties = schema.get("properties", {})
        if not set(schema.get("required", [])) <= set(value):
            raise ValueError("MISSING_PACKET_FIELD")
        if schema.get("additionalProperties") is False and set(value) - set(properties):
            raise ValueError("EXTRA_PACKET_FIELD")
        for key, child in value.items():
            if key in properties:
                validate_shape(child, properties[key])
    elif isinstance(value, list):
        for child in value:
            validate_shape(child, schema.get("items", {}))


def screen_packet(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            screen_packet(key)
            screen_packet(child)
    elif isinstance(value, list):
        for child in value:
            screen_packet(child)
    elif isinstance(value, str):
        if re.search(r"https?://|file://|\b(?:19|20)\d{2}-\d{2}-\d{2}\b|\bSPXW?\b", value, re.I):
            raise ValueError("PACKET_IDENTITY_CUE")
        if re.search(r"[/\\][\w.-]+\.(?:json|jsonl|parquet|csv|zip)\b", value, re.I):
            raise ValueError("PACKET_IDENTITY_CUE")


def request_body(raw: dict[str, Any], prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
    content = canonical(raw["packet"]).decode()
    if raw["retry_error_code"]:
        content += "\nRETRY_ERROR_CODE=" + raw["retry_error_code"]
    return {
        "model": raw["model_id"],
        "input": [{"role": "system", "content": prompt}, {"role": "user", "content": content}],
        "text": {
            "format": {
                "type": "json_schema",
                "name": raw["schema_name"],
                "schema": schema,
                "strict": True,
            }
        },
        "max_output_tokens": raw["max_output_tokens"],
        "store": False,
    }
