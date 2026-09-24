"""Unusable provider usage retains the durable reservation after dispatch."""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from spx_research.agents.graphs import logical_decision_id, run_decision
from spx_research.cli.execution import policy_provider
from spx_research.config import Models, Profile
from spx_research.engine.policy import PolicyError
from spx_research.isolation.client import DockerGateway
from spx_research.llm.budget import Budget
from spx_research.llm.openai_gateway import OpenAIGateway
from tests.unit.test_baseline_engine import _profile_dict
from tests.unit.test_llm_pipeline import _spread_ctx
from tests.unit.test_runtime_decisions import _runtime_deps, _sheet

INVALID = [
    None,
    {},
    {"input_tokens": 10},
    {"output_tokens": 5},
    {"input_tokens": -1, "output_tokens": 5},
    {"input_tokens": 10, "output_tokens": "5"},
    {"input_tokens": 10, "output_tokens": True},
    {"input_tokens": 10, "output_tokens": 5, "input_tokens_details": {"cached_tokens": 20}},
    {"input_tokens": 10, "output_tokens": 5, "input_tokens_details": {"cached_tokens": -1}},
    {"input_tokens": 10, "output_tokens": 5, "input_tokens_details": {"cached_tokens": None}},
    {"input_tokens": 10, "output_tokens": 5, "input_tokens_details": []},
]


@pytest.mark.parametrize("transport", ["sdk", "docker"])
@pytest.mark.parametrize("usage", INVALID)
def test_invalid_usage_retains_reservation_and_raw_evidence(
    tmp_path, monkeypatch, transport, usage
):
    sheet = _sheet()
    calls = []
    if transport == "sdk":

        class Client:
            def __init__(self):
                self.responses = self

            def create(self, **kwargs):
                calls.append(kwargs)
                sdk_usage = SimpleNamespace(**usage) if isinstance(usage, dict) else usage
                if isinstance(usage, dict) and isinstance(usage.get("input_tokens_details"), dict):
                    sdk_usage.input_tokens_details = SimpleNamespace(
                        **usage["input_tokens_details"]
                    )
                return SimpleNamespace(status="incomplete", output_text="", usage=sdk_usage)

        gateway = OpenAIGateway(Client(), "mock-1", sheet, allow_real_calls=True)
    else:

        def docker(command, **kwargs):
            if command[1:3] == ["image", "inspect"]:
                return SimpleNamespace(stdout="sha256:" + "0" * 64)
            calls.append(kwargs)
            request = json.loads(kwargs["input"])
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "request_hash": request["request_hash"],
                        "provider": {
                            "id": "offline-provider-stub",
                            "status": "incomplete",
                            "model": "mock-1",
                            "usage": usage,
                            "output": [],
                        },
                    }
                )
            )

        monkeypatch.setattr("spx_research.isolation.client.subprocess.run", docker)
        gateway = DockerGateway(image="fixture", socket_volume="fixture", sheets={"mock-1": sheet})
    deps = _runtime_deps(tmp_path, gateway, Budget(Decimal(1), sheet))
    ctx = _spread_ctx()
    with pytest.raises(PolicyError, match="INVALID_USAGE"):
        run_decision(deps, ctx)
    totals = deps.runtime.budget_totals("run-1")
    assert totals["committed"] == 0 and totals["reserved"] == Decimal(".0328")
    attempts = deps.runtime.list_attempts("run-1", logical_decision_id(deps, ctx))
    assert len(attempts) == 1 and attempts[0].actual_usd is None
    assert attempts[0].outcome == "BILLING_UNCERTAIN"
    response = attempts[0].response["model_response"]
    assert response["error_code"] == "INVALID_USAGE" and response["billing_uncertain"] is True
    assert response["provider_metadata"]["raw_usage"] == usage
    with pytest.raises(PolicyError, match="BILLING_UNCERTAIN"):
        run_decision(deps, ctx)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "provider,interface", [("anthropic", "responses"), ("openai", "chat_completions")]
)
def test_paid_cli_rejects_contradictory_provider_interface(tmp_path, provider, interface):
    profile = Profile.model_validate(_profile_dict())
    profile.models = Models(provider=provider, interface=interface)
    with pytest.raises(ValueError, match="UNSUPPORTED_PROVIDER_INTERFACE"):
        policy_provider(
            policy="llm",
            profile=profile,
            run_id="run",
            branch_id="main",
            tape_path=tmp_path / "tape.jsonl",
            model_id=None,
            budget_usd=None,
            price_in=None,
            price_out=None,
            manifest_id="fixture",
            ledger=None,
        )
