"""Recovery, billing, and blinded-economic-context regressions; no provider calls."""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from spx_research.agents.graphs import logical_decision_id, run_decision
from spx_research.agents.llm_policy import LLMPolicy
from spx_research.engine.policy import PolicyError
from spx_research.epistemics.harness import Harness
from spx_research.epistemics.producers import manager_atoms, spread_atoms
from spx_research.epistemics.types import HarnessError
from spx_research.llm.budget import Budget, PriceSheet
from spx_research.llm.gateway import MockGateway, TapeGateway
from spx_research.llm.openai_gateway import OpenAIGateway
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import ModelError, ModelRequest, ModelResponse
from spx_research.persistence.runtime import InMemoryRunStore
from tests.unit.test_llm_pipeline import KEY, T0, _deps, _manager_ctx, _spread_ctx


@pytest.mark.parametrize("equity", [None, Decimal(0), Decimal(-100)])
def test_missing_or_nonpositive_equity_keeps_management_available(tmp_path, equity):
    ctx = _manager_ctx()
    view = replace(ctx.manager_view, equity_usd=equity)
    assert not any(a.metric == "available_risk_fraction" for a in manager_atoms(view, T0))
    result = run_decision(
        _runtime_deps(tmp_path, MockGateway(prefer_kind="NO_CHANGE")),
        replace(ctx, manager_view=view),
    )
    assert result[0].kind == "NO_CHANGE"


def _runtime_deps(tmp_path: Path, gateway: Any = None, budget: Budget | None = None) -> Any:
    deps = _deps(tmp_path, gateway, budget)
    runtime = InMemoryRunStore()
    runtime.begin_run("run-1", {"format_version": 2, "profile_id": "fixture"})
    deps.runtime = runtime
    deps.ledger = runtime.observation_ledger
    return deps


def _sheet() -> PriceSheet:
    return PriceSheet("fixture", "mock-1", Decimal(1), Decimal(1), model_context_limit=32000)


def test_bound_covers_cached_input_even_if_its_frozen_rate_is_higher():
    sheet = replace(_sheet(), cached_input_per_million=Decimal(3))
    req = ModelRequest("prompt", {}, "schema", "mock-1", max_output_tokens=800)
    assert sheet.bound(req) == sheet.cost(32000, 800, 32000)
    assert sheet.bound(req) > sheet.cost(32000, 800, 0)


@pytest.mark.parametrize("has_response", [False, True])
def test_audited_billing_reconciliation_reuses_saved_response_or_fixed_retry(
    tmp_path, has_response
):
    class UnknownBilling(MockGateway):
        def complete(self, req, *args, **kwargs):
            if self.calls == 0:
                if has_response:
                    return replace(super().complete(req, *args, **kwargs), billing_uncertain=True)
                self.calls += 1
                raise ModelError("TIMEOUT", billing_uncertain=True)
            return super().complete(req, *args, **kwargs)

    gateway = UnknownBilling()
    deps = _runtime_deps(tmp_path, gateway, Budget(Decimal(1), _sheet()))
    ctx = _spread_ctx()
    with pytest.raises(PolicyError, match="BILLING_UNCERTAIN"):
        run_decision(deps, ctx)
    did = logical_decision_id(deps, ctx)
    attempt = deps.runtime.list_attempts(ctx.run_id, did)[0]
    deps.runtime.reconcile_attempt(
        ctx.run_id, attempt.attempt_id, actual_usd=Decimal(".01"), evidence_ref="fixture-receipt"
    )
    run_decision(replace(deps, prepared_cache={}), ctx)
    attempts = deps.runtime.list_attempts(ctx.run_id, did)
    assert deps.runtime.budget_totals(ctx.run_id)["committed"] == Decimal(".01")
    if has_response:
        assert gateway.calls == len(attempts) == 1
        assert attempts[0].response["model_response"]["billing_uncertain"] is True
        accepted = deps.runtime.load_decision(ctx.run_id, did).result["response"]
        assert accepted["billing_uncertain"] is False
        assert Decimal(accepted["cost_usd"]) == Decimal(".01")
    else:
        assert gateway.calls == len(attempts) == 2
        assert attempts[1].request["retry_error_code"] == "PROVIDER_FAILED"


def test_same_barrier_keeps_exact_request_after_observation_commit(tmp_path: Path) -> None:
    deps = _runtime_deps(tmp_path)
    policy = LLMPolicy(deps)
    ctx = _spread_ctx()
    first = policy.decide(ctx)
    staged = policy.staged_observations(ctx)
    assert deps.ledger.deliveries("run-1", "main", "a1") == []
    deps.runtime.commit_barrier("run-1", "fixture-barrier", 0, [], {}, observations=staged)
    policy.finalize(ctx)
    restarted = replace(deps, prepared_cache={})
    second = LLMPolicy(restarted).decide(ctx)
    assert first == second
    assert deps.gateway.calls == 1
    assert len(deps.tape) == 1
    assert len(deps.runtime.list_attempts("run-1", logical_decision_id(deps, ctx))) == 1


def test_same_identity_with_changed_context_is_rejected(tmp_path: Path) -> None:
    deps = _runtime_deps(tmp_path)
    ctx = _spread_ctx()
    run_decision(deps, ctx)
    changed = replace(ctx, spread_view=replace(ctx.spread_view, days_held=25))
    with pytest.raises(PolicyError, match="DECISION_CONTEXT_MISMATCH"):
        run_decision(deps, changed)
    assert deps.gateway.calls == 1


class RateLimitThenGood(MockGateway):
    def complete(
        self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
    ) -> ModelResponse:
        if self.calls == 0:
            self.calls += 1
            raise ModelError("RATE_LIMIT")
        return super().complete(req, system_text, schema)


def test_retry_success_replays_without_recreating_failed_attempt(tmp_path: Path) -> None:
    deps = _runtime_deps(tmp_path, RateLimitThenGood())
    ctx = _spread_ctx()
    result = run_decision(deps, ctx)
    restarted = replace(deps, prepared_cache={}, tape=DecisionTape(tmp_path / "tape.jsonl"))
    assert run_decision(restarted, ctx) == result
    assert deps.gateway.calls == 2
    records = list(deps.tape._records.values())
    assert "RETRY_ERROR_CODE=RATE_LIMIT" in records[0].request["body"]["input"][1]["content"]
    offline = replace(
        deps,
        runtime=None,
        ledger=InMemoryRunStore().observation_ledger,
        gateway=TapeGateway(restarted.tape),
        prepared_cache={},
    )
    assert run_decision(offline, ctx) == result


def test_response_survives_crash_before_validation(tmp_path: Path) -> None:
    deps = _runtime_deps(tmp_path)
    ctx = _spread_ctx()

    def crash(*args: Any) -> Any:
        raise RuntimeError("power loss after response storage")

    deps.harness.validate = crash
    with pytest.raises(RuntimeError, match="power loss"):
        run_decision(deps, ctx)
    restarted = replace(deps, harness=Harness(KEY), prepared_cache={})
    run_decision(restarted, ctx)
    assert deps.gateway.calls == 1


def test_crash_after_dispatch_retains_reservation_without_redispatch(tmp_path: Path) -> None:
    deps = _runtime_deps(tmp_path, budget=Budget(Decimal(".1"), _sheet()))
    ctx = _spread_ctx()
    complete = deps.runtime.complete_attempt

    def crash(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("power loss before outcome storage")

    deps.runtime.complete_attempt = crash
    with pytest.raises(RuntimeError, match="power loss"):
        run_decision(deps, ctx)
    deps.runtime.complete_attempt = complete
    with pytest.raises(PolicyError, match="BILLING_UNCERTAIN"):
        run_decision(replace(deps, prepared_cache={}), ctx)
    assert deps.gateway.calls == 1
    totals = deps.runtime.budget_totals("run-1")
    assert totals["reserved"] == Decimal(".0328")
    assert totals["committed"] == 0


class IncompleteClient:
    def __init__(self) -> None:
        self.responses = self
        self.calls = 0

    def create(self, **kwargs: Any) -> Any:
        self.calls += 1
        return SimpleNamespace(
            status="incomplete",
            output_text="",
            usage=SimpleNamespace(
                input_tokens=4096,
                output_tokens=800,
                input_tokens_details=SimpleNamespace(cached_tokens=0),
            ),
        )


def test_incomplete_usage_is_charged_before_retry_budget_check(tmp_path: Path) -> None:
    client = IncompleteClient()
    sheet = _sheet()
    gateway = OpenAIGateway(client, "mock-1", sheet, allow_real_calls=True)
    deps = _runtime_deps(tmp_path, gateway, Budget(Decimal(".033"), sheet))
    with pytest.raises(PolicyError, match="BUDGET_EXCEEDED"):
        run_decision(deps, _spread_ctx())
    assert client.calls == 1
    totals = deps.runtime.budget_totals("run-1")
    assert totals["committed"] == Decimal(".004896")
    assert totals["reserved"] == 0
    assert len(deps.tape) == 0


def test_uncertain_transport_never_releases_or_repeats_attempt(tmp_path: Path) -> None:
    class TimedOut(MockGateway):
        def complete(self, *args: Any, **kwargs: Any) -> ModelResponse:
            self.calls += 1
            raise ModelError("TIMEOUT", billing_uncertain=True)

    deps = _runtime_deps(tmp_path, TimedOut(), Budget(Decimal(".1"), _sheet()))
    for _ in range(2):
        with pytest.raises(PolicyError, match="BILLING_UNCERTAIN"):
            run_decision(replace(deps, prepared_cache={}), _spread_ctx())
    assert deps.gateway.calls == 1
    assert deps.runtime.budget_totals("run-1")["reserved"] == Decimal(".0328")


def test_full_context_reservation_uses_requested_models_price() -> None:
    small = _sheet()
    costly = PriceSheet("other", "expensive", Decimal(10), Decimal(20), model_context_limit=64000)
    budget = Budget(Decimal(1), small, {"mock-1": small, "expensive": costly})
    req = ModelRequest("p", {}, "s", "expensive")
    assert budget.price_for(req.model_id).bound(req) == Decimal(".656")
    with pytest.raises(ModelError, match="PRICE_MODEL_MISMATCH"):
        small.bound(req)
    with pytest.raises(ModelError, match="MISSING_MODEL_CONTEXT_LIMIT"):
        PriceSheet("missing", "expensive", Decimal(1), Decimal(1)).bound(req)


def test_gateway_retains_usage_and_counts_cached_input_once() -> None:
    class Client:
        responses: Any

        def __init__(self) -> None:
            self.responses = self

        def create(self, **kwargs: Any) -> Any:
            return SimpleNamespace(
                status="completed",
                output_text="not json",
                usage=SimpleNamespace(
                    input_tokens=1000,
                    output_tokens=100,
                    input_tokens_details=SimpleNamespace(cached_tokens=600),
                ),
            )

    sheet = PriceSheet("p", "m", Decimal(10), Decimal(20), Decimal(2), model_context_limit=32000)
    gateway = OpenAIGateway(Client(), "m", sheet, allow_real_calls=True)
    result = gateway.complete(ModelRequest("p", {}, "s", "m"))
    assert result.error_code == "SCHEMA"
    assert result.cost_usd == Decimal(".0072")


def test_risk_and_holding_context_are_economic_not_identity_cues(tmp_path: Path) -> None:
    deps = _deps(tmp_path)
    ctx = _spread_ctx()
    run_decision(deps, ctx)
    packet = next(iter(deps.tape._records.values())).request["packet"]
    risk = next(p for p in packet["premises"] if p["metric"] == "candidate_max_risk")
    assert risk["value"] == "0.23" and risk["unit"] == "fraction_of_equity"
    metrics = {p["metric"]: p["value"] for p in packet["premises"]}
    assert metrics["direction_mandate"] == "BULL_PUT_CREDIT"
    assert metrics["loss_activation_days"] == "25"
    entry = next(m for m in packet["action_menu"] if m["kind"] == "OPEN")
    attrs = {a["name"]: a["value"] for a in entry["attributes"]}
    assert attrs["dte"] == "45" and attrs["credit_to_width"] == "0.08"
    changed = spread_atoms(replace(ctx.spread_view, days_held=25), T0)
    assert next(a.value for a in changed if a.metric == "days_held") == "25"
    with pytest.raises(HarnessError, match="MISSING_POSITIVE_EQUITY"):
        spread_atoms(replace(ctx.spread_view, equity_usd=None), T0)


def test_tape_is_sealed_and_legacy_and_torn_files_stay_unchanged(tmp_path: Path) -> None:
    deps = _deps(tmp_path)
    run_decision(deps, _spread_ctx())
    path = tmp_path / "tape.jsonl"
    raw = path.read_text()
    assert json.loads(raw.splitlines()[-1])["type"] == "SEAL"
    assert not DecisionTape(path).incomplete
    path.write_text("\n".join(raw.splitlines()[:-1]) + "\n")
    incomplete = DecisionTape(path)
    assert incomplete.incomplete
    with pytest.raises(ValueError, match="READ_ONLY"):
        incomplete.export()
    legacy = tmp_path / "legacy.jsonl"
    record = json.loads(raw.splitlines()[0])
    for key in (
        "format_version",
        "record_hash",
        "model_request",
        "prepared",
        "result",
        "decision_id",
    ):
        record.pop(key, None)
    legacy.write_text(json.dumps(record) + "\n")
    original = legacy.read_bytes()
    old = DecisionTape(legacy)
    assert old.legacy and len(old) == 1
    with pytest.raises(ValueError, match="READ_ONLY"):
        old.export()
    assert legacy.read_bytes() == original
    empty = DecisionTape(tmp_path / "empty.jsonl")
    empty.export()
    assert not DecisionTape(empty.path).incomplete


def test_atom_identity_conflicts_fail_closed() -> None:
    ledger = InMemoryRunStore().observation_ledger
    atom = spread_atoms(_spread_ctx(False).spread_view, T0)[0]
    ledger.put_atom(atom)
    with pytest.raises(HarnessError, match="ATOM_IDENTITY_CONFLICT"):
        ledger.put_atom(replace(atom, value="999"))


def test_assessments_and_observations_commit_together(tmp_path: Path) -> None:
    from tests.unit.test_llm_pipeline import _AssessingGateway

    deps = _runtime_deps(tmp_path, _AssessingGateway())
    policy = LLMPolicy(deps)
    ctx = _spread_ctx(False)
    policy.decide(ctx)
    assert deps.ledger.assessments("run-1", "main", "a1") == []
    deps.runtime.commit_barrier(
        "run-1",
        "barrier",
        0,
        [],
        {},
        assessments=[asdict(a) for a in policy.staged_assessments(ctx)],
        observations=policy.staged_observations(ctx),
    )
    assert len(deps.ledger.assessments("run-1", "main", "a1")) == 1
    assert deps.ledger.deliveries("run-1", "main", "a1")


def test_shared_public_alias_preserves_exact_requests_across_private_runs(tmp_path: Path) -> None:
    left = _deps(tmp_path / "left")
    right = _deps(tmp_path / "right")
    left.public_alias_namespace = right.public_alias_namespace = "comparison-v2.1"
    ctx = _spread_ctx()
    run_decision(left, ctx)
    run_decision(right, replace(ctx, run_id="private-run-other", branch_id="private-fork"))
    a = next(iter(left.tape._records.values()))
    b = next(iter(right.tape._records.values()))
    assert a.request["body"] == b.request["body"]
    assert a.request_hash == b.request_hash
    assert a.decision_id != b.decision_id


def test_export_crash_keeps_previous_complete_tape(tmp_path: Path, monkeypatch: Any) -> None:
    import spx_research.llm.tape as tape_module

    deps = _deps(tmp_path)
    run_decision(deps, _spread_ctx(False))
    before = deps.tape.path.read_bytes()

    def crash(*args: Any) -> Any:
        raise OSError("interrupted atomic replace")

    monkeypatch.setattr(tape_module.os, "replace", crash)
    with pytest.raises(OSError, match="atomic replace"):
        deps.tape.export()
    assert deps.tape.path.read_bytes() == before
    assert len(DecisionTape(deps.tape.path)) == 1
    assert list(tmp_path.glob(".tape-*")) == []


def test_malformed_typed_assessment_is_quarantined_not_a_type_crash(tmp_path: Path) -> None:
    class BadType(MockGateway):
        def complete(
            self, req: ModelRequest, system_text: str = "", schema: dict[str, Any] | None = None
        ) -> ModelResponse:
            result = super().complete(req, system_text, schema)
            bad = {**result.parsed, "confidence_label": []}
            return replace(result, parsed=bad, text=json.dumps(bad))

    deps = _runtime_deps(tmp_path, BadType())
    with pytest.raises(PolicyError, match="EPISTEMIC:INVALID_CONFIDENCE"):
        run_decision(deps, _spread_ctx(False))
    assert deps.gateway.calls == 1
    assert len(deps.ledger.incidents("run-1")) == 1
