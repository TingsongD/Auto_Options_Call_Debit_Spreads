"""M4/H-07: model gateway + decision-tape + LangGraph pipeline tests.

Covers: end-to-end spread/manager decisions via MockGateway, tape persistence
and crash-recovery reuse (T41/T45), bounded retries with quarantine and no
prose leak (TK27), budget reservation (T37), request-hash determinism, and the
real-call gate on OpenAIGateway.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from spx_research.agents.graphs import PolicyDeps, run_decision
from spx_research.agents.llm_policy import LLMPolicy
from spx_research.domain.state import Agent, AgentState
from spx_research.domain.types import Contract, CreditSpread, Direction, PricePoints, Right
from spx_research.engine.policy import (
    DecisionContext,
    LimitTemplate,
    ManagerView,
    PolicyError,
    SpreadView,
)
from spx_research.epistemics.harness import Harness
from spx_research.epistemics.store import InMemoryObservationLedger
from spx_research.llm.budget import Budget, PriceSheet
from spx_research.llm.gateway import MockGateway, TapeGateway
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import ModelError, ModelRequest, ModelResponse

T0 = datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
KEY = b"pipeline-test-secret-32bytes......"


def _leg(right: Right, strike: int, cid: str) -> Contract:
    return Contract(
        contract_id=cid,
        root="SPXW",
        right=right,
        strike_points=PricePoints(Decimal(strike)),
        expiration_local_date=__import__("datetime").date(2024, 2, 16),
        exercise_style="EUROPEAN",
        settlement_style="PM",
        multiplier=100,
        price_increment=Decimal("0.05"),
    )


def _cand(cid: str = "cand:1") -> Any:
    from spx_research.features.candidates import Candidate

    spread = CreditSpread(
        _leg(Right.PUT, 4700, "s1"), _leg(Right.PUT, 4675, "l1"), Direction.BULL_PUT_CREDIT
    )
    return Candidate(
        cid,
        spread,
        Direction.BULL_PUT_CREDIT,
        45,
        Decimal("2"),
        Decimal("2300"),
        Decimal("2510"),
        Decimal("0.3"),
        (),
    )


def _spread_ctx(with_candidate: bool = True) -> DecisionContext:
    agent = Agent("a1", "SPREAD", Direction.BULL_PUT_CREDIT, AgentState.SEEKING_ENTRY, T0, T0)
    cands = (_cand(),) if with_candidate else ()
    tpls = (LimitTemplate("entry:cand:1", "NATURAL", Decimal("2")),) if with_candidate else ()
    view = SpreadView(agent, None, None, None, 0, cands, tpls, ())
    return DecisionContext("run-1", "main", "a1", "SPREAD", T0, 0, 30, spread_view=view)


def _manager_ctx() -> DecisionContext:
    view = ManagerView(
        as_of_utc=T0,
        active_bullish=0,
        active_bearish=0,
        reserved_bullish=0,
        reserved_bearish=0,
        capacity=3,
        bullish_target=2,
        bearish_target=1,
        paused=False,
        available_usd=Decimal("10000"),
        reservations=(),
    )
    return DecisionContext("run-1", "main", "manager-1", "MANAGER", T0, 0, 30, manager_view=view)


def _deps(
    tmp: Any, gateway: Any = None, budget: Budget | None = None, max_retries: int = 2
) -> PolicyDeps:
    return PolicyDeps(
        harness=Harness(KEY),
        ledger=InMemoryObservationLedger(),
        gateway=gateway or MockGateway(),
        tape=DecisionTape(tmp / "tape.jsonl"),
        profile=None,  # type: ignore[arg-type]
        budget=budget,
        max_retries=max_retries,
        model_id="mock-1",
    )


def test_mock_pipeline_wait(tmp_path: Any) -> None:
    deps = _deps(tmp_path)
    proposal, witness = run_decision(deps, _spread_ctx(with_candidate=False))
    assert proposal.kind == "WAIT"
    assert witness["status"] == "ACCEPTED"
    assert witness["parametric_ignorance_proven"] is False


def test_mock_pipeline_open(tmp_path: Any) -> None:
    deps = _deps(tmp_path, MockGateway(prefer_kind="OPEN"))
    proposal, _w = run_decision(deps, _spread_ctx())
    assert proposal.kind == "OPEN"
    assert proposal.candidate_id == "cand:1"
    assert proposal.limit_template_id == "entry:cand:1"


def test_mock_manager_allocate(tmp_path: Any) -> None:
    deps = _deps(tmp_path, MockGateway(prefer_kind="ALLOCATE"))
    proposal, _ = run_decision(deps, _manager_ctx())
    assert proposal.kind == "ALLOCATE"
    assert proposal.allocation == {"bullish": 2, "bearish": 1}


def test_tape_persistence_and_replay(tmp_path: Any) -> None:
    deps = _deps(tmp_path, MockGateway(prefer_kind="OPEN"))
    run_decision(deps, _spread_ctx())
    assert len(deps.tape) == 1
    # crash-recovery: new tape object over same file; TapeGateway replays
    deps2 = PolicyDeps(
        harness=deps.harness,
        ledger=InMemoryObservationLedger(),
        gateway=TapeGateway(DecisionTape(tmp_path / "tape.jsonl")),
        tape=DecisionTape(tmp_path / "tape.jsonl"),
        profile=None,
        max_retries=2,
        model_id="mock-1",  # type: ignore[arg-type]
    )
    proposal, _ = run_decision(deps2, _spread_ctx())
    assert proposal.kind == "OPEN"


def test_request_hash_deterministic(tmp_path: Any) -> None:
    deps = _deps(tmp_path)
    run_decision(deps, _spread_ctx())
    req = next(iter(deps.tape._records.values())).request_hash
    assert req.startswith("req_") and len(req) == 28


def test_retry_on_invalid_response_then_quarantine(tmp_path: Any) -> None:
    class BadThenGood:
        def __init__(self) -> None:
            self.calls = 0
            self.good = MockGateway()

        def complete(
            self, req: ModelRequest, system_text: str = "", schema: dict | None = None
        ) -> ModelResponse:
            self.calls += 1
            if self.calls == 1:
                bad = dict(self.good.complete(req).parsed)
                bad["action_id"] = "act_forged_future_hint"
                return ModelResponse(
                    req.request_hash(), json.dumps(bad), bad, "mock-1", 0, 0, Decimal(0)
                )
            return self.good.complete(req)

    deps = _deps(tmp_path, BadThenGood())
    proposal, _ = run_decision(deps, _spread_ctx(with_candidate=False))
    assert proposal.kind == "WAIT"
    incidents = deps.ledger.incidents("run-1")
    assert len(incidents) == 1
    assert "UNKNOWN_ACTION" in incidents[0].code


def test_persistent_failure_raises_policy_error(tmp_path: Any) -> None:
    class AlwaysBad:
        def complete(
            self, req: ModelRequest, system_text: str = "", schema: dict | None = None
        ) -> ModelResponse:
            bad = {"action_id": "nope"}
            return ModelResponse(
                req.request_hash(), json.dumps(bad), bad, "mock-1", 0, 0, Decimal(0)
            )

    deps = _deps(tmp_path, AlwaysBad(), max_retries=1)
    with pytest.raises(PolicyError):
        run_decision(deps, _spread_ctx(with_candidate=False))
    assert len(deps.ledger.incidents("run-1")) == 2  # initial + 1 retry


def test_budget_exceeded_blocks_call(tmp_path: Any) -> None:
    sheet = PriceSheet("ps-1", "mock-1", Decimal("10"), Decimal("30"))
    budget = Budget(Decimal("0.000001"), sheet)
    deps = _deps(tmp_path, MockGateway(), budget=budget)
    with pytest.raises(PolicyError) as e:
        run_decision(deps, _spread_ctx(with_candidate=False))
    assert "BUDGET" in e.value.code or "NO_PROPOSAL" in e.value.code
    # no response was taped — the call never happened
    assert len(deps.tape) == 0


def test_openai_gateway_gate(tmp_path: Any) -> None:
    from spx_research.llm.openai_gateway import OpenAIGateway

    with pytest.raises(ModelError, match="REAL_CALLS_DISABLED"):
        OpenAIGateway(
            client=None, model_id="gpt-x", sheet=PriceSheet("ps", "gpt-x", Decimal(0), Decimal(0))
        )


def test_llm_policy_adapter(tmp_path: Any) -> None:
    deps = _deps(tmp_path)
    policy = LLMPolicy(deps)
    p = policy.decide(_spread_ctx(with_candidate=False))
    assert p.kind == "WAIT"


def test_engine_with_llm_policy_end_to_end(tmp_path: Any) -> None:
    """M4-06 shape: the scheduler drives LLMPolicy over a synthetic session;
    every decision goes through packet -> gateway -> witness -> resolve."""
    from datetime import date, timedelta

    from spx_research.config import Profile
    from spx_research.data.availability import Archive
    from spx_research.data.synthetic import SyntheticSpec, generate
    from spx_research.engine.scheduler import Engine
    from spx_research.persistence.events import InMemoryEventStore
    from spx_research.temporal.calendar import build_weekday_manifest
    from tests.unit.test_baseline_engine import _profile_dict

    start = date(2024, 1, 2)
    end = date(2024, 1, 3)
    cal = build_weekday_manifest("cal-llm", start, end + timedelta(days=60))
    root = tmp_path / "ds"
    generate(
        root,
        SyntheticSpec(
            "llm-smoke",
            11,
            start,
            end,
            expiries=(date(2024, 2, 16),),
            strike_step=Decimal("25"),
            strikes_each_side=24,
        ),
        cal,
    )
    profile = Profile.model_validate(_profile_dict())
    deps = PolicyDeps(
        harness=Harness(KEY),
        ledger=InMemoryObservationLedger(),
        gateway=MockGateway(),
        tape=DecisionTape(tmp_path / "tape.jsonl"),
        profile=profile,
        max_retries=1,
        model_id="mock-1",
    )
    engine = Engine(
        profile, cal, Archive(root / "llm-smoke"), InMemoryEventStore(), lambda _r: LLMPolicy(deps)
    )
    result = engine.run(start, end)
    assert len(result.events) > 10
    kinds = {e.payload["kind"] for e in result.events if e.type == "DECISION_MADE"}
    assert "ALLOCATE" in kinds  # manager allocated through the pipeline
    assert len(deps.tape) > 0  # every accepted decision was taped
