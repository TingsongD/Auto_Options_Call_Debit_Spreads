"""LangGraph decision pipeline for manager and spread actors (M4-04 / H-07).

Each decision is a fresh single-pass graph over local state: prepare the
deliveries + blinded packet, call the gateway (tape-first), validate the
structured proposal, resolve the menu token to internal ids. Bounded retries
on validation failure reuse the same packet — rejected prose never re-enters
context (it is quarantined to the private incident vault).

Checkpoints are per (run, branch, actor, decision barrier) via an in-memory
saver for M4; the durable checkpointer arrives with the Postgres milestone.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from spx_research.config import Profile
from spx_research.engine.policy import DecisionContext, PolicyError, Proposal
from spx_research.epistemics.harness import Harness, digest
from spx_research.epistemics.producers import (
    compile_for_actor,
    make_context,
    manager_atoms,
    manager_menu,
    spread_atoms,
    spread_menu,
)
from spx_research.epistemics.reducer import reduce_belief
from spx_research.epistemics.store import (
    AssessmentRecord,
    Incident,
    InMemoryObservationLedger,
    ObservationLedger,
)
from spx_research.epistemics.types import (
    Atom,
    Compiled,
    Context,
    HarnessError,
    MenuChoice,
)
from spx_research.llm.budget import Budget
from spx_research.llm.gateway import ModelGateway, RecordedDecisionGateway
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import (
    ModelError,
    ModelRequest,
    ModelResponse,
    response_dict,
    response_from_dict,
)
from spx_research.persistence.events import LedgerError


@dataclass
class PolicyDeps:
    harness: Harness
    ledger: ObservationLedger
    gateway: ModelGateway
    tape: DecisionTape
    profile: Profile
    budget: Budget | None = None
    max_retries: int = 2
    max_output_tokens: int = 800
    model_id: str = "mock-1"
    model_ids: dict[str, str] | None = None  # per-role pin overrides model_id
    retry_backoff_seconds: float = 0.0  # pause between failed attempts
    private_manifest_id: str = "local"
    system_prompts: dict[str, str] | None = None  # role -> prompt text
    schemas: dict[str, dict[str, Any]] | None = None  # schema_name -> schema
    runtime: Any = None
    public_alias_namespace: str | None = None
    prepared_cache: dict[str, dict[str, Any]] = field(default_factory=dict)


class DecisionRun(TypedDict, total=False):
    ctx: DecisionContext
    hctx: Context
    atoms: list[Atom]
    menu: list[MenuChoice]
    compiled: Compiled
    request: ModelRequest
    response: ModelResponse
    witness: dict[str, Any]
    proposal: Proposal
    pending_assessments: list[AssessmentRecord]
    error: str
    attempts: int
    decision_id: str
    prepared: dict[str, Any]
    observations: dict[str, Any]
    fatal: bool


def _schema_name(role: str) -> str:
    return "spread_decision" if role == "SPREAD" else "manager_decision"


def _plain(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def logical_decision_id(deps: PolicyDeps, ctx: DecisionContext) -> str:
    policy = deps.profile.model_dump(mode="json") if deps.profile is not None else {}
    return "decision_" + digest(
        [
            ctx.run_id,
            ctx.branch_id,
            ctx.actor_id,
            ctx.role,
            ctx.as_of_utc.isoformat(),
            getattr(ctx, "base_ledger_seq", 0),
            getattr(ctx, "base_ledger_hash", "genesis"),
            policy,
            deps.system_prompts,
            deps.schemas,
            deps.model_ids,
            deps.model_id,
            deps.public_alias_namespace,
        ]
    )


def _atom_restore(raw: dict[str, Any]) -> Atom:
    data = dict(raw)
    for key in ("published_at", "available_at", "subject_at"):
        data[key] = datetime.fromisoformat(data[key])
    data["recipients"] = tuple(data["recipients"])
    data["dependencies"] = tuple(data["dependencies"])
    return Atom(**data)


def _restore(prepared: dict[str, Any]) -> dict[str, Any]:
    raw = prepared["compiled"]
    cr = dict(raw["context"])
    cr["as_of"] = datetime.fromisoformat(cr["as_of"])
    context = Context(**cr)
    menu = {
        key: MenuChoice(
            **{
                **value,
                "required_atoms": tuple(value["required_atoms"]),
                "attributes": tuple(tuple(a) for a in value.get("attributes", [])),
            }
        )
        for key, value in raw["action_map"].items()
    }
    compiled = Compiled(
        raw["public"],
        context,
        {key: _atom_restore(value) for key, value in raw["premise_map"].items()},
        menu,
    )
    return {
        "hctx": context,
        "compiled": compiled,
        "request": ModelRequest(**prepared["model_request"]),
        "observations": prepared["observations"],
        "prepared": prepared,
    }


def _prepare(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    ctx = state["ctx"]
    decision_id = logical_decision_id(deps, ctx)
    context_hash = digest(_plain(asdict(ctx)))
    record = deps.runtime.load_decision(ctx.run_id, decision_id) if deps.runtime else None
    taped = deps.tape.decision(decision_id)
    saved = (
        record.request
        if record is not None
        else taped.prepared
        if taped is not None
        else deps.prepared_cache.get(decision_id)
    )
    if saved is not None:
        if saved["context_hash"] != context_hash:
            raise PolicyError("DECISION_CONTEXT_MISMATCH")
        deps.prepared_cache[decision_id] = saved
        restored = _restore(saved)
        restored.update({"decision_id": decision_id, "attempts": 0})
        return restored  # type: ignore[return-value]
    if ctx.role == "SPREAD":
        assert ctx.spread_view is not None
        atoms = spread_atoms(ctx.spread_view, ctx.as_of_utc)
        menu = spread_menu(ctx.spread_view, atoms)
    else:
        assert ctx.manager_view is not None
        atoms = manager_atoms(ctx.manager_view, ctx.as_of_utc)
        menu = manager_menu(ctx.manager_view, atoms)
    hctx = make_context(
        ctx.run_id,
        ctx.branch_id,
        ctx.actor_id,
        ctx.role,
        ctx.as_of_utc,
        ctx.session_index,
        ctx.minute_from_open,
        deps.private_manifest_id,
    )
    if deps.public_alias_namespace is not None:
        hctx = replace(hctx, alias_namespace=deps.public_alias_namespace)
    prior = reduce_belief(deps.ledger, deps.harness, hctx)
    overlay = InMemoryObservationLedger()
    for atom in deps.ledger.atoms().values():
        overlay.put_atom(atom)
    for delivery in deps.ledger.deliveries(ctx.run_id, ctx.branch_id, ctx.actor_id):
        overlay.deliver(
            delivery.run_id,
            delivery.branch_id,
            delivery.actor_id,
            delivery.atom_id,
            delivery.delivered_at,
        )
    for assessment in deps.ledger.assessments(ctx.run_id, ctx.branch_id, ctx.actor_id):
        overlay.put_assessment(assessment)
    deliveries = []
    for atom in atoms:
        overlay.put_atom(atom)
        deliveries.append(
            overlay.deliver(ctx.run_id, ctx.branch_id, ctx.actor_id, atom.atom_id, ctx.as_of_utc)
        )
    compiled = compile_for_actor(
        overlay,
        deps.harness,
        hctx,
        menu,
        premise_ids={a.atom_id for a in atoms},
        prior_belief_hash=prior.belief_hash,
    )
    schema_name = _schema_name(ctx.role)
    schema = (deps.schemas or {}).get(schema_name)
    system_text = (deps.system_prompts or {}).get(ctx.role.lower(), "")
    req = ModelRequest(
        system_prompt_id=f"{ctx.role.lower()}_v2.1",
        packet=compiled.public,
        schema_name=schema_name,
        model_id=(deps.model_ids or {}).get(ctx.role.lower(), deps.model_id),
        max_output_tokens=deps.max_output_tokens,
        system_prompt_hash=hashlib.sha256(system_text.encode()).hexdigest(),
        schema_hash=digest(schema),
        system_text=system_text,
        output_schema=schema,
    )
    prepared = _plain(
        {
            "context_hash": context_hash,
            "compiled": asdict(compiled),
            "model_request": asdict(req),
            "observations": {
                "atoms": [asdict(a) for a in atoms],
                "deliveries": [asdict(d) for d in deliveries],
            },
        }
    )
    if deps.runtime is not None:
        deps.runtime.prepare_decision(ctx.run_id, decision_id, prepared)
    deps.prepared_cache[decision_id] = prepared
    return {
        "hctx": hctx,
        "atoms": atoms,
        "menu": menu,
        "compiled": compiled,
        "request": req,
        "attempts": 0,
        "decision_id": decision_id,
        "prepared": prepared,
        "observations": prepared["observations"],
    }


def _call_model(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    ctx = state["ctx"]
    req = state["request"]
    decision_id = state["decision_id"]
    record = deps.runtime.load_decision(ctx.run_id, decision_id) if deps.runtime else None
    if record is not None and record.status == "ACCEPTED":
        result = record.result
        return {
            "response": response_from_dict(result["response"]),
            "request": ModelRequest(**result["request"]),
            "error": "",
        }
    taped = deps.tape.decision(decision_id)
    if taped is not None:
        request = ModelRequest(**taped.result["request"]) if taped.result else req
        return {"response": taped.response, "request": request, "error": ""}
    history = deps.runtime.list_attempts(ctx.run_id, decision_id) if deps.runtime else []
    if any(
        attempt.actual_usd is not None and attempt.actual_usd > attempt.reserved_usd
        for attempt in history
    ):
        return {"error": "BUDGET_OVERRUN", "fatal": True}
    if not state.get("error") and history:
        last = history[-1]
        if last.outcome == "DISPATCH_RESERVED" and isinstance(
            deps.gateway, RecordedDecisionGateway
        ):
            # The frozen local source is repeatable and cannot incur a bill.
            replay_request, replay_response = deps.gateway.replay_decision(req)
            if last.request != asdict(replay_request) or last.reserved_usd != 0:
                return {"error": "REPLAY_ATTEMPT_IDENTITY_MISMATCH", "fatal": True}
            deps.runtime.complete_attempt(
                ctx.run_id,
                last.attempt_id,
                response={
                    "request": asdict(replay_request),
                    "model_response": response_dict(replay_response),
                },
                actual_usd=Decimal(0),
                outcome="REPLAYED",
            )
            return {"request": replay_request, "response": replay_response, "error": ""}
        if last.outcome in {"DISPATCH_RESERVED", "BILLING_UNCERTAIN"}:
            if last.outcome == "DISPATCH_RESERVED":
                deps.runtime.complete_attempt(
                    ctx.run_id,
                    last.attempt_id,
                    response=None,
                    actual_usd=None,
                    outcome="BILLING_UNCERTAIN",
                    error_code="BILLING_UNCERTAIN",
                )
            return {"error": "BILLING_UNCERTAIN", "fatal": True}
        if last.response is not None and last.outcome in {"COMPLETED", "REPLAYED"}:
            return {
                "response": response_from_dict(last.response["model_response"]),
                "request": ModelRequest(**last.response["request"]),
                "error": "",
            }
        if last.response is not None and last.outcome == "RECONCILED":
            saved_response = response_from_dict(last.response["model_response"])
            if (saved_response.outcome == "COMPLETED" and not saved_response.error_code) or (
                saved_response.error_code == "INVALID_USAGE"
                and (saved_response.provider_metadata or {}).get("output_error_code") == ""
            ):
                assert last.actual_usd is not None
                recovered_response = replace(
                    saved_response,
                    cost_usd=last.actual_usd,
                    billing_uncertain=False,
                    outcome="COMPLETED",
                    error_code="",
                    provider_metadata={
                        **(saved_response.provider_metadata or {}),
                        "billing_reconciled_attempt_id": last.attempt_id,
                        "original_usage_billing_uncertain": saved_response.billing_uncertain,
                    },
                )
                return {
                    "response": recovered_response,
                    "request": ModelRequest(**last.response["request"]),
                    "error": "",
                }
        if len(history) > deps.max_retries:
            return {"error": last.error_code or "RETRY_EXHAUSTED", "fatal": True}
        req = replace(req, retry_error_code=last.error_code or "PROVIDER_FAILED")
    elif state.get("error"):
        req = replace(req, retry_error_code=state["error"])
    reservation = Decimal(0)
    attempt_number = len(history) + 1 if deps.runtime else state.get("attempts", 0) + 1
    attempt_id = f"{decision_id}:{attempt_number}"
    started = False
    try:
        replayed = None
        if isinstance(deps.gateway, RecordedDecisionGateway):
            req, replayed = deps.gateway.replay_decision(req)
        if deps.budget is not None:
            reservation = deps.budget.price_for(req.model_id).bound(req)
        if deps.runtime:
            cap = deps.budget.cap if deps.budget is not None else Decimal(0)
            deps.runtime.start_attempt(
                ctx.run_id, decision_id, attempt_id, reservation, cap, request=asdict(req)
            )
            started = True
        elif deps.budget is not None:
            deps.budget.reserve_amount(reservation)
        resp = replayed or deps.gateway.complete(req, req.system_text, req.output_schema)
    except (ModelError, LedgerError) as exc:
        code = exc.code if isinstance(exc, ModelError) else str(exc)
        uncertain = isinstance(exc, ModelError) and exc.billing_uncertain
        if deps.runtime and started:
            deps.runtime.complete_attempt(
                ctx.run_id,
                attempt_id,
                response=None,
                actual_usd=None if uncertain else Decimal(0),
                outcome="FAILED",
                error_code=code,
            )
        elif deps.runtime is None and deps.budget is not None and reservation and not uncertain:
            deps.budget.abort(reservation)
        if deps.retry_backoff_seconds > 0 and state.get("attempts", 0) < deps.max_retries:
            time.sleep(deps.retry_backoff_seconds)
        return {
            "error": "BILLING_UNCERTAIN" if uncertain else code,
            "attempts": attempt_number,
            "fatal": uncertain
            or code
            in {
                "BUDGET_EXCEEDED",
                "PRICE_MODEL_MISMATCH",
                "INVALID_BUDGET",
                "ATTEMPT_ALREADY_RESERVED",
            },
        }
    if deps.runtime:
        deps.runtime.complete_attempt(
            ctx.run_id,
            attempt_id,
            response={"request": asdict(req), "model_response": response_dict(resp)},
            actual_usd=None if resp.billing_uncertain else resp.cost_usd,
            outcome=resp.outcome,
            error_code=resp.error_code,
        )
    elif deps.budget is not None and not resp.billing_uncertain:
        deps.budget.commit(reservation, resp.cost_usd)
    if resp.billing_uncertain:
        return {
            "error": resp.error_code or "BILLING_UNCERTAIN",
            "fatal": True,
            "attempts": attempt_number,
        }
    if resp.request_hash != req.request_hash() or resp.model_id != req.model_id:
        return {"error": "EPISTEMIC:MODEL_RESPONSE_IDENTITY_MISMATCH", "fatal": True}
    overrun = bool(deps.budget and (resp.cost_usd > reservation or deps.budget.overrun))
    if deps.runtime and deps.budget:
        totals = deps.runtime.budget_totals(ctx.run_id)
        overrun = overrun or totals["committed"] + totals["reserved"] > totals["cap"]
    if overrun:
        return {"error": "BUDGET_OVERRUN", "fatal": True, "attempts": attempt_number}
    return {
        "response": resp,
        "request": req,
        "error": resp.error_code,
        "attempts": attempt_number if resp.error_code else attempt_number - 1,
        "fatal": resp.billing_uncertain and not resp.error_code,
    }


def ctx_role(state: DecisionRun) -> str:
    return state["ctx"].role.lower()


def _validate(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    if state.get("error"):
        return {}
    ctx = state["ctx"]
    parsed = state["response"].parsed
    code = ""
    witness: dict[str, Any] = {}
    if (
        state["response"].request_hash != state["request"].request_hash()
        or state["response"].model_id != state["request"].model_id
    ):
        code = "MODEL_RESPONSE_IDENTITY_MISMATCH"
    elif not isinstance(parsed, dict):
        code = "SCHEMA"  # response wasn't even an object — a model fault
    else:
        try:
            # Only structural model-output failures (HarnessError) become
            # quarantined incidents; a programming error here must crash,
            # not be mislabeled as a model schema failure.
            witness = deps.harness.validate(parsed, state["compiled"])
        except HarnessError as e:
            code = str(e)
    if code:
        deps.ledger.quarantine(
            Incident(
                deps.ledger.next_incident_id(ctx.run_id),
                ctx.run_id,
                ctx.branch_id,
                ctx.actor_id,
                code,
                parsed if isinstance(parsed, dict) else {"raw": str(parsed)[:500]},
                ctx.as_of_utc,
            )
        )
        return {
            "error": "EPISTEMIC:" + code,
            "attempts": state.get("attempts", 0) + 1,
            "fatal": True,
        }
    req = state["request"]
    witness["private_decision_id"] = state["decision_id"]
    witness["request_hash"] = req.request_hash()
    result = {
        "request": asdict(req),
        "response": response_dict(state["response"]),
        "witness": witness,
    }
    if deps.runtime:
        attempts = deps.runtime.list_attempts(ctx.run_id, state["decision_id"])
        deps.runtime.accept_decision(
            ctx.run_id,
            state["decision_id"],
            result=result,
            attempt_id=attempts[-1].attempt_id if attempts else None,
        )
    if deps.tape.lookup(req.request_hash()) is None:
        deps.tape.append(
            req,
            state["response"],
            ctx.as_of_utc.isoformat(),
            decision_id=state["decision_id"],
            prepared=state["prepared"],
            result=result,
        )
    return {"witness": witness, "error": ""}


def _route(state: DecisionRun, deps: PolicyDeps) -> str:
    if state.get("fatal"):
        return END
    if not state.get("error"):
        return "resolve"
    if state.get("attempts", 0) <= deps.max_retries and state.get("error") in {
        "RATE_LIMIT",
        "TRANSPORT",
        "TIMEOUT",
        "INCOMPLETE",
        "REFUSAL",
        "SCHEMA",
        "PROVIDER_FAILED",
    }:
        return "call_model"
    return END


def _resolve(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    action_id = state["response"].parsed["action_id"]
    choice = state["compiled"].action_map[action_id]
    parsed = state["response"].parsed
    ctx = state["ctx"]
    # Bounded memory: validated assessment updates are staged, not written —
    # the engine may still reject the proposal. ``LLMPolicy.commit`` persists
    # them only once the proposal is accepted; they then re-appear in later
    # packets as typed assessments — never as facts, never as raw prose (H-02).
    premise_map = state["compiled"].premise_map
    pending = [
        AssessmentRecord(
            actor_id=ctx.actor_id,
            topic=u["topic"],
            assessment=u["assessment"],
            confidence_label=u["confidence_label"],
            premise_atom_ids=tuple(
                premise_map[t].atom_id for t in u["premise_tokens"] if t in premise_map
            ),
            accepted_at=ctx.as_of_utc,
            decision_token=parsed["decision_token"],
            run_id=ctx.run_id,
            branch_id=ctx.branch_id,
        )
        for u in parsed.get("assessment_updates", [])
    ]
    proposal = Proposal(
        kind=choice.kind,
        candidate_id=(choice.target_internal_id if choice.kind == "OPEN" else None),
        position_id=(choice.target_internal_id if choice.kind in ("HOLD", "CLOSE") else None),
        limit_template_id=choice.limit_internal_id,
        retire_reservation_ids=(
            (choice.target_internal_id,)
            if choice.kind == "RETIRE_SEARCH_SLOTS" and choice.target_internal_id
            else ()
        ),
        allocation=_manager_allocation(state) if choice.kind == "ALLOCATE" else None,
        reason_codes=tuple(parsed.get("reason_codes", ())),
        uncertainty_codes=tuple(parsed.get("uncertainty_codes", ("NONE_IDENTIFIED",))),
    )
    return {"proposal": proposal, "pending_assessments": pending}


def _manager_allocation(state: DecisionRun) -> dict[str, int]:
    """ALLOCATE resolves to the deterministic deficit fill (menu-bounded)."""
    view = state["ctx"].manager_view
    if view is None:
        return {}
    bull = max(view.bullish_target - (view.active_bullish + view.reserved_bullish), 0)
    bear = max(view.bearish_target - (view.active_bearish + view.reserved_bearish), 0)
    committed = (
        view.active_bullish + view.active_bearish + view.reserved_bullish + view.reserved_bearish
    )
    free = view.capacity - committed
    out: dict[str, int] = {}
    while free > 0 and (bull > 0 or bear > 0):
        if bull >= bear and bull > 0:
            out["bullish"] = out.get("bullish", 0) + 1
            bull -= 1
        else:
            out["bearish"] = out.get("bearish", 0) + 1
            bear -= 1
        free -= 1
    return out


def build_decision_graph(deps: PolicyDeps) -> Any:
    g = StateGraph(DecisionRun)
    g.add_node("prepare", lambda s: _prepare(s, deps))
    g.add_node("call_model", lambda s: _call_model(s, deps))
    g.add_node("validate", lambda s: _validate(s, deps))
    g.add_node("resolve", lambda s: _resolve(s, deps))
    g.set_entry_point("prepare")
    g.add_edge("prepare", "call_model")
    g.add_edge("call_model", "validate")
    g.add_conditional_edges(
        "validate",
        lambda s: _route(s, deps),
        {"resolve": "resolve", "call_model": "call_model", END: END},
    )
    g.add_edge("resolve", END)
    return g.compile(checkpointer=InMemorySaver())


def run_decision(
    deps: PolicyDeps, ctx: DecisionContext
) -> tuple[Proposal, dict[str, Any], list[AssessmentRecord]]:
    """Execute one decision barrier; raises PolicyError on terminal failure.

    The third return element is the staged assessment writes — persisted by
    the caller (``LLMPolicy.commit``) only after the engine accepts the
    proposal, so rejected proposals leave no belief-state trace.
    """
    graph = build_decision_graph(deps)
    thread = f"{ctx.run_id}:{ctx.branch_id}:{ctx.actor_id}:{ctx.as_of_utc.isoformat()}"
    try:
        final = graph.invoke(
            {"ctx": ctx},
            config={"configurable": {"thread_id": thread}},
        )
    except HarnessError as exc:
        raise PolicyError("EPISTEMIC:" + str(exc)) from exc
    if final.get("error") or "proposal" not in final:
        raise PolicyError(final.get("error") or "NO_PROPOSAL")
    return (
        final["proposal"],
        final.get("witness", {}),
        final.get("pending_assessments", []),
    )
