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

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from spx_research.config import Profile
from spx_research.engine.policy import DecisionContext, PolicyError, Proposal
from spx_research.epistemics.harness import Harness
from spx_research.epistemics.producers import (
    compile_for_actor,
    make_context,
    manager_atoms,
    manager_menu,
    spread_atoms,
    spread_menu,
)
from spx_research.epistemics.store import Incident, ObservationLedger
from spx_research.epistemics.types import Atom, Compiled, Context, MenuChoice
from spx_research.llm.budget import Budget
from spx_research.llm.gateway import ModelGateway
from spx_research.llm.tape import DecisionTape
from spx_research.llm.types import ModelError, ModelRequest, ModelResponse


@dataclass
class PolicyDeps:
    harness: Harness
    ledger: ObservationLedger
    gateway: ModelGateway
    tape: DecisionTape
    profile: Profile
    budget: Budget | None = None
    max_retries: int = 2
    model_id: str = "mock-1"
    private_manifest_id: str = "local"
    system_prompts: dict[str, str] | None = None  # role -> prompt text
    schemas: dict[str, dict[str, Any]] | None = None  # schema_name -> schema


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
    error: str
    attempts: int


def _schema_name(role: str) -> str:
    return "spread_decision" if role == "SPREAD" else "manager_decision"


def _prepare(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    ctx = state["ctx"]
    if ctx.role == "SPREAD":
        assert ctx.spread_view is not None
        atoms = spread_atoms(ctx.spread_view, ctx.as_of_utc)
        menu = spread_menu(ctx.spread_view, atoms)
    else:
        assert ctx.manager_view is not None
        atoms = manager_atoms(ctx.manager_view, ctx.as_of_utc)
        menu = manager_menu(ctx.manager_view, atoms)
    for a in atoms:
        deps.ledger.put_atom(a)
        deps.ledger.deliver(ctx.run_id, ctx.branch_id, ctx.actor_id, a.atom_id, ctx.as_of_utc)
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
    compiled = compile_for_actor(deps.ledger, deps.harness, hctx, menu)
    return {
        "hctx": hctx,
        "atoms": atoms,
        "menu": menu,
        "compiled": compiled,
        "request": ModelRequest(
            system_prompt_id=f"{ctx.role.lower()}_v2",
            packet=compiled.public,
            schema_name=_schema_name(ctx.role),
            model_id=deps.model_id,
        ),
        "attempts": 0,
    }


def _call_model(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    req = state["request"]
    rec = deps.tape.lookup(req.request_hash())
    if rec is not None:
        return {"response": rec.response, "error": ""}
    reservation = Decimal(0)
    try:
        if deps.budget is not None:
            reservation = deps.budget.reserve(
                input_tokens=4096, max_output_tokens=req.max_output_tokens
            )
        schema = (deps.schemas or {}).get(req.schema_name)
        system_text = (deps.system_prompts or {}).get(ctx_role(state), "")
        resp = deps.gateway.complete(req, system_text, schema)
    except ModelError as e:
        if deps.budget is not None and reservation:
            deps.budget.abort(reservation)
        return {"error": e.code, "attempts": state.get("attempts", 0) + 1}
    if deps.budget is not None:
        deps.budget.commit(reservation, Decimal(str(resp.cost_usd)))
    # Tape only validated responses (see _validate): a rejected response is
    # quarantined, never replayed, and a retry must re-dispatch the request.
    return {"response": resp, "error": ""}


def ctx_role(state: DecisionRun) -> str:
    return state["ctx"].role.lower()


def _validate(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    if state.get("error"):
        return {}
    ctx = state["ctx"]
    try:
        witness = deps.harness.validate(state["response"].parsed, state["compiled"])
    except Exception as e:  # HarnessError or schema-shape failure
        code = str(e) if isinstance(e, ValueError) else "SCHEMA"
        deps.ledger.quarantine(
            Incident(
                deps.ledger.next_incident_id(ctx.run_id),
                ctx.run_id,
                ctx.branch_id,
                ctx.actor_id,
                code,
                dict(state["response"].parsed),
                ctx.as_of_utc,
            )
        )
        return {"error": code, "attempts": state.get("attempts", 0) + 1}
    req = state["request"]
    if deps.tape.lookup(req.request_hash()) is None:
        deps.tape.append(req, state["response"], ctx.as_of_utc.isoformat())
    return {"witness": witness, "error": ""}


def _route(state: DecisionRun, deps: PolicyDeps) -> str:
    if not state.get("error"):
        return "resolve"
    if state.get("attempts", 0) <= deps.max_retries:
        return "call_model"
    return END


def _resolve(state: DecisionRun, deps: PolicyDeps) -> DecisionRun:
    action_id = state["response"].parsed["action_id"]
    choice = state["compiled"].action_map[action_id]
    parsed = state["response"].parsed
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
    return {"proposal": proposal}


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


def run_decision(deps: PolicyDeps, ctx: DecisionContext) -> tuple[Proposal, dict[str, Any]]:
    """Execute one decision barrier; raises PolicyError on terminal failure."""
    graph = build_decision_graph(deps)
    thread = f"{ctx.run_id}:{ctx.branch_id}:{ctx.actor_id}:{ctx.as_of_utc.isoformat()}"
    final = graph.invoke(
        {"ctx": ctx},
        config={"configurable": {"thread_id": thread}},
    )
    if final.get("error") or "proposal" not in final:
        raise PolicyError(final.get("error") or "NO_PROPOSAL")
    return final["proposal"], final.get("witness", {})
