"""Engine → evidence bridge (H-02/H-03).

Converts private engine decision views into typed atoms, records their
recipient-scoped deliveries on the observation ledger, builds the finite
engine-generated action menu, and compiles the blinded packet. Atom ids are
content-addressed so identical observed prefixes produce identical ids — a
precondition for future-suffix invariance.
"""

from __future__ import annotations

from datetime import datetime

from spx_research.engine.policy import ManagerView, SpreadView
from spx_research.epistemics.egress import egress_check
from spx_research.epistemics.harness import Harness, digest
from spx_research.epistemics.store import ObservationLedger
from spx_research.epistemics.types import Atom, Compiled, Context, MenuChoice


def atom_id_for(metric: str, value: str, unit: str, kind: str, available_at: datetime) -> str:
    """Content-addressed id: same fact, same id, in any dataset copy."""
    return "atm_" + digest([metric, value, unit, kind, available_at.isoformat()])[:20]


def _atom(metric: str, value: str, at: datetime) -> Atom:
    from spx_research.epistemics.types import VOCAB

    unit, _allowed = VOCAB[metric]
    aid = atom_id_for(metric, value, unit, "OBSERVATION", at)
    return Atom(aid, metric, value, unit, "OBSERVATION", at, at, at)


def _deliver_all(
    ledger: ObservationLedger,
    ctx_parts: tuple[str, str, str],
    atoms: list[Atom],
    at: datetime,
) -> None:
    run_id, branch_id, actor_id = ctx_parts
    for a in atoms:
        ledger.put_atom(a)
        ledger.deliver(run_id, branch_id, actor_id, a.atom_id, at)


def spread_atoms(view: SpreadView, as_of: datetime) -> list[Atom]:
    """Facts a spread agent is entitled to at its review (all relative)."""
    out: list[Atom] = []
    out.append(_atom("available_slots", "0" if view.position else "1", as_of))
    if view.position is not None and view.profit_fraction is not None:
        out.append(_atom("profit_fraction", str(view.profit_fraction), as_of))
    for c in view.candidates:
        out.append(_atom("candidate_max_risk", str(c.max_loss_usd), as_of))
    return out


def spread_menu(view: SpreadView, atoms: list[Atom]) -> list[MenuChoice]:
    slots = atoms[0]  # always present
    menu: list[MenuChoice] = []
    if view.agent.state.name == "SEEKING_ENTRY":
        menu.append(MenuChoice("act-wait", "WAIT", (slots.atom_id,)))
        risk_by_strike = {a.value: a.atom_id for a in atoms if a.metric == "candidate_max_risk"}
        tpl_by_id = {t2.template_id: t2 for t2 in view.entry_limit_templates}
        for c in view.candidates:
            tpl = tpl_by_id.get(f"entry:{c.candidate_id}")
            if tpl is None:
                continue
            req = (slots.atom_id, risk_by_strike[str(c.max_loss_usd)])
            menu.append(
                MenuChoice(
                    f"act-open:{c.candidate_id}",
                    "OPEN",
                    req,
                    target_internal_id=c.candidate_id,
                    limit_internal_id=tpl.template_id,
                )
            )
    else:
        prof = next((a for a in atoms if a.metric == "profit_fraction"), None)
        base = (prof.atom_id,) if prof else (slots.atom_id,)
        menu.append(
            MenuChoice(
                "act-hold",
                "HOLD",
                base,
                target_internal_id=(view.position.position_id if view.position else None),
            )
        )
        for tpl in view.exit_limit_templates:
            menu.append(
                MenuChoice(
                    "act-close",
                    "CLOSE",
                    base,
                    target_internal_id=(view.position.position_id if view.position else None),
                    limit_internal_id=tpl.template_id,
                )
            )
    return menu


def manager_atoms(view: ManagerView, as_of: datetime) -> list[Atom]:
    out = [
        _atom(
            "available_slots",
            str(
                view.capacity
                - (
                    view.active_bullish
                    + view.active_bearish
                    + view.reserved_bullish
                    + view.reserved_bearish
                )
            ),
            as_of,
        ),
        _atom("direction_target", str(view.bullish_target), as_of),
        _atom(
            "bull_deficit",
            str(view.bullish_target - (view.active_bullish + view.reserved_bullish)),
            as_of,
        ),
        _atom(
            "bear_deficit",
            str(view.bearish_target - (view.active_bearish + view.reserved_bearish)),
            as_of,
        ),
    ]
    for fact in view.macro_facts:
        out.append(_atom(fact["metric"], str(fact["value"]), as_of))
    return out


def manager_menu(view: ManagerView, atoms: list[Atom]) -> list[MenuChoice]:
    base = tuple(a.atom_id for a in atoms[:1])  # available_slots premise
    menu = [MenuChoice("act-nochange", "NO_CHANGE", base)]
    deficits = tuple(
        a.atom_id for a in atoms if a.metric in ("bull_deficit", "bear_deficit", "direction_target")
    )
    menu.append(MenuChoice("act-allocate", "ALLOCATE", base + deficits))
    menu.append(MenuChoice("act-pause", "PAUSE_NEW_ALLOCATIONS", base))
    menu.append(MenuChoice("act-resume", "RESUME_NEW_ALLOCATIONS", base))
    live = [r for r in view.reservations]
    if live:
        menu.append(
            MenuChoice(
                "act-retire",
                "RETIRE_SEARCH_SLOTS",
                base,
                target_internal_id=live[0].reservation_id,
            )
        )
    return menu


def make_context(
    run_id: str,
    branch_id: str,
    actor_id: str,
    role: str,
    as_of: datetime,
    session_index: int,
    minute_from_open: int,
    private_manifest_id: str,
    prior_visible_belief_hash: str = "empty",
) -> Context:
    return Context(
        run_id=run_id,
        branch_id=branch_id,
        actor_id=actor_id,
        actor_role=role,
        as_of=as_of,
        session_index=session_index,
        minute_from_open=minute_from_open,
        alias_namespace=f"{run_id}:{branch_id}",
        private_manifest_id=private_manifest_id,
        prior_visible_belief_hash=prior_visible_belief_hash,
    )


def compile_for_actor(
    ledger: ObservationLedger,
    harness: Harness,
    ctx: Context,
    menu: list[MenuChoice],
) -> Compiled:
    """Compile + egress-check a blinded packet for one actor.

    The actor's verified belief state supplies the prior-belief hash, carried
    assessments, and unknowns — local reconstruction, no provider state.
    """
    from dataclasses import replace

    from spx_research.epistemics.reducer import reduce_belief

    belief = reduce_belief(ledger, harness, ctx)
    ctx2 = replace(ctx, prior_visible_belief_hash=belief.belief_hash)
    compiled = harness.compile(
        ledger.atoms(),
        ledger.deliveries(ctx2.run_id, ctx2.branch_id, ctx2.actor_id),
        ctx2,
        menu,
        assessments=belief.assessments,
        unknowns=belief.unknowns,
    )
    egress_check(compiled.public, ctx2)
    return compiled
