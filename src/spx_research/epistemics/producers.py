"""Engine → evidence bridge (H-02/H-03).

Converts private engine decision views into typed atoms, records their
recipient-scoped deliveries on the observation ledger, builds the finite
engine-generated action menu, and compiles the blinded packet. Atom ids are
content-addressed so identical observed prefixes produce identical ids — a
precondition for future-suffix invariance.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from spx_research.engine.policy import ManagerView, SpreadView
from spx_research.epistemics.egress import egress_check
from spx_research.epistemics.harness import Harness, digest
from spx_research.epistemics.store import ObservationLedger
from spx_research.epistemics.types import VOCAB, Atom, Compiled, Context, HarnessError, MenuChoice


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


def _risk_fraction(loss: Decimal, equity: Decimal | None) -> Decimal:
    if equity is None or not equity.is_finite() or equity <= 0:
        raise HarnessError("MISSING_POSITIVE_EQUITY")
    return loss / equity


def spread_atoms(view: SpreadView, as_of: datetime) -> list[Atom]:
    """Recipient-scoped economics and frozen advisory rules, without dates."""
    out = [_atom("available_slots", "0" if view.position else "1", as_of)]
    if view.agent.direction is not None:
        out.append(_atom("direction_mandate", view.agent.direction.value, as_of))
    for metric, value in (
        ("days_held", view.days_held),
        ("profit_band_low", view.advisory_profit_low),
        ("profit_band_high", view.advisory_profit_high),
        ("loss_band_low", view.advisory_loss_low),
        ("loss_band_high", view.advisory_loss_high),
        ("loss_activation_days", view.loss_activation_days),
        ("advisory_rule", "DISCRETIONARY"),
    ):
        out.append(_atom(metric, str(value), as_of))
    if view.as_of_dte is not None:
        out.append(_atom("dte", str(view.as_of_dte), as_of))
    if view.position is not None and view.profit_fraction is not None:
        out.append(_atom("profit_fraction", str(view.profit_fraction), as_of))
    for c in view.candidates:
        out.append(
            _atom("candidate_max_risk", str(_risk_fraction(c.max_loss_usd, view.equity_usd)), as_of)
        )
    out.extend(_macro_atoms(view.macro_facts))
    return out


def _attributes(**values: object) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (name, str(value), VOCAB[name][0]) for name, value in values.items() if value is not None
    )


def spread_menu(view: SpreadView, atoms: list[Atom]) -> list[MenuChoice]:
    slots = next(a for a in atoms if a.metric == "available_slots")
    menu: list[MenuChoice] = []
    if view.agent.state.name == "SEEKING_ENTRY":
        menu.append(MenuChoice("act-wait", "WAIT", (slots.atom_id,)))
        risk_atoms = [a for a in atoms if a.metric == "candidate_max_risk"]
        tpl_by_id = {t2.template_id: t2 for t2 in view.entry_limit_templates}
        for c, risk in zip(view.candidates, risk_atoms, strict=True):
            tpl = tpl_by_id.get(f"entry:{c.candidate_id}")
            if tpl is None:
                continue
            req = (slots.atom_id, risk.atom_id)
            menu.append(
                MenuChoice(
                    f"act-open:{c.candidate_id}",
                    "OPEN",
                    req,
                    target_internal_id=c.candidate_id,
                    limit_internal_id=tpl.template_id,
                    attributes=_attributes(
                        direction_mandate=c.direction.value,
                        dte=c.dte,
                        spread_width=c.spread.width_points,
                        credit_to_width=c.credit_points / c.spread.width_points,
                        limit_to_width=tpl.limit_points / c.spread.width_points,
                        candidate_max_risk=_risk_fraction(c.max_loss_usd, view.equity_usd),
                        short_delta=c.short_delta,
                        short_moneyness=(
                            Decimal(c.spread.short.strike_points) / view.spot_points
                            if view.spot_points
                            else None
                        ),
                        long_moneyness=(
                            Decimal(c.spread.long.strike_points) / view.spot_points
                            if view.spot_points
                            else None
                        ),
                    ),
                )
            )
    elif view.position is not None:
        prof = next((a for a in atoms if a.metric == "profit_fraction"), None)
        base = (prof.atom_id,) if prof else (slots.atom_id,)
        menu.append(
            MenuChoice(
                "act-hold",
                "HOLD",
                base,
                target_internal_id=view.position.position_id,
            )
        )
        for tpl in view.exit_limit_templates:
            menu.append(
                MenuChoice(
                    f"act-close:{tpl.template_id}",
                    "CLOSE",
                    base,
                    target_internal_id=view.position.position_id,
                    limit_internal_id=tpl.template_id,
                    attributes=_attributes(
                        limit_to_width=tpl.limit_points / view.position.spread.width_points,
                        spread_width=view.position.spread.width_points,
                    ),
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
    out.extend(_macro_atoms(view.macro_facts))
    if view.equity_usd is not None and view.equity_usd.is_finite() and view.equity_usd > 0:
        out.append(
            _atom(
                "available_risk_fraction",
                str(_risk_fraction(view.available_usd, view.equity_usd)),
                as_of,
            )
        )
    out.append(_atom("bullish_count", str(view.active_bullish + view.reserved_bullish), as_of))
    out.append(_atom("bearish_count", str(view.active_bearish + view.reserved_bearish), as_of))
    return out


def _macro_atoms(facts: tuple[dict[str, Any], ...]) -> list[Atom]:
    out: list[Atom] = []
    for fact in facts:
        unit, _allowed = VOCAB[fact["metric"]]
        # Bind all provenance fields, not merely value+availability.
        aid = (
            "atm_"
            + digest(
                [
                    fact["metric"],
                    str(fact["value"]),
                    unit,
                    fact["published_at"].isoformat(),
                    fact["available_at"].isoformat(),
                    fact["subject_at"].isoformat(),
                ]
            )[:24]
        )
        out.append(
            Atom(
                aid,
                fact["metric"],
                str(fact["value"]),
                unit,
                "OBSERVATION",
                fact["published_at"],
                fact["available_at"],
                fact["subject_at"],
            )
        )
    return out


def manager_menu(view: ManagerView, atoms: list[Atom]) -> list[MenuChoice]:
    base = (next(a for a in atoms if a.metric == "available_slots").atom_id,)
    menu = [MenuChoice("act-nochange", "NO_CHANGE", base)]
    bull_deficit = view.bullish_target - (view.active_bullish + view.reserved_bullish)
    bear_deficit = view.bearish_target - (view.active_bearish + view.reserved_bearish)
    free = view.capacity - (
        view.active_bullish + view.active_bearish + view.reserved_bullish + view.reserved_bearish
    )
    if (bull_deficit > 0 or bear_deficit > 0) and free > 0 and not view.paused:
        deficits = tuple(
            a.atom_id
            for a in atoms
            if a.metric in ("bull_deficit", "bear_deficit", "direction_target")
        )
        menu.append(MenuChoice("act-allocate", "ALLOCATE", base + deficits))
    if not view.paused:
        menu.append(MenuChoice("act-pause", "PAUSE_NEW_ALLOCATIONS", base))
    else:
        menu.append(MenuChoice("act-resume", "RESUME_NEW_ALLOCATIONS", base))
    for r in view.reservations:
        menu.append(
            MenuChoice(
                f"act-retire:{r.reservation_id}",
                "RETIRE_SEARCH_SLOTS",
                base,
                target_internal_id=r.reservation_id,
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
    premise_ids: set[str] | None = None,
    prior_belief_hash: str | None = None,
) -> Compiled:
    """Compile + egress-check a blinded packet for one actor.

    The actor's verified belief state supplies carried assessments and
    unknowns — local reconstruction, no provider state. ``premise_ids``
    bounds the packet's evidence list to the current barrier's batch (the
    producers re-emit persistent macro facts each barrier, so they stay
    visible); ``premise_map`` still covers every delivered atom so carried
    assessment tokens keep resolving. ``prior_belief_hash`` should be the
    belief reduced *before* this barrier's deliveries — the caller computes
    it pre-delivery so the token names the prior state, not the belief the
    packet itself just created.
    """
    from dataclasses import replace

    from spx_research.epistemics.reducer import reduce_belief

    belief = reduce_belief(ledger, harness, ctx)
    ctx2 = replace(
        ctx,
        prior_visible_belief_hash=(
            prior_belief_hash if prior_belief_hash is not None else belief.belief_hash
        ),
    )
    compiled = harness.compile(
        ledger.atoms(),
        ledger.deliveries(ctx2.run_id, ctx2.branch_id, ctx2.actor_id),
        ctx2,
        menu,
        assessments=belief.assessments,
        unknowns=belief.unknowns,
        premise_ids=premise_ids,
    )
    egress_check(compiled.public, ctx2)
    return compiled
