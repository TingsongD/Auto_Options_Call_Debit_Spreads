"""Mechanical comparison policy (M3-02).

Deterministic, non-LLM: selects from the same candidate set under the same
capacity rules (§13.1). It is a causal control, not a claim about the AI
policy; it legitimately sees the private context since it is trusted code.
"""

from __future__ import annotations

from decimal import Decimal

from spx_research.engine.policy import DecisionContext, Proposal


class MechanicalPolicy:
    """Central-profile mechanical baseline.

    Spread: first eligible candidate at natural quote-side credit; close at
    >=35% profit or <=-25% loss once the activation holding age is reached.
    Manager: allocate toward the configured 2:1 count target, largest deficit
    first; never force a trade; pause state is respected.
    """

    def __init__(
        self,
        profit_trigger: Decimal = Decimal("0.35"),
        loss_trigger: Decimal = Decimal("-0.25"),
        loss_activation_days: int = 25,
        min_entry_credit_fraction: Decimal = Decimal("0.05"),
    ) -> None:
        self.profit_trigger = profit_trigger
        self.loss_trigger = loss_trigger
        self.loss_activation_days = loss_activation_days
        self.min_entry_credit_fraction = min_entry_credit_fraction

    def decide(self, ctx: DecisionContext) -> Proposal:
        if ctx.role == "MANAGER":
            return self._manager(ctx)
        return self._spread(ctx)

    def _spread(self, ctx: DecisionContext) -> Proposal:
        view = ctx.spread_view
        if view is None:
            return Proposal("WAIT", reason_codes=("INSUFFICIENT_EVIDENCE",))
        state = view.agent.state.name
        if state == "SEEKING_ENTRY":
            for c in view.candidates:
                frac = c.credit_points / c.spread.width_points
                if frac >= self.min_entry_credit_fraction:
                    tpl = next(
                        (
                            t2
                            for t2 in view.entry_limit_templates
                            if t2.template_id == f"entry:{c.candidate_id}"
                        ),
                        None,
                    )
                    if tpl is None:
                        break
                    return Proposal(
                        "OPEN",
                        candidate_id=c.candidate_id,
                        limit_template_id=tpl.template_id,
                        reason_codes=("ENTRY_CRITERIA",),
                    )
            return Proposal("WAIT", reason_codes=("ENTRY_CRITERIA", "QUOTE_QUALITY"))
        if state == "OPEN" and view.position is not None:
            if view.profit_fraction is not None and view.current_close_debit_points is not None:
                take_profit = view.profit_fraction >= self.profit_trigger
                aged_loss = (
                    view.days_held >= self.loss_activation_days
                    and view.profit_fraction <= self.loss_trigger
                )
                if take_profit or aged_loss:
                    t = view.exit_limit_templates[0] if view.exit_limit_templates else None
                    if t is not None:
                        return Proposal(
                            "CLOSE",
                            position_id=view.position.position_id,
                            limit_template_id=t.template_id,
                            reason_codes=("PROFIT_BAND" if take_profit else "LOSS_BAND",),
                        )
            return Proposal(
                "HOLD",
                position_id=view.position.position_id if view.position else None,
                reason_codes=("MAINTAIN_THESIS",),
            )
        return Proposal("WAIT", reason_codes=("LIFECYCLE_RESTRICTION",))

    def _manager(self, ctx: DecisionContext) -> Proposal:
        view = ctx.manager_view
        if view is None:
            return Proposal("NO_CHANGE", reason_codes=("INSUFFICIENT_EVIDENCE",))
        if view.paused:
            return Proposal("NO_CHANGE", reason_codes=("LIFECYCLE_RESTRICTION",))
        bull_deficit = view.bullish_target - (view.active_bullish + view.reserved_bullish)
        bear_deficit = view.bearish_target - (view.active_bearish + view.reserved_bearish)
        committed = (
            view.active_bullish
            + view.reserved_bullish
            + view.active_bearish
            + view.reserved_bearish
        )
        alloc: dict[str, int] = {}
        free = view.capacity - committed
        while free > 0 and (bull_deficit > 0 or bear_deficit > 0):
            if bull_deficit >= bear_deficit and bull_deficit > 0:
                alloc["bullish"] = alloc.get("bullish", 0) + 1
                bull_deficit -= 1
            else:
                alloc["bearish"] = alloc.get("bearish", 0) + 1
                bear_deficit -= 1
            free -= 1
        if alloc:
            return Proposal("ALLOCATE", allocation=alloc, reason_codes=("ALLOCATION_DEFICIT",))
        return Proposal("NO_CHANGE", reason_codes=("MAINTAIN_THESIS",))
