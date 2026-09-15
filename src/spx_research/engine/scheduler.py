"""Deterministic minute scheduler (M3-03) — the §6.2 ordered loop.

At each simulated minute ``t``: settle due positions → resolve eligible orders
→ session-close expirations → manager review if triggered → spread reviews at
their grid times → commit. New agents first act at their *next* regular grid
review; no same-minute lifecycle recursion; no same-minute fills (T06); pending
exits keep capital encumbered (D14).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from spx_research.config import Profile
from spx_research.data.availability import Archive
from spx_research.domain.state import (
    AgentState,
    Event,
    Order,
    OrderIntent,
    OrderStatus,
    PositionStatus,
    ReservationStatus,
)
from spx_research.domain.types import Direction, DomainError, Quote
from spx_research.engine.accounting import estimated_liquidation_pnl_usd, usd
from spx_research.engine.execution import FillStatus, Intent, PackageOrder, try_fill
from spx_research.engine.ledger import EngineState, fold, spread_payload
from spx_research.engine.policy import (
    DecisionContext,
    LimitTemplate,
    ManagerView,
    Policy,
    PolicyError,
    Proposal,
    Rejection,
    SpreadView,
    validate_manager_proposal,
    validate_spread_proposal,
)
from spx_research.engine.settlement import expiration_liability_points
from spx_research.features.candidates import Candidate, build_candidates
from spx_research.persistence.events import EventStore
from spx_research.temporal.calendar import NY, UTC_TZ, CalendarManifest, SessionDay


@dataclass
class RunResult:
    run_id: str
    events: list[Event]
    final_state: EngineState
    decisions: list[dict[str, Any]] = field(default_factory=list)


def _quote_from_row(row: dict[str, Any]) -> Quote:
    return Quote(
        contract_id=row["contract_id"],
        snapshot_at_utc=row["snapshot_at_utc"],
        bid_points=Decimal(str(row["bid_points"])),
        ask_points=Decimal(str(row["ask_points"])),
        bid_size_contracts=int(row["bid_size_contracts"]),
        ask_size_contracts=int(row["ask_size_contracts"]),
        simulated_available_at_utc=row["simulated_available_at_utc"],
        quote_event_time_known=bool(row.get("quote_event_time_known", False)),
        quality_flags=tuple(row.get("quality_flags") or ()),
    )


class Engine:
    """M3 baseline engine: trusted deterministic simulation of the strategy."""

    def __init__(
        self,
        profile: Profile,
        calendar: CalendarManifest,
        archive: Archive,
        store: EventStore,
        policy_provider: Callable[[str], Policy],
        run_id: str = "run-0001",
        branch_id: str = "main",
    ) -> None:
        for section in ("universe", "clock", "portfolio", "exit_policy", "manager", "execution"):
            if getattr(profile, section) is None:
                raise DomainError(f"MISSING_CONFIG_SECTION:{section}")
        self.profile = profile
        self.calendar = calendar
        self.archive = archive
        self.store = store
        self.policy_provider = policy_provider
        self.run_id = run_id
        self.branch_id = branch_id
        from spx_research.engine.accounting import AccountSnapshot

        assert profile.portfolio is not None
        assert profile.portfolio.initial_capital_usd is not None
        self.state = EngineState(
            run_id,
            AccountSnapshot(
                usd(profile.portfolio.initial_capital_usd),
                usd(Decimal(0)),
            ),
        )
        self._id_counter = 0
        self._manager_due = False
        self.decisions: list[dict[str, Any]] = []

    # -- ids & events -----------------------------------------------------

    def _new_id(self, prefix: str) -> str:
        self._id_counter += 1
        return f"{prefix}-{self._id_counter:05d}"

    def _emit(self, t: datetime, phase: str, type_: str, payload: dict[str, Any]) -> None:
        committed = self.store.append(
            Event(self.run_id, self.state.seq + 1, t, phase, type_, payload),
            expected_seq=self.state.seq,
        )
        fold(self.state, committed)

    def _emit_witness(self, t: datetime, pol: Policy) -> None:
        """Persist the validated decision witness (auditable proof object).

        Only policies that run the harness pipeline expose ``last_witness``;
        the mechanical policy leaves no witness.
        """
        witness = getattr(pol, "last_witness", None)
        if witness:
            self._emit(t, "DECISION", "DECISION_WITNESS", dict(witness))

    # -- public run loop --------------------------------------------------

    def run(self, start: date, end: date) -> RunResult:
        p = self.profile
        first = self.calendar.session_days(start, end)
        boot = first[0].open_utc() if first else datetime.combine(start, datetime.min.time())
        assert p.portfolio is not None
        self._emit(
            boot,
            "RUN",
            "RUN_STARTED",
            {
                "profile_id": p.profile_id,
                "mode": p.mode,
                "initial_cash_usd": str(p.portfolio.initial_capital_usd),
            },
        )
        last_t = boot
        for sess in self.calendar.session_days(start, end):
            last_t = self._session(sess)
        self._emit(last_t, "RUN", "RUN_ENDED", {})
        return RunResult(self.run_id, self.store.events(self.run_id), self.state, self.decisions)

    def _session(self, sess: SessionDay) -> datetime:
        p = self.profile
        assert p.clock is not None
        grid = set(self.calendar.review_times(sess.day, p.clock.agent_review_minutes))
        first_review = min(grid) if grid else None
        last_offset = sess.minute_offsets()[-1]
        last_t = self.calendar.utc_minute(sess.day, last_offset)
        for offset in sess.minute_offsets():
            t = self.calendar.utc_minute(sess.day, offset)
            if t == first_review:
                self._manager_due = True  # first daily manager review
            self._settle_due(sess.day, t)
            self._resolve_orders(t)
            if offset == last_offset:
                self._session_close(sess, t)
            elif self._manager_due:
                self._manager_review(t, sess, offset)
                self._manager_due = False
            if t in grid:
                self._spread_reviews(sess, t, offset)
        # PM-settled positions: values publish after the close (synthetic:
        # close+30m). One post-close pass books same-day settlement; if the
        # value is still absent the per-minute check retries next session.
        self._settle_due(sess.day, sess.close_utc() + timedelta(minutes=60))
        return last_t

    # -- phases ------------------------------------------------------------

    def _settle_due(self, day: date, t: datetime) -> None:
        """Settle positions whose expiry has passed and whose PM value is now
        available. Runs per-minute during the session, once post-close on the
        expiry day itself, and keeps retrying on later sessions so a
        late-published value still settles (T36)."""
        assert self.profile.execution is not None
        for pos in sorted(self.state.open_positions(), key=lambda p: p.position_id):
            expiry = pos.spread.expiration_local_date
            if expiry > day:
                continue  # not yet expiring
            st = self.archive.settlement_for(expiry)
            if (
                st is None
                or st["simulated_available_at_utc"] > t
                or st.get("value_index_points") is None
            ):
                if expiry == day:
                    # Report the gap on the expiry day only; later-session
                    # retries stay quiet to avoid DATA_GAP spam.
                    self._emit(
                        t,
                        "SETTLEMENT",
                        "DATA_GAP",
                        {
                            "position_id": pos.position_id,
                            "kind": "settlement_unavailable",
                        },
                    )
                continue
            settle_value = Decimal(str(st["value_index_points"]))
            liability = expiration_liability_points(pos.spread, settle_value)
            fee = self.profile.execution.closing_fee_per_leg_usd
            assert fee is not None
            fees = usd(fee * 2)
            self._emit(
                t,
                "SETTLEMENT",
                "POSITION_SETTLED",
                {
                    "position_id": pos.position_id,
                    "settlement_value_points": str(settle_value),
                    "liability_points": str(liability),
                    "fees_usd": str(fees),
                    "final_status": "SETTLED",
                },
            )
            self._retire_agent(pos.agent_id, t, "SETTLED")
            self._manager_due = True

    def _resolve_orders(self, t: datetime) -> None:
        assert self.profile.execution is not None
        ex = self.profile.execution
        for order in sorted(self.state.orders.values(), key=lambda o: o.order_id):
            if order.status is not OrderStatus.PENDING:
                continue
            fee_leg = (
                ex.closing_fee_per_leg_usd
                if order.intent is OrderIntent.CLOSE
                else ex.opening_fee_per_leg_usd
            ) or Decimal(0)
            sq_row = self.archive.quote_at(order.spread.short.contract_id, t)
            lq_row = self.archive.quote_at(order.spread.long.contract_id, t)
            short_q = _quote_from_row(sq_row) if sq_row else None
            long_q = _quote_from_row(lq_row) if lq_row else None
            pkg = PackageOrder(
                order_id=order.order_id,
                intent=Intent[order.intent.name],
                limit_points=order.limit_points,
                submitted_at_utc=order.submitted_at_utc,
                first_eligible_at_utc=order.first_eligible_at_utc,
            )
            outcome = try_fill(pkg, short_q, long_q, t)
            if outcome.status is FillStatus.FILLED and outcome.package_price_points is not None:
                fees = usd(fee_leg * 2)
                self._apply_fill(order, outcome.package_price_points, fees, t)
            elif outcome.status in (
                FillStatus.LIMIT_NOT_MET,
                FillStatus.EXPIRED,
                FillStatus.QUOTE_MISSING,
                FillStatus.QUOTE_UNUSABLE,
                FillStatus.SIZE_INADEQUATE,
            ):
                self._expire_order(order, t, outcome.status.name)
            # NOT_YET_ELIGIBLE: leave pending

    def _expire_order(self, order: Order, t: datetime, reason: str) -> None:
        self._emit(
            t,
            "ORDER",
            "ORDER_RESOLVED",
            {
                "order_id": order.order_id,
                "status": "EXPIRED",
                "reason": reason,
            },
        )
        agent = self.state.agents[order.agent_id]
        if order.intent is OrderIntent.OPEN:
            res = self.state.reservations.get(agent.reservation_id or "")
            if res and res.status is ReservationStatus.ENTRY_PENDING:
                self._emit(
                    t,
                    "ORDER",
                    "RESERVATION_STATUS",
                    {
                        "reservation_id": res.reservation_id,
                        "status": "SEEKING_ENTRY",
                    },
                )
            self._agent_state(order.agent_id, "SEEKING_ENTRY", t, self._next_grid(t))
        else:
            self._agent_state(order.agent_id, "OPEN", t, self._next_grid(t))

    def _apply_fill(self, order: Order, price: Decimal, fees: Decimal, t: datetime) -> None:
        self._emit(
            t,
            "ORDER",
            "ORDER_RESOLVED",
            {
                "order_id": order.order_id,
                "status": "FILLED",
                "price_points": str(price),
                "fees_usd": str(fees),
            },
        )
        agent = self.state.agents[order.agent_id]
        if order.intent is OrderIntent.OPEN:
            res = self.state.reservations[agent.reservation_id or ""]
            pos_id = self._new_id("pos")
            self._emit(
                t,
                "ORDER",
                "RESERVATION_STATUS",
                {
                    "reservation_id": res.reservation_id,
                    "status": "FILLED",
                },
            )
            self._emit(
                t,
                "POSITION",
                "POSITION_OPENED",
                {
                    "position": {
                        "position_id": pos_id,
                        "agent_id": agent.agent_id,
                        "spread": spread_payload(order.spread),
                        "entry_credit_points": str(price),
                        "entry_fees_usd": str(fees),
                        "entry_at_utc": t.isoformat(),
                        "entry_ny_date": self.calendar.ny_date(t).isoformat(),
                        "reserve_usd": str(res.reserve_usd),
                    },
                },
            )
            self._agent_state(order.agent_id, "OPEN", t, self._next_grid(t), position_id=pos_id)
        else:
            pos = self.state.positions[agent.position_id or ""]
            self._emit(
                t,
                "POSITION",
                "POSITION_CLOSED",
                {
                    "position_id": pos.position_id,
                    "close_debit_points": str(price),
                    "fees_usd": str(fees),
                    "final_status": "CLOSED",
                },
            )
            self._retire_agent(order.agent_id, t, "CLOSED")
        self._manager_due = True

    def _session_close(self, sess: SessionDay, t: datetime) -> None:
        # Cancel still-pending orders; expire live reservations (D: session close).
        for order in sorted(self.state.orders.values(), key=lambda o: o.order_id):
            if order.status is OrderStatus.PENDING:
                self._expire_order(order, t, "SESSION_END")
        for res in sorted(self.state.reservations.values(), key=lambda r: r.reservation_id):
            if res.status in (ReservationStatus.SEEKING_ENTRY, ReservationStatus.ENTRY_PENDING):
                self._emit(
                    t,
                    "RESERVATION",
                    "RESERVATION_RELEASED",
                    {
                        "reservation_id": res.reservation_id,
                        "final_status": "EXPIRED",
                    },
                )
                agent = self.state.agents.get(res.agent_id)
                if agent and agent.state in (AgentState.SEEKING_ENTRY, AgentState.ENTRY_PENDING):
                    self._retire_agent(res.agent_id, t, "EXPIRED")
                self._manager_due = True  # applies next session's first review

    # -- decision phases ----------------------------------------------------

    def _macro_facts(self, t: datetime) -> tuple[dict[str, Any], ...]:
        """Available macro vintages, shaped for ``manager_atoms`` (H6).

        ``FED_MEETING_SCHEDULE`` rows carry an absolute future date; that date
        cannot cross the egress gate, so it is expressed relative to ``t`` via
        the ``minutes_to_meeting`` metric instead.
        """
        facts: list[dict[str, Any]] = []
        require_pub = bool(
            self.profile.macro
            and self.profile.macro.publication_timestamp_required_for_intraday_use
        )
        for r in self.archive.macro_visible_at(t):
            if require_pub and r.get("public_release_at_utc") is None:
                continue  # intraday use requires a real publication timestamp
            subject = r["observation_period_end"]
            if isinstance(subject, date) and not isinstance(subject, datetime):
                subject = datetime.combine(subject, time.min, tzinfo=UTC_TZ)
            metric = str(r["series_id"])
            value = str(r["value"])
            if metric == "FED_MEETING_SCHEDULE":
                meeting_ts = datetime.combine(
                    date.fromisoformat(value), time(14, 0), tzinfo=NY
                ).astimezone(UTC_TZ)
                metric = "minutes_to_meeting"
                value = str(max(0, int((meeting_ts - t).total_seconds() // 60)))
            facts.append(
                {
                    "metric": metric,
                    "value": value,
                    "published_at": r["public_release_at_utc"],
                    "available_at": r["simulated_available_at_utc"],
                    "subject_at": subject,
                }
            )
        return tuple(facts)

    def _manager_review(self, t: datetime, sess: SessionDay, offset: int) -> None:
        p = self.profile
        assert p.portfolio is not None
        counts = self._direction_counts()
        capacity = p.portfolio.max_open_or_reserved_slots
        ratio = p.portfolio.bullish_weight / max(p.portfolio.bearish_weight, 1)
        bull_target = round(capacity * ratio / (ratio + 1))
        view = ManagerView(
            as_of_utc=t,
            active_bullish=counts["active_bull"],
            active_bearish=counts["active_bear"],
            reserved_bullish=counts["reserved_bull"],
            reserved_bearish=counts["reserved_bear"],
            capacity=capacity,
            bullish_target=bull_target,
            bearish_target=capacity - bull_target,
            paused=self.state.paused,
            available_usd=self.state.account.available(),
            reservations=tuple(
                r
                for r in self.state.reservations.values()
                if r.status is ReservationStatus.SEEKING_ENTRY
            ),
            macro_facts=self._macro_facts(t),
        )
        ctx = DecisionContext(
            self.run_id,
            self.branch_id,
            "manager-1",
            "MANAGER",
            t,
            session_index=0,
            minute_from_open=offset,
            manager_view=view,
        )
        try:
            pol = self.policy_provider("MANAGER")
            proposal = pol.decide(ctx)
            validate_manager_proposal(ctx, proposal)
            self._check_manager_capacity(proposal, t)
        except PolicyError as e:
            self._emit(t, "DECISION", "BARRIER_PAUSED", {"actor_id": "manager-1", "code": e.code})
            return
        except Rejection as e:
            self.decisions.append(
                {
                    "actor": "manager-1",
                    "at": t.isoformat(),
                    "proposal": proposal.kind,
                    "rejected": e.code,
                }
            )
            self._emit(
                t, "DECISION", "DECISION_REJECTED", {"actor_id": "manager-1", "code": e.code}
            )
            return
        self.decisions.append(
            {"actor": "manager-1", "at": t.isoformat(), "proposal": proposal.kind}
        )
        self._emit_witness(t, pol)
        self._emit(
            t,
            "DECISION",
            "DECISION_MADE",
            {
                "actor_id": "manager-1",
                "kind": proposal.kind,
                "reason_codes": list(proposal.reason_codes),
            },
        )
        self._apply_manager(t, proposal)

    def _reservation_reserve_usd(self, t: datetime) -> Decimal:
        """Full-width encumbrance for any candidate using the real contract
        multiplier of visible contracts (H3) — never a hardcoded 100x."""
        from spx_research.engine.accounting import reserve_required_usd

        assert self.profile.universe is not None and self.profile.portfolio is not None
        max_width = max(self.profile.universe.spread_widths_index_points)
        buffer = self.profile.portfolio.reservation_buffer_usd or Decimal(0)
        mult = max(
            (int(c["multiplier"]) for c in self.archive.contracts_visible_at(t)),
            default=100,
        )
        return reserve_required_usd(max_width, mult, buffer)

    def _check_manager_capacity(self, p: Proposal, t: datetime) -> None:
        """Validate an ALLOCATE against risk caps before it is committed —
        over-cap is a policy Rejection, not a crash (H3)."""
        if p.kind != "ALLOCATE" or not p.allocation:
            return
        assert self.profile.portfolio is not None
        reserve = self._reservation_reserve_usd(t)
        n_new = sum(p.allocation.values())
        per_spread = self.profile.portfolio.max_per_spread_initial_risk_usd
        aggregate = self.profile.portfolio.max_aggregate_committed_risk_usd
        if per_spread is not None and reserve > per_spread:
            raise Rejection("RESERVE_EXCEEDS_RISK_LIMIT")
        if aggregate is not None and self.state.account.reserved + reserve * n_new > aggregate:
            raise Rejection("RESERVE_EXCEEDS_RISK_LIMIT")

    def _apply_manager(self, t: datetime, p: Proposal) -> None:
        prof = self.profile
        assert prof.portfolio is not None and prof.universe is not None
        if p.kind == "ALLOCATE" and p.allocation:
            reserve = self._reservation_reserve_usd(t)
            for direction_name in sorted(p.allocation):
                direction = (
                    Direction.BULL_PUT_CREDIT
                    if direction_name == "bullish"
                    else Direction.BEAR_CALL_CREDIT
                )
                for _ in range(p.allocation[direction_name]):
                    if self.state.account.available() < reserve:
                        break  # capacity check already ran; belt-and-suspenders
                    res_id, agent_id = self._new_id("res"), self._new_id("agent")
                    sess = self.calendar.session(self.calendar.ny_date(t))
                    close_t = sess.close_utc() if sess else t
                    self._emit(
                        t,
                        "RESERVATION",
                        "RESERVATION_HELD",
                        {
                            "reservation_id": res_id,
                            "agent_id": agent_id,
                            "direction": direction.value,
                            "reserve_usd": str(reserve),
                            "created_at_utc": t.isoformat(),
                            "expires_at_utc": close_t.isoformat(),
                        },
                    )
                    self._emit(
                        t,
                        "AGENT",
                        "AGENT_CREATED",
                        {
                            "agent_id": agent_id,
                            "role": "SPREAD",
                            "direction": direction.value,
                            "state": "SEEKING_ENTRY",
                            "created_at_utc": t.isoformat(),
                            "next_review_at_utc": self._next_grid(t).isoformat(),
                            "reservation_id": res_id,
                        },
                    )
        elif p.kind == "PAUSE_NEW_ALLOCATIONS":
            self._emit(t, "MANAGER", "MANAGER_PAUSED", {})
        elif p.kind == "RESUME_NEW_ALLOCATIONS":
            self._emit(t, "MANAGER", "MANAGER_RESUMED", {})
        elif p.kind == "RETIRE_SEARCH_SLOTS":
            for res_id in p.retire_reservation_ids:
                res = self.state.reservations[res_id]
                self._emit(
                    t,
                    "RESERVATION",
                    "RESERVATION_RELEASED",
                    {"reservation_id": res_id, "final_status": "RELEASED"},
                )
                self._retire_agent(res.agent_id, t, "CANCELLED")

    def _spread_reviews(self, sess: SessionDay, t: datetime, offset: int) -> None:
        for agent in sorted(self.state.live_agents(), key=lambda a: a.agent_id):
            if agent.role != "SPREAD" or agent.next_review_at_utc > t:
                continue
            if agent.state not in (AgentState.SEEKING_ENTRY, AgentState.OPEN):
                continue
            view = self._spread_view(agent, sess, t)
            ctx = DecisionContext(
                self.run_id,
                self.branch_id,
                agent.agent_id,
                "SPREAD",
                t,
                session_index=0,
                minute_from_open=offset,
                spread_view=view,
            )
            try:
                pol = self.policy_provider("SPREAD")
                proposal = pol.decide(ctx)
                validate_spread_proposal(ctx, proposal)
            except PolicyError as e:
                self._emit(
                    t, "DECISION", "BARRIER_PAUSED", {"actor_id": agent.agent_id, "code": e.code}
                )
                continue
            except Rejection as e:
                self._emit(
                    t, "DECISION", "DECISION_REJECTED", {"actor_id": agent.agent_id, "code": e.code}
                )
                continue
            self.decisions.append(
                {"actor": agent.agent_id, "at": t.isoformat(), "proposal": proposal.kind}
            )
            self._emit_witness(t, pol)
            self._emit(
                t,
                "DECISION",
                "DECISION_MADE",
                {
                    "actor_id": agent.agent_id,
                    "kind": proposal.kind,
                    "reason_codes": list(proposal.reason_codes),
                },
            )
            self._apply_spread(agent, view, t, proposal)

    def _spread_view(self, agent: Any, sess: SessionDay, t: datetime) -> SpreadView:
        pos = self.state.positions.get(agent.position_id) if agent.position_id else None
        close_debit, frac, days = None, None, 0
        if pos and pos.status is PositionStatus.OPEN:
            assert self.profile.execution is not None
            sq = self.archive.quote_at(pos.spread.short.contract_id, t)
            lq = self.archive.quote_at(pos.spread.long.contract_id, t)
            if sq and lq:
                close_debit = Decimal(str(sq["ask_points"])) - Decimal(str(lq["bid_points"]))
                fee = self.profile.execution.closing_fee_per_leg_usd or Decimal(0)
                pnl = estimated_liquidation_pnl_usd(
                    pos.entry_credit_points,
                    close_debit,
                    pos.spread.multiplier,
                    pos.entry_fees_usd,
                    usd(fee * 2),
                )
                gross = pos.entry_credit_points * pos.spread.multiplier
                frac = pnl / gross if gross > 0 else None
            days = (sess.day - pos.entry_ny_date).days
        cands: tuple[Candidate, ...] = ()
        entry_tpls: tuple[LimitTemplate, ...] = ()
        if agent.state is AgentState.SEEKING_ENTRY:
            assert self.profile.universe is not None
            assert self.profile.portfolio is not None
            res = self.state.reservations[agent.reservation_id or ""]
            cands = tuple(
                build_candidates(
                    self.archive,
                    t,
                    sess.day,
                    res.direction,
                    dte_range=self.profile.universe.entry_dte_range,
                    widths=self.profile.universe.spread_widths_index_points,
                    delta_range=self.profile.universe.short_abs_delta_range,
                    reserve_per_spread_usd=res.reserve_usd,
                    max_risk_usd=self.profile.portfolio.max_per_spread_initial_risk_usd,
                    max_candidates=self.profile.universe.max_candidates_per_direction,
                    target_dte=self.profile.universe.target_entry_dte_calendar_days,
                    allowed_roots=tuple(self.profile.universe.allowed_contract_roots),
                )
            )
            # One approved limit template per candidate (natural quote-side credit).
            entry_tpls = tuple(
                LimitTemplate(f"entry:{c.candidate_id}", "NATURAL", c.credit_points) for c in cands
            )
        exit_tpls: tuple[LimitTemplate, ...] = ()
        if pos and close_debit is not None and close_debit > 0:
            exit_tpls = (LimitTemplate("exit-natural", "NATURAL", close_debit),)
        return SpreadView(agent, pos, close_debit, frac, days, cands, entry_tpls, exit_tpls)

    def _apply_spread(self, agent: Any, view: SpreadView, t: datetime, p: Proposal) -> None:
        assert self.profile.clock is not None
        delay = timedelta(seconds=self.profile.clock.simulated_execution_delay_seconds)
        if p.kind == "OPEN":
            cand = next(c for c in view.candidates if c.candidate_id == p.candidate_id)
            tpl = next(
                t2 for t2 in view.entry_limit_templates if t2.template_id == p.limit_template_id
            )
            order_id = self._new_id("ord")
            self._emit(
                t,
                "ORDER",
                "ORDER_SUBMITTED",
                {
                    "order_id": order_id,
                    "agent_id": agent.agent_id,
                    "spread": spread_payload(cand.spread),
                    "intent": "OPEN",
                    "limit_points": str(tpl.limit_points),
                    "submitted_at_utc": t.isoformat(),
                    "first_eligible_at_utc": (t + delay).isoformat(),
                },
            )
            res = self.state.reservations[agent.reservation_id or ""]
            self._emit(
                t,
                "ORDER",
                "RESERVATION_STATUS",
                {"reservation_id": res.reservation_id, "status": "ENTRY_PENDING"},
            )
            self._agent_state(agent.agent_id, "ENTRY_PENDING", t, t + delay)
        elif p.kind == "CLOSE":
            pos = view.position
            tpl = next(
                t2 for t2 in view.exit_limit_templates if t2.template_id == p.limit_template_id
            )
            if pos is None:
                return
            order_id = self._new_id("ord")
            self._emit(
                t,
                "ORDER",
                "ORDER_SUBMITTED",
                {
                    "order_id": order_id,
                    "agent_id": agent.agent_id,
                    "spread": spread_payload(pos.spread),
                    "intent": "CLOSE",
                    "limit_points": str(tpl.limit_points),
                    "submitted_at_utc": t.isoformat(),
                    "first_eligible_at_utc": (t + delay).isoformat(),
                },
            )
            self._agent_state(agent.agent_id, "EXIT_PENDING", t, t + delay)
        else:  # WAIT or HOLD — next regular review
            self._agent_state(agent.agent_id, agent.state.name, t, self._next_grid(t))

    # -- helpers ------------------------------------------------------------

    def _agent_state(
        self,
        agent_id: str,
        state: str,
        t: datetime,
        next_review: datetime,
        position_id: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "agent_id": agent_id,
            "state": state,
            "next_review_at_utc": next_review.isoformat(),
        }
        if position_id is not None:
            payload["position_id"] = position_id
        self._emit(t, "AGENT", "AGENT_STATE", payload)

    def _retire_agent(self, agent_id: str, t: datetime, final: str) -> None:
        self._emit(
            t,
            "AGENT",
            "AGENT_STATE",
            {
                "agent_id": agent_id,
                "state": final,
                "next_review_at_utc": t.isoformat(),
            },
        )
        self._emit(
            t,
            "AGENT",
            "AGENT_STATE",
            {
                "agent_id": agent_id,
                "state": "ARCHIVED",
                "next_review_at_utc": t.isoformat(),
            },
        )

    def _direction_counts(self) -> dict[str, int]:
        out = {"active_bull": 0, "active_bear": 0, "reserved_bull": 0, "reserved_bear": 0}
        for pos in self.state.open_positions():
            if pos.spread.direction is Direction.BULL_PUT_CREDIT:
                out["active_bull"] += 1
            else:
                out["active_bear"] += 1
        for res in self.state.reservations.values():
            if res.status in (ReservationStatus.SEEKING_ENTRY, ReservationStatus.ENTRY_PENDING):
                if res.direction is Direction.BULL_PUT_CREDIT:
                    out["reserved_bull"] += 1
                else:
                    out["reserved_bear"] += 1
        return out

    def _next_grid(self, t: datetime) -> datetime:
        assert self.profile.clock is not None
        return t + timedelta(minutes=self.profile.clock.agent_review_minutes)
