"""Deterministic minute scheduler (M3-03) — the §6.2 ordered loop.

At each simulated minute: resolve prior exits/entries, settle due positions,
record valuations, then atomically commit all decisions from one frozen view.
A durable phase cursor resumes interrupted work without duplicating effects.
New agents first act at their next regular grid review; pending exits retain
their capital. Data or model failures halt the clock at the failed phase.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, cast

from spx_research.config import Profile
from spx_research.data.availability import Archive
from spx_research.data.validation import on_increment, quote_from_row, validate_pair
from spx_research.domain.results import (
    PauseCategory,
    PauseReason,
    RunStatus,
    ValuationQuality,
    ValuationRecord,
)
from spx_research.domain.state import (
    AgentState,
    Event,
    Order,
    OrderIntent,
    OrderStatus,
    Position,
    PositionStatus,
    ReservationStatus,
    event_hash,
)
from spx_research.domain.types import Direction, DomainError, Quote
from spx_research.engine.accounting import estimated_liquidation_pnl_usd, usd
from spx_research.engine.execution import FillStatus, Intent, PackageOrder, try_fill
from spx_research.engine.ledger import EngineState, fold, replay, spread_payload
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
from spx_research.engine.settlement import expiration_liability_points, require_settlement_value
from spx_research.features.candidates import Candidate, build_candidates
from spx_research.persistence.events import EventStore, payload_hash
from spx_research.temporal.calendar import NY, UTC_TZ, CalendarManifest, SessionDay


@dataclass
class RunResult:
    run_id: str
    events: list[Event]
    final_state: EngineState
    decisions: list[dict[str, Any]] = field(default_factory=list)
    status: RunStatus = "COMPLETED"
    pause: PauseReason | None = None
    valuations: list[ValuationRecord] = field(default_factory=list)
    scored_end_valuation: ValuationRecord | None = None
    runoff_summary: dict[str, Any] = field(default_factory=dict)
    research_validity: str = "UNVALIDATED"


def _quote_from_row(row: dict[str, Any]) -> Quote:
    return quote_from_row(row)


class DataPause(ValueError):
    """Required data is unavailable; the clock must remain at this phase."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


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
        self.status: RunStatus = "RUNNING"
        self.pause: PauseReason | None = None
        self._buffer: list[Event] | None = None
        self._cursor: dict[str, Any] = {}
        self._current_valuation: ValuationRecord | None = None
        self._quote_cache: dict[tuple[str, datetime], dict[str, Any] | None] = {}
        self._candidate_cache: dict[tuple[Any, ...], tuple[Candidate, ...]] = {}
        self._candidate_findings: list[dict[str, Any]] = []
        self._barrier_id = ""
        self._base_tip = (0, "genesis")
        self._run_sessions: list[SessionDay] = []
        self._run_end: date | None = None
        self._scored_session_day: date | None = None

    # -- ids & events -----------------------------------------------------

    def _new_id(self, prefix: str) -> str:
        self._id_counter += 1
        return f"{prefix}-{self._id_counter:05d}"

    def _emit(self, t: datetime, phase: str, type_: str, payload: dict[str, Any]) -> None:
        if self._buffer is not None:
            event = Event(self.run_id, self.state.seq + 1, t, phase, type_, payload)
            self._buffer.append(event)
            fold(self.state, event)
            return
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

    def _runtime_cursor(self, **updates: Any) -> dict[str, Any]:
        cursor = dict(self._cursor)
        cursor.update(updates)
        cursor.update(id_counter=self._id_counter, manager_due=self._manager_due)
        return cursor

    def _state_hash(self) -> str:
        blob = json.dumps(asdict(self.state), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()

    def _persist_pause(self, t: datetime, phase: str, category: PauseCategory, code: str) -> None:
        self.status = "PAUSED"
        self.pause = {"category": category, "code": code, "phase": phase, "at": t.isoformat()}
        if category == "DATA" and code != "RUNOFF_INCOMPLETE":
            self.pause["valuation"] = {
                "as_of": t.isoformat(),
                "cash_usd": str(self.state.account.cash),
                "reserved_usd": str(self.state.account.reserved),
                "fees_paid_usd": str(self.state.account.fees_paid),
                "mid_equity_usd": None,
                "net_liquidation_equity_usd": None,
                "quality": "UNPRICEABLE",
                "reason_code": code,
                "positions": [
                    {
                        "position_id": p.position_id,
                        "quality": "UNPRICEABLE",
                        "reason_code": code,
                        "mid_liability_usd": None,
                        "liquidation_liability_usd": None,
                    }
                    for p in self.state.open_positions()
                ],
            }
        self._cursor = self._runtime_cursor(simulated_at=t.isoformat(), next_phase=phase)
        persist = getattr(self.store, "persist_cursor", None)
        if callable(persist):
            persist(self.run_id, self._cursor, "PAUSED", self.pause)

    def _atomic(
        self,
        t: datetime,
        phase: str,
        work: Callable[[], None],
        next_cursor: dict[str, Any],
        *,
        assessments: list[Any] | None = None,
        observations: dict[str, Any] | None = None,
    ) -> None:
        """Stage domain effects and atomically commit them with their recovery cursor."""
        base_state = self.state
        base_counter, base_due = self._id_counter, self._manager_due
        base_valuation = self._current_valuation
        # Every aggregate member is immutable; only the projection dictionaries
        # need copying. Deep-copying years of retired agents at every minute
        # would turn an otherwise linear simulation into quadratic work.
        self.state = replace(
            base_state,
            agents=dict(base_state.agents),
            positions=dict(base_state.positions),
            orders=dict(base_state.orders),
            reservations=dict(base_state.reservations),
        )
        self._buffer = []
        try:
            work()
            events = self._buffer
            cursor = self._runtime_cursor(**next_cursor)
            linked_hash = self.store.tip(self.run_id)[1]
            for event in events:
                linked_hash = event.with_hashes(payload_hash(event.payload), linked_hash).event_hash
            barrier_id = f"{self.branch_id}:{t.isoformat()}:{phase}"
            commit = getattr(self.store, "commit_barrier", None)
            cursor.update(ledger_seq=self.state.seq, ledger_hash=linked_hash)
            if callable(commit):
                cursor["state_hash"] = self._state_hash()
            if callable(commit):
                commit(
                    self.run_id,
                    barrier_id,
                    base_state.seq,
                    events,
                    cursor,
                    assessments=assessments or [],
                    observations=observations,
                )
            else:
                append_batch = getattr(self.store, "append_batch", None)
                if not callable(append_batch):
                    raise DomainError("ATOMIC_EVENT_STORE_REQUIRED")
                append_batch(events, expected_seq=base_state.seq)
            self._cursor = cursor
        except BaseException:
            self.state = base_state
            self._id_counter, self._manager_due = base_counter, base_due
            self._current_valuation = base_valuation
            raise
        finally:
            self._buffer = None

    def _result(self) -> RunResult:
        events = self.store.events(self.run_id)
        valuations = [
            cast(ValuationRecord, e.payload) for e in events if e.type == "VALUATION_RECORDED"
        ]
        if self.pause and self.pause.get("valuation"):
            valuations.append(self.pause["valuation"])
        scored = [
            cast(ValuationRecord, e.payload) for e in events if e.type == "SCORED_END_VALUATION"
        ]
        decisions = [
            {
                "actor": e.payload["actor_id"],
                "at": e.sim_time_utc.isoformat(),
                "proposal": e.payload.get("kind", "REJECTED"),
                **({"rejected": e.payload["code"]} if e.type == "DECISION_REJECTED" else {}),
            }
            for e in events
            if e.type in ("DECISION_MADE", "DECISION_REJECTED")
        ]
        return RunResult(
            self.run_id,
            events,
            self.state,
            decisions,
            self.status,
            self.pause,
            valuations,
            scored[-1] if scored else None,
            {
                "included": bool(
                    self.profile.study
                    and self.profile.study.runoff_end_date
                    and self.profile.study.scored_end_date
                    and self.profile.study.runoff_end_date > self.profile.study.scored_end_date
                ),
                "open_positions": len(self.state.open_positions()),
                "complete": self.status == "COMPLETED" and not self.state.open_positions(),
            },
        )

    def run(self, start: date, end: date) -> RunResult:
        """Start or resume the same run at its last atomically committed phase."""
        assert self.profile.portfolio is not None
        assert self.profile.portfolio.initial_capital_usd is not None
        initial_cash = self.profile.portfolio.initial_capital_usd
        sessions = self.calendar.session_days(start, end)
        self._run_sessions, self._run_end = sessions, end
        cutoff = self.profile.study.scored_end_date if self.profile.study else None
        self._scored_session_day = max(
            (s.day for s in sessions if cutoff is not None and s.day <= cutoff),
            default=None,
        )
        boot = (
            sessions[0].open_utc() if sessions else datetime.combine(start, time.min, tzinfo=UTC_TZ)
        )
        runtime_load = getattr(self.store, "load_run", None)
        record = runtime_load(self.run_id) if callable(runtime_load) else None
        if record is None:
            begin = getattr(self.store, "begin_run", None)
            if callable(begin):
                begin(
                    self.run_id,
                    {
                        "format_version": 2,
                        "engine_version": "2.1",
                        "profile": self.profile.model_dump(mode="json"),
                        "calendar_id": self.calendar.calendar_id,
                        "data_manifest": getattr(self.archive, "manifest", {}),
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    },
                )
        existing = self.store.events(self.run_id)
        if existing:
            if record is None or not record.cursor:
                raise DomainError("RESUME_CURSOR_REQUIRED")
            previous_hash = "genesis"
            for index, event in enumerate(existing, 1):
                if (
                    event.run_id != self.run_id
                    or event.seq != index
                    or event.previous_hash != previous_hash
                    or event.payload_hash != payload_hash(event.payload)
                    or event.event_hash != event_hash(event)
                ):
                    raise DomainError("RESUME_EVENT_CHAIN_MISMATCH")
                previous_hash = event.event_hash
            self.state = replay(self.run_id, initial_cash, existing)
            self._cursor = dict(record.cursor)
            self._id_counter = int(self._cursor.get("id_counter", 0))
            self._manager_due = bool(self._cursor.get("manager_due", False))
            if (self._cursor.get("ledger_seq"), self._cursor.get("ledger_hash")) != self.store.tip(
                self.run_id
            ) or self._cursor.get("state_hash") != self._state_hash():
                raise DomainError("RESUME_CURSOR_LINEAGE_MISMATCH")
            previous = [
                cast(ValuationRecord, e.payload) for e in existing if e.type == "VALUATION_RECORDED"
            ]
            self._current_valuation = previous[-1] if previous else None
            if record.status == "COMPLETED" or self._cursor.get("next_phase") == "COMPLETE":
                self.status = "COMPLETED"
                return self._result()
            if record.pause and record.pause.get("category") == "EPISTEMIC":
                self.status, self.pause = "PAUSED", cast(PauseReason, dict(record.pause))
                return self._result()
            if record.pause and record.pause.get("code") in {
                "BILLING_UNCERTAIN",
                "INVALID_USAGE",
                "BUDGET_OVERRUN",
                "BUDGET_EXCEEDED",
            }:
                self.status, self.pause = "PAUSED", cast(PauseReason, dict(record.pause))
                return self._result()
        else:
            self._cursor = {"session_index": 0, "minute_offset": 0, "next_phase": "MARKET"}
            self._atomic(
                boot,
                "START",
                lambda: self._emit(
                    boot,
                    "RUN",
                    "RUN_STARTED",
                    {
                        "profile_id": self.profile.profile_id,
                        "mode": self.profile.mode,
                        "initial_cash_usd": str(initial_cash),
                    },
                ),
                self._cursor,
            )
        self.status, self.pause = "RUNNING", None
        for session_index, sess in enumerate(sessions):
            if session_index < int(self._cursor.get("session_index", 0)):
                continue
            self._session(sess, session_index)
            if self._is_paused():
                return self._result()
        last_t = self.store.events(self.run_id)[-1].sim_time_utc if existing or sessions else boot
        if self.state.open_positions():
            self._persist_pause(last_t, "FINAL", "DATA", "RUNOFF_INCOMPLETE")
            return self._result()
        self._atomic(
            last_t,
            "END",
            lambda: self._emit(
                last_t,
                "RUN",
                "RUN_ENDED",
                {
                    "research_validity": "UNVALIDATED",
                },
            ),
            {"next_phase": "COMPLETE", "simulated_at": last_t.isoformat()},
        )
        self.status = "COMPLETED"
        persist = getattr(self.store, "persist_cursor", None)
        if callable(persist):
            persist(self.run_id, self._cursor, "COMPLETED", None)
        return self._result()

    def _session(self, sess: SessionDay, session_index: int) -> None:
        assert self.profile.clock is not None
        grid = set(self.calendar.review_times(sess.day, self.profile.clock.agent_review_minutes))
        first_review = min(grid) if grid else None
        start_offset = int(self._cursor.get("minute_offset", 0))
        self._quote_cache.clear()
        self._candidate_cache.clear()
        for offset in sess.minute_offsets():
            if offset < start_offset:
                continue
            t = self.calendar.utc_minute(sess.day, offset)
            phase = self._cursor.get("next_phase", "MARKET")
            self._quote_cache.clear()
            self._candidate_cache.clear()
            self._candidate_findings.clear()
            if phase == "MARKET":

                def market(at: datetime = t) -> None:
                    self._resolve_orders(at)
                    self._settle_due(sess.day, at)
                    self._record_valuation(at)
                    if at == first_review:
                        self._manager_due = True

                try:
                    self._atomic(
                        t,
                        "MARKET",
                        market,
                        {
                            "session_index": session_index,
                            "minute_offset": offset,
                            "next_phase": "DECISION",
                            "simulated_at": t.isoformat(),
                        },
                    )
                except DataPause as exc:
                    self._persist_pause(t, "MARKET", "DATA", exc.code)
                    return
            if self._cursor.get("next_phase") == "DECISION":
                try:
                    self._decision_barrier(sess, t, offset, session_index, t in grid)
                except DataPause as exc:
                    self._persist_pause(t, "DECISION", "DATA", exc.code)
                    return
                except PolicyError as exc:
                    category: PauseCategory = (
                        "BUDGET"
                        if "BUDGET" in exc.code
                        or exc.code in {"BILLING_UNCERTAIN", "INVALID_USAGE"}
                        else (
                            "EPISTEMIC"
                            if any(
                                s in exc.code
                                for s in (
                                    "EVIDENCE",
                                    "EGRESS",
                                    "SCOPE",
                                    "DEPENDENCY",
                                    "ASSESSMENT",
                                    "EPISTEMIC",
                                )
                            )
                            else "MODEL"
                        )
                    )
                    self._persist_pause(t, "DECISION", category, exc.code)
                    return
        close_t = sess.close_utc()
        if self._cursor.get("next_phase") != "SETTLEMENT":
            try:

                def close() -> None:
                    self._session_close(sess, close_t)
                    self._settle_due(sess.day, close_t)
                    self._record_valuation(close_t)
                    if self._scored_session_day == sess.day:
                        self._emit(
                            close_t,
                            "VALUATION",
                            "SCORED_END_VALUATION",
                            dict(self._current_valuation or {}),
                        )

                self._atomic(
                    close_t,
                    "CLOSE",
                    close,
                    {
                        "session_index": session_index,
                        "minute_offset": len(sess.minute_offsets()),
                        "next_phase": "SETTLEMENT",
                        "simulated_at": close_t.isoformat(),
                    },
                )
            except DataPause as exc:
                self._persist_pause(close_t, "CLOSE", "DATA", exc.code)
                return
        # Private archive availability drives settlement events, never a model-visible schedule.
        times: set[datetime] = set()
        next_open = (
            self._run_sessions[session_index + 1].open_utc()
            if session_index + 1 < len(self._run_sessions)
            else datetime.combine(
                (self._run_end or sess.day) + timedelta(days=1), time.min, tzinfo=NY
            ).astimezone(UTC_TZ)
        )
        for pos in self.state.open_positions():
            if pos.spread.expiration_local_date > sess.day:
                continue
            row = self.archive.settlement_for(pos.spread.expiration_local_date)
            if row and row.get("simulated_available_at_utc") is not None:
                available = row["simulated_available_at_utc"]
                if close_t < available < next_open:
                    times.add(available)
        for t in sorted(times):
            if t.isoformat() <= str(self._cursor.get("settlement_processed_through", "")):
                continue
            try:

                def settle(at: datetime = t) -> None:
                    self._settle_due(sess.day, at)
                    self._record_valuation(at, allow_unknown=True)

                self._atomic(
                    t,
                    "SETTLEMENT",
                    settle,
                    {
                        "next_phase": "SETTLEMENT",
                        "settlement_processed_through": t.isoformat(),
                        "simulated_at": t.isoformat(),
                    },
                )
            except DataPause as exc:
                self._persist_pause(t, "SETTLEMENT", "DATA", exc.code)
                return
        cursor = self._runtime_cursor(
            session_index=session_index + 1,
            minute_offset=0,
            next_phase="MARKET",
        )
        self._cursor = cursor
        persist = getattr(self.store, "persist_cursor", None)
        if callable(persist):
            persist(self.run_id, cursor, "RUNNING", None)

    def _is_paused(self) -> bool:
        return self.status == "PAUSED"

    def _last_trading_time(self, pos: Position) -> datetime:
        known = pos.spread.short.last_trading_at_utc
        if known is not None:
            return known
        session = self.calendar.session(pos.spread.expiration_local_date)
        if session is None:
            raise DataPause("MISSING_EXPIRY_SESSION")
        return session.close_utc()

    def _settle_due(self, day: date, t: datetime) -> None:
        assert self.profile.execution is not None
        for pos in sorted(self.state.open_positions(), key=lambda p: p.position_id):
            if pos.spread.expiration_local_date > day or t < self._last_trading_time(pos):
                continue
            row = self.archive.settlement_for(pos.spread.expiration_local_date)
            if row is None or row.get("value_index_points") is None:
                raise DataPause("MISSING_SETTLEMENT_VALUE")
            available = row.get("simulated_available_at_utc")
            published, settled = row.get("published_at_utc"), row.get("settled_at_utc")
            if available is None or published is None or settled is None:
                raise DataPause("UNVERIFIED_SETTLEMENT_TIME")
            if available < max(published, settled) or settled < self._last_trading_time(pos):
                raise DataPause("INVALID_SETTLEMENT_TIME")
            if available > t:
                continue
            value = require_settlement_value(Decimal(str(row["value_index_points"])))
            fee = getattr(self.profile.execution, "settlement_fee_per_leg_usd", None)
            if fee is None:
                if self.profile.mode != "synthetic_test":
                    raise DataPause("MISSING_SETTLEMENT_FEE")
                fee = self.profile.execution.closing_fee_per_leg_usd
            assert fee is not None
            self._emit(
                t,
                "SETTLEMENT",
                "POSITION_SETTLED",
                {
                    "position_id": pos.position_id,
                    "settlement_value_points": str(value),
                    "liability_points": str(expiration_liability_points(pos.spread, value)),
                    "fees_usd": str(usd(fee * 2)),
                    "final_status": "SETTLED",
                    "published_at_utc": published.isoformat(),
                    "settled_at_utc": settled.isoformat(),
                    "cash_availability_policy": "immediate_on_verified_publication",
                    "fee_assumption": (
                        "synthetic_closing_fee"
                        if getattr(self.profile.execution, "settlement_fee_per_leg_usd", None)
                        is None
                        else "explicit_settlement_fee"
                    ),
                },
            )
            self._retire_agent(pos.agent_id, t, "SETTLED")
            self._manager_due = True

    def _quote_row(self, contract_id: str, t: datetime) -> dict[str, Any] | None:
        key = (contract_id, t)
        if key not in self._quote_cache:
            age = self.profile.quality.max_quote_age_seconds if self.profile.quality else 300
            ids = {contract_id}
            for position in self.state.open_positions():
                ids.update((position.spread.short.contract_id, position.spread.long.contract_id))
            for order in self.state.orders.values():
                if order.status is OrderStatus.PENDING:
                    ids.update((order.spread.short.contract_id, order.spread.long.contract_id))
            rows = self.archive.session_quotes(sorted(ids), t, max_age_seconds=age)
            self._quote_cache.update({(cid, t): rows.get(cid) for cid in ids})
        return self._quote_cache[key]

    def _pair(self, spread: Any, t: datetime) -> tuple[Quote, Quote]:
        rows = [self._quote_row(c.contract_id, t) for c in (spread.short, spread.long)]
        if any(row is None for row in rows):
            raise DataPause("REQUIRED_QUOTE_MISSING")
        try:
            short, long = (_quote_from_row(row) for row in rows if row is not None)
            if (short.contract_id, long.contract_id) != (
                spread.short.contract_id,
                spread.long.contract_id,
            ):
                raise DomainError("QUOTE_CONTRACT_MISMATCH")
            validate_pair(
                short,
                long,
                t,
                max_snapshot_age_seconds=(
                    self.profile.quality.max_quote_age_seconds if self.profile.quality else 300
                ),
                max_event_age_seconds=(
                    self.profile.execution.max_known_quote_age_seconds
                    if self.profile.execution
                    else 60
                ),
            )
        except (ValueError, TypeError, ArithmeticError) as exc:
            code = str(exc) if isinstance(exc, DomainError) else "INVALID_QUOTE_VALUE"
            raise DataPause(f"INVALID_REQUIRED_QUOTE:{code}") from exc
        return short, long

    def _record_valuation(self, t: datetime, *, allow_unknown: bool = False) -> None:
        assert self.profile.execution is not None
        fee = self.profile.execution.closing_fee_per_leg_usd or Decimal(0)
        positions: list[dict[str, Any]] = []
        mid_total = liquidation_total = fees_total = Decimal(0)
        unknown = False
        quality: ValuationQuality = "OK"
        for pos in sorted(self.state.open_positions(), key=lambda p: p.position_id):
            if t >= self._last_trading_time(pos):
                unknown = True
                if quality != "UNPRICEABLE":
                    quality = "PENDING_SETTLEMENT"
                positions.append(
                    {
                        "position_id": pos.position_id,
                        "quality": "PENDING_SETTLEMENT",
                        "mid_liability_usd": None,
                        "liquidation_liability_usd": None,
                    }
                )
                continue
            try:
                short, long = self._pair(pos.spread, t)
                if short.ask_size_contracts < 1 or long.bid_size_contracts < 1:
                    raise DataPause("POSITION_LIQUIDATION_SIZE_INADEQUATE")
            except DataPause as exc:
                if not allow_unknown:
                    raise
                unknown = True
                quality = "UNPRICEABLE"
                positions.append(
                    {
                        "position_id": pos.position_id,
                        "quality": "UNPRICEABLE",
                        "reason_code": exc.code,
                        "mid_liability_usd": None,
                        "liquidation_liability_usd": None,
                    }
                )
                continue
            mid = (
                ((short.bid_points + short.ask_points) - (long.bid_points + long.ask_points))
                / 2
                * pos.spread.multiplier
            )
            liquid = (short.ask_points - long.bid_points) * pos.spread.multiplier
            closing_fees = usd(fee * 2)
            mid_total += mid
            liquidation_total += liquid
            fees_total += closing_fees
            positions.append(
                {
                    "position_id": pos.position_id,
                    "mid_liability_usd": str(usd(mid)),
                    "liquidation_liability_usd": str(usd(liquid)),
                    "estimated_closing_fees_usd": str(closing_fees),
                    **(
                        {"anomalies": ["LIQUIDATION_ABOVE_WIDTH"]}
                        if liquid > pos.spread.width_points * pos.spread.multiplier
                        else {}
                    ),
                    "quality": "OK"
                    if short.quote_event_time_known and long.quote_event_time_known
                    else "UNKNOWN_EVENT_AGE",
                    "source_refs": [
                        {
                            "contract_id": q.contract_id,
                            "snapshot_at": q.snapshot_at_utc.isoformat(),
                            "available_at": q.simulated_available_at_utc.isoformat(),
                            "bid_points": str(q.bid_points),
                            "ask_points": str(q.ask_points),
                        }
                        for q in (short, long)
                    ],
                }
            )
        self._current_valuation = {
            "as_of": t.isoformat(),
            "cash_usd": str(self.state.account.cash),
            "reserved_usd": str(self.state.account.reserved),
            "available_capital_usd": str(self.state.account.available()),
            "fees_paid_usd": str(self.state.account.fees_paid),
            "mid_liability_usd": None if unknown else str(usd(mid_total)),
            "liquidation_liability_usd": None if unknown else str(usd(liquidation_total)),
            "estimated_closing_fees_usd": str(usd(fees_total)),
            "mid_equity_usd": None if unknown else str(usd(self.state.account.cash - mid_total)),
            "net_liquidation_equity_usd": None
            if unknown
            else str(usd(self.state.account.cash - liquidation_total - fees_total)),
            "quality": quality,
            "positions": positions,
        }
        self._emit(t, "VALUATION", "VALUATION_RECORDED", dict(self._current_valuation))

    def _equity(self) -> Decimal | None:
        if self._current_valuation is None:
            return None
        value = self._current_valuation["net_liquidation_equity_usd"]
        return Decimal(value) if value is not None else None

    def _has_positive_equity(self) -> bool:
        equity = self._equity()
        return equity is not None and equity.is_finite() and equity > 0

    def _resolve_orders(self, t: datetime) -> None:
        assert self.profile.execution is not None
        ex = self.profile.execution
        for order in sorted(
            self.state.orders.values(), key=lambda o: (o.intent is OrderIntent.OPEN, o.order_id)
        ):
            if order.status is not OrderStatus.PENDING:
                continue
            if t < order.first_eligible_at_utc:
                continue
            if t > order.first_eligible_at_utc:
                self._expire_order(order, t, "EXPIRED")
                continue
            last_trading = order.spread.short.last_trading_at_utc
            if last_trading is not None and t >= last_trading:
                self._expire_order(order, t, "CONTRACT_NOT_TRADABLE")
                continue
            fee_leg = (
                ex.closing_fee_per_leg_usd
                if order.intent is OrderIntent.CLOSE
                else ex.opening_fee_per_leg_usd
            ) or Decimal(0)
            short_q, long_q = self._pair(order.spread, t)
            pkg = PackageOrder(
                order_id=order.order_id,
                intent=Intent[order.intent.name],
                limit_points=order.limit_points,
                submitted_at_utc=order.submitted_at_utc,
                first_eligible_at_utc=order.first_eligible_at_utc,
            )
            outcome = try_fill(pkg, short_q, long_q, t)
            if outcome.status is FillStatus.FILLED and outcome.package_price_points is not None:
                if not on_increment(
                    outcome.package_price_points, order.spread.short.price_increment
                ):
                    raise DataPause("PACKAGE_PRICE_INCREMENT")
                if order.intent is OrderIntent.OPEN:
                    credit = outcome.package_price_points
                    if credit <= 0 or credit >= order.spread.width_points:
                        raise DataPause("INVALID_OPEN_CREDIT")
                    assert self.profile.portfolio is not None
                    maximum = self.profile.portfolio.max_per_spread_initial_risk_usd
                    risk = (order.spread.width_points - credit) * order.spread.multiplier
                    if maximum is not None and risk > maximum:
                        self._expire_order(order, t, "RISK_LIMIT_AT_FILL")
                        continue
                fees = usd(fee_leg * 2)
                self._apply_fill(order, outcome.package_price_points, fees, t)
            elif outcome.status in (
                FillStatus.LIMIT_NOT_MET,
                FillStatus.EXPIRED,
                FillStatus.SIZE_INADEQUATE,
            ):
                self._expire_order(order, t, outcome.status.name)
            elif outcome.status in (FillStatus.QUOTE_MISSING, FillStatus.QUOTE_UNUSABLE):
                raise DataPause(outcome.status.name)
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
        short, long = self._pair(order.spread, t)
        self._emit(
            t,
            "ORDER",
            "ORDER_RESOLVED",
            {
                "order_id": order.order_id,
                "status": "FILLED",
                "price_points": str(price),
                "fees_usd": str(fees),
                "fill_id": f"{order.order_id}:fill",
                "execution_policy_id": "natural_quote_sides:first_eligible_minute:v2.1",
                "price_increment_points": str(order.spread.short.price_increment),
                "source_quotes": [
                    {
                        "contract_id": q.contract_id,
                        "snapshot_at": q.snapshot_at_utc.isoformat(),
                        "available_at": q.simulated_available_at_utc.isoformat(),
                        "bid_points": str(q.bid_points),
                        "ask_points": str(q.ask_points),
                        "bid_size_contracts": q.bid_size_contracts,
                        "ask_size_contracts": q.ask_size_contracts,
                    }
                    for q in (short, long)
                ],
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
                        "entry_order_id": order.order_id,
                        "entry_fill_id": f"{order.order_id}:fill",
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
                    "exit_order_id": order.order_id,
                    "exit_fill_id": f"{order.order_id}:fill",
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

    def _manager_context(self, t: datetime, offset: int, session_index: int) -> DecisionContext:
        assert self.profile.portfolio is not None
        portfolio = self.profile.portfolio
        counts = self._direction_counts()
        capacity = portfolio.max_open_or_reserved_slots
        total_weight = portfolio.bullish_weight + portfolio.bearish_weight
        bull_target = (capacity * portfolio.bullish_weight + total_weight // 2) // total_weight
        view = ManagerView(
            as_of_utc=t,
            active_bullish=counts["active_bull"],
            active_bearish=counts["active_bear"],
            reserved_bullish=counts["reserved_bull"],
            reserved_bearish=counts["reserved_bear"],
            capacity=capacity,
            bullish_target=bull_target,
            bearish_target=capacity - bull_target,
            paused=self.state.paused or not self._has_positive_equity(),
            available_usd=self.state.account.available(),
            reservations=tuple(
                r
                for r in self.state.reservations.values()
                if r.status is ReservationStatus.SEEKING_ENTRY
            ),
            macro_facts=self._macro_facts(t),
            equity_usd=self._equity(),
        )
        return DecisionContext(
            self.run_id,
            self.branch_id,
            "manager-1",
            "MANAGER",
            t,
            session_index,
            offset,
            manager_view=view,
            base_ledger_seq=self._base_tip[0],
            base_ledger_hash=self._base_tip[1],
            barrier_id=self._barrier_id,
        )

    def _decision_barrier(
        self,
        sess: SessionDay,
        t: datetime,
        offset: int,
        session_index: int,
        on_grid: bool,
    ) -> None:
        """All due actors observe one state; no accepted action commits before the last response."""
        assert self.profile.clock is not None
        self._base_tip = self.store.tip(self.run_id)
        self._barrier_id = f"{self.branch_id}:{t.isoformat()}:DECISION"
        cutoff = self.profile.study.scored_end_date if self.profile.study else None
        runoff = cutoff is not None and sess.day > cutoff
        contexts: list[DecisionContext] = []
        manager_due = (
            self._manager_due and offset >= self.profile.clock.agent_review_minutes and not runoff
        )
        if manager_due:
            contexts.append(self._manager_context(t, offset, session_index))
        if on_grid:
            for agent in sorted(self.state.live_agents(), key=lambda a: a.agent_id):
                if agent.role != "SPREAD" or agent.next_review_at_utc > t:
                    continue
                if agent.state not in (AgentState.SEEKING_ENTRY, AgentState.OPEN):
                    continue
                if runoff and agent.state is AgentState.SEEKING_ENTRY:
                    continue
                pos = self.state.positions.get(agent.position_id or "")
                if pos is not None and t >= self._last_trading_time(pos):
                    continue
                view = self._spread_view(agent, sess, t)
                contexts.append(
                    DecisionContext(
                        self.run_id,
                        self.branch_id,
                        agent.agent_id,
                        "SPREAD",
                        t,
                        session_index,
                        offset,
                        spread_view=view,
                        base_ledger_seq=self._base_tip[0],
                        base_ledger_hash=self._base_tip[1],
                        barrier_id=self._barrier_id,
                    )
                )
        prepared: list[
            tuple[Policy, DecisionContext, Proposal, dict[str, Any] | None, str | None]
        ] = []
        try:
            for ctx in contexts:
                policy = self.policy_provider(ctx.role)
                try:
                    proposal = policy.decide(ctx)
                except BaseException:
                    self._discard_pending(policy, ctx)
                    raise
                rejection = None
                try:
                    if ctx.role == "MANAGER":
                        validate_manager_proposal(ctx, proposal)
                        self._check_manager_capacity(proposal, t)
                    else:
                        validate_spread_proposal(ctx, proposal)
                except Rejection as exc:
                    rejection = exc.code
                witness = deepcopy(getattr(policy, "last_witness", None))
                prepared.append((policy, ctx, proposal, witness, rejection))
        except BaseException:
            for policy, ctx, *_ in prepared:
                self._discard_pending(policy, ctx)
            raise
        assessments: list[Any] = []
        observations: dict[str, Any] = {"atoms": [], "deliveries": []}
        accepted: list[tuple[Policy, DecisionContext]] = []
        rejected: list[tuple[Policy, DecisionContext]] = []

        def commit_actions() -> None:
            unique_findings = {json.dumps(f, sort_keys=True): f for f in self._candidate_findings}
            for key in sorted(unique_findings):
                self._emit(t, "DATA", "DATA_QUALITY_FINDING", unique_findings[key])
            if manager_due:
                self._manager_due = False
            for policy, ctx, proposal, witness, reason in prepared:
                # Manager retirement/allocation can invalidate a spread proposal from
                # the frozen view. Record the deterministic rejection, never resample.
                if reason is None and ctx.role == "SPREAD":
                    current = self.state.agents.get(ctx.actor_id)
                    previous = ctx.spread_view.agent if ctx.spread_view else None
                    if current is None or previous is None or current.state is not previous.state:
                        reason = "STATE_CHANGED_AT_COMMIT"
                if reason is not None:
                    if witness:
                        self._emit(t, "DECISION", "DECISION_WITNESS", witness)
                    self._emit(
                        t,
                        "DECISION",
                        "DECISION_REJECTED",
                        {
                            "actor_id": ctx.actor_id,
                            "kind": proposal.kind,
                            "code": reason,
                            **(
                                {
                                    "private_decision_id": witness["private_decision_id"],
                                    "request_hash": witness["request_hash"],
                                }
                                if witness
                                and "private_decision_id" in witness
                                and "request_hash" in witness
                                else {}
                            ),
                        },
                    )
                    rejected.append((policy, ctx))
                    continue
                if witness:
                    self._emit(t, "DECISION", "DECISION_WITNESS", witness)
                self._emit(
                    t,
                    "DECISION",
                    "DECISION_MADE",
                    {
                        "actor_id": ctx.actor_id,
                        "kind": proposal.kind,
                        "reason_codes": list(proposal.reason_codes),
                        "barrier_id": self._barrier_id,
                    },
                )
                if ctx.role == "MANAGER":
                    self._apply_manager(t, proposal)
                else:
                    assert ctx.spread_view is not None
                    self._apply_spread(ctx.spread_view.agent, ctx.spread_view, t, proposal)
                staged = getattr(policy, "staged_assessments", None)
                if callable(staged):
                    assessments.extend(staged(ctx))
                staged_obs = getattr(policy, "staged_observations", None)
                if callable(staged_obs):
                    bundle = staged_obs(ctx)
                    if isinstance(bundle, tuple):
                        atoms, deliveries = bundle
                    else:
                        atoms, deliveries = bundle.get("atoms", []), bundle.get("deliveries", [])
                    observations["atoms"].extend(atoms)
                    observations["deliveries"].extend(deliveries)
                accepted.append((policy, ctx))

        try:
            self._atomic(
                t,
                "DECISION",
                commit_actions,
                {
                    "session_index": session_index,
                    "minute_offset": offset + 1,
                    "next_phase": "MARKET",
                    "simulated_at": t.isoformat(),
                },
                assessments=assessments,
                observations=observations,
            )
        except BaseException:
            for policy, ctx, *_ in prepared:
                self._discard_pending(policy, ctx)
            raise
        for policy, ctx in rejected:
            self._discard_pending(policy, ctx)
        durable = callable(getattr(self.store, "commit_barrier", None))
        for policy, ctx in accepted:
            finalize = getattr(policy, "finalize", None)
            if durable and callable(finalize):
                finalize(ctx, persisted=True)
            else:
                self._commit_pending(policy, ctx)

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

    def _committed_risk_usd(self) -> Decimal:
        """Worst-case loss of everything already committed: open positions at
        their true initial risk (width - entry credit) x multiplier, plus live
        reservations at their full encumbrance (their future credit is not
        yet known, so reserve is the conservative bound)."""
        total = Decimal(0)
        for pos in self.state.open_positions():
            width = pos.spread.width_points
            total += (width - pos.entry_credit_points) * pos.spread.multiplier
        for res in self.state.reservations.values():
            if res.status in (ReservationStatus.SEEKING_ENTRY, ReservationStatus.ENTRY_PENDING):
                total += res.reserve_usd
        return total

    def _check_manager_capacity(self, p: Proposal, t: datetime) -> None:
        """Validate an ALLOCATE against risk caps before it is committed —
        over-cap is a policy Rejection, not a crash (H3).

        The aggregate cap compares *initial risk* — positions at
        (width - credit) x multiplier, reservations at reserve — against the
        proposed new commitments. The per-spread cap is enforced at candidate
        build time where the real credit is known; comparing a reservation's
        full-width encumbrance to an initial-risk cap would reject every
        allocation regardless of actual risk."""
        if p.kind != "ALLOCATE" or not p.allocation:
            return
        if not self._has_positive_equity():
            raise Rejection("EQUITY_NOT_POSITIVE")
        assert self.profile.portfolio is not None
        reserve = self._reservation_reserve_usd(t)
        n_new = sum(p.allocation.values())
        aggregate = self.profile.portfolio.max_aggregate_committed_risk_usd
        if aggregate is not None and self._committed_risk_usd() + reserve * n_new > aggregate:
            raise Rejection("RESERVE_EXCEEDS_RISK_LIMIT")

    def _apply_manager(self, t: datetime, p: Proposal) -> None:
        prof = self.profile
        assert prof.portfolio is not None and prof.universe is not None
        if p.kind == "ALLOCATE" and p.allocation:
            reserve = self._reservation_reserve_usd(t)
            requested = dict(p.allocation)
            allocated = {"bullish": 0, "bearish": 0}
            for direction_name in sorted(p.allocation):
                direction = (
                    Direction.BULL_PUT_CREDIT
                    if direction_name == "bullish"
                    else Direction.BEAR_CALL_CREDIT
                )
                for _ in range(p.allocation[direction_name]):
                    if self.state.account.available() < reserve:
                        break  # cash is the binding constraint, not the count
                    allocated[direction_name] += 1
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
            shortfall = {
                k: requested[k] - allocated[k] for k in requested if allocated[k] < requested[k]
            }
            if shortfall:
                self._emit(
                    t,
                    "DECISION",
                    "ALLOCATION_SHORTFALL",
                    {
                        "requested": requested,
                        "allocated": allocated,
                        "unfilled": shortfall,
                        "available_usd": str(self.state.account.available()),
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

    def _spread_view(self, agent: Any, sess: SessionDay, t: datetime) -> SpreadView:
        quality = self.profile.quality
        max_quote_age = quality.max_quote_age_seconds if quality else 300
        max_greek_age = quality.max_greek_age_seconds if quality else 86400
        require_validated = bool(
            getattr(self.profile.universe, "delta_requires_validated_source", True)
            and getattr(quality, "fail_on_unvalidated_greeks_for_delta_filter", True)
        )
        pos = self.state.positions.get(agent.position_id) if agent.position_id else None
        close_debit, frac, days = None, None, 0
        if pos and pos.status is PositionStatus.OPEN:
            assert self.profile.execution is not None
            sq, lq = self._pair(pos.spread, t)
            close_debit = sq.ask_points - lq.bid_points
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
        if agent.state is AgentState.SEEKING_ENTRY and self._has_positive_equity():
            assert self.profile.universe is not None
            assert self.profile.portfolio is not None
            res = self.state.reservations[agent.reservation_id or ""]
            health_check = getattr(self.archive, "session_health", None)
            if callable(health_check):
                health = health_check(t, max_quote_age)
                if health != "HEALTHY":
                    raise DataPause(health)
            key = (t, res.direction, res.reserve_usd)
            if key not in self._candidate_cache:
                self._candidate_cache[key] = tuple(
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
                        max_quote_age_seconds=max_quote_age,
                        max_greek_age_seconds=max_greek_age,
                        require_validated_greeks=require_validated,
                        max_event_age_seconds=(
                            self.profile.execution.max_known_quote_age_seconds
                            if self.profile.execution
                            else 60
                        ),
                        findings=self._candidate_findings,
                    )
                )
            cands = self._candidate_cache[key]
            # One approved limit template per candidate (natural quote-side credit).
            entry_tpls = tuple(
                LimitTemplate(f"entry:{c.candidate_id}", "NATURAL", c.credit_points) for c in cands
            )
        exit_tpls: tuple[LimitTemplate, ...] = ()
        if pos and close_debit is not None and close_debit >= 0:
            exit_tpls = (LimitTemplate("exit-natural", "NATURAL", close_debit),)
        assert self.profile.exit_policy is not None
        exit_policy = self.profile.exit_policy
        index_reader = getattr(self.archive, "index_at", None)
        index = index_reader(t) if callable(index_reader) else None
        return SpreadView(
            agent,
            pos,
            close_debit,
            frac,
            days,
            cands,
            entry_tpls,
            exit_tpls,
            equity_usd=self._equity(),
            as_of_dte=(pos.spread.dte_calendar_days(sess.day) if pos else None),
            advisory_profit_low=exit_policy.profit_review_band[0],
            advisory_profit_high=exit_policy.profit_review_band[1],
            advisory_loss_low=exit_policy.loss_review_band[0],
            advisory_loss_high=exit_policy.loss_review_band[1],
            loss_activation_days=exit_policy.loss_activation_days_held,
            macro_facts=self._macro_facts(t),
            spot_points=(Decimal(str(index["value_index_points"])) if index else None),
        )

    def _apply_spread(self, agent: Any, view: SpreadView, t: datetime, p: Proposal) -> None:
        assert self.profile.clock is not None
        delay = timedelta(seconds=self.profile.clock.simulated_execution_delay_seconds)
        if p.kind == "OPEN":
            cutoff = self.profile.study.scored_end_date if self.profile.study else None
            if cutoff is not None and self.calendar.ny_date(t) > cutoff:
                raise Rejection("ENTRY_AFTER_SCORED_END")
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

    @staticmethod
    def _commit_pending(pol: Policy | None, ctx: DecisionContext) -> None:
        """Persist assessments staged by an LLM policy now that the engine
        has accepted the proposal. Non-LLM policies stage nothing."""
        if pol is None:
            return
        commit = getattr(pol, "commit", None)
        if callable(commit):
            commit(ctx)

    @staticmethod
    def _discard_pending(pol: Policy | None, ctx: DecisionContext) -> None:
        """Drop staged assessments for a proposal the engine rejected (or a
        barrier that failed) — a decision that never took effect leaves no
        belief-state trace."""
        if pol is None:
            return
        discard = getattr(pol, "discard", None)
        if callable(discard):
            discard(ctx)

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
        for session in self.calendar.sessions:
            if session.day < self.calendar.ny_date(t):
                continue
            for review in self.calendar.review_times(
                session.day, self.profile.clock.agent_review_minutes
            ):
                if review > t:
                    return review
        # End-of-calendar marker: no real review exists here; the run cannot
        # execute this timestamp because it is outside the immutable calendar.
        return t + timedelta(days=1)
