"""Engine state aggregate and event fold (replay/replay-equivalence core).

``EngineState`` is rebuilt purely by folding committed events — recovery never
trusts an unrelated checkpoint as financial truth (T40/T41).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from spx_research.domain.state import (
    Agent,
    AgentState,
    Event,
    Order,
    OrderIntent,
    OrderStatus,
    Position,
    PositionStatus,
    Reservation,
    ReservationStatus,
)
from spx_research.domain.types import CreditSpread, Direction, DomainError, PricePoints, Right
from spx_research.engine.accounting import (
    AccountSnapshot,
    apply_entry_fill,
    apply_exit_fill,
    apply_settlement,
    hold_reservation,
    release_reservation,
    usd,
)


def _contract(p: dict[str, Any]) -> Any:
    from spx_research.domain.types import Contract

    return Contract(
        contract_id=p["contract_id"],
        root=p["root"],
        right=Right(p["right"]),
        strike_points=PricePoints(Decimal(str(p["strike_points"]))),
        expiration_local_date=date.fromisoformat(p["expiration_local_date"]),
        exercise_style=p["exercise_style"],
        settlement_style=p["settlement_style"],
        multiplier=int(p["multiplier"]),
        price_increment=Decimal(str(p["price_increment"])),
        last_trading_at_utc=(
            datetime.fromisoformat(p["last_trading_at_utc"])
            if p.get("last_trading_at_utc")
            else None
        ),
        settlement_event_at_utc=(
            datetime.fromisoformat(p["settlement_event_at_utc"])
            if p.get("settlement_event_at_utc")
            else None
        ),
        settlement_value_symbol=p.get("settlement_value_symbol"),
    )


def _spread(p: dict[str, Any]) -> CreditSpread:
    return CreditSpread(_contract(p["short"]), _contract(p["long"]), Direction(p["direction"]))


def contract_payload(c: Any) -> dict[str, Any]:
    return {
        "contract_id": c.contract_id,
        "root": c.root,
        "right": c.right.value,
        "strike_points": str(c.strike_points),
        "expiration_local_date": c.expiration_local_date.isoformat(),
        "exercise_style": c.exercise_style,
        "settlement_style": c.settlement_style,
        "multiplier": c.multiplier,
        "price_increment": str(c.price_increment),
        "last_trading_at_utc": (
            c.last_trading_at_utc.isoformat() if c.last_trading_at_utc else None
        ),
        "settlement_event_at_utc": (
            c.settlement_event_at_utc.isoformat() if c.settlement_event_at_utc else None
        ),
        "settlement_value_symbol": c.settlement_value_symbol,
    }


def spread_payload(s: CreditSpread) -> dict[str, Any]:
    return {
        "short": contract_payload(s.short),
        "long": contract_payload(s.long),
        "direction": s.direction.value,
    }


@dataclass
class EngineState:
    run_id: str
    account: AccountSnapshot
    agents: dict[str, Agent] = field(default_factory=dict)
    positions: dict[str, Position] = field(default_factory=dict)
    orders: dict[str, Order] = field(default_factory=dict)
    reservations: dict[str, Reservation] = field(default_factory=dict)
    paused: bool = False
    seq: int = 0

    def live_agents(self) -> list[Agent]:
        terminal = {AgentState.ARCHIVED, AgentState.EXPIRED, AgentState.CANCELLED}
        return [a for a in self.agents.values() if a.state not in terminal]

    def open_positions(self) -> list[Position]:
        return [p for p in self.positions.values() if p.status is PositionStatus.OPEN]


def fold(state: EngineState, ev: Event) -> EngineState:
    """Apply one committed event to the aggregate. Order is the log's order."""
    p = ev.payload
    st = state
    if ev.run_id != st.run_id:
        raise DomainError("CROSS_RUN_EVENT")
    if ev.seq != st.seq + 1:
        raise DomainError("EVENT_SEQUENCE_MISMATCH")
    st.seq = ev.seq
    if ev.type == "RESERVATION_HELD":
        st.reservations[p["reservation_id"]] = Reservation(
            p["reservation_id"],
            p["agent_id"],
            Direction(p["direction"]),
            Decimal(p["reserve_usd"]),
            datetime.fromisoformat(p["created_at_utc"]),
            datetime.fromisoformat(p["expires_at_utc"]),
        )
        st.account = hold_reservation(st.account, Decimal(p["reserve_usd"]))
    elif ev.type == "RESERVATION_STATUS":
        r = st.reservations[p["reservation_id"]]
        st.reservations[r.reservation_id] = replace(r, status=ReservationStatus(p["status"]))
    elif ev.type == "RESERVATION_RELEASED":
        r = st.reservations[p["reservation_id"]]
        st.reservations[r.reservation_id] = replace(r, status=ReservationStatus(p["final_status"]))
        st.account = release_reservation(st.account, r.reserve_usd)
    elif ev.type == "AGENT_CREATED":
        st.agents[p["agent_id"]] = Agent(
            p["agent_id"],
            p["role"],
            Direction(p["direction"]) if p["direction"] else None,
            AgentState(p["state"]),
            datetime.fromisoformat(p["created_at_utc"]),
            datetime.fromisoformat(p["next_review_at_utc"]),
            p.get("reservation_id"),
            p.get("position_id"),
        )
    elif ev.type == "AGENT_STATE":
        a = st.agents[p["agent_id"]]
        st.agents[a.agent_id] = replace(
            a,
            state=AgentState(p["state"]),
            next_review_at_utc=datetime.fromisoformat(p["next_review_at_utc"]),
            reservation_id=p.get("reservation_id", a.reservation_id),
            position_id=p.get("position_id", a.position_id),
        )
    elif ev.type == "ORDER_SUBMITTED":
        st.orders[p["order_id"]] = Order(
            p["order_id"],
            p["agent_id"],
            _spread(p["spread"]),
            OrderIntent(p["intent"]),
            Decimal(p["limit_points"]),
            datetime.fromisoformat(p["submitted_at_utc"]),
            datetime.fromisoformat(p["first_eligible_at_utc"]),
        )
    elif ev.type == "ORDER_RESOLVED":
        st.orders[p["order_id"]] = replace(
            st.orders[p["order_id"]], status=OrderStatus(p["status"])
        )
    elif ev.type == "POSITION_OPENED":
        pos = p["position"]
        spread = _spread(pos["spread"])
        st.positions[pos["position_id"]] = Position(
            pos["position_id"],
            pos["agent_id"],
            spread,
            Decimal(pos["entry_credit_points"]),
            Decimal(pos["entry_fees_usd"]),
            datetime.fromisoformat(pos["entry_at_utc"]),
            date.fromisoformat(pos["entry_ny_date"]),
            Decimal(pos["reserve_usd"]),
        )
        # Reservation reserve converts in place to the position reserve.
        st.account = apply_entry_fill(
            st.account,
            Decimal(pos["entry_credit_points"]),
            spread.multiplier,
            Decimal(pos["entry_fees_usd"]),
            Decimal(pos["reserve_usd"]),
        )
    elif ev.type == "POSITION_CLOSED":
        pos = st.positions[p["position_id"]]
        st.positions[pos.position_id] = replace(pos, status=PositionStatus(p["final_status"]))
        st.account = apply_exit_fill(
            st.account,
            Decimal(p["close_debit_points"]),
            pos.spread.multiplier,
            Decimal(p["fees_usd"]),
            pos.reserve_usd,
        )
    elif ev.type == "POSITION_SETTLED":
        pos = st.positions[p["position_id"]]
        st.positions[pos.position_id] = replace(pos, status=PositionStatus(p["final_status"]))
        st.account = apply_settlement(
            st.account,
            Decimal(p["liability_points"]),
            pos.spread.multiplier,
            Decimal(p["fees_usd"]),
            pos.reserve_usd,
        )
    elif ev.type == "MANAGER_PAUSED":
        st.paused = True
    elif ev.type == "MANAGER_RESUMED":
        st.paused = False
    return st


def replay(run_id: str, initial_cash: Decimal, events: list[Event]) -> EngineState:
    st = EngineState(run_id, AccountSnapshot(usd(initial_cash), usd(Decimal(0))))
    for ev in events:
        st = fold(st, ev)
    return st
