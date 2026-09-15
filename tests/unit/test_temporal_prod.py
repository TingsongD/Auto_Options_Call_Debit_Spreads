"""H-01..H-06 productionization battery.

Covers: fail-closed deliveries (TK01/TK02), recipient isolation (TK08),
prefix-only reduction, future-suffix packet byte-invariance (TK12) plus the
positive control (TK13), egress rejection of dates/symbols/ids (TK15/TK16),
assessment round-trip into later packets, quarantine without prose leak
(TK27), and new-agent memory isolation (TK10/TK11).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from spx_research.epistemics.harness import Harness, digest
from spx_research.epistemics.producers import (
    _atom,
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
)
from spx_research.epistemics.types import Atom, Context, HarnessError

T0 = datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
KEY = b"test-alias-secret-key-32bytes..!"


def _ctx(actor: str, role: str = "SPREAD", as_of: datetime = T0) -> Context:
    return make_context("run-1", "main", actor, role, as_of, 0, 30, "mft_priv")


def _obs(metric: str, value: str, at: datetime, **kw: object) -> Atom:
    a = _atom(metric, value, at)
    return replace(a, **kw) if kw else a


class TestLedger:
    def test_deliver_before_availability_fails_closed(self) -> None:
        led = InMemoryObservationLedger()
        a = _obs("policy_rate_bps", "525", T0, available_at=T0 + timedelta(hours=1))
        led.put_atom(a)
        with pytest.raises(HarnessError, match="DELIVERY_BEFORE_AVAILABILITY"):
            led.deliver("run-1", "main", "agent-1", a.atom_id, T0)

    def test_wrong_recipient_delivery_refused(self) -> None:
        led = InMemoryObservationLedger()
        a = _obs("policy_rate_bps", "525", T0, recipients=("manager-1",))
        led.put_atom(a)
        with pytest.raises(HarnessError, match="WRONG_RECIPIENT"):
            led.deliver("run-1", "main", "agent-9", a.atom_id, T0)

    def test_deliveries_scoped_to_actor(self) -> None:
        led = InMemoryObservationLedger()
        a = _obs("policy_rate_bps", "525", T0)
        led.put_atom(a)
        led.deliver("run-1", "main", "manager-1", a.atom_id, T0)
        assert led.deliveries("run-1", "main", "agent-2") == []
        assert len(led.deliveries("run-1", "main", "manager-1")) == 1


class TestReducer:
    def test_prefix_only_and_unknowns(self) -> None:
        led = InMemoryObservationLedger()
        h = Harness(KEY)
        past = _obs("policy_rate_bps", "525", T0 - timedelta(hours=1))
        future = _obs("expected_rate_bps", "550", T0 + timedelta(hours=1))
        led.put_atom(past)
        led.put_atom(future)
        led.deliver("run-1", "main", "agent-1", past.atom_id, T0)
        # a future atom cannot even be delivered early — simulate a logged
        # future-dated delivery (delivered at its own later time)
        led.deliver("run-1", "main", "agent-1", future.atom_id, T0 + timedelta(hours=1))
        b = reduce_belief(led, h, _ctx("agent-1"))
        assert [a.atom_id for a in b.facts] == [past.atom_id]
        assert "UNKNOWN_FUTURE_POLICY_PATH" in b.unknowns
        assert b.belief_hash == reduce_belief(led, h, _ctx("agent-1")).belief_hash

    def test_new_agent_has_empty_belief(self) -> None:
        led = InMemoryObservationLedger()
        h = Harness(KEY)
        a = _obs("policy_rate_bps", "525", T0 - timedelta(hours=1))
        led.put_atom(a)
        led.deliver("run-1", "main", "agent-old", a.atom_id, T0)
        old = reduce_belief(led, h, _ctx("agent-old"))
        new = reduce_belief(led, h, _ctx("agent-new"))
        assert new.facts == ()
        assert new.belief_hash != old.belief_hash  # no inherited private memory


class TestPacketBoundary:
    def _built(
        self, extra_atoms: list[Atom] | None = None, value: str = "525"
    ) -> tuple[InMemoryObservationLedger, object, Context]:
        led = InMemoryObservationLedger()
        h = Harness(KEY)
        ctx = _ctx("agent-1")
        a = _obs("policy_rate_bps", value, T0 - timedelta(minutes=30))
        led.put_atom(a)
        for extra in extra_atoms or []:
            led.put_atom(extra)
        led.deliver("run-1", "main", "agent-1", a.atom_id, T0)
        from spx_research.engine.policy import SpreadView

        agent = _mk_agent()
        view = SpreadView(agent, None, None, None, 0, (), (), ())
        atoms = spread_atoms(view, ctx.as_of)
        spread_menu(view, atoms)
        for at in atoms:
            led.put_atom(at)
            led.deliver("run-1", "main", "agent-1", at.atom_id, ctx.as_of)
        return led, h, ctx

    def test_future_suffix_invariance_and_positive_control(self) -> None:
        """TK12/TK13: suffix atoms can't alter the packet; prefix changes do."""
        future_atom = _obs(
            "expected_rate_bps",
            "999",
            T0 - timedelta(minutes=30),
        )
        led_a, h, ctx = self._built()
        led_b, _, _ = self._built(extra_atoms=[future_atom])
        pa = compile_for_actor(led_a, h, ctx, _menu(led_a, ctx)).public
        pb = compile_for_actor(led_b, h, ctx, _menu(led_b, ctx)).public
        assert pa == pb
        led_c, _, _ = self._built(value="550")
        pc = compile_for_actor(led_c, h, ctx, _menu(led_c, ctx)).public
        assert pa != pc  # observed-prefix change must change the packet

    def test_egress_rejects_dates_symbols_ids(self) -> None:
        led, h, ctx = self._built()
        comp = compile_for_actor(led, h, ctx, _menu(led, ctx))
        pub = comp.public
        for bad in (
            "2024-01-02",
            "SPXW-2024-02-16-P4700",
            "run-1",
            "agent-1",
            "mft_abcdef0123456789",
            "/data/quotes/session=2024-01-02.parquet",
        ):
            tampered = dict(pub)
            tampered["premises"] = [*pub["premises"], {"leak": bad}]
            from spx_research.epistemics.egress import egress_check

            with pytest.raises(HarnessError, match="EGRESS_LEAK"):
                egress_check(tampered, ctx)

    def test_manager_packet_builds(self) -> None:
        led = InMemoryObservationLedger()
        h = Harness(KEY)
        ctx = _ctx("manager-1", "MANAGER")
        from spx_research.engine.policy import ManagerView

        view = ManagerView(
            as_of_utc=T0,
            active_bullish=1,
            active_bearish=0,
            reserved_bullish=0,
            reserved_bearish=0,
            capacity=3,
            bullish_target=2,
            bearish_target=1,
            paused=False,
            available_usd=Decimal("5000"),
            reservations=(),
        )
        atoms = manager_atoms(view, T0)
        for a in atoms:
            led.put_atom(a)
            led.deliver("run-1", "main", "manager-1", a.atom_id, T0)
        comp = compile_for_actor(led, h, ctx, manager_menu(view, atoms))
        kinds = {m["kind"] for m in comp.public["action_menu"]}
        assert {"NO_CHANGE", "ALLOCATE", "PAUSE_NEW_ALLOCATIONS", "RESUME_NEW_ALLOCATIONS"} <= kinds


def _mk_agent() -> object:
    from spx_research.domain.state import Agent, AgentState
    from spx_research.domain.types import Direction

    return Agent("agent-1", "SPREAD", Direction.BULL_PUT_CREDIT, AgentState.SEEKING_ENTRY, T0, T0)


def _menu(led: InMemoryObservationLedger, ctx: Context) -> list:
    from spx_research.engine.policy import SpreadView

    view = SpreadView(_mk_agent(), None, None, None, 0, (), (), ())
    atoms = spread_atoms(view, ctx.as_of)
    return spread_menu(view, atoms)


class TestAssessmentAndQuarantine:
    def _compiled(self) -> tuple:
        led = InMemoryObservationLedger()
        h = Harness(KEY)
        ctx = _ctx("agent-1")
        view_atoms = spread_atoms(
            __import__("spx_research.engine.policy", fromlist=["SpreadView"]).SpreadView(
                _mk_agent(), None, None, None, 0, (), (), ()
            ),
            T0,
        )
        for a in view_atoms:
            led.put_atom(a)
            led.deliver("run-1", "main", "agent-1", a.atom_id, T0)
        menu = spread_menu(
            __import__("spx_research.engine.policy", fromlist=["SpreadView"]).SpreadView(
                _mk_agent(), None, None, None, 0, (), (), ()
            ),
            view_atoms,
        )
        comp = compile_for_actor(led, h, ctx, menu)
        return led, h, ctx, comp

    def test_assessment_round_trip(self) -> None:
        led, h, ctx, comp = self._compiled()
        premise = comp.public["premises"][0]["token"]
        proposal = {
            "schema_version": "2.0",
            "actor_role": "SPREAD",
            "decision_token": comp.public["decision_token"],
            "packet_token": comp.public["packet_token"],
            "prior_belief_token": comp.public["prior_belief_token"],
            "action_id": comp.public["action_menu"][0]["action_id"],
            "premise_tokens": [premise],
            "assessment_updates": [
                {
                    "topic": "RATE_OUTLOOK",
                    "assessment": "UNCERTAIN",
                    "premise_tokens": [premise],
                    "confidence_label": "LOW",
                }
            ],
            "reason_codes": ["MAINTAIN_THESIS"],
            "uncertainty_codes": ["NONE_IDENTIFIED"],
            "confidence_label": "MEDIUM",
        }
        w = h.validate(proposal, comp)
        assert w["status"] == "ACCEPTED"
        rec = AssessmentRecord(
            "agent-1",
            "RATE_OUTLOOK",
            "UNCERTAIN",
            "LOW",
            tuple(comp.premise_map[p].atom_id for p in proposal["premise_tokens"]),
            ctx.as_of,
            proposal["decision_token"],
        )
        led.put_assessment(rec)
        # next packet carries the assessment (bounded memory)
        comp2 = compile_for_actor(led, h, _ctx("agent-1"), _menu(led, _ctx("agent-1")))
        assert comp2.public["assessments"][0]["topic"] == "RATE_OUTLOOK"

    def test_rejection_quarantined_without_prose_leak(self) -> None:
        led, h, _ctx2, comp = self._compiled()
        bad = {"action_id": "act_forged", "premise_tokens": []}
        with pytest.raises(HarnessError) as exc:
            h.validate(bad, comp)
        code = str(exc.value)
        led.quarantine(
            Incident(
                led.next_incident_id("run-1"),
                "run-1",
                "main",
                "agent-1",
                code,
                dict(bad),
                T0,
            )
        )
        incidents = led.incidents("run-1")
        assert len(incidents) == 1 and incidents[0].code == code
        # retry context carries only the code — never the rejected payload
        retry_context = {"last_error": code}
        assert "act_forged" not in str(retry_context)
        assert digest(comp.public)  # packet still well-formed
