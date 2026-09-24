"""PostgreSQL runtime recovery and transaction tests on an explicit test DSN."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import sqlalchemy as sa

from spx_research.domain.state import Event
from spx_research.epistemics.types import Atom, Delivery
from spx_research.persistence import schema as S
from spx_research.persistence.events import LedgerError
from spx_research.persistence.runtime import PostgresRunStore

NOW = datetime(2024, 1, 2, 15, tzinfo=UTC)


def prepared(pg):
    store = PostgresRunStore(pg)
    store.begin_run("runtime-run", {"format_version": 2, "profile_id": "synthetic"})
    return store


def test_restart_preserves_exact_request_response_and_cost(pg):
    store = prepared(pg)
    request = {"context": {"packet_token": "pkt_x"}, "model_request": {"retry_error_code": ""}}
    store.prepare_decision("runtime-run", "d", request)
    store.start_attempt("runtime-run", "d", "a", Decimal("0.5"), Decimal("1"))
    response = {"request": request, "model_response": {"text": "validated later"}}
    store.complete_attempt(
        "runtime-run",
        "a",
        response=response,
        actual_usd=Decimal("0.123456789012345"),
        outcome="SUCCESS",
    )
    restored = PostgresRunStore(pg)
    assert restored.load_decision("runtime-run", "d").request == request
    assert restored.load_decision("runtime-run", "d").response == response
    assert restored.budget_totals("runtime-run")["committed"] == Decimal("0.123456789012345")
    restored.complete_attempt(
        "runtime-run",
        "a",
        response=response,
        actual_usd=Decimal("0.123456789012345"),
        outcome="SUCCESS",
    )
    assert len(restored.list_attempts("runtime-run", "d")) == 1


def test_concurrent_reservations_cannot_spend_the_same_budget(pg):
    store = prepared(pg)
    store.prepare_decision("runtime-run", "a", {})
    store.prepare_decision("runtime-run", "b", {})

    def reserve(attempt):
        try:
            PostgresRunStore(pg).start_attempt(
                "runtime-run", attempt, attempt, Decimal("0.75"), Decimal("1")
            )
            return "OK"
        except LedgerError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(reserve, ["a", "b"])) == ["BUDGET_EXCEEDED", "OK"]
    assert store.budget_totals("runtime-run")["reserved"] == Decimal("0.75")


def test_barrier_commits_events_knowledge_cursor_and_outbox_once(pg):
    store = prepared(pg)
    atom = Atom(
        "runtime-atom", "policy_rate_bps", "425", "basis_points", "OBSERVATION", NOW, NOW, NOW
    )
    delivery = Delivery("runtime-run", "main", "actor", atom.atom_id, NOW)
    observations = {"atoms": [asdict(atom)], "deliveries": [asdict(delivery)]}
    event = Event("runtime-run", 1, NOW, "DECISION", "DECISION_MADE", {})
    cursor = {"next_phase": "MARKET", "minute_offset": 16}
    store.prepare_barrier("runtime-run", "bar", 0, {}, {"next_phase": "DECISION"})
    expected = store.commit_barrier(
        "runtime-run",
        "bar",
        0,
        [event],
        cursor,
        observations=observations,
        outbox=[{"topic": "audit", "payload": {}}],
    )
    assert (
        store.commit_barrier(
            "runtime-run",
            "bar",
            0,
            [event],
            cursor,
            observations=observations,
            outbox=[{"topic": "audit", "payload": {}}],
        )
        == expected
    )
    assert store.load_run("runtime-run").cursor == cursor
    assert store.observation_ledger.deliveries("runtime-run", "main", "actor") == [delivery]
    with pg.connect() as conn:
        assert conn.execute(sa.text("SELECT count(*) FROM outbox")).scalar_one() == 1


def test_bad_observation_rolls_back_entire_barrier(pg):
    store = prepared(pg)
    event = Event("runtime-run", 1, NOW, "DECISION", "DECISION_MADE", {})
    with pytest.raises(LedgerError, match="DELIVERY_BEFORE_AVAILABILITY"):
        store.commit_barrier(
            "runtime-run",
            "bar",
            0,
            [event],
            {"next_phase": "DONE"},
            observations={
                "deliveries": [asdict(Delivery("runtime-run", "main", "actor", "missing", NOW))]
            },
        )
    assert store.events("runtime-run") == []
    assert store.load_run("runtime-run").cursor == {}
    with pg.connect() as conn:
        assert conn.execute(sa.text("SELECT count(*) FROM runtime_barriers")).scalar_one() == 0


def test_unresolved_attempt_budget_survives_process_restart(pg):
    store = prepared(pg)
    store.prepare_decision("runtime-run", "d", {})
    store.start_attempt("runtime-run", "d", "a", Decimal("1"), Decimal("1"))
    restarted = PostgresRunStore(pg)
    assert restarted.list_attempts("runtime-run", "d")[0].outcome == "DISPATCH_RESERVED"
    restarted.complete_attempt(
        "runtime-run", "a", response=None, actual_usd=None, outcome="TIMEOUT"
    )
    with pytest.raises(LedgerError, match="BILLING_UNCERTAIN"):
        restarted.start_attempt("runtime-run", "d", "b", Decimal("0.01"), Decimal("1"))
    assert restarted.budget_totals("runtime-run")["reserved"] == 1


def test_runtime_tables_are_denied_to_inference_role(pg):
    prepared(pg)
    with pg.connect() as conn:
        conn.execute(sa.text("SET ROLE spx_inference"))
        with pytest.raises(sa.exc.ProgrammingError):
            conn.execute(sa.text("SELECT manifest FROM runtime_runs"))
        conn.rollback()


@pytest.mark.parametrize("amount", ["-0.1", "NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("column", ["reserved_usd", "actual_usd", "budget_cap"])
def test_database_rejects_invalid_money_without_application_validation(pg, column, amount):
    store = prepared(pg)
    store.prepare_decision("runtime-run", "d", {})
    if column == "budget_cap":
        statement = S.runtime_runs.update().values(budget_cap=Decimal(amount))
    else:
        values = dict(
            run_id="runtime-run",
            attempt_id="bad",
            decision_id="d",
            reserved_usd=Decimal(0),
            actual_usd=None,
            outcome="FAILED",
            error_code="",
            request={},
        )
        values[column] = Decimal(amount)
        statement = S.runtime_attempts.insert().values(**values)
    with pytest.raises(sa.exc.IntegrityError), pg.begin() as conn:
        conn.execute(statement)


def test_database_links_attempts_and_acceptance_to_the_same_decision(pg):
    store = prepared(pg)
    store.prepare_decision("runtime-run", "one", {})
    store.prepare_decision("runtime-run", "two", {})
    with pytest.raises(sa.exc.IntegrityError), pg.begin() as conn:
        conn.execute(
            S.runtime_attempts.insert().values(
                run_id="runtime-run",
                attempt_id="orphan",
                decision_id="missing",
                reserved_usd=0,
                actual_usd=None,
                outcome="DISPATCH_RESERVED",
                error_code="",
            )
        )
    store.start_attempt("runtime-run", "one", "attempt-one", Decimal(0), Decimal(0))
    with pytest.raises(sa.exc.IntegrityError), pg.begin() as conn:
        conn.execute(
            S.runtime_decisions.update()
            .where(S.runtime_decisions.c.decision_id == "two")
            .values(accepted_attempt_id="attempt-one")
        )


def test_database_rejects_invalid_status_and_mutated_frozen_inputs(pg):
    store = prepared(pg)
    store.prepare_decision("runtime-run", "d", {"request": "original"})
    store.start_attempt(
        "runtime-run", "d", "a", Decimal(0), Decimal(0), request={"wire": "original"}
    )
    for statement in (
        S.runtime_runs.update().values(status="PAUSED_MODEL"),
        S.runtime_runs.update().values(manifest={"changed": True}),
        S.runtime_decisions.update().values(request={"changed": True}),
        S.runtime_attempts.update().values(request={"changed": True}),
    ):
        with pytest.raises(sa.exc.IntegrityError), pg.begin() as conn:
            conn.execute(statement)
    assert store.load_decision("runtime-run", "d").request == {"request": "original"}
