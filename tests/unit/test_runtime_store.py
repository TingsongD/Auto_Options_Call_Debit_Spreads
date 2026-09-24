"""Runtime transition contracts; paid attempts and barriers have different commits."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from spx_research.domain.state import Event
from spx_research.epistemics.types import Atom, Delivery
from spx_research.persistence.events import LedgerError
from spx_research.persistence.runtime import InMemoryRunStore

NOW = datetime(2024, 1, 2, 15, tzinfo=UTC)


def event(seq=1, payload=None):
    return Event("run", seq, NOW, "DECISION", "DECISION_MADE", payload or {})


@pytest.fixture
def store():
    s = InMemoryRunStore()
    s.begin_run("run", {"format_version": 2, "profile_id": "synthetic"})
    return s


def test_manifest_and_prepared_request_are_immutable(store):
    request = {"packet": {"value": "old"}}
    store.prepare_decision("run", "d", request)
    request["packet"]["value"] = "new"
    assert store.load_decision("run", "d").request["packet"]["value"] == "old"
    with pytest.raises(LedgerError, match="REQUEST_MISMATCH"):
        store.prepare_decision("run", "d", request)
    with pytest.raises(LedgerError, match="MANIFEST_MISMATCH"):
        store.begin_run("run", {"format_version": 2, "profile_id": "changed"})


def test_known_and_uncertain_attempts_are_paid_once(store):
    store.prepare_decision("run", "d", {"packet": {}})
    store.start_attempt("run", "d", "a1", Decimal("0.4"), Decimal("1"))
    done = dict(
        response={"model_response": {"text": "x"}}, actual_usd=Decimal("0.3"), outcome="SUCCESS"
    )
    store.complete_attempt("run", "a1", **done)
    store.complete_attempt("run", "a1", **done)
    store.start_attempt("run", "d", "a2", Decimal("0.6"), Decimal("1"))
    store.complete_attempt("run", "a2", response=None, actual_usd=None, outcome="TIMEOUT")
    assert store.budget_totals("run") == {
        "committed": Decimal("0.3"),
        "reserved": Decimal("0.6"),
        "cap": Decimal("1"),
    }
    with pytest.raises(LedgerError, match="BILLING_UNCERTAIN"):
        store.start_attempt("run", "d", "a3", Decimal("0.2"), Decimal("1"))
    assert len(store.list_attempts("run", "d")) == 2
    assert store.load_run("run").status == "PAUSED"


def test_overrun_is_recorded_instead_of_rolling_back_real_cost(store):
    store.prepare_decision("run", "d", {})
    store.start_attempt("run", "d", "a", Decimal("0.5"), Decimal("1"))
    store.complete_attempt("run", "a", response={}, actual_usd=Decimal("1.2"), outcome="SUCCESS")
    assert store.budget_totals("run")["committed"] == Decimal("1.2")
    assert store.load_run("run").pause["code"] == "BUDGET_EXCEEDED"


def test_barrier_retry_and_observations_commit_together(store):
    atom = Atom("a", "policy_rate_bps", "425", "basis_points", "OBSERVATION", NOW, NOW, NOW)
    delivery = Delivery("run", "main", "actor", "a", NOW)
    observations = {"atoms": [asdict(atom)], "deliveries": [asdict(delivery)]}
    cursor = {"phase": "AFTER_DECISIONS"}
    first = store.commit_barrier("run", "bar", 0, [event()], cursor, observations=observations)
    second = store.commit_barrier("run", "bar", 0, [event()], cursor, observations=observations)
    assert first == second
    assert len(store.events("run")) == 1
    assert store.observation_ledger.deliveries("run", "main", "actor") == [delivery]
    with pytest.raises(LedgerError, match="BARRIER_COMMIT_MISMATCH"):
        store.commit_barrier("run", "bar", 0, [event(payload={"changed": True})], cursor)


def test_invalid_observation_rolls_back_events_cursor_and_outbox(store):
    with pytest.raises(Exception, match="MISSING_OBSERVATION"):
        store.commit_barrier(
            "run",
            "bar",
            0,
            [event()],
            {"phase": "DONE"},
            observations={"deliveries": [asdict(Delivery("run", "b", "a", "missing", NOW))]},
            outbox=[{"topic": "test", "payload": {}}],
        )
    assert store.events("run") == []
    assert store.load_run("run").cursor == {}
    assert not store._rows.get("outbox")


def test_batch_is_atomic_and_input_payload_cannot_mutate_the_log(store):
    with pytest.raises(LedgerError, match="SEQUENCE_MISMATCH"):
        store.append_batch([event(1), event(3)], 0)
    assert store.events("run") == []
    payload = {"nested": {"value": 1}}
    store.append(event(payload=payload), 0)
    payload["nested"]["value"] = 2
    returned = store.events("run")
    returned[0].payload["nested"]["value"] = 3
    assert store.events("run")[0].payload["nested"]["value"] == 1


def test_legacy_runs_cannot_be_adopted_for_resume():
    store = InMemoryRunStore()
    store._event_store.append(event(), 0)
    with pytest.raises(LedgerError, match="LEGACY_RUN_READ_ONLY"):
        store.begin_run("run", {"format_version": 2})


def test_billing_reconciliation_requires_evidence_and_preserves_original_attempt(store):
    store.prepare_decision("run", "d", {})
    store.start_attempt("run", "d", "a", Decimal("1"), Decimal("1"), request={"retry": "TIMEOUT"})
    store.complete_attempt("run", "a", response=None, actual_usd=None, outcome="TIMEOUT")
    with pytest.raises(LedgerError, match="RECONCILIATION_EVIDENCE_REQUIRED"):
        store.reconcile_attempt("run", "a", actual_usd=Decimal("0"), evidence_ref="")
    store.reconcile_attempt(
        "run", "a", actual_usd=Decimal("0.2"), evidence_ref="receipt:known-provider-request"
    )
    assert store.budget_totals("run")["committed"] == Decimal("0.2")
    assert store.budget_totals("run")["reserved"] == 0
    assert store.load_run("run").status == "PAUSED"
    assert store.load_run("run").pause["code"] == "BILLING_RECONCILED"
    last = store.journal("run")[-1]
    assert last["payload"]["previous"]["outcome"] == "BILLING_UNCERTAIN"
    assert last["payload"]["previous"]["request"] == {"retry": "TIMEOUT"}


def test_reused_attempt_identity_rejects_changed_wire_request(store):
    store.prepare_decision("run", "d", {})
    store.start_attempt("run", "d", "a", Decimal("0.1"), Decimal("1"), request={"body": "first"})
    with pytest.raises(LedgerError, match="ATTEMPT_ID_MISMATCH"):
        store.start_attempt(
            "run", "d", "a", Decimal("0.1"), Decimal("1"), request={"body": "changed"}
        )
    with pytest.raises(LedgerError, match="BILLING_UNCERTAIN"):
        store.start_attempt("run", "d", "b", Decimal("0.1"), Decimal("1"))


def test_one_attempt_reservation_grants_only_one_dispatch(store):
    store.prepare_decision("run", "d", {})
    store.start_attempt("run", "d", "a", Decimal("0.1"), Decimal("1"))
    with pytest.raises(LedgerError, match="ATTEMPT_ALREADY_RESERVED"):
        store.start_attempt("run", "d", "a", Decimal("0.1"), Decimal("1"))
    assert store.budget_totals("run")["reserved"] == Decimal("0.1")


def test_final_event_and_completed_status_commit_atomically(store):
    ended = Event("run", 1, NOW, "RUN", "RUN_ENDED", {})
    store.commit_barrier("run", "end", 0, [ended], {"next_phase": "COMPLETE"})
    assert store.load_run("run").status == "COMPLETED"
    assert store.load_run("run").cursor["next_phase"] == "COMPLETE"


def test_stale_worker_cannot_overwrite_committed_cursor(store):
    stale = {"ledger_seq": 0, "ledger_hash": "genesis", "next_phase": "DECISION"}
    store.persist_cursor("run", stale, "RUNNING")
    committed = store.commit_barrier("run", "bar", 0, [event()], {"next_phase": "MARKET"})
    current = {"ledger_seq": 1, "ledger_hash": committed[0].event_hash, "next_phase": "MARKET"}
    store.persist_cursor("run", current, "RUNNING")
    with pytest.raises(LedgerError, match="STALE_CURSOR_WRITE"):
        store.persist_cursor("run", stale, "PAUSED", {"code": "ATTEMPT_ALREADY_RESERVED"})
    assert store.load_run("run").cursor == current
    assert store.load_run("run").status == "RUNNING"


@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED"])
def test_terminal_status_cannot_be_downgraded(store, terminal):
    store.persist_cursor("run", {}, terminal)
    with pytest.raises(LedgerError, match="TERMINAL_RUN_IMMUTABLE"):
        store.persist_cursor("run", {}, "PAUSED")
    assert store.load_run("run").status == terminal


def test_tip_and_new_barrier_do_not_read_entire_history(store, monkeypatch):
    def history_is_not_needed(*_args):
        raise AssertionError("read entire event history")

    monkeypatch.setattr(store._event_store, "events", history_is_not_needed)
    assert store.tip("run") == (0, "genesis")
    store.prepare_barrier("run", "bar", 0, {}, {})
    store.commit_barrier("run", "bar", 0, [event()], {})
    assert store.tip("run")[0] == 1
    assert store.load_run("run").cursor == {}


def test_per_attempt_overrun_blocks_recovered_acceptance_and_further_dispatch(store):
    store.prepare_decision("run", "d", {})
    store.start_attempt("run", "d", "a", Decimal("0.1"), Decimal("10"))
    store.complete_attempt("run", "a", response={}, actual_usd=Decimal("0.2"), outcome="SUCCESS")
    assert store.load_run("run").pause["code"] == "BUDGET_OVERRUN"
    assert store.budget_totals("run")["committed"] == Decimal("0.2")
    with pytest.raises(LedgerError, match="BUDGET_OVERRUN"):
        store.accept_decision("run", "d", result={}, attempt_id="a")
    store.prepare_decision("run", "d2", {})
    with pytest.raises(LedgerError, match="BUDGET_OVERRUN"):
        store.start_attempt("run", "d2", "a2", Decimal("1"), Decimal("10"))


def test_reconciliation_never_clears_an_epistemic_pause(store):
    store.prepare_decision("run", "d", {})
    store.start_attempt("run", "d", "a", Decimal("1"), Decimal("1"))
    store.complete_attempt("run", "a", response=None, actual_usd=None, outcome="TIMEOUT")
    pause = {"category": "EPISTEMIC", "code": "UNSUPPORTED_ASSERTION"}
    store.persist_cursor("run", {}, "PAUSED", pause)
    store.reconcile_attempt("run", "a", actual_usd=Decimal("0.2"), evidence_ref="receipt:123")
    assert store.load_run("run").pause == pause


@pytest.mark.parametrize("status", ["COMPLETED", "FAILED", "PAUSED"])
def test_dispatch_cannot_bypass_terminal_or_epistemic_pause(store, status):
    store.prepare_decision("run", "d", {})
    store.persist_cursor("run", {}, status, {"category": "EPISTEMIC", "code": "FUTURE_FACT"})
    with pytest.raises(LedgerError, match=r"TERMINAL_RUN_IMMUTABLE|EPISTEMIC_RUN_PAUSED"):
        store.start_attempt("run", "d", "a", Decimal(0), Decimal(0))
    assert store.list_attempts("run", "d") == []
