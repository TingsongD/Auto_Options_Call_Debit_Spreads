"""M5/H-08 integration + fault-injection tests against live Postgres.

Covers: durable event append + hash chain, single-writer CAS, transactional
outbox, observation-ledger round-trip, role denial for the inference role,
and run-scoped RLS isolation (T40/T41/T46, H-08).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa

from spx_research.domain.state import Event
from spx_research.engine.ledger import replay
from spx_research.epistemics.types import Atom
from spx_research.persistence.events import LedgerError
from spx_research.persistence.postgres import PostgresEventStore, PostgresObservationLedger


def _ev(run_id: str, seq: int, t: str = "RUN_STARTED", payload: dict | None = None) -> Event:
    return Event(
        run_id=run_id,
        seq=seq,
        sim_time_utc=datetime(2020, 1, 2, 14, 30, tzinfo=UTC),
        phase="SIM",
        type=t,
        payload=payload or {"k": "v"},
    )


def _atom(atom_id: str, run_recipients=("PUBLIC",)) -> Atom:
    t = datetime(2020, 1, 2, 0, 0, tzinfo=UTC)
    return Atom(
        atom_id=atom_id,
        metric="policy_rate_bps",
        value="425",
        unit="basis_points",
        kind="OBSERVATION",
        published_at=t,
        available_at=t,
        subject_at=t,
        recipients=tuple(run_recipients),
    )


def test_event_append_and_tip(pg):
    store = PostgresEventStore(pg)
    rid = f"it-{uuid.uuid4().hex[:8]}"
    e1 = store.append(_ev(rid, 1), expected_seq=0)
    store.append(_ev(rid, 2, "RUN_ENDED"), expected_seq=e1.seq)
    assert store.tip(rid)[0] == 2
    evs = store.events(rid)
    assert [e.seq for e in evs] == [1, 2]
    import hashlib

    expected = hashlib.sha256((evs[0].payload_hash + str(evs[0].seq)).encode()).hexdigest()[:24]
    assert evs[1].previous_hash == expected  # hash chain matches in-memory rule


def test_single_writer_cas(pg):
    """Concurrent/stale expected_seq must fail — only one writer advances."""
    store = PostgresEventStore(pg)
    rid = f"it-{uuid.uuid4().hex[:8]}"
    store.append(_ev(rid, 1), expected_seq=0)
    with pytest.raises(LedgerError, match="SEQUENCE_MISMATCH"):
        store.append(_ev(rid, 2), expected_seq=0)  # stale tip
    with pytest.raises(LedgerError, match="SEQUENCE_MISMATCH"):
        store.append(_ev(rid, 5), expected_seq=1)  # gap in seq


def test_outbox_atomic_with_event(pg):
    """Outbox rows commit in the same transaction as the event (H-08)."""
    store = PostgresEventStore(pg)
    rid = f"it-{uuid.uuid4().hex[:8]}"
    store.append(
        _ev(rid, 1),
        expected_seq=0,
        outbox=[{"topic": "audit", "payload": {"note": "x"}}],
    )
    with pg.connect() as conn:
        conn.execute(sa.text("SELECT set_config('app.run_id', :r, true)"), {"r": rid})
        n = conn.execute(
            sa.text("SELECT count(*) FROM outbox WHERE run_id = :r"), {"r": rid}
        ).scalar_one()
        assert n == 1
    # A failed append must not leave outbox rows behind.
    with pytest.raises(LedgerError):
        store.append(
            _ev(rid, 9),
            expected_seq=0,
            outbox=[{"topic": "audit", "payload": {"note": "orphan"}}],
        )
    with pg.connect() as conn:
        conn.execute(sa.text("SELECT set_config('app.run_id', :r, true)"), {"r": rid})
        n = conn.execute(
            sa.text("SELECT count(*) FROM outbox WHERE run_id = :r"), {"r": rid}
        ).scalar_one()
        assert n == 1


def test_observation_ledger_round_trip(pg):
    led = PostgresObservationLedger(pg)
    rid = f"it-{uuid.uuid4().hex[:8]}"
    aid = f"at-{uuid.uuid4().hex[:8]}"
    led.put_atom(_atom(aid))
    d = led.deliver(rid, "br-1", "agent-1", aid, datetime(2020, 1, 2, 1, 0, tzinfo=UTC))
    assert d.atom_id == aid
    assert len(led.deliveries(rid, "br-1", "agent-1")) == 1
    assert aid in led.atoms()


def test_replay_from_postgres_matches_inmemory(pg):
    """Crash recovery: events in Postgres fold to the same state (T40/T41)."""
    from decimal import Decimal

    from spx_research.persistence.events import InMemoryEventStore

    mem = InMemoryEventStore()
    pg_store = PostgresEventStore(pg)
    rid = f"it-{uuid.uuid4().hex[:8]}"
    for seq, t in enumerate(("RUN_STARTED", "DECISION_MADE", "RUN_ENDED"), start=1):
        e = _ev(rid, seq, t, {"k": seq})
        mem.append(e, expected_seq=seq - 1)
        pg_store.append(e, expected_seq=seq - 1)
    assert [e.payload_hash for e in mem.events(rid)] == [
        e.payload_hash for e in pg_store.events(rid)
    ]
    s_pg = replay(rid, Decimal("10000"), pg_store.events(rid))
    s_mem = replay(rid, Decimal("10000"), mem.events(rid))
    assert s_pg.account.cash == s_mem.account.cash


def test_inference_role_denied(pg):
    """The inference role cannot read or write any private table (H-08)."""
    rid = f"it-{uuid.uuid4().hex[:8]}"
    PostgresEventStore(pg).append(_ev(rid, 1), expected_seq=0)
    with pg.connect() as conn:
        conn.execute(sa.text("SET ROLE spx_inference"))
        conn.execute(sa.text("SAVEPOINT sp1"))
        with pytest.raises(sa.exc.ProgrammingError):
            conn.execute(sa.text("SELECT * FROM events"))
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT sp1"))
        with pytest.raises(sa.exc.ProgrammingError):
            conn.execute(
                sa.text(
                    "INSERT INTO incidents (incident_id, run_id, branch_id, actor_id,"
                    " code, rejected_payload, at_utc) VALUES ('x', :r, 'b', 'a', 'c',"
                    " '{}', now())"
                ),
                {"r": rid},
            )
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT sp1"))
        conn.execute(sa.text("RESET ROLE"))
        conn.rollback()


def test_run_scoped_rls(pg):
    """Under spx_engine + RLS, run A's rows are invisible to run B's scope."""
    rid_a, rid_b = f"it-a-{uuid.uuid4().hex[:6]}", f"it-b-{uuid.uuid4().hex[:6]}"
    store = PostgresEventStore(pg)
    store.append(_ev(rid_a, 1), expected_seq=0)
    store.append(_ev(rid_b, 1), expected_seq=0)
    with pg.connect() as conn:
        conn.execute(sa.text("SET ROLE spx_engine"))
        conn.execute(sa.text("SELECT set_config('app.run_id', :r, false)"), {"r": rid_a})
        rows = conn.execute(sa.text("SELECT run_id FROM events")).all()
        assert {r.run_id for r in rows} == {rid_a}
        conn.execute(sa.text("RESET ROLE"))
        conn.rollback()
