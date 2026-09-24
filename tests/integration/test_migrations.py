"""Fresh and historical migrations in disposable schemas; never restamp data."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.config import Config

from alembic import command
from spx_research.domain.state import Event
from spx_research.persistence.events import payload_hash
from spx_research.persistence.postgres import PostgresEventStore
from spx_research.persistence.runtime import PostgresRunStore


@pytest.fixture
def isolated_migration_db(pg_engine, monkeypatch):
    schema = "migration_" + uuid.uuid4().hex
    with pg_engine.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
    url = pg_engine.url.update_query_dict({"options": f"-csearch_path={schema}"})
    engine = sa.create_engine(url)
    monkeypatch.setenv("SPX_DB_DSN", url.render_as_string(hide_password=False))
    root = Path(__file__).resolve().parents[2]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    try:
        yield cfg, engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))


def test_clean_upgrade_does_not_create_event_hash_twice(isolated_migration_db):
    cfg, engine = isolated_migration_db
    command.upgrade(cfg, "head")
    assert "event_hash" in {c["name"] for c in sa.inspect(engine).get_columns("events")}
    assert sa.inspect(engine).has_table("runtime_attempts")


def test_populated_runtime_five_upgrade_preserves_requests_and_paid_amounts(isolated_migration_db):
    cfg, engine = isolated_migration_db
    command.upgrade(cfg, "0005_attempt_request")
    store = PostgresRunStore(engine)
    store.begin_run("new", {"format_version": 2})
    store.prepare_decision("new", "decision", {"frozen": "context"})
    store.start_attempt(
        "new", "decision", "attempt", Decimal("0.5"), Decimal("1"), request={"wire": "frozen"}
    )
    store.complete_attempt(
        "new",
        "attempt",
        response={"result": "saved"},
        actual_usd=Decimal("0.123456789012345"),
        outcome="COMPLETED",
    )
    store.accept_decision("new", "decision", result={"validated": True}, attempt_id="attempt")
    before = (
        store.load_run("new"),
        store.load_decision("new", "decision"),
        store.list_attempts("new", "decision"),
        store.journal("new"),
    )
    command.upgrade(cfg, "head")
    after = (
        store.load_run("new"),
        store.load_decision("new", "decision"),
        store.list_attempts("new", "decision"),
        store.journal("new"),
    )
    assert after == before


@pytest.mark.parametrize("revision", ["0001_initial", "0002_events_append_only"])
def test_legacy_envelopes_are_preserved_during_upgrade(isolated_migration_db, revision):
    cfg, engine = isolated_migration_db
    command.upgrade(cfg, revision)
    now = datetime(2024, 1, 2, tzinfo=UTC)
    original = []
    with engine.begin() as conn:
        conn.execute(
            sa.text("INSERT INTO runs(run_id,profile_id,started_at_utc) VALUES ('old','test',:t)"),
            {"t": now},
        )
        prev = "genesis"
        for seq in (1, 2):
            payload = {"value": seq}
            ph = payload_hash(payload)
            conn.execute(
                sa.text(
                    "INSERT INTO events(run_id,seq,sim_time_utc,phase,type,payload,"
                    "payload_hash,previous_hash) "
                    "VALUES ('old',:s,:t,'RUN','RUN_STARTED',CAST(:p AS json),:h,:v)"
                ),
                {"s": seq, "t": now, "p": json.dumps(payload), "h": ph, "v": prev},
            )
            original.append(prev)
            prev = hashlib.sha256(f"{ph}{seq}".encode()).hexdigest()[:24]
    command.upgrade(cfg, "head")
    with engine.connect() as conn:
        audit = (
            conn.execute(
                sa.text("SELECT original_envelope FROM event_hash_migration_audit ORDER BY seq")
            )
            .scalars()
            .all()
        )
    assert [a["previous_hash"] for a in audit] == original
    events = PostgresEventStore(engine).events("old")
    assert events[1].previous_hash == events[0].event_hash


def test_revision_two_with_already_present_modern_column(isolated_migration_db):
    cfg, engine = isolated_migration_db
    command.upgrade(cfg, "0002_events_append_only")
    with engine.begin() as conn:
        conn.execute(sa.text("ALTER TABLE events ADD COLUMN event_hash TEXT NOT NULL"))
    event = Event("modern", 1, datetime(2024, 1, 2, tzinfo=UTC), "RUN", "RUN_STARTED", {})
    before = PostgresEventStore(engine).append(event, 0)
    command.upgrade(cfg, "head")
    assert PostgresEventStore(engine).events("modern") == [before]


def test_already_migrated_three_logs_remain_unchanged(isolated_migration_db):
    cfg, engine = isolated_migration_db
    command.upgrade(cfg, "0003_event_envelope_hash")
    event = Event("modern", 1, datetime(2024, 1, 2, tzinfo=UTC), "RUN", "RUN_STARTED", {})
    before = PostgresEventStore(engine).append(event, 0)
    command.upgrade(cfg, "head")
    assert PostgresEventStore(engine).events("modern") == [before]


def test_corrupt_modern_hash_aborts_instead_of_repairing(isolated_migration_db):
    cfg, engine = isolated_migration_db
    command.upgrade(cfg, "0002_events_append_only")
    with engine.begin() as conn:
        conn.execute(sa.text("ALTER TABLE events ADD COLUMN event_hash TEXT NOT NULL"))
    event = Event("broken", 1, datetime(2024, 1, 2, tzinfo=UTC), "RUN", "RUN_STARTED", {})
    PostgresEventStore(engine).append(event, 0)
    with engine.begin() as conn:
        conn.execute(sa.text("UPDATE events SET event_hash='tampered'"))
    with pytest.raises(RuntimeError, match="CORRUPT_EVENT_LOG"):
        command.upgrade(cfg, "head")
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT event_hash FROM events")).scalar_one() == "tampered"
