"""Integration fixtures: live Postgres via SPX_TEST_DSN (docker-compose db).

Tests skip cleanly when no DSN is set or the database is unreachable —
unit/property suites never depend on a running database.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa

DSN = os.environ.get("SPX_TEST_DSN")


def _available(dsn: str) -> bool:
    try:
        eng = sa.create_engine(dsn, connect_args={"connect_timeout": 2})
        with eng.connect():
            return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def pg_engine() -> Iterator[sa.engine.Engine]:
    if not DSN or not _available(DSN):
        if os.environ.get("SPX_REQUIRE_POSTGRES") == "1":
            pytest.fail("explicit SPX_TEST_DSN is required and must be reachable")
        pytest.skip("SPX_TEST_DSN postgres unavailable")
    engine = sa.create_engine(DSN)
    # apply migrations once per session
    from alembic.config import Config

    from alembic import command

    root = Path(__file__).resolve().parents[2]
    os.environ["SPX_DB_DSN"] = DSN
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    command.upgrade(cfg, "head")
    yield engine
    engine.dispose()


@pytest.fixture()
def pg(pg_engine: sa.engine.Engine) -> Iterator[sa.engine.Engine]:
    """Migrated engine with all tables truncated between tests."""
    from spx_research.persistence.schema import ALL_TABLES

    with pg_engine.begin() as conn:
        for t in ALL_TABLES:
            conn.execute(sa.text(f"TRUNCATE {t} CASCADE"))
    yield pg_engine
