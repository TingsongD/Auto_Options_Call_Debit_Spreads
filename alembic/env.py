"""Alembic environment: DSN from SPX_DB_DSN env var (secrets stay out of git)."""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine

from alembic import context
from spx_research.persistence.schema import metadata

config = context.config
target_metadata = metadata


def _dsn() -> str:
    dsn = os.environ.get("SPX_DB_DSN")
    if not dsn:
        sys.exit("SPX_DB_DSN is not set — e.g. postgresql+psycopg://user:pass@host/db")
    return dsn


def run_migrations_offline() -> None:
    context.configure(
        url=_dsn(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_dsn())
    with engine.connect() as conn:
        context.configure(connection=conn, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
