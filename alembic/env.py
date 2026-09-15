"""Alembic environment: DSN from SPX_DB_DSN env var (secrets stay out of git)."""

from __future__ import annotations

import os

from sqlalchemy import create_engine

from alembic import context
from spx_research.persistence.schema import metadata

config = context.config
target_metadata = metadata


def run_migrations_online() -> None:
    dsn = os.environ["SPX_DB_DSN"]
    engine = create_engine(dsn)
    with engine.connect() as conn:
        context.configure(connection=conn, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
