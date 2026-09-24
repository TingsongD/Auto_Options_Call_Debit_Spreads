"""Freeze every outgoing attempt request before dispatch, including retries."""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0005_attempt_request"
down_revision = "0004_runtime_journal"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runtime_attempts", sa.Column("request", sa.JSON))


def downgrade() -> None:
    raise RuntimeError("ATTEMPT_AUDIT_DOWNGRADE_REQUIRES_BACKUP_RESTORE")
