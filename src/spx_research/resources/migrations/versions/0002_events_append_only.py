"""Events table is append-only for the engine role (no UPDATE/DELETE).

Revision ID: 0002_events_append_only
"""

from __future__ import annotations

from alembic import op

revision = "0002_events_append_only"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("REVOKE UPDATE, DELETE ON events FROM spx_engine")


def downgrade() -> None:
    op.execute("GRANT UPDATE ON events TO spx_engine")
