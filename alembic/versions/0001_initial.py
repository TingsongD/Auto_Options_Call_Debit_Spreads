"""Initial schema: run-scoped event/epistemics tables + roles + RLS (M5-01/H-08).

Revision ID: 0001_initial
"""

from __future__ import annotations

from alembic import op
from spx_research.persistence.schema import ALL_TABLES, metadata

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

# Tables whose rows are fenced to the run set via `SET app.run_id`.
_RUN_SCOPED = (
    "runs",
    "events",
    "outbox",
    "observation_deliveries",
    "assessments",
    "incidents",
    "costs",
    "checkpoints",
)


def upgrade() -> None:
    bind = op.get_bind()
    metadata.create_all(bind)

    # Roles: engine has DML on everything (subject to RLS); inference gets
    # nothing — a model-side worker can authenticate but cannot read or write
    # any private table.
    op.execute(
        "DO $$ BEGIN "
        "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'spx_engine') "
        "THEN CREATE ROLE spx_engine NOLOGIN; END IF; "
        "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'spx_inference') "
        "THEN CREATE ROLE spx_inference NOLOGIN; END IF; "
        "END $$"
    )
    for t in ALL_TABLES:
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON {t} TO spx_engine")
    op.execute("GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO spx_engine")
    # Let the migrating user assume the roles (tests exercise SET ROLE).
    op.execute(
        "DO $$ BEGIN EXECUTE format('GRANT spx_engine, spx_inference TO %I', current_user); END $$"
    )

    for t in _RUN_SCOPED:
        op.execute(f"ALTER TABLE {t} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY run_scope ON {t} USING (run_id = current_setting('app.run_id', true))"
        )
    # Evidence atoms are a shared corpus — run scoping lives on deliveries.
    op.execute("ALTER TABLE evidence_atoms ENABLE ROW LEVEL SECURITY")
    op.execute("CREATE POLICY shared_corpus ON evidence_atoms USING (true)")


def downgrade() -> None:
    for t in ALL_TABLES:
        op.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
