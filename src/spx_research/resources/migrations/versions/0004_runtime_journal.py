"""Durable runtime-v2 decisions, attempts, budgets, barriers and cursors.

Revision ID: 0004_runtime_journal
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0004_runtime_journal"
down_revision = "0003_event_envelope_hash"
branch_labels = None
depends_on = None

_TABLES = (
    "runtime_runs",
    "runtime_decisions",
    "runtime_attempts",
    "runtime_barriers",
    "runtime_journal",
)


def upgrade() -> None:
    op.create_table(
        "runtime_runs",
        sa.Column("run_id", sa.Text, sa.ForeignKey("runs.run_id"), primary_key=True),
        sa.Column("manifest", sa.JSON, nullable=False),
        sa.Column("manifest_hash", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("cursor", sa.JSON, nullable=False),
        sa.Column("pause", sa.JSON),
        sa.Column("budget_cap", sa.Numeric()),
    )
    op.create_table(
        "runtime_decisions",
        sa.Column("run_id", sa.Text, sa.ForeignKey("runtime_runs.run_id"), primary_key=True),
        sa.Column("decision_id", sa.Text, primary_key=True),
        sa.Column("request_hash", sa.Text, nullable=False),
        sa.Column("request", sa.JSON, nullable=False),
        sa.Column("response", sa.JSON),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("result", sa.JSON),
        sa.Column("accepted_attempt_id", sa.Text),
    )
    op.create_table(
        "runtime_attempts",
        sa.Column("run_id", sa.Text, sa.ForeignKey("runtime_runs.run_id"), primary_key=True),
        sa.Column("attempt_id", sa.Text, primary_key=True),
        sa.Column("decision_id", sa.Text, nullable=False),
        sa.Column("reserved_usd", sa.Numeric(), nullable=False),
        sa.Column("actual_usd", sa.Numeric()),
        sa.Column("outcome", sa.Text, nullable=False),
        sa.Column("response", sa.JSON),
        sa.Column("error_code", sa.Text, nullable=False),
    )
    op.create_table(
        "runtime_barriers",
        sa.Column("run_id", sa.Text, sa.ForeignKey("runtime_runs.run_id"), primary_key=True),
        sa.Column("barrier_id", sa.Text, primary_key=True),
        sa.Column("expected_seq", sa.BigInteger, nullable=False),
        sa.Column("request_hashes", sa.JSON, nullable=False),
        sa.Column("cursor", sa.JSON, nullable=False),
        sa.Column("batch_hash", sa.Text),
        sa.Column("last_seq", sa.BigInteger),
        sa.Column("status", sa.Text, nullable=False),
    )
    op.create_table(
        "runtime_journal",
        sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column("run_id", sa.Text, sa.ForeignKey("runtime_runs.run_id"), nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("payload", sa.JSON, nullable=False),
        sa.Column("at_utc", sa.DateTime(timezone=True), nullable=False),
    )
    for table in _TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY run_scope ON {table} "
            "USING (run_id = current_setting('app.run_id', true))"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON {table} TO spx_engine")
    op.execute("REVOKE UPDATE ON runtime_journal FROM spx_engine")
    op.execute("GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO spx_engine")
    # An already-applied older 0003 did not create an audit table. Do not
    # fabricate lost pre-migration data; create only an empty audit surface.
    if not sa.inspect(op.get_bind()).has_table("event_hash_migration_audit"):
        op.create_table(
            "event_hash_migration_audit",
            sa.Column("run_id", sa.Text, primary_key=True),
            sa.Column("seq", sa.BigInteger, primary_key=True),
            sa.Column("original_envelope", sa.JSON, nullable=False),
            sa.Column("original_sha256", sa.Text, nullable=False),
        )
    op.execute("ALTER TABLE event_hash_migration_audit ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY run_scope ON event_hash_migration_audit "
        "USING (run_id = current_setting('app.run_id', true))"
    )
    op.execute("GRANT SELECT ON event_hash_migration_audit TO spx_engine")
    op.create_unique_constraint(
        "uq_assessment_decision_topic",
        "assessments",
        ["run_id", "branch_id", "actor_id", "decision_token", "topic"],
    )


def downgrade() -> None:
    raise RuntimeError("RUNTIME_DOWNGRADE_REQUIRES_BACKUP_RESTORE")
