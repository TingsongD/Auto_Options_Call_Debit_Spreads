"""Initial schema: run-scoped event/epistemics tables + roles + RLS (M5-01/H-08).

Revision ID: 0001_initial
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# Frozen revision-0001 DDL. Never import mutable application metadata.
metadata = sa.MetaData()

runs = sa.Table(
    "runs",
    metadata,
    sa.Column("run_id", sa.Text, primary_key=True),
    sa.Column("profile_id", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False, server_default="RUNNING"),
    sa.Column("manifest", sa.JSON, nullable=False, server_default="{}"),
    sa.Column("started_at_utc", sa.DateTime(timezone=True), nullable=False),
    sa.Column("ended_at_utc", sa.DateTime(timezone=True)),
)

events = sa.Table(
    "events",
    metadata,
    sa.Column("run_id", sa.Text, sa.ForeignKey("runs.run_id"), nullable=False),
    sa.Column("seq", sa.BigInteger, nullable=False),
    sa.Column("sim_time_utc", sa.DateTime(timezone=True), nullable=False),
    sa.Column("phase", sa.Text, nullable=False),
    sa.Column("type", sa.Text, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("payload_hash", sa.Text, nullable=False),
    sa.Column("previous_hash", sa.Text, nullable=False),
    sa.PrimaryKeyConstraint("run_id", "seq"),
)

outbox = sa.Table(
    "outbox",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("run_id", sa.Text, nullable=False),
    sa.Column("event_seq", sa.BigInteger, nullable=False),
    sa.Column("topic", sa.Text, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("emitted", sa.Boolean, nullable=False, server_default=sa.false()),
)

atoms = sa.Table(
    "evidence_atoms",
    metadata,
    sa.Column("atom_id", sa.Text, primary_key=True),
    sa.Column("metric", sa.Text, nullable=False),
    sa.Column("value", sa.Text, nullable=False),
    sa.Column("unit", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("subject_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("source_checked", sa.Boolean, nullable=False, server_default=sa.true()),
    sa.Column("recipients", sa.JSON, nullable=False),
    sa.Column("dependencies", sa.JSON, nullable=False, server_default="[]"),
    sa.Column("transform", sa.Text),
)

deliveries = sa.Table(
    "observation_deliveries",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("run_id", sa.Text, nullable=False),
    sa.Column("branch_id", sa.Text, nullable=False),
    sa.Column("actor_id", sa.Text, nullable=False),
    sa.Column("atom_id", sa.Text, sa.ForeignKey("evidence_atoms.atom_id"), nullable=False),
    sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("run_id", "branch_id", "actor_id", "atom_id"),
)

assessments = sa.Table(
    "assessments",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("run_id", sa.Text, nullable=False),
    sa.Column("branch_id", sa.Text, nullable=False),
    sa.Column("actor_id", sa.Text, nullable=False),
    sa.Column("topic", sa.Text, nullable=False),
    sa.Column("assessment", sa.Text, nullable=False),
    sa.Column("confidence_label", sa.Text, nullable=False),
    sa.Column("premise_atom_ids", sa.JSON, nullable=False),
    sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("decision_token", sa.Text, nullable=False),
)

incidents = sa.Table(
    "incidents",
    metadata,
    sa.Column("incident_id", sa.Text, primary_key=True),
    sa.Column("run_id", sa.Text, nullable=False),
    sa.Column("branch_id", sa.Text, nullable=False),
    sa.Column("actor_id", sa.Text, nullable=False),
    sa.Column("code", sa.Text, nullable=False),
    sa.Column("rejected_payload", sa.JSON, nullable=False),
    sa.Column("at_utc", sa.DateTime(timezone=True), nullable=False),
)

costs = sa.Table(
    "costs",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("run_id", sa.Text, nullable=False),
    sa.Column("request_hash", sa.Text, nullable=False),
    sa.Column("model_id", sa.Text, nullable=False),
    sa.Column("input_tokens", sa.Integer, nullable=False),
    sa.Column("output_tokens", sa.Integer, nullable=False),
    sa.Column("cost_usd", sa.Numeric(14, 6), nullable=False),
    sa.Column("at_utc", sa.DateTime(timezone=True), nullable=False),
)

checkpoints = sa.Table(
    "checkpoints",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("run_id", sa.Text, nullable=False),
    sa.Column("thread_id", sa.Text, nullable=False),
    sa.Column("ledger_seq", sa.BigInteger, nullable=False),
    sa.Column("belief_hash", sa.Text, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("at_utc", sa.DateTime(timezone=True), nullable=False),
)

ALL_TABLES = (
    "runs",
    "events",
    "outbox",
    "evidence_atoms",
    "observation_deliveries",
    "assessments",
    "incidents",
    "costs",
    "checkpoints",
)


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
