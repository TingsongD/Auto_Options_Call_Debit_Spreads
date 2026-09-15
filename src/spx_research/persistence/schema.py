"""Logical schema (M5-01): SQLAlchemy Core metadata mirroring the Alembic DDL.

The event log is the financial authority; every other table is a projection
or a private record. All rows are run-scoped so row-level security can fence
actors and runs apart (H-08).
"""

from __future__ import annotations

import sqlalchemy as sa

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
