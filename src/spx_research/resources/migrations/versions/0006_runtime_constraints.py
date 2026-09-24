"""Enforce durable runtime relationships, money domains and frozen identities."""

from __future__ import annotations

from alembic import op

revision = "0006_runtime_constraints"
down_revision = "0005_attempt_request"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_foreign_key(
        "fk_attempt_prepared_decision",
        "runtime_attempts",
        "runtime_decisions",
        ["run_id", "decision_id"],
        ["run_id", "decision_id"],
    )
    op.create_unique_constraint(
        "uq_attempt_decision_identity", "runtime_attempts", ["run_id", "decision_id", "attempt_id"]
    )
    op.create_foreign_key(
        "fk_decision_accepted_attempt",
        "runtime_decisions",
        "runtime_attempts",
        ["run_id", "decision_id", "accepted_attempt_id"],
        ["run_id", "decision_id", "attempt_id"],
    )
    op.create_check_constraint(
        "ck_runtime_run_status",
        "runtime_runs",
        "status IN ('RUNNING', 'PAUSED', 'COMPLETED', 'FAILED')",
    )
    op.create_check_constraint(
        "ck_runtime_decision_status",
        "runtime_decisions",
        "status IN ('PREPARED', 'RESPONSE_RECORDED', 'ACCEPTED')",
    )
    for table, column, name in (
        ("runtime_runs", "budget_cap", "ck_runtime_budget_amount"),
        ("runtime_attempts", "reserved_usd", "ck_runtime_reservation_amount"),
        ("runtime_attempts", "actual_usd", "ck_runtime_actual_amount"),
    ):
        op.create_check_constraint(name, table, f"{column} >= 0 AND {column} < 'Infinity'::numeric")
    for table, protected, conditional in (
        (
            "runtime_runs",
            ["run_id", "manifest", "manifest_hash"],
            "(OLD.budget_cap IS NOT NULL AND NEW.budget_cap IS DISTINCT FROM OLD.budget_cap)",
        ),
        (
            "runtime_decisions",
            ["run_id", "decision_id", "request_hash", "request"],
            "(OLD.status = 'ACCEPTED' AND (NEW.status <> OLD.status OR "
            "NEW.result::jsonb IS DISTINCT FROM OLD.result::jsonb OR "
            "NEW.accepted_attempt_id IS DISTINCT FROM OLD.accepted_attempt_id))",
        ),
        (
            "runtime_attempts",
            ["run_id", "decision_id", "attempt_id", "reserved_usd", "request"],
            "(OLD.actual_usd IS NOT NULL AND NEW.actual_usd IS DISTINCT FROM OLD.actual_usd)",
        ),
    ):
        comparisons = [
            f"to_jsonb(NEW)->'{column}' IS DISTINCT FROM to_jsonb(OLD)->'{column}'"
            for column in protected
        ]
        predicate = " OR ".join([*comparisons, conditional])
        op.execute(
            f"CREATE FUNCTION {table}_frozen_identity() RETURNS trigger LANGUAGE plpgsql AS $$ "
            f"BEGIN IF {predicate} THEN "
            "RAISE EXCEPTION 'FROZEN_RUNTIME_IDENTITY' USING ERRCODE = '23514'; "
            "END IF; RETURN NEW; END $$"
        )
        op.execute(
            f"CREATE TRIGGER frozen_identity BEFORE UPDATE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {table}_frozen_identity()"
        )


def downgrade() -> None:
    raise RuntimeError("RUNTIME_CONSTRAINT_DOWNGRADE_REQUIRES_BACKUP_RESTORE")
