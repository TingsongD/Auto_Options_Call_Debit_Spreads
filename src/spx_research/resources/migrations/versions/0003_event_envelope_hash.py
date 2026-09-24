"""Freeze envelope hashing and preserve legacy envelopes before conversion.

Revision ID: 0003_event_envelope_hash

Some revision-0001/0002 databases already contain event_hash because the old
initial migration imported live metadata. Accommodate that known shape without
restamping, deleting events, or silently repairing corrupt chains.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import sqlalchemy as sa

from alembic import op

revision = "0003_event_envelope_hash"
down_revision = "0002_events_append_only"
branch_labels = None
depends_on = None


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def upgrade() -> None:
    bind = op.get_bind()
    cols = {c["name"] for c in sa.inspect(bind).get_columns("events")}
    if "event_hash" not in cols:
        op.add_column("events", sa.Column("event_hash", sa.Text, nullable=True))
    if not sa.inspect(bind).has_table("event_hash_migration_audit"):
        op.create_table(
            "event_hash_migration_audit",
            sa.Column("run_id", sa.Text, primary_key=True),
            sa.Column("seq", sa.BigInteger, primary_key=True),
            sa.Column("original_envelope", sa.JSON, nullable=False),
            sa.Column("original_sha256", sa.Text, nullable=False),
        )
    rows = bind.execute(sa.text("SELECT * FROM events ORDER BY run_id, seq")).mappings().all()
    tips: dict[str, tuple[int, str, str, bool]] = {}
    audit = sa.table(
        "event_hash_migration_audit",
        sa.column("run_id"),
        sa.column("seq"),
        sa.column("original_envelope", sa.JSON),
        sa.column("original_sha256"),
    )
    for row in rows:
        r = dict(row)
        seq, prev, legacy_prev, modern = tips.get(
            r["run_id"], (0, "genesis", "genesis", bool(r["event_hash"]))
        )
        if r["seq"] != seq + 1 or _digest(r["payload"]) != r["payload_hash"]:
            raise RuntimeError("CORRUPT_LEGACY_EVENT_LOG: preserve backup and investigate")
        if bool(r["event_hash"]) != modern:
            raise RuntimeError("MIXED_EVENT_HASH_FORMAT: explicit repair required")
        envelope = {
            "run_id": r["run_id"],
            "seq": r["seq"],
            "sim_time_utc": r["sim_time_utc"].isoformat(),
            "phase": r["phase"],
            "type": r["type"],
            "payload_hash": r["payload_hash"],
            "previous_hash": prev,
        }
        current_hash = _digest(envelope)
        if modern:
            if r["previous_hash"] != prev or r["event_hash"] != current_hash:
                raise RuntimeError("CORRUPT_EVENT_LOG: migration will not rehash tampered events")
        else:
            if r["previous_hash"] != legacy_prev:
                raise RuntimeError("CORRUPT_LEGACY_CHAIN: migration will not repair missing events")
            original = json.loads(json.dumps(r, default=str))
            bind.execute(
                audit.insert().values(
                    run_id=r["run_id"],
                    seq=r["seq"],
                    original_envelope=original,
                    original_sha256=_digest(original),
                )
            )
            bind.execute(
                sa.text(
                    "UPDATE events SET previous_hash=:p,event_hash=:h WHERE run_id=:r AND seq=:s"
                ),
                {"p": prev, "h": current_hash, "r": r["run_id"], "s": r["seq"]},
            )
        old_tip = hashlib.sha256(f"{r['payload_hash']}{r['seq']}".encode()).hexdigest()[:24]
        tips[r["run_id"]] = (r["seq"], current_hash, old_tip, modern)
    op.alter_column("events", "event_hash", nullable=False)


def downgrade() -> None:
    raise RuntimeError("IRREVERSIBLE_HASH_MIGRATION: restore the preserved database backup")
