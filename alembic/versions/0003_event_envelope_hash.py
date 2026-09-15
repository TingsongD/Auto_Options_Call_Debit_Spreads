"""Events gain event_hash: envelope-bound digest over the full event.

Revision ID: 0003_event_envelope_hash

The old chain link (sha256(payload_hash + seq)[:24]) did not bind the event
type, sim time, phase, or run id — those fields could be edited without
detection. ``event_hash`` covers run_id, seq, sim_time_utc, phase, type,
payload_hash and previous_hash; ``previous_hash`` now stores the prior
event's ``event_hash`` (or 'genesis'). The backfill recomputes both columns
per run in seq order.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0003_event_envelope_hash"
down_revision = "0002_events_append_only"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("events", sa.Column("event_hash", sa.Text, nullable=True))
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT run_id, seq, sim_time_utc, phase, type, payload_hash"
            " FROM events ORDER BY run_id, seq"
        )
    ).all()
    from spx_research.domain.state import Event
    from spx_research.domain.state import event_hash as _eh

    tips: dict[str, str] = {}
    for r in rows:
        prev = tips.get(r.run_id, "genesis")
        e = Event(
            run_id=r.run_id,
            seq=r.seq,
            sim_time_utc=r.sim_time_utc,
            phase=r.phase,
            type=r.type,
            payload={},
            payload_hash=r.payload_hash,
            previous_hash=prev,
        )
        eh = _eh(e)
        bind.execute(
            sa.text(
                "UPDATE events SET previous_hash = :p, event_hash = :h"
                " WHERE run_id = :r AND seq = :s"
            ),
            {"p": prev, "h": eh, "r": r.run_id, "s": r.seq},
        )
        tips[r.run_id] = eh
    op.alter_column("events", "event_hash", nullable=False)


def downgrade() -> None:
    op.drop_column("events", "event_hash")
