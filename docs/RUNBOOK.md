# Operations Runbook

Scope: deterministic research runs only. There is no live trading path, no
broker connection, and no un-gated model call. The event log is the financial
authority; checkpoints and reports are projections.

## Services

- `docker compose up -d db` — Postgres 16 on `127.0.0.1:5433`
  (db `spx_research`, user `spx_dev`; dev-only credentials, never committed).
- `SPX_DB_DSN` env var selects the DSN, e.g.
  `postgresql+psycopg://spx_dev:spx_dev_local_only@127.0.0.1:5433/spx_research`.
- `uv run spx-research migrate` — apply Alembic migrations.
- `uv run spx-research run profile.yaml --dataset-root ds --out out/run-1
  --store postgres` — durable run.

## Recovery

1. **Crash mid-run**: the committed prefix is authoritative. Read
   `SELECT * FROM events WHERE run_id = :r ORDER BY seq` or
   `events.jsonl`, then `uv run spx-research replay out/run-1/events.jsonl`.
   Replay must reproduce the same final cash/reserves — a mismatch means the
   log was corrupted (verify `payload_hash`/`previous_hash` chain).
2. **Sequence conflict** (`SEQUENCE_MISMATCH`): another writer holds the run
   or a retry raced. The advisory lock serializes writers; a stale
   `expected_seq` means the caller's tip is old — reload events and retry.
3. **Model-call failure / budget exhaustion**: the scheduler pauses the
   decision barrier (agent state `PAUSED`) rather than inventing an action.
   Inspect `incidents` for rejected proposals (private; never feed back to a
   model), then resume by re-running the barrier — the decision tape replays
   already-validated responses by request hash (no duplicate billing).
4. **Incident inspection**:
   `SELECT incident_id, code, at_utc FROM incidents WHERE run_id = :r` —
   `rejected_payload` is quarantined; do not copy it into prompts or memory.
5. **Outbox drain**: rows in `outbox` with `emitted = false` are committed
   side-effects awaiting dispatch; they are written atomically with their
   event, so a missing row means the event never committed.

## Isolation

- `spx_engine` role: DML on all tables, subject to per-run RLS
  (`SET app.run_id`). `spx_inference` role: connect-only, zero table grants —
  model-side workers cannot read archive, events, incidents, or other runs.
- Evidence atoms are a shared corpus; run scoping lives on
  `observation_deliveries`, `assessments`, `incidents`, `events`, `costs`.

## Budget

`Budget` reserves estimated cost before dispatch and commits actuals after.
`BUDGET_EXCEEDED` is a structured policy failure, not a crash. Check
`costs` for per-request token/cost lineage.

## Classification

Every report carries the fixed label
`HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED`. Do not relabel a run
"clean" — the harness bounds inputs; it cannot prove absence of pretrained
historical knowledge.
