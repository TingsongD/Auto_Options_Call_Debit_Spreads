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
  --store postgres` — durable run: events land in the `events` table and the
  observation ledger (atoms/deliveries/assessments/incidents) lands in
  Postgres too — writes are idempotent so barrier retries are safe.

## Recovery

1. **Crash mid-run**: the committed prefix is authoritative. Read
   `SELECT * FROM events WHERE run_id = :r ORDER BY seq` or
   `events.jsonl`, then `uv run spx-research replay out/run-1/events.jsonl`.
   Replay verifies the `event_hash`/`previous_hash` chain over the full
   envelope (type, phase, sim_time, run_id) before folding; then run
   `uv run spx-research leakage-eval out/run-1` — its `hash_chain_ok` and
   `log_hash_match` checks catch post-hoc edits to `events.jsonl`.
2. **Sequence conflict** (`SEQUENCE_MISMATCH`): another writer holds the run
   or a retry raced. The advisory lock serializes writers; a stale
   `expected_seq` means the caller's tip is old — reload events and retry.
3. **Model-call failure / budget exhaustion**: the scheduler pauses the
   decision barrier (agent state `PAUSED`) rather than inventing an action.
   Inspect `incidents` for rejected proposals (private; never feed back to a
   model). There is no mid-run resume: restart the run from scratch with the
   same `--tape` path — the decision tape replays already-validated
   responses by request hash, so restarted barriers do not re-bill or
   re-dispatch completed model calls.
4. **Incident inspection**:
   `SELECT incident_id, code, at_utc FROM incidents WHERE run_id = :r` —
   `rejected_payload` is quarantined; do not copy it into prompts or memory.
5. **Outbox**: rows in `outbox` are committed side-effects written
   atomically with their event — a missing row means the event never
   committed. No drainer ships yet; the table is reserved for future
   consumers and stays `emitted = false`.

## Isolation

- `spx_engine` role: DML on all tables, subject to per-run RLS
  (`SET app.run_id`). `spx_inference` role: connect-only, zero table grants —
  model-side workers cannot read archive, events, incidents, or other runs.
- Evidence atoms are a shared corpus; run scoping lives on
  `observation_deliveries`, `assessments`, `incidents`, `events`, `costs`.

## Budget

`Budget` reserves estimated cost before dispatch and commits actuals after.
`BUDGET_EXCEEDED` is a structured policy failure, not a crash. Spend state
is in-process (the `costs` table is reserved schema; nothing writes it yet),
so a crashed run restarts its budget from zero — rerun with a fresh tape.

## Classification

Every report carries the fixed label
`HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED`. Do not relabel a run
"clean" — the harness bounds inputs; it cannot prove absence of pretrained
historical knowledge.
