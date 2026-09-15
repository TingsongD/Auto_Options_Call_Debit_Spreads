# Developer instructions — app

The governing specification is `../spx_ai_handover_v2/` (read `DEV_HANDOVER.md`,
`docs/TEMPORAL_HARNESS.md`, `docs/LEAKAGE_EVALUATION.md`, and the package's own
`AGENTS.md`). `docs/DECISIONS.md` lists unapproved owner choices — do not run
real data or model calls until those gates clear.

## Scaffolded across M0–M6 areas

The spec's milestones are acceptance gates, not component checklists — the
decision register (`../spx_ai_handover_v2/docs/DECISIONS.md` /
`docs/DECISION_REGISTER.md`) is still all-pending, so nothing here may run
against real data or real models. Present surface:

- `config.py`, `preflight.py`, `contracts.py`: typed profiles, mode-aware
  preflight, spec-directory contract resolution (schemas/prompts are resolved
  at runtime, not vendored).
- `domain/`, `temporal/`, `engine/`: Decimal accounting, NY calendar + review
  grid, execution + PM settlement, minute scheduler, event-sourced ledger,
  mechanical policy, replay/report.
- `data/`: deterministic synthetic Parquet dataset generator, availability
  gateway (session-scoped, no overnight forward-fill), QA, adapter stubs.
- `epistemics/`: observation ledger, knowledge reducer, blinded projector,
  egress gate, producer bridge, incident quarantine.
- `llm/`, `agents/`: gateway protocol (mock/tape/gated OpenAI), decision
  tape, budget ledger, LangGraph decision barrier, `LLMPolicy` adapter.
- `persistence/`: Postgres event store (advisory-lock single-writer,
  hash-chained, transactional outbox) + observation ledger; Alembic
  migrations 0001–0003 with `spx_engine`/`spx_inference` roles and per-run RLS.
- `research/`: experiment registry (content-addressed lineage), leakage
  evaluator (hash-chain, replay, tape egress scan, run-comparison probe).
- `cli/main.py`: `validate-config`, `generate-synthetic`, `run`
  (`--store memory|postgres`, `--policy mechanical|llm-mock|llm`, `--tape`,
  `--model`, `--budget-usd`), `replay`, `migrate`, `leakage-eval`,
  `register-run`.

## Verify

```bash
uv run pytest                 # unit+integration (Postgres tests skip without SPX_TEST_DSN)
uv run ruff check src tests alembic
uv run mypy src
docker compose up -d db       # Postgres on 127.0.0.1:5433
uv run spx-research migrate   # SPX_DB_DSN selects the DSN
```

`docs/RUNBOOK.md` covers recovery, isolation, budget, and the fixed
classification label.
