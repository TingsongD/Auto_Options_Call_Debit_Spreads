# Developer instructions

The governing specification is `../spx_ai_handover_v2/`. For financial semantics,
read its `DEV_HANDOVER.md` and `docs/DATA_DICTIONARY.md`; for model visibility,
read `docs/TEMPORAL_HARNESS.md` and `docs/LEAKAGE_EVALUATION.md` there. Runtime
contracts are the versioned derivative in `src/spx_research/resources/contracts`.
Changes to that derivative must update its bundle version, examples, prompts,
semantic validators and checksums together. Installed applications use the
packaged bundle and reject mismatched external overrides.

For recovery, budgets, isolation, artifact identity or migrations, read
[docs/RUNBOOK.md](docs/RUNBOOK.md) before editing. Frozen historical migrations
must not import current application metadata. New schema changes get forward
migrations and populated-upgrade tests. Keep packaged migration copies identical.

Use the shared RunStore transaction boundary for financial effects and visible
knowledge. Persist requests and attempts before inference; keep model calls
outside transactions. Real dates, contracts and private mappings remain in the
trusted engine. Synthetic acceptance uses no paid calls or licensed data.
Owner decisions in [docs/DECISION_REGISTER.md](docs/DECISION_REGISTER.md) remain
pending unless an explicit owner approval is recorded.

For completion, run lint, type checks, the application and reference suites,
installed-wheel checks, and the PostgreSQL/Docker acceptance commands in
[docs/ACCEPTANCE_MATRIX.md](docs/ACCEPTANCE_MATRIX.md). Use a disposable database
selected explicitly by `SPX_TEST_DSN`; integration fixtures clear its tables.
Never use a developer's existing database for test fixtures.
