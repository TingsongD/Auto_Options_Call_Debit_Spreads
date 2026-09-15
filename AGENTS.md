# Developer instructions — app scaffold

The governing specification is `../spx_ai_handover_v2/` (read `DEV_HANDOVER.md`,
`docs/TEMPORAL_HARNESS.md`, `docs/LEAKAGE_EVALUATION.md`, and the package's own
`AGENTS.md`). `docs/DECISIONS.md` lists unapproved owner choices — do not run
real data or model calls until those gates clear.

Current contents: empty module skeleton under `src/spx_research/` plus the
ported temporal-knowledge-harness seed in `epistemics/` (types + compiler +
validator, 38 synthetic tests in `tests/unit/`). Everything else is unbuilt.

Verify changes with `uv run pytest`, `uv run ruff check`, `uv run mypy src`.
