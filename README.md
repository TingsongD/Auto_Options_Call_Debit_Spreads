# spx-research

Application scaffold for the SPX credit-spread research platform. The governing
specification lives one level up in `../spx_ai_handover_v2/` — read
`DEV_HANDOVER.md`, `docs/TEMPORAL_HARNESS.md`, and `docs/LEAKAGE_EVALUATION.md`
before implementing. `docs/DECISIONS.md` there lists owner approvals that remain
open; nothing here may run against real data or real models until those gates
clear.

**Governance gate:** all 33 owner decisions in `docs/DECISION_REGISTER.md` are
`_pending_` and the vendor data-rights probe (`docs/DATA_RIGHTS_PROBE.md`) is
unchecked — this codebase is a tested scaffold, not an approved system.

## Layout

`src/spx_research/` follows the spec's target decomposition: `domain`, `data`,
`temporal`, `epistemics` (the ported temporal-knowledge-harness seed),
`features`, `engine`, `agents`, `llm`, `persistence`, `reporting`, `research`,
`cli`, `dashboard`. Tests live in
`tests/{unit,property,integration,golden,fault_injection}`.

## Commands

```bash
uv sync            # create/update .venv from uv.lock (Python 3.12)
uv run pytest      # unit + property + integration tests (synthetic only, no network)
uv run ruff check  # lint
uv run mypy src    # type check
```

## Boundaries (non-negotiable, from the spec)

- The financial engine owns clock, contracts, orders, reserves, fills, fees,
  cash, settlement. TKH owns verified observations, provenance, the blinded
  packet, and factual belief updates. Models choose from engine-generated
  action menus only.
- Public packets use relative clocks and opaque IDs; real dates, contract
  strings, and full-history manifests stay in private engine records.
- No model override of deterministic validation; no leaked prose in retries;
  no provider-managed conversation state in the blinded profile.
- Passing tests never labels a run as free of pretrained historical knowledge.
- Secrets and licensed data stay out of git and telemetry.
