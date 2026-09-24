# spx-research

Synthetic SPX credit-spread research engine with durable decision recovery. The governing
specification lives one level up in `../spx_ai_handover_v2/` — read
`DEV_HANDOVER.md`, `docs/TEMPORAL_HARNESS.md`, and `docs/LEAKAGE_EVALUATION.md`
before implementing. `docs/DECISIONS.md` there lists owner approvals that remain
open; nothing here may run against real data or real models until those gates
clear.

**Governance gate:** all 33 owner decisions in `docs/DECISION_REGISTER.md` are
`_pending_` and the vendor data-rights probe (`docs/DATA_RIGHTS_PROBE.md`) is
unchecked — this codebase is a tested scaffold, not an approved system.

## Research goal and historical blinding

The goal is to test whether an AI can make discretionary option-spread decisions
from information available at each decision time and earn consistent profits
after trading costs. Profitability is the hypothesis to investigate, not an
established result of this implementation.

We adapted ideas from [UserHarness](https://arxiv.org/html/2605.27721v1) to
strengthen the simulated "fog of war": time-filtered observations, explicit
belief updates, blinded model inputs and external validation. The AI still makes
the judgment call among permitted actions. This setup controls information
supplied by the application; it does not erase pretrained memories or prove
that remembered historical outcomes cannot influence a choice. Its effectiveness
against that remaining risk has not yet been measured in real-model experiments.
See [Historical blinding and pretrained knowledge](docs/USER_GUIDE.md#historical-blinding-and-pretrained-knowledge)
for the paper's contribution, our adaptation and the limits of the evidence.

## Layout

`src/spx_research/` follows the spec's target decomposition: `domain`, `data`,
`temporal`, `epistemics` (the ported temporal-knowledge-harness seed),
`features`, `engine`, `agents`, `llm`, `persistence`, `reporting`, `research`,
`cli`, `dashboard`. Tests live in
`tests/{unit,property,integration,golden,fault_injection}`.

## Commands

```bash
uv sync --frozen    # install the locked environment (Python 3.12)
uv run pytest      # synthetic tests; see acceptance matrix for mandatory service checks
uv run ruff check  # lint
uv run mypy src    # type check
```

## Documentation

- [User guide](docs/USER_GUIDE.md): first synthetic run, mock decisions, reading
  results, replay, and recovery.
- [Operations runbook](docs/RUNBOOK.md): PostgreSQL, spending controls, isolated
  inference, backups, and incident handling.
- [Development log](docs/DEV_LOG.md): dated changes, verification, and remaining work.
- [Acceptance matrix](docs/ACCEPTANCE_MATRIX.md): review findings mapped to regressions.
- [Implementation validation](docs/IMPLEMENTATION_VALIDATION.md): recorded results
  from the September 21 patch acceptance run.
- [Decision register](docs/DECISION_REGISTER.md) and
  [data-rights probe](docs/DATA_RIGHTS_PROBE.md): outstanding approval gates.

Start with the user guide. It uses generated data and requires no provider key,
paid calls, or database for the basic walkthrough.

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

Runtime contracts, prompts, schemas and examples ship in the wheel. See
[the runbook](docs/RUNBOOK.md) for frozen manifests, resume, budgets and Docker
inference isolation, and [the acceptance matrix](docs/ACCEPTANCE_MATRIX.md) for
verification and remaining gated historical work.
