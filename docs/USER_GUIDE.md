# User guide

This application simulates SPX one-lot bull-put and bear-call credit spreads.
The supported starting point is generated synthetic data with a mechanical or
mock policy. It does not connect to a broker. Historical ingestion and paid
experiments remain gated by the [decision register](DECISION_REGISTER.md).

## 1. Install and choose a workspace

Use Python 3.12 and `uv`. Run the following commands from the `app/` directory:

```sh
uv sync --frozen
uv run spx-research --help
mkdir -p out/guide-demo
```

The walkthrough writes only under `out/guide-demo`, which is ignored by Git.
Choose a new directory for a second walkthrough; run output directories cannot
be reused. Runtime schemas, prompts and example profiles are packaged with the
application, so the sibling handover folder is not needed to run it.

## 2. Create a synthetic profile

The packaged research profile is deliberately incomplete, and the synthetic
profile is an accounting fixture rather than a complete run configuration. This
snippet combines their defaults and supplies explicit demonstration dates and
fees. These values are fictitious software-test inputs, not owner approvals.

```sh
uv run python - <<'PY'
from pathlib import Path
import yaml
from spx_research.config import load_profile
from spx_research.contracts import example_config_path

research = load_profile(example_config_path("research")).model_dump(mode="json")
synthetic = load_profile(example_config_path("synthetic")).model_dump(
    mode="json", exclude_none=True
)
profile = {**research, **synthetic}
profile["profile_id"] = "guide-synthetic"
profile["study"] = {
    "start_date": "2024-01-02",
    "scored_end_date": "2024-01-03",
    "runoff_end_date": "2024-01-03",
}
profile["execution"] = {
    "opening_fee_per_leg_usd": "1.00",
    "closing_fee_per_leg_usd": "1.00",
    "settlement_fee_per_leg_usd": "0.00",
}
Path("out/guide-demo/profile.yaml").write_text(yaml.safe_dump(profile, sort_keys=False))
PY
uv run spx-research validate-config out/guide-demo/profile.yaml
```

Keep all real-data, real-model and broker permissions disabled. Validation
checks the configuration; it does not verify a dataset or grant approval.

## 3. Generate data and run

```sh
uv run spx-research generate-synthetic \
  --out out/guide-demo/data --dataset-id guide-data \
  --start 2024-01-02 --end 2024-01-03 --seed 7

uv run spx-research run out/guide-demo/profile.yaml \
  --dataset-root out/guide-demo/data/guide-data \
  --out out/guide-demo/mechanical --store memory --policy mechanical
```

The dataset path includes `guide-data`: it contains `manifest.json`,
`calendar.json`, and the generated partitions. The generator's weekday calendar
is a synthetic fixture, not an approved historical exchange calendar.

The run validates dataset checksums and freezes its effective inputs before
execution. The memory store is sufficient for this demonstration but cannot
resume after the process exits. Use PostgreSQL from the start if recovery is
needed (section 7).

With the profile and seed above, the verified mechanical walkthrough completes
without trades and retains its initial $10,000. This checks the workflow with a
healthy but ineligible candidate universe; it is not a trading-performance
example. The [acceptance matrix](ACCEPTANCE_MATRIX.md) points to fixtures that
exercise entries, exits, settlement and independently reconciled accounting.

Read the printed status. A short study can end with unresolved positions and
therefore report `PAUSED` with a nonzero exit code. That preserves the holdings;
the engine does not force a liquidation just to finish the example. The output
artifacts remain available for inspection. To study a different date range or
runoff period, create a new profile, matching dataset, and run directory.

## 4. Read and check the results

```sh
uv run python -m json.tool out/guide-demo/mechanical/report.json
uv run spx-research replay out/guide-demo/mechanical/events.jsonl
uv run spx-research leakage-eval out/guide-demo/mechanical
uv run spx-research register-run out/guide-demo/mechanical \
  --registry out/guide-demo/registry.jsonl
```

| File | Purpose |
|---|---|
| `run_manifest.json` | Frozen profile, inputs, code/contracts and policy identity |
| `run_result.json` | Export digests and final run result |
| `report.json` | Status, pause reason, cash, fees, valuations, coverage, incidents and costs |
| `events.jsonl` | Committed financial event chain; input to financial replay |
| `journal.json` | Export of preparations, attempts, accounting and recovery evidence |
| `decision_tape.jsonl` | Sealed export of accepted model decisions; mechanical runs have no model decisions |
| `run_locations.json` | Local dataset location used for recovery |
| `leakage_report.json` | Audit output created by `leakage-eval` |

In `report.json`, check `status` and `pause` first. Then inspect
`final_valuation`, `scored_end_valuation`, `runoff`, `open_positions`,
`trading_fees_usd`, `model_costs`, and `model_attempts`. Cash alone is not equity:
position liabilities reduce equity, while reserves reduce available capital.
Unknown valuation quality means a value is unavailable, not zero.

The scored-end valuation describes the study boundary; runoff outcomes describe
subsequent position management. No new entries or allocations occur in runoff.
Execution completion, artifact audit success, and research validity are separate.
An audit `PASS` covers its listed checks; it does not certify strategy returns or
prove that a model lacks future historical knowledge. Unperformed diagnostics
remain `NOT_RUN`, and synthetic research validity remains `UNVALIDATED`.

For a local report viewer in a source checkout:

```sh
uv run streamlit run src/spx_research/dashboard/app.py -- \
  --run-dir out/guide-demo/mechanical
```

## 5. Exercise mock model decisions

Keep the same alias key when replaying, comparing or resuming a model run. This
literal key is only for the synthetic walkthrough; use a privately stored random
key of at least 16 characters for separately approved operational work.

```sh
export SPX_ALIAS_KEY='synthetic-guide-only-key-2026'
uv run spx-research run out/guide-demo/profile.yaml \
  --dataset-root out/guide-demo/data/guide-data \
  --out out/guide-demo/mock --store memory --policy llm-mock
uv run spx-research leakage-eval out/guide-demo/mock
```

`llm-mock` makes no paid calls. Its default in-process transport exercises policy
requests and validation. To exercise the actual Docker boundary, use the offline
gateway instructions in the [runbook](RUNBOOK.md#spending-and-inference-boundary).
Changing the policy to `llm` is not part of this walkthrough or an approval.

## 6. Replay or compare model runs

Financial `replay` folds events and checks their chain. Policy replay instead
uses a sealed, nonempty version 2 decision tape:

```sh
uv run spx-research run out/guide-demo/profile.yaml \
  --dataset-root out/guide-demo/data/guide-data \
  --out out/guide-demo/mock-replay --policy llm-replay \
  --tape out/guide-demo/mock/decision_tape.jsonl
```

Freshly compiled requests must match the source requests. Keep the same profile,
data and alias key. The source is copied to `replay_input.jsonl` and its digest is
frozen. Original usage stays as provenance; replay adds no provider charges.
Replay is not recovery, and legacy or truncated tapes cannot supply new-format
policy replay.

To compare two mock runs with matching visible alias scopes, supply an explicit
inclusive cutoff with a timezone:

```sh
uv run spx-research leakage-eval out/guide-demo/mock \
  --control-dir out/guide-demo/mock-replay \
  --cutoff 2024-01-02T21:00:00+00:00 --expect invariant
```

`invariant` expects equal complete requests through the cutoff. For a deliberate
positive control that changes already-visible information, use `--expect changed`.
The selected expectation determines success. Missing evidence cannot pass.

## 7. Use durable recovery

For a new recoverable run, configure an application PostgreSQL database, migrate
it, and select `--store postgres`. In a source checkout, the provided development
Compose service uses port 5433 and a persistent named volume:

```sh
docker compose -f docker-compose.yml up -d db
export SPX_DB_DSN='postgresql+psycopg://spx_dev:spx_dev_local_only@127.0.0.1:5433/spx_research'
uv run spx-research migrate
uv run spx-research run out/guide-demo/profile.yaml \
  --dataset-root out/guide-demo/data/guide-data \
  --out out/guide-demo/durable --store postgres --policy mechanical
```

Wait for the database to become healthy before migrating. Use this connection
only for the local development service; configure a separate DSN for any other
database. Back up an existing database before upgrading it. Acceptance-test
fixtures require a separate disposable database because they clear tables.

After an interruption, restore the same environment and connection, then run:

```sh
uv run spx-research resume out/guide-demo/durable
```

Resume verifies frozen inputs and reads committed state from PostgreSQL. It
reuses persisted responses and regenerates exports. A relocated identical
dataset can be supplied with `--dataset-root NEW_PATH`. Changed inputs, code,
contracts or alias keys cannot be substituted into the old run. A memory run or
legacy export cannot be converted into a recoverable run by changing a flag.

Resume does not clear unresolved data, spending or epistemic incidents. See the
[runbook](RUNBOOK.md#recovery) for incident handling and backup/restore procedures.

## Common outcomes

| Outcome | What to do |
|---|---|
| `OUTPUT_DIRECTORY_NOT_EMPTY` | Use a new output directory; use `resume` for an existing recoverable run. |
| Configuration or dataset `BLOCK` | Read the error, correct the new experiment's inputs, and validate again. |
| Data pause / unknown valuation | Inspect coverage and missing quotes. Preserve holdings; corrected data creates a new experiment. |
| Unresolved holdings at runoff end | Inspect the runoff report. A longer study requires new frozen inputs. |
| `FROZEN_INPUTS_CHANGED` | Restore the exact original environment or start a new experiment. |
| `RESUME_REQUIRES_AUTHORITATIVE_POSTGRES` | Inspect/replay the memory run; choose PostgreSQL for the next run. |
| Budget or uncertain billing pause | Follow audited receipt reconciliation in the runbook; do not erase reservations. |
| Epistemic incident | Preserve the quarantined evidence. A replacement policy decision requires a separate experiment. |
| Audit failure | Inspect `leakage_report.json`; restore authoritative exports where possible rather than editing evidence. |

Run `uv run spx-research COMMAND --help` for each command's current options.
