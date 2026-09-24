# Operations runbook

For a first synthetic run, follow the [user guide](USER_GUIDE.md). Changes are
recorded in the [development log](DEV_LOG.md); dated acceptance results are in
[implementation validation](IMPLEMENTATION_VALIDATION.md).

The application is a synthetic research implementation. Historical adapters,
data licences, paid experiments and owner approvals remain separate work in
[DECISION_REGISTER.md](DECISION_REGISTER.md) and [DATA_RIGHTS_PROBE.md](DATA_RIGHTS_PROBE.md).
The governing specification remains `../spx_ai_handover_v2/`.

## Installation and contracts

Install with Python 3.12 and `uv sync --frozen`, or install a built wheel.
Runtime contracts are the checksum-pinned 2.1 bundle shipped in the wheel.
Profile YAML remains version 2.0. An explicit `SPX_SPEC_DIR` override must contain
exactly the recorded bundle; editing a prompt or schema requires a new bundle
version and experiment. The sibling handover is not a runtime dependency.

`SPX_DB_DSN` selects PostgreSQL. `spx-research migrate` applies the packaged
historical migrations. Back up a populated database before upgrading. Migration
0003 verifies existing evidence and preserves the previous envelopes in
`event_hash_migration_audit`; corruption aborts the upgrade.

## Runs and frozen inputs

`spx-research run PROFILE --dataset-root DATASET --out NEW_DIRECTORY
--store postgres --policy mechanical` freezes the effective profile, CLI dates,
data/calendar digests, code/dependency identity and contracts before execution.
For model policies it also freezes model IDs, prices, limits, settings, budget,
public alias namespace and an operator alias-key identifier. The secret itself
is never recorded. Set `SPX_ALIAS_KEY` to an operator-controlled random secret
of at least 16 characters for mock or model policies.

A new run gets a unique run ID unless `--run-id` is supplied. Reusing an existing
run ID or nonempty output directory is rejected. `register-run RUN_DIR
--registry REGISTRY.jsonl` reads frozen inputs; it preserves every distinct run
that shares an experiment identity. Generated tapes are outputs, not identity
inputs. For explicit replay use `--policy llm-replay --tape SOURCE/decision_tape.jsonl`.
The sealed v2 source is copied to `replay_input.jsonl` and its digest becomes a
frozen input. Freshly compiled public requests must match exactly, including
accepted retry bytes. Replay retains original usage as provenance and records
zero new billed cost. Legacy, truncated and empty tapes cannot supply new-format
recovery; legacy tapes remain readable through `DecisionTape` and financial
exports via `replay EVENTS.jsonl`.

Execution status is `RUNNING`, `PAUSED`, `COMPLETED` or `FAILED`. Manager allocation
pause is a separate trading state. A completed synthetic run still has research
validity `UNVALIDATED`. Missing required data stops the clock at the failing
phase, retains holdings and reports unknown equity. Healthy coverage without an
eligible candidate is an ordinary wait. No new entries or allocations occur in
runoff; unresolved holdings prevent completion and are never forcibly liquidated.

## Recovery

PostgreSQL is authoritative for resumable and paid runs. The memory store has the
same transaction semantics for offline tests, but cannot survive process exit.
The financial cursor records the processed phase, simulated time, ledger tip and
barrier identity. A minute commits prior orders and settlement before freezing
one snapshot for its decision batch. Requests, prior beliefs, staged observations
and action mappings are persisted before dispatch. Model calls hold no database
transaction open. Financial decisions, witnesses, accepted assessments,
observation visibility, outbox records and cursor commit together.

Run `spx-research resume RUN_DIR`. To relocate an identical dataset, supply
`--dataset-root NEW_PATH`. Resume verifies frozen inputs and the database record,
then continues at its cursor using saved responses. Changes to data, prices,
code, contracts or the alias key require a new experiment. Unsupported legacy
runs are replay-only; do not fabricate cursors or provenance for them.

`events.jsonl`, `journal.json`, `decision_tape.jsonl` and `report.json` are atomic
exports. `run_manifest.json` is frozen input evidence; `run_result.json` binds
final output digests separately. A v2 tape has record hashes and an end seal.
A truncated tape import is incomplete and read-only. Resume regenerates exports
from PostgreSQL; it never appends to malformed bytes. Keep copies of damaged
exports when investigating a storage failure.

For an epistemic incident, inspect the private incident ledger. The barrier
remains paused on resume. A replacement policy answer requires a separately
identified experiment. Rejected prose must not become retry context.

Outbox rows are durable delivery intents committed with the financial batch.
A consumer must deduplicate by its stable identity and acknowledge only after
its own delivery transaction. No external delivery consumer ships here;
`emitted=false` does not mean financial effects are absent.

## Spending and inference boundary

Each attempt has its own durable reservation, request and result. Exact frozen
model prices include cached input, and a frozen model context limit supplies
the conservative input reservation. Missing rates or limits block dispatch.
Provider refusal, incomplete output and invalid JSON still reconcile usage.
There are no SDK-managed retries. Known recoverable failures may retry within
the frozen limit; evidence/scope/menu/lineage violations pause immediately.

Unknown dispatch or billing retains its reservation across restarts. Use the
trusted `RunStore.reconcile_attempt(..., actual_usd=..., evidence_ref=...)` API
only with provider receipt evidence. This appends the original attempt and
receipt reference to the journal. A resolved billing pause becomes
`BILLING_RECONCILED`; `resume` reuses the saved response or makes an explicitly
counted retry when no response was recorded. It neither fabricates a response
nor clears an epistemic incident. An overrun blocks further dispatch even when
the total run cap has not yet been reached.

Offline isolation acceptance uses:

```sh
docker build --network=none -t spx-inference:2.1 -f Dockerfile.isolation .
docker compose -f compose.isolation.yaml up -d mock-gateway
spx-research run PROFILE --dataset-root DATASET --out NEW_DIRECTORY \
  --policy llm-mock --transport docker --gateway-volume spx_mock_gateway_socket
```

The engine starts the digest-pinned worker as UID 65532 with no network, a
read-only filesystem, no capabilities, no privilege escalation, and bounded CPU,
RAM and processes. Its only mounted volume contains the gateway Unix socket.
Archive, repository, host home, credentials, database and Docker socket are absent.
The worker receives only a versioned blinded packet, approved contract references,
model settings, request digest and fixed retry code.

The provider deployment profile separates a credential-bearing, networkless
gateway from a fixed-function egress proxy. Only the proxy has a network; it
permits one TLS hostname, one API path and frozen model IDs, rejects private IPs
and redirects, and does not accept arbitrary URLs. Set `SPX_PROVIDER_KEY_FILE`
and `SPX_ALLOWED_MODELS` in an approved deployment. Provider credentials never
enter the engine or worker. Real calls additionally require approved profile
permissions, PostgreSQL, Docker transport, exact prices and a bounded budget.
No paid deployment is authorized or exercised by synthetic acceptance.

## Backup and restore

1. Stop writers or obtain a consistent PostgreSQL backup with `pg_dump --format=custom`.
   Include all application tables, the Alembic revision, migration audit, runtime
   journal, prepared decisions/attempts, incidents, observation ledger and outbox.
2. Back up frozen manifests, the identical datasets/calendar, installed application
   version and dependency lock, and the alias secret through the operator's secret
   store. Keep credentials outside artifact bundles.
3. Restore into a separate database using `pg_restore`; restore role grants/RLS
   using the migration definitions and configure the engine DSN. Never restore
   over an active run database as a recovery experiment.
4. Verify the restored manifest and ledger chain, compare attempt counts and
   budget totals with the backup, then invoke `resume` against the restored DSN.
   Regenerate exports from that journal and run the audit. A tape alone is not a
   recovery backup.

## Audits and acceptance

The [user guide's blinding explanation](USER_GUIDE.md#historical-blinding-and-pretrained-knowledge)
distinguishes the UserHarness-inspired information boundary from removal of
pretrained knowledge. Invariance checks verify the application's requests;
accepted evidence references do not prove the internal cause of an AI judgment.
Do not present a successful audit as measured elimination of historical memory.

`leakage-eval RUN_DIR` requires the applicable evidence. For two runs add
`--control-dir CONTROL --cutoff AWARE_ISO_TIMESTAMP --expect invariant|changed`.
Controls share the visible alias namespace/key and corresponding actor scopes.
Comparison includes complete canonical provider requests through the cutoff,
including packet hashes, prompts, schemas, model and settings. `changed` is the
positive control and succeeds only when observed requests actually differ.

Missing artifacts, incomplete tapes or failed comparisons cannot report PASS.
Diagnostics not performed remain `NOT_RUN`; model temporal provenance defaults
to `UNKNOWN`. Every report keeps
`HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED` and
`parametric_future_knowledge_excluded=false`.

The [acceptance matrix](ACCEPTANCE_MATRIX.md) maps findings to regressions.
PostgreSQL and offline Docker checks are mandatory in CI. Local runs without
explicit test-service configuration may skip those integrations, which is not
complete implementation acceptance.
