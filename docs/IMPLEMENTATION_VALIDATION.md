# Patch implementation validation

Validated locally on 2026-09-21 with Python 3.12, PostgreSQL 16 and Docker.
The implementation covers the 20 review findings and supporting recovery,
spending, context, accounting, audit and inference-isolation safeguards.
See the [acceptance matrix](ACCEPTANCE_MATRIX.md) for the finding-to-test mapping
and the [runbook](RUNBOOK.md) for operation and recovery.

## Completed checks

| Check | Result |
|---|---|
| Full application suite, with PostgreSQL and Docker integrations required | **392 passed**, no skips, 119.04 seconds |
| Unmodified governing reference suite | **38 passed** |
| Ruff over source, tests, migrations and scripts | Passed |
| Strict mypy over application source | Passed, 67 source files |
| Git whitespace/error check | Passed |
| Source distribution and wheel build | Passed |
| Wheel installed into a separate environment and run from `/tmp`, without the sibling specification | Passed: packaged contracts, dependency identity, migration head, CLI, synthetic execution and artifact audit |

The PostgreSQL checks use a disposable database. They cover clean installation,
populated supported historical upgrades, hash-corruption rejection, durable
attempt reservations, database constraints, atomic commits and recovery after a
lost commit acknowledgement. The final migration is `0006_runtime_constraints`.

The Docker checks exercise the actual restricted worker and an offline gateway,
including denied access to private files, credentials, host/database services,
arbitrary networks and privilege escalation. No real provider request was made.

The full synthetic archive controls exercise the CLI, frozen manifests, journals,
sealed tapes and audit commands. Future-only mutations preserve earlier complete
model requests; mutations to already-visible observations trigger the expected
change. Independent accounting fixtures reconcile cash, fees, reserves,
liabilities and equity against hand-calculated expectations.

## Acceptance boundary

These results establish the synthetic implementation checks above. They do not
validate historical data, strategy returns, model temporal provenance or live
behavior. Historical adapters, licensed-data acquisition/QA, paid deployment,
prospective research and owner approvals remain separate gated work.

Legacy artifacts remain inspectable/replayable. Durable resume requires the new
frozen manifest and PostgreSQL journal; missing legacy provenance is not invented.
Unperformed research diagnostics remain `NOT_RUN`, and research validity remains
`UNVALIDATED`. The updated CI requires PostgreSQL and Docker checks; the results
recorded here are local results, not a claim that hosted CI has already run.
