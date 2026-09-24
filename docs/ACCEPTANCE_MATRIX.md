# Finding-to-test acceptance matrix

The implementation acceptance scope is synthetic operation, including a real
PostgreSQL journal and a network-isolated worker using an offline gateway.
The governing handover remains the source of strategy and temporal requirements.
Existing historical artifacts remain unchanged; v2 recovery requires a v2 journal.

| Finding | Required behavior | Regression evidence |
|---|---|---|
| 1 | Frozen migrations; fresh and populated upgrades; detect corruption | `tests/integration/test_migrations.py` |
| 2 | Failed actor stops the clock and rolls back its whole decision batch | `test_engine_safety.py::test_failed_actor_rolls_back_whole_decision_barrier_and_resumes_exactly` |
| 3 | Missing position/order quotes pause with holdings and unknown equity | `test_engine_safety.py::test_missing_second_fill_quote_commits_no_partial_market_effects`, `test_held_quote_outage_retains_position_and_unknown_equity` |
| 4 | Refusals/incomplete/invalid output retain usage and consume budget | `test_runtime_decisions.py::test_incomplete_usage_is_charged_before_retry_budget_check`, `test_gateway_retains_usage_and_counts_cached_input_once` |
| 5 | Epistemic violations quarantine and pause without another answer | `test_llm_pipeline.py`, `test_runtime_decisions.py::test_malformed_typed_assessment_is_quarantined_not_a_type_crash` |
| 6 | Runoff manages existing positions without entries or allocations | `test_engine_safety.py::test_runoff_only_manages_existing_positions_and_settles_at_publication` |
| 7 | Verify dataset type, provenance, boundaries and checksums before use | `test_cli_recovery.py::test_type_and_boundaries_rejected_before_archive_access`, `test_synthetic_data.py` |
| 8 | Missing or incomplete audit evidence never passes | `test_leakage_artifacts.py::test_missing_required_artifact_never_passes`, `test_truncated_last_tape_record_is_not_a_clean_scan` |
| 9 | Compare complete attempted requests through cutoff; positive control changes | `test_leakage_artifacts.py::test_exact_requests_require_cutoff_and_respect_positive_control`, `test_prompt_drift_cannot_hide_behind_identical_action`, `test_rejected_dispatches_are_part_of_prefix_comparison`, `test_end_to_end_controls.py` |
| 10 | Recover journal/cursor without duplicate financial effects | `test_engine_safety.py`, `tests/integration/test_runtime.py`, `tests/integration/test_cli_resume.py` |
| 11 | Accepted retry links to its logical decision and exact bytes | `test_runtime_decisions.py::test_retry_success_replays_without_recreating_failed_attempt` |
| 12 | Retry/restart preserves prior belief and prepared observations | `test_runtime_decisions.py::test_same_barrier_keeps_exact_request_after_observation_commit`, `test_assessments_and_observations_commit_together` |
| 13 | Sealed atomic tape exports; legacy/torn input remains read-only | `test_runtime_decisions.py::test_tape_is_sealed_and_legacy_and_torn_files_stay_unchanged`, `test_export_crash_keeps_previous_complete_tape` |
| 14 | Candidate risk uses current equity; context includes lawful economics | `test_runtime_decisions.py::test_risk_and_holding_context_are_economic_not_identity_cues`, `tests/golden/test_golden_baseline.py` |
| 15 | Review time follows the session-anchored grid after a fill | `test_engine_safety.py::test_next_review_after_fill_is_next_anchored_grid`, `test_calendar.py` |
| 16 | Invalid quote snapshots cannot create candidates or marks/fills | `test_engine_safety.py::test_bad_quotes_never_enter_candidate_menu`, `test_execution.py` |
| 17 | Both option rights and candidate-specific limit binding validated | `test_engine_safety.py::test_mixed_right_and_cross_candidate_limits_rejected` |
| 18 | Close/settlement timestamps and terminal timestamp are ordered | `test_engine_safety.py::test_runoff_only_manages_existing_positions_and_settles_at_publication`, `tests/golden/test_golden_baseline.py` |
| 19 | Frozen identities preserve every run; changed inputs reject recovery | `test_cli_recovery.py`, `test_runtime_store.py`, `test_experiments.py` |
| 20 | Wheel includes pinned contracts, prompts, examples, migrations and lock | `scripts/check_installed.py`, `test_contracts.py` |
| Recovery extension | Crashes around dispatch/response/commit; no duplicate charges/knowledge | `test_runtime_decisions.py`, `test_runtime_store.py`, `tests/integration/test_runtime.py` |
| Spending extension | Model-specific full-context reservation, cached pricing, concurrency and audited uncertain billing | `test_runtime_decisions.py`, `test_runtime_store.py`, `tests/integration/test_runtime.py`, `test_usage_guard.py` |
| Isolation extension | Actual Docker worker cannot access archive, home, credentials, DB/host or arbitrary network | `tests/integration/test_isolation_docker.py`, `test_isolation.py` |
| Storage constraints | Prepared-decision/accepted-attempt foreign keys, finite money, immutable identities | `tests/integration/test_runtime.py`, `tests/integration/test_migrations.py` |
| Accounting extension | Hand-reconciled cash, fees, liabilities, equity and reserve, plus fixed digest | `tests/golden/test_golden_baseline.py` |
| Context boundary | Versioned 2.1 semantics and governing reference unchanged | `test_temporal_harness.py`, `test_temporal_prod.py`, `reference/` |

Unqualified application completion requires all commands below to pass. The
PostgreSQL URL must name a disposable database: these fixtures truncate tables.

```sh
uv sync --frozen
uv run ruff check src tests alembic scripts
uv run mypy src
SPX_TEST_DSN=postgresql+psycopg://USER:PASS@HOST/TEST_DB \
  SPX_REQUIRE_POSTGRES=1 SPX_TEST_ISOLATION=1 uv run pytest -q
uv run python -m unittest discover -s reference -q
uv build
uv venv /tmp/spx-installed --python 3.12
uv pip install --python /tmp/spx-installed/bin/python dist/*.whl
cp scripts/check_installed.py /tmp/check-spx-installed.py
(cd /tmp && env -u SPX_SPEC_DIR /tmp/spx-installed/bin/python check-spx-installed.py)
```

Build `spx-inference:2.1` from `Dockerfile.isolation` before Docker tests. The
integration fixture creates and removes its own gateway container and volume.
CI requires both services; it cannot silently skip their checks.

Historical ingestion adapters, licensed-data QA, prospective/behavioral research,
paid provider deployment and owner approvals remain gated follow-up work.
Unperformed behavioral diagnostics are `NOT_RUN`; synthetic test success does
not establish parametric ignorance or validate strategy returns.
