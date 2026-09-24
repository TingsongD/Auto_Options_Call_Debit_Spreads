# Development log

This log records completed work, its verification, and remaining boundaries.
Entries describe changes made in the workspace; they do not imply a release,
commit, hosted CI result, deployment, or owner approval.

## 2026-09-24 — Research goal and UserHarness clarification

- Added the discretionary AI trading research question to the README and user guide.
- Explained the UserHarness-inspired observation/belief boundary, blinding,
  external validation, isolation and future-change controls, with a paper link.
- Distinguished application input isolation from erasing pretrained knowledge,
  including how a model could recognize an episode while citing valid evidence.
- Clarified that the AI retains discretion among allowed actions, profitability
  remains unproven, and real-model residual-leakage measurements remain unperformed.
- Linked the explanation from the operations runbook and retained conservative
  reporting labels. Checked documentation links, anchors and whitespace; no
  application behavior changed and no model calls or service tests were run.

## 2026-09-24 — Documentation and user onboarding

- Reviewed the existing README, operations runbook, acceptance matrix and
  implementation validation record against the current CLI and packaged profiles.
- Added [USER_GUIDE.md](USER_GUIDE.md) with a synthetic setup walkthrough,
  complete profile preparation, dataset generation, mechanical/mock runs,
  report interpretation, replay/comparison, PostgreSQL recovery and troubleshooting.
- Added this development log, including the earlier patch work below.
- Added a documentation index to the README and cross-links from the runbook.
  Changed the README installation example to `uv sync --frozen` so it uses the
  recorded dependency lock.
- Verification: executed the guide's profile, generation, mechanical/mock,
  replay, registration and audit/comparison commands in a temporary directory
  using the existing locked environment. All three runs completed; financial
  replay, artifact audits and the expected request-invariance comparison passed.
  This small fixture made no trades, as documented in the guide. Local document
  links and whitespace checks also passed. No database or Docker service was
  started for this documentation check. The September 21 full acceptance results
  below remain a dated record, not a claim that the complete service suite was
  rerun for documentation edits.

## 2026-09-21 — Review patches and supporting safeguards

Implemented the approved plan covering all 20 findings and related safeguards.
The [acceptance matrix](ACCEPTANCE_MATRIX.md) maps individual findings to tests.

### Installation and contracts

- Froze migration 0001's historical definitions and made migration 0003 handle
  genuine older schemas and existing event hashes while preserving evidence.
- Added migrations 0004–0006 for the runtime journal, attempts, requests, budgets,
  cursor state, relationships and immutable identities.
- Packaged checksum-pinned contract bundle 2.1, prompts, examples, migrations and
  the dependency lock; external overrides must match the recorded bundle.

### Recovery, spending and isolation

- Added equivalent memory/PostgreSQL RunStore interfaces, durable decision
  preparation, exact per-attempt requests, stable decision IDs and accepted-attempt links.
- Made decision batches, financial events, accepted knowledge, outbox records and
  cursors commit atomically; model calls run outside database transactions.
- Added verified `resume`, sealed atomic tape exports and explicit policy replay.
  Legacy artifacts remain readable without invented recovery provenance.
- Persisted reservations, costs and uncertain billing across restarts; disabled
  SDK retries and retained usage for refusals, malformed and incomplete outputs.
- Added exact-model pricing, cached-input rates, conservative context reservations,
  overrun stops and audited receipt reconciliation. Epistemic incidents quarantine
  responses and pause without silently requesting another answer.
- Added restricted Docker workers, a separate credential gateway and an allowlisting
  proxy, exercised with an offline gateway and actual access-denial tests.

### Engine, context and audit

- Added archive provenance/checksum checks and shared quote validation; outages
  pause with positions retained and unavailable valuations explicitly unknown.
- Corrected both-leg rights, candidate/limit binding, session-anchored reviews,
  close/settlement ordering and entry-free runoff. Unsupported policies fail early.
- Added minute liabilities/equity and scored-end/runoff reporting, plus blinded
  candidate economics, holding age, DTE, advisory bands and lawful macro context.
- Froze manifests before execution and retained every run independently of shared
  experiment identity. Artifact digests are recorded separately from inputs.
- Required complete audit evidence and compared complete canonical requests with
  explicit cutoffs and invariance/change expectations.
- Added independently reconciled accounting fixtures, real synthetic archive
  controls, mandatory PostgreSQL/Docker CI checks and standalone wheel verification.

### Recorded verification

- Full application suite: **392 passed**, no skips, with PostgreSQL and Docker required.
- Governing reference suite: **38 passed**.
- Ruff, strict mypy (67 source files), whitespace checks and distribution build passed.
- Installed wheel passed from outside the checkout, including a synthetic run and audit.
- No paid model calls or real-data acquisition occurred. The task's disposable
  PostgreSQL test container was removed after validation.

See [IMPLEMENTATION_VALIDATION.md](IMPLEMENTATION_VALIDATION.md) for scope and evidence.

## Remaining gated work

Historical ingestion adapters, licensed-data acquisition and QA, paid provider
deployment, prospective/behavioral research, broker integration and owner approvals
were outside this implementation. Consult [DECISION_REGISTER.md](DECISION_REGISTER.md)
and [DATA_RIGHTS_PROBE.md](DATA_RIGHTS_PROBE.md) before changing that scope.
Passing synthetic tests does not establish strategy performance or parametric
ignorance. Unperformed diagnostics stay `NOT_RUN`; research validity stays
`UNVALIDATED`.

For future entries, record the date, purpose, affected behavior/documents, checks
actually performed, and any unresolved work. Keep historical results dated.
