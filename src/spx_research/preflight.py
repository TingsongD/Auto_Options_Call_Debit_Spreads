"""Preflight gate: fields that must not be invented block the run (T49).

``check`` returns findings; any ``BLOCK`` finding prevents the profile's run
mode from starting. ``synthetic_test`` needs only the fixture block and must
keep every external permission disabled.
"""

from __future__ import annotations

from dataclasses import dataclass

from spx_research.config import Profile


@dataclass(frozen=True)
class Finding:
    severity: str  # "BLOCK" | "WARN"
    code: str
    path: str
    detail: str


# (section, attribute) -> why it is required before a scored historical run
_REQUIRED_FOR_RESEARCH: tuple[tuple[str, str, str], ...] = (
    ("portfolio", "initial_capital_usd", "account capital must be configured, not invented"),
    ("portfolio", "max_per_spread_initial_risk_usd", "per-spread risk limit required"),
    ("portfolio", "max_aggregate_committed_risk_usd", "aggregate risk limit required"),
    ("portfolio", "reservation_buffer_usd", "fee/slippage reservation buffer required"),
    ("execution", "transaction_cost_profile_id", "fee profile must be explicit"),
    ("execution", "opening_fee_per_leg_usd", "opening fees must be explicit"),
    ("execution", "closing_fee_per_leg_usd", "closing fees must be explicit"),
    ("execution", "settlement_fee_profile_id", "settlement fees must be explicit"),
    ("execution", "settlement_cash_availability_policy_id", "settlement cash timing policy"),
    ("clock", "calendar_manifest_id", "versioned exchange calendar required"),
    ("study", "data_manifest_id", "immutable data manifest required"),
    ("study", "start_date", "study start required"),
    ("study", "scored_end_date", "scored end date required"),
    ("study", "runoff_end_date", "runoff boundary required"),
    ("study", "warmup_policy_id", "feature warmup policy required"),
    ("study", "split_protocol_id", "chronological split protocol required"),
    ("models", "resolved_model_ids_manifest", "model IDs/snapshots must be resolved by probe"),
    ("models", "price_sheet_id", "dated price sheet required for cost accounting"),
    ("models", "experiment_api_budget_usd", "approved API budget required"),
    ("harness", "feature_vocabulary_manifest_id", "approved public vocabulary required"),
    ("harness", "approved_rule_manifest_id", "registered rule set required"),
    ("harness", "blinding_manifest_id", "blinding configuration manifest required"),
    ("harness", "recipient_bootstrap_policy_id", "agent warmup/scope policy required"),
    ("harness", "price_template_manifest_id", "approved limit-price templates required"),
    ("harness", "temporal_test_report_id", "temporal test evidence required"),
    ("harness", "leakage_protocol_id", "frozen leakage protocol required"),
    ("harness", "diagnostic_budget_usd", "diagnostic budget required"),
)

_REQUIRED_APPROVALS: tuple[tuple[str, str], ...] = (
    ("policy_approved", "owner policy signoff"),
    ("data_rights_approved", "data licence confirmation"),
    ("external_model_data_transfer_approved", "model-provider data transfer permission"),
    ("discretionary_loss_acknowledged", "discretionary-loss-band acknowledgement"),
)


def check(profile: Profile) -> list[Finding]:
    """Return preflight findings for the profile's declared mode."""
    findings: list[Finding] = []
    if profile.mode == "synthetic_test":
        return _check_synthetic(profile, findings)
    return _check_research(profile, findings)


def _check_synthetic(profile: Profile, findings: list[Finding]) -> list[Finding]:
    p = profile.permissions
    for name, on in (
        ("real_data_requests", p.real_data_requests),
        ("real_model_requests", p.real_model_requests),
        ("broker_writes", p.broker_writes),
    ):
        if on:
            findings.append(
                Finding(
                    "BLOCK",
                    "PERMISSION_IN_SYNTHETIC",
                    f"permissions.{name}",
                    "synthetic profiles must never request real services",
                )
            )
    if profile.fixture is None:
        findings.append(
            Finding(
                "BLOCK",
                "MISSING_FIXTURE",
                "fixture",
                "synthetic_test mode requires the fixture section",
            )
        )
    if profile.models is not None and profile.models.provider != "mock":
        findings.append(
            Finding(
                "WARN",
                "NONMOCK_PROVIDER",
                "models.provider",
                "synthetic runs should use the mock provider",
            )
        )
    return findings


def _check_research(profile: Profile, findings: list[Finding]) -> list[Finding]:
    for section_name, attr, detail in _REQUIRED_FOR_RESEARCH:
        section = getattr(profile, section_name, None)
        if section is None:
            findings.append(
                Finding(
                    "BLOCK",
                    "MISSING_SECTION",
                    section_name,
                    f"required section absent; cannot check {attr}",
                )
            )
            continue
        if getattr(section, attr, "missing") in (None, "missing"):
            findings.append(Finding("BLOCK", "MISSING_FIELD", f"{section_name}.{attr}", detail))
    for section_name in (
        "universe",
        "clock",
        "study",
        "portfolio",
        "exit_policy",
        "manager",
        "execution",
        "macro",
        "models",
        "quality",
        "harness",
    ):
        if getattr(profile, section_name, None) is None:
            findings.append(
                Finding(
                    "BLOCK",
                    "MISSING_SECTION",
                    section_name,
                    "required section absent from research profile",
                )
            )
    appr = profile.approvals
    for attr, detail in _REQUIRED_APPROVALS:
        if appr is None or not getattr(appr, attr):
            findings.append(Finding("BLOCK", "MISSING_APPROVAL", f"approvals.{attr}", detail))
    if appr is not None and (appr.owner is None or appr.approved_at_utc is None):
        findings.append(
            Finding(
                "BLOCK",
                "MISSING_APPROVAL_META",
                "approvals.owner/approved_at_utc",
                "approver identity and timestamp required",
            )
        )
    if profile.harness is not None and profile.harness.behavioral_diagnostic_report_id is None:
        findings.append(
            Finding(
                "WARN",
                "NO_BEHAVIORAL_DIAGNOSTIC",
                "harness.behavioral_diagnostic_report_id",
                "behavioral leakage diagnostics not yet attached",
            )
        )
    return findings


def blocking(findings: list[Finding]) -> list[Finding]:
    return [f for f in findings if f.severity == "BLOCK"]
