"""Preflight gate: fields that must not be invented block the run (T49).

``check`` returns findings; any ``BLOCK`` finding prevents the profile's run
mode from starting. ``synthetic_test`` needs only the fixture block and must
keep every external permission disabled.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from decimal import Decimal
from typing import Any

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
    ("execution", "settlement_fee_per_leg_usd", "numeric settlement fees must be explicit"),
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
    _check_supported(profile, findings)
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


def _check_supported(profile: Profile, findings: list[Finding]) -> None:
    supported: dict[str, dict[str, Any]] = {
        "execution": {
            "reference_model": "natural_quote_sides",
            "order_style": "package_limit",
            "fill_window": "first_eligible_minute_only",
            "require_both_legs_same_snapshot": True,
            "require_relevant_side_size_at_least": 1,
            "no_overnight_quote_forward_fill": True,
            "quote_event_age_policy": "known_age_limit_else_disclose_snapshot_only",
            "settlement_cash_availability_policy_id": (None, "immediate_on_verified_publication"),
        },
        "clock": {
            "review_anchor": "session_open_plus_review_interval",
            "respect_half_days": True,
            "research_session_open": time(9, 30),
            "research_session_close": time(16, 0),
            "out_of_cycle_spread_reviews": False,
        },
        "portfolio": {
            "capital_model": "full_width_cash_encumbrance",
            "ratio_is_soft_target": True,
            "no_forced_trade_for_ratio": True,
        },
        "exit_policy": {
            "mandatory_loss_stop_enabled": False,
            "mandatory_exit_before_dte": None,
            "holding_age_basis": "new_york_calendar_date_difference",
            "pnl_numerator": "net_estimated_liquidation_pnl",
            "profit_denominator": "gross_initial_credit",
            "loss_denominator": "gross_initial_credit",
            "early_discretionary_exit_allowed": True,
            "may_hold_beyond_profit_band": True,
        },
        "manager": {
            "triggers": ["first_daily_review", "fills_or_closures", "reservation_expiry"],
            "coalesce_events_by_minute": True,
            "reservation_expiry": "research_session_close",
            "new_agent_first_action": "next_regular_agent_review",
        },
        "study": {"final_position_policy": "runoff_to_close_or_settlement"},
        "quality": {
            "missing_open_position_quote": "pause_validated_run",
            "invalid_candidate_quote": "reject_candidate_and_report",
            "coverage_outage": "pause_not_no_trade",
            "fail_on_future_feature": True,
            "fail_on_missing_settlement_value": True,
        },
    }
    for section_name, values in supported.items():
        section = getattr(profile, section_name)
        if section is None:
            continue
        for name, expected in values.items():
            value = getattr(section, name)
            allowed = expected if isinstance(expected, tuple) else (expected,)
            if value not in allowed:
                findings.append(
                    Finding(
                        "BLOCK",
                        "UNSUPPORTED_SETTING",
                        f"{section_name}.{name}",
                        "the engine does not implement this setting",
                    )
                )
    for section_name in ("portfolio", "execution", "models", "quality", "universe", "exit_policy"):
        section = getattr(profile, section_name)
        if section is None:
            continue
        for name, value in section.__dict__.items():
            if isinstance(value, Decimal) and (not value.is_finite() or value < 0):
                findings.append(
                    Finding(
                        "BLOCK",
                        "INVALID_NUMBER",
                        f"{section_name}.{name}",
                        "must be finite and nonnegative",
                    )
                )
    if profile.study:
        s = profile.study
        dates = [d for d in (s.start_date, s.scored_end_date, s.runoff_end_date) if d is not None]
        if dates != sorted(dates):
            findings.append(
                Finding("BLOCK", "INVALID_BOUNDARIES", "study", "dates are out of order")
            )
