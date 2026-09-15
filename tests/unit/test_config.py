"""M0-03 typed configuration and preflight gate (T49)."""

import pytest
import yaml
from pydantic import ValidationError

from spx_research.config import Profile, load_profile
from spx_research.contracts import SpecNotFoundError, example_config_path
from spx_research.preflight import blocking, check

try:
    SYNTHETIC_YAML = example_config_path("synthetic")
    RESEARCH_YAML = example_config_path("research")
    HAS_SPEC = True
except SpecNotFoundError:
    HAS_SPEC = False

needs_spec = pytest.mark.skipif(not HAS_SPEC, reason="spec package not found")


@needs_spec
def test_synthetic_profile_parses_and_passes_preflight():
    profile = load_profile(SYNTHETIC_YAML)
    assert profile.mode == "synthetic_test"
    assert profile.fixture is not None
    assert profile.fixture.expected_final_cash_usd == 10066
    assert blocking(check(profile)) == []


@needs_spec
def test_research_draft_blocks_on_missing_fields_and_approvals():
    """T49: unapproved research profile must fail preflight, listing gaps."""
    profile = load_profile(RESEARCH_YAML)
    assert profile.mode == "historical_research"
    assert profile.universe is not None
    assert profile.universe.entry_dte_range == (40, 50)
    findings = blocking(check(profile))
    codes = {f.code for f in findings}
    assert "MISSING_FIELD" in codes and "MISSING_APPROVAL" in codes
    paths = {f.path for f in findings}
    assert "portfolio.initial_capital_usd" in paths
    assert "models.experiment_api_budget_usd" in paths
    assert "harness.price_template_manifest_id" in paths
    assert "approvals.data_rights_approved" in paths


def test_unknown_top_level_key_rejected():
    with pytest.raises(ValidationError):
        Profile.model_validate(
            {"profile_id": "x", "mode": "synthetic_test", "permissions": {}, "bogus": 1}
        )


def test_unknown_nested_key_rejected():
    with pytest.raises(ValidationError):
        Profile.model_validate(
            {
                "profile_id": "x",
                "mode": "synthetic_test",
                "permissions": {"live_trading": True},
            }
        )


def test_broker_writes_always_rejected():
    with pytest.raises(ValidationError):
        Profile.model_validate(
            {
                "profile_id": "x",
                "mode": "historical_research",
                "permissions": {"broker_writes": True},
            }
        )


def test_parametric_ignorance_claim_rejected():
    with pytest.raises(ValidationError):
        Profile.model_validate(
            {
                "profile_id": "x",
                "mode": "historical_research",
                "permissions": {},
                "harness": {"claim_parametric_ignorance": True},
            }
        )


def test_synthetic_profile_with_real_permission_blocked():
    raw = yaml.safe_load(
        """
schema_version: '2.0'
profile_id: bad_synth
mode: synthetic_test
permissions: {real_model_requests: true}
fixture:
  description: x
  multiplier: 100
  width_index_points: '10.00'
  initial_credit_index_points: '2.00'
  final_debit_index_points: '1.30'
  opening_fee_per_leg_usd: '1.00'
  closing_fee_per_leg_usd: '1.00'
  expected_gross_max_profit_usd: '200.00'
  expected_gross_max_expiration_loss_usd: '800.00'
  expected_net_closed_pnl_usd: '66.00'
  expected_final_cash_usd: '10066.00'
  expected_profit_fraction_of_gross_credit: '0.33'
"""
    )
    profile = Profile.model_validate(raw)
    findings = blocking(check(profile))
    assert any(f.code == "PERMISSION_IN_SYNTHETIC" for f in findings)


def test_minimal_valid_research_profile_shape():
    """A fully populated research profile parses (values here are synthetic)."""
    profile = Profile.model_validate(
        {
            "profile_id": "unit",
            "mode": "historical_research",
            "permissions": {},
            "universe": {
                "allowed_contract_roots": ["SPXW"],
                "strategies": ["BULL_PUT_CREDIT", "BEAR_CALL_CREDIT"],
                "target_entry_dte_calendar_days": 45,
                "entry_dte_range": [40, 50],
                "spread_widths_index_points": [5, 10],
                "short_abs_delta_range": ["0.15", "0.35"],
                "max_candidates_per_direction": 12,
            },
            "clock": {"agent_review_minutes": 15, "research_session_open": "09:30"},
        }
    )
    assert profile.clock is not None
    assert profile.clock.research_session_open.hour == 9
    assert profile.universe is not None
    assert profile.universe.spread_widths_index_points[0] == 5
