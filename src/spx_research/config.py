"""Typed run-profile configuration (M0-03).

Pydantic models mirroring the spec's ``config/*.example.yaml`` profiles.
Unknown keys are rejected; fields that must not be invented stay ``None`` and
are blocked by :mod:`spx_research.preflight` for the relevant run mode.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

SCHEMA_VERSION: Literal["2.0"] = "2.0"


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _hhmm(v: Any) -> time:
    if isinstance(v, time):
        return v
    if isinstance(v, str):
        try:
            parsed = time.fromisoformat(v)
            if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
                raise ValueError("session time must be a local whole minute")
            return parsed
        except ValueError as exc:
            raise ValueError(f"expected HH:MM, got {v!r}") from exc
    raise ValueError(f"expected HH:MM, got {v!r}")


class Permissions(Section):
    real_data_requests: bool = False
    real_model_requests: bool = False
    broker_writes: bool = False


class Approvals(Section):
    policy_approved: bool = False
    data_rights_approved: bool = False
    external_model_data_transfer_approved: bool = False
    discretionary_loss_acknowledged: bool = False
    owner: str | None = None
    approved_at_utc: datetime | None = None


class Universe(Section):
    underlying: Literal["SPX"] = "SPX"
    allowed_contract_roots: list[str]
    settlement: Literal["PM"] = "PM"
    strategies: list[Literal["BULL_PUT_CREDIT", "BEAR_CALL_CREDIT"]]
    quantity_per_leg: Literal[1] = 1
    same_expiry_required: Literal[True] = True
    target_entry_dte_calendar_days: int
    entry_dte_range: tuple[int, int]
    spread_widths_index_points: list[Decimal]
    short_abs_delta_range: tuple[Decimal, Decimal]
    delta_requires_validated_source: bool = True
    max_candidates_per_direction: int

    @field_validator("entry_dte_range", "short_abs_delta_range", mode="before")
    @classmethod
    def _pair(cls, v: Any) -> Any:
        if isinstance(v, list) and len(v) == 2:
            return (v[0], v[1])
        return v


class Clock(Section):
    market_resolution_seconds: Literal[60] = 60
    agent_review_minutes: Literal[10, 15, 20]
    review_anchor: str = "session_open_plus_review_interval"
    include_review_at_close: Literal[False] = False
    timezone: Literal["America/New_York"] = "America/New_York"
    research_session_open: time = time(9, 30)
    research_session_close: time = time(16, 0)
    calendar_manifest_id: str | None = None
    respect_half_days: bool = True
    simulated_execution_delay_seconds: int = 60
    out_of_cycle_spread_reviews: bool = False

    @field_validator("research_session_open", "research_session_close", mode="before")
    @classmethod
    def _parse_hhmm(cls, v: Any) -> time:
        return _hhmm(v)

    @field_validator("simulated_execution_delay_seconds")
    @classmethod
    def _delay_on_grid(cls, v: int) -> int:
        # Orders become eligible only at t + delay and fill at exactly that
        # minute (T06); a delay that is not a whole number of market minutes
        # can never land on the grid and silently expires every order.
        if v <= 0 or v % 60 != 0:
            raise ValueError(
                "simulated_execution_delay_seconds must be a positive multiple of "
                "market_resolution_seconds (60)"
            )
        return v


class Study(Section):
    data_manifest_id: str | None = None
    start_date: date | None = None
    scored_end_date: date | None = None
    runoff_end_date: date | None = None
    minimum_history_years: int = 8
    warmup_policy_id: str | None = None
    final_position_policy: str = "runoff_to_close_or_settlement"
    split_protocol_id: str | None = None


class Portfolio(Section):
    initial_capital_usd: Decimal | None = None
    max_open_or_reserved_slots: int = 3
    bullish_weight: int = 2
    bearish_weight: int = 1
    ratio_basis: Literal["count"] = "count"
    ratio_is_soft_target: bool = True
    no_forced_trade_for_ratio: bool = True
    max_per_spread_initial_risk_usd: Decimal | None = None
    max_aggregate_committed_risk_usd: Decimal | None = None
    reservation_buffer_usd: Decimal | None = None
    capital_model: str = "full_width_cash_encumbrance"
    cross_spread_margin_offsets: Literal[False] = False
    pending_exits_release_capital: Literal[False] = False


class ExitPolicy(Section):
    pnl_numerator: str = "net_estimated_liquidation_pnl"
    profit_denominator: str = "gross_initial_credit"
    profit_review_band: tuple[Decimal, Decimal] = (Decimal("0.3"), Decimal("0.4"))
    may_hold_beyond_profit_band: bool = True
    loss_denominator: str = "gross_initial_credit"
    loss_review_band: tuple[Decimal, Decimal] = (Decimal("0.2"), Decimal("0.3"))
    holding_age_basis: str = "new_york_calendar_date_difference"
    loss_activation_days_held: int = 25
    early_loss_notice_days_held: int = 20
    early_discretionary_exit_allowed: bool = True
    # Reserved surface — parsed and validated but not consumed by the engine
    # yet; they gate owner decisions D02/D05 in the decision register. The
    # spec forbids hard stops by default, so nothing reads these today.
    mandatory_loss_stop_enabled: bool = False
    mandatory_exit_before_dte: int | None = None

    @field_validator("profit_review_band", "loss_review_band", mode="before")
    @classmethod
    def _pair(cls, v: Any) -> Any:
        if isinstance(v, list) and len(v) == 2:
            return (v[0], v[1])
        return v


class ManagerCfg(Section):
    triggers: list[str] = ["first_daily_review", "fills_or_closures", "reservation_expiry"]
    coalesce_events_by_minute: bool = True
    reservation_expiry: str = "research_session_close"
    new_agent_first_action: str = "next_regular_agent_review"


class Execution(Section):
    reference_model: str = "natural_quote_sides"
    order_style: str = "package_limit"
    fill_window: str = "first_eligible_minute_only"
    require_both_legs_same_snapshot: bool = True
    require_relevant_side_size_at_least: int = 1
    quote_event_age_policy: str = "known_age_limit_else_disclose_snapshot_only"
    max_known_quote_age_seconds: int = 60
    no_overnight_quote_forward_fill: bool = True
    clip_close_debit_to_expiry_width: Literal[False] = False
    transaction_cost_profile_id: str | None = None
    opening_fee_per_leg_usd: Decimal | None = None
    closing_fee_per_leg_usd: Decimal | None = None
    settlement_fee_profile_id: str | None = None
    settlement_fee_per_leg_usd: Decimal | None = None
    settlement_cash_availability_policy_id: str | None = None


class Macro(Section):
    vintage_source: str = "ALFRED_plus_original_releases"
    first_features: list[str] = ["fed_target_decisions", "trailing_policy_changes", "DGS10"]
    publication_timestamp_required_for_intraday_use: bool = True
    missing_release_time: str = "conservative_approved_policy_or_exclude"
    interpolate_daily_yields_to_minutes: Literal[False] = False
    current_web_browsing: Literal[False] = False


class Models(Section):
    provider: str = "openai"
    interface: str = "responses"
    spread_role_candidate: str | None = None
    manager_role_candidate: str | None = None
    resolved_model_ids_manifest: str | None = None
    prompt_version: str = "v2.1"
    structured_output: Literal[True] = True
    store: Literal[False] = False
    unrestricted_tools: Literal[False] = False
    model_fallback_allowed: Literal[False] = False
    maximum_concurrent_requests: int = 3
    retry_attempts_after_initial: int = 2
    unresolved_failure_policy: str = "pause_before_decision_barrier_commit"
    max_output_tokens_per_call: int = 800
    price_sheet_id: str | None = None
    experiment_api_budget_usd: Decimal | None = None
    model_context_limits: dict[str, int] = {}
    budget_action: str = "checkpoint_and_pause"
    provider_managed_conversation: Literal[False] = False
    previous_response_id: None = None
    opaque_reasoning_replay: Literal[False] = False
    uninspected_compaction: Literal[False] = False

    @model_validator(mode="after")
    def _bounded_requests(self) -> Self:
        if self.maximum_concurrent_requests < 1 or self.retry_attempts_after_initial < 0:
            raise ValueError("concurrency must be positive and retries nonnegative")
        if not 1 <= self.max_output_tokens_per_call <= 32768:
            raise ValueError("max_output_tokens_per_call must be between 1 and 32768")
        if any(limit <= 0 for limit in self.model_context_limits.values()):
            raise ValueError("model context limits must be positive")
        return self


class Quality(Section):
    """Data-quality policy knobs. ``max_*_age_seconds`` and
    ``fail_on_unvalidated_greeks_for_delta_filter`` are consumed by the
    engine; the remaining fields land with the QA/ingest commands."""

    missing_open_position_quote: str = "pause_validated_run"
    invalid_candidate_quote: str = "reject_candidate_and_report"
    coverage_outage: str = "pause_not_no_trade"
    fail_on_future_feature: bool = True
    fail_on_unvalidated_greeks_for_delta_filter: bool = True
    fail_on_missing_settlement_value: bool = True
    # Staleness bounds: a snapshot older than its bound is treated as absent.
    # Quotes default to 5 minutes (synthetic series is minute-dense); Greeks
    # default to a day (providers typically refresh once per session).
    max_quote_age_seconds: int = 300
    max_greek_age_seconds: int = 86400


class HarnessCfg(Section):
    name: str = "TemporalKnowledgeHarness"
    version: str = "2.1"
    view_mode: str = "asof_blinded"
    knowledge_contract: str = "verified_observation_and_typed_assessment"
    claim_parametric_ignorance: Literal[False] = False
    require_actor_delivery: bool = True
    require_dependency_closure: bool = True
    require_source_vintages: bool = True
    unknown_release_time: str = "exclude_or_approved_conservative_bound"
    model_can_write_realized_facts: Literal[False] = False
    model_can_override_gate: Literal[False] = False
    private_engine_context_required: bool = True
    real_dates_in_model_packet: Literal[False] = False
    raw_contract_symbols_in_model_packet: Literal[False] = False
    raw_source_text_in_default_packet: Literal[False] = False
    public_packet_ids_depend_on_hidden_suffix: Literal[False] = False
    context_memory: str = "local_validated_belief_state"
    action_selection: str = "engine_generated_menu_with_price_templates"
    free_text_persistent_memory: Literal[False] = False
    failed_response_reused_as_context: Literal[False] = False
    epistemic_failure_policy: str = "quarantine_and_pause_scored_barrier"
    inference_archive_access: Literal[False] = False
    evaluation_writeback: Literal[False] = False
    feature_vocabulary_manifest_id: str | None = None
    approved_rule_manifest_id: str | None = None
    blinding_manifest_id: str | None = None
    recipient_bootstrap_policy_id: str | None = None
    price_template_manifest_id: str | None = None
    temporal_test_report_id: str | None = None
    leakage_protocol_id: str | None = None
    behavioral_diagnostic_report_id: str | None = None
    diagnostic_budget_usd: Decimal | None = None
    study_label: str = "HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED"
    real_calls: bool | None = None  # synthetic profile convenience flag


class SyntheticFixture(Section):
    """Fictitious accounting fixture; never scored as strategy performance."""

    description: str
    multiplier: int
    width_index_points: Decimal
    initial_credit_index_points: Decimal
    final_debit_index_points: Decimal
    opening_fee_per_leg_usd: Decimal
    closing_fee_per_leg_usd: Decimal
    expected_gross_max_profit_usd: Decimal
    expected_gross_max_expiration_loss_usd: Decimal
    expected_net_closed_pnl_usd: Decimal
    expected_final_cash_usd: Decimal
    expected_profit_fraction_of_gross_credit: Decimal


class Profile(BaseModel):
    """A complete run profile. Sections absent in YAML stay ``None``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["2.0"] = SCHEMA_VERSION
    profile_id: str
    status: str = "draft_unapproved"
    mode: Literal["historical_research", "synthetic_test", "forward_frozen"]
    permissions: Permissions
    approvals: Approvals | None = None
    universe: Universe | None = None
    clock: Clock | None = None
    study: Study | None = None
    portfolio: Portfolio | None = None
    exit_policy: ExitPolicy | None = None
    manager: ManagerCfg | None = None
    execution: Execution | None = None
    macro: Macro | None = None
    models: Models | None = None
    quality: Quality | None = None
    harness: HarnessCfg | None = None
    fixture: SyntheticFixture | None = None

    @model_validator(mode="after")
    def _permissions_honest(self) -> Self:
        if self.permissions.broker_writes:
            raise ValueError("broker_writes must remain false: no live trading")
        if (
            self.mode == "historical_research"
            and self.harness is not None
            and self.harness.study_label != "HISTORICAL_ASOF_BLINDED_PARAMETRIC_RISK_UNRESOLVED"
        ):
            raise ValueError("unrecognized study_label; parametric risk stays unresolved")
        return self


def load_profile(path: str | Path) -> Profile:
    """Parse a YAML profile file into a typed :class:`Profile`."""
    with Path(path).open() as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"profile {path} is not a YAML mapping")
    return Profile.model_validate(raw)


def aware_utc_now() -> datetime:
    """Wall-clock helper restricted to ingestion/manifest metadata, never as-of logic."""
    return datetime.now(UTC)
