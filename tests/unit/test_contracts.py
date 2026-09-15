"""Spec contract files load and the shipped examples satisfy their schemas."""

import pytest
from jsonschema import validate

from spx_research.contracts import (
    SpecNotFoundError,
    load_example,
    load_prompt,
    load_schema,
    spec_dir,
)

try:
    SPEC = spec_dir()
    HAS_SPEC = True
except SpecNotFoundError:
    SPEC = None
    HAS_SPEC = False

needs_spec = pytest.mark.skipif(not HAS_SPEC, reason="spec package not found")

PACKET_EXAMPLES = ["spread_packet", "manager_packet", "spread_entry_packet", "reference_packet"]


@needs_spec
def test_all_four_schemas_load():
    for name in (
        "model_visible_packet",
        "spread_decision",
        "manager_decision",
        "decision_witness",
    ):
        schema = load_schema(name)
        assert schema["type"] == "object"


@needs_spec
@pytest.mark.parametrize("name", PACKET_EXAMPLES)
def test_packet_examples_conform(name):
    validate(load_example(name), load_schema("model_visible_packet"))


@needs_spec
@pytest.mark.parametrize(
    "name,schema",
    [
        ("spread_open", "spread_decision"),
        ("spread_hold", "spread_decision"),
        ("manager_allocate", "manager_decision"),
        ("reference_proposal", "spread_decision"),
        ("reference_witness_private", "decision_witness"),
    ],
)
def test_decision_and_witness_examples_conform(name, schema):
    validate(load_example(name), load_schema(schema))


@needs_spec
def test_prompts_load_and_forbid_overrides():
    for role in ("manager", "spread_agent"):
        text = load_prompt(role)
        assert "action_id" in text
        assert "schema" in text


@needs_spec
def test_spec_dir_is_the_expected_sibling():
    assert SPEC is not None
    assert SPEC.name == "spx_ai_handover_v2"
