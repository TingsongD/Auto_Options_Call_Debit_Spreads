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

SPEC = spec_dir()

PACKET_EXAMPLES = ["spread_packet", "manager_packet", "spread_entry_packet", "reference_packet"]


def test_all_four_schemas_load():
    for name in (
        "model_visible_packet",
        "spread_decision",
        "manager_decision",
        "decision_witness",
    ):
        schema = load_schema(name)
        assert schema["type"] == "object"


@pytest.mark.parametrize("name", PACKET_EXAMPLES)
def test_packet_examples_conform(name):
    validate(load_example(name), load_schema("model_visible_packet"))


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


def test_prompts_load_and_forbid_overrides():
    for role in ("manager", "spread_agent"):
        text = load_prompt(role)
        assert "action_id" in text
        assert "schema" in text


def test_spec_dir_is_the_installed_bundle():
    assert SPEC is not None
    assert SPEC.name == "contracts"
    assert (SPEC / "bundle.json").is_file()


def test_override_cannot_silently_change_contracts(tmp_path, monkeypatch):
    import shutil

    root = tmp_path / "override"
    shutil.copytree(spec_dir(), root)
    monkeypatch.setenv("SPX_SPEC_DIR", str(root))
    assert load_schema("spread_decision")["type"] == "object"
    (root / "prompts" / "manager.md").write_text("different instructions")
    with pytest.raises(SpecNotFoundError):
        load_prompt("manager")


def test_packaged_migrations_and_dependency_lock_match_checkout():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    resources = spec_dir().parent
    assert (root / "uv.lock").read_bytes() == (resources / "dependency.lock").read_bytes()
    source_files = {p.relative_to(root / "alembic"): p for p in (root / "alembic").rglob("*.py")}
    bundled_files = {
        p.relative_to(resources / "migrations"): p for p in (resources / "migrations").rglob("*.py")
    }
    assert source_files.keys() == bundled_files.keys()
    assert all(p.read_bytes() == bundled_files[rel].read_bytes() for rel, p in source_files.items())
