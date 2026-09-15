"""M2 synthetic data layer: generation, manifest, availability, QA."""

import json
from datetime import UTC, date, datetime, timedelta

import pytest

from spx_research.data.availability import Archive
from spx_research.data.manifests import DataManifest
from spx_research.data.qa import coverage_report
from spx_research.data.synthetic import SyntheticSpec, generate
from spx_research.temporal.calendar import build_weekday_manifest


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    root = tmp_path_factory.mktemp("data")
    cal = build_weekday_manifest(
        "test-cal",
        date(2019, 1, 2),
        date(2019, 1, 4),
        holidays=frozenset({date(2019, 1, 3)}),
    )
    spec = SyntheticSpec(
        dataset_id="t1",
        seed=42,
        start=date(2019, 1, 2),
        end=date(2019, 1, 4),
        expiries=(date(2019, 2, 15), date(2019, 2, 22)),
        strikes_each_side=4,
    )
    manifest = generate(root, spec, cal)
    return root / "t1", manifest, cal


def test_manifest_and_files(dataset):
    root, manifest, _ = dataset
    assert (root / "manifest.json").is_file()
    disk = json.loads((root / "manifest.json").read_text())
    # Content-addressed: syn-<content hash>, not the spec's dataset_id.
    assert disk["manifest_id"].startswith("syn-")
    assert disk["manifest_id"] == f"syn-{manifest.content_id()[4:]}"
    assert manifest.dataset_kind == "synthetic"
    assert len(manifest.normalized_files) >= 4  # contracts + per-session + greeks + macro


def test_determinism(dataset, tmp_path):
    """Same seed + calendar reproduces identical file bytes (replayable data)."""
    _, manifest, cal = dataset
    spec = SyntheticSpec(
        dataset_id="t1",
        seed=42,
        start=date(2019, 1, 2),
        end=date(2019, 1, 4),
        expiries=(date(2019, 2, 15), date(2019, 2, 22)),
        strikes_each_side=4,
    )
    m2 = generate(tmp_path, spec, cal)
    assert {f.path: f.sha256 for f in manifest.normalized_files} == {
        f.path: f.sha256 for f in m2.normalized_files
    }


def test_availability_gates_future(dataset):
    """T08: a record available after as_of is invisible."""
    root, _, _ = dataset
    arc = Archive(root)
    arc.close()
    arc2 = Archive(root)
    early = arc2.quote_at("SPXW-2019-02-15-P4995", datetime(2019, 1, 1, tzinfo=UTC))
    assert early is None
    later = arc2.quote_at("SPXW-2019-02-15-P4995", datetime(2019, 1, 2, 15, 0, tzinfo=UTC))
    assert later is not None
    arc2.close()


def test_contract_listing_visibility(dataset):
    """T04: only contracts with verified observation by as_of are visible."""
    root, _, _ = dataset
    arc = Archive(root)
    all_c = arc.contracts()
    assert all_c
    vis = arc.contracts_visible_at(datetime(2019, 1, 2, 15, 0, tzinfo=UTC))
    assert len(vis) == len(all_c)  # listed 30 days before start
    arc.close()


def test_macro_timing(dataset):
    """T10: a DGS10 value published after research close is not visible intraday."""
    root, _, _ = dataset
    arc = Archive(root)
    day = date(2019, 1, 2)
    morning = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=15)
    assert arc.macro_visible_at(morning, "DGS10") == []
    evening = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=22)
    rows = arc.macro_visible_at(evening, "DGS10")
    assert len(rows) == 1
    assert rows[0]["unit"] == "percent"
    # Fed decision: announced 14:00 ET on the middle session.
    rows = arc.macro_visible_at(datetime(2019, 1, 4, 21, 0, tzinfo=UTC))
    series = {r["series_id"] for r in rows}
    assert "FED_TARGET_UPPER_BPS" in series
    assert "FED_MEETING_SCHEDULE" in series  # schedule, not outcome
    arc.close()


def test_settlement_lookup(dataset):
    root, _, _ = dataset
    arc = Archive(root)
    s = arc.settlement_for(date(2019, 2, 15))
    assert s is not None and s["value_index_points"] > 0
    arc.close()


def test_coverage_report(dataset):
    root, _, cal = dataset
    rep = coverage_report(root, cal, date(2019, 1, 2), date(2019, 1, 4))
    assert rep.missing_sessions == ()  # holiday correctly not expected
    codes = {f.code for f in rep.findings}
    assert "UNKNOWN_EVENT_AGE" in codes  # snapshot-only provenance labeled
    # A calendar that *expects* the holiday as a session detects the gap.
    cal2 = build_weekday_manifest("expect-all", date(2019, 1, 2), date(2019, 1, 4))
    rep2 = coverage_report(root, cal2, date(2019, 1, 2), date(2019, 1, 4))
    assert rep2.missing_sessions == (date(2019, 1, 3),)


def test_manifest_content_id_stable(dataset):
    _, manifest, _ = dataset
    assert manifest.content_id() == manifest.content_id()
    other = DataManifest(**{**manifest.__dict__, "provider": "other"})
    assert other.content_id() != manifest.content_id()


def test_manifest_id_is_content_addressed(dataset, tmp_path):
    """Same dataset name + different seed must produce a different manifest id
    (dataset_manifest_id is the experiment-registry dedupe key)."""
    _, manifest, cal = dataset
    spec = SyntheticSpec(
        dataset_id="t1",  # same name, different seed → different identity
        seed=43,
        start=date(2019, 1, 2),
        end=date(2019, 1, 4),
        expiries=(date(2019, 2, 15), date(2019, 2, 22)),
        strikes_each_side=4,
    )
    m2 = generate(tmp_path, spec, cal)
    assert m2.manifest_id != manifest.manifest_id
    # Same seed again → identical content → identical manifest id.
    m3 = generate(tmp_path / "again", spec, cal)
    assert m3.manifest_id == m2.manifest_id
