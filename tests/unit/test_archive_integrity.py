"""Archive identity and provenance are checked before querying model-visible rows."""

import json
import shutil
from dataclasses import asdict
from datetime import timedelta

import polars as pl
import pytest

from spx_research.data.availability import Archive, AvailabilityError, validate_archive
from spx_research.data.manifests import file_record
from spx_research.data.qa import coverage_report
from spx_research.data.synthetic import SyntheticSpec, generate
from tests.engine_support import EXPIRY, START, tiny_calendar


@pytest.fixture
def archive_root(tmp_path):
    generate(
        tmp_path,
        SyntheticSpec("tiny", 42, START, START, expiries=(EXPIRY,), strikes_each_side=1),
        tiny_calendar(),
    )
    return tmp_path / "tiny"


def test_manifest_hash_failure_blocks_archive_open(archive_root):
    path = next((archive_root / "quotes").glob("session=*.parquet"))
    path.write_bytes(path.read_bytes() + b"mutated")
    with pytest.raises(AvailabilityError, match="MANIFEST_CHECKSUM_MISMATCH"):
        Archive(archive_root)


def test_unlisted_partition_is_not_queryable(archive_root):
    source = next((archive_root / "quotes").glob("session=*.parquet"))
    shutil.copyfile(source, archive_root / "quotes" / "session=2024-01-03.parquet")
    with pytest.raises(AvailabilityError, match="UNMANIFESTED_OR_MISSING_DATA"):
        Archive(archive_root)


def test_synthetic_label_requires_synthetic_rows_even_with_matching_checksums(archive_root):
    quote_path = next((archive_root / "quotes").glob("session=*.parquet"))
    rows = pl.read_parquet(quote_path)
    rows.with_columns(pl.lit(["PROVIDER"]).alias("quality_flags")).write_parquet(quote_path)
    manifest_path = archive_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    replacement = asdict(file_record(archive_root, quote_path, rows.height))
    manifest["normalized_files"] = [
        replacement if r["path"] == replacement["path"] else r for r in manifest["normalized_files"]
    ]
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(AvailabilityError, match="SYNTHETIC_PROVENANCE_MISMATCH"):
        validate_archive(archive_root, expected_kind="synthetic")


def test_wrong_dataset_kind_blocks_before_read(archive_root):
    with pytest.raises(AvailabilityError, match="DATASET_KIND_MISMATCH"):
        validate_archive(archive_root, expected_kind="provider")


def test_empty_archive_coverage_reports_missing_sessions(tmp_path):
    (tmp_path / "quotes").mkdir()
    report = coverage_report(tmp_path, tiny_calendar(), START, START + timedelta(days=1))
    assert report.missing_sessions == (START,)
    assert report.findings[0].code == "MISSING_SESSION"
