"""Run from outside the checkout after installing the wheel into a fresh env."""

import json
import tempfile
from datetime import date, time, timedelta
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from typer.testing import CliRunner

from spx_research.cli.execution import execute
from spx_research.cli.main import app
from spx_research.config import Profile, load_profile
from spx_research.contracts import (
    bundle_manifest,
    example_config_path,
    load_example,
    load_prompt,
    load_schema,
    spec_dir,
)
from spx_research.data.synthetic import SyntheticSpec, generate
from spx_research.research.artifacts import code_identity
from spx_research.research.leakage import evaluate_run
from spx_research.temporal.calendar import CalendarManifest, SessionDay

assert bundle_manifest()["bundle_version"] == "2.1"
assert spec_dir().name == "contracts"
assert load_example("manager_packet")["schema_version"] == "2.1"
assert load_schema("manager_decision")["properties"]["schema_version"]["enum"] == ["2.1"]
assert load_prompt("manager")
assert code_identity()["dependency_lock_sha256"]
config = Config()
config.set_main_option("script_location", str(spec_dir().parent / "migrations"))
assert ScriptDirectory.from_config(config).get_current_head() == "0006_runtime_constraints"
assert CliRunner().invoke(app, ["--help"]).exit_code == 0
with tempfile.TemporaryDirectory(prefix="spx-wheel-") as temporary:
    root = Path(temporary)
    day = date(2024, 1, 2)
    calendar = CalendarManifest("wheel-check", "1", (SessionDay(day, time(9, 30), time(10), True),))
    generate(
        root,
        SyntheticSpec(
            dataset_id="data", seed=7, start=day, end=day, expiries=(day + timedelta(days=45),)
        ),
        calendar,
    )
    (root / "data" / "calendar.json").write_text(
        json.dumps(
            {
                "calendar_id": calendar.calendar_id,
                "version": calendar.version,
                "sessions": [
                    {"date": day.isoformat(), "open": "09:30", "close": "10:00", "half_day": True}
                ],
            }
        )
    )
    research = load_profile(example_config_path("research")).model_dump(mode="json")
    synthetic = load_profile(example_config_path("synthetic")).model_dump(
        mode="json", exclude_none=True
    )
    raw = {**research, **synthetic}
    raw["study"] = {
        "start_date": str(day),
        "scored_end_date": str(day),
        "runoff_end_date": str(day),
    }
    raw["execution"] = {"opening_fee_per_leg_usd": "1", "closing_fee_per_leg_usd": "1"}
    result = execute(
        profile=Profile.model_validate(raw),
        dataset_root=root / "data",
        out=root / "run",
        start=day,
        end=day,
        run_id="wheel-check",
        store_kind="memory",
        policy="mechanical",
        policy_options={},
    )
    assert result.status == "COMPLETED"
    assert evaluate_run(root / "run")["success"] is True
print(
    json.dumps(
        {
            "installed_bundle": str(spec_dir()),
            "migration_head": "0006_runtime_constraints",
            "status": "PASS",
        }
    )
)
