"""Operational dashboard (M5): run status, event counts, incidents, costs.

Reads a run output directory (events.jsonl + report.json + run_manifest.json)
produced by `spx-research run`. Launch with:

    streamlit run src/spx_research/dashboard/app.py -- --run-dir out/run-0001

Private-only view: shows event types, cash/reserves, incident counts and
budget usage — never model prompts, packets, or raw proposals.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_run(run_dir: Path) -> dict[str, Any]:
    events_path = run_dir / "events.jsonl"
    events = (
        [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
        if events_path.exists()
        else []
    )
    report = {}
    rp = run_dir / "report.json"
    if rp.exists():
        report = json.loads(rp.read_text())
    manifest = {}
    mp = run_dir / "run_manifest.json"
    if mp.exists():
        manifest = json.loads(mp.read_text())
    return {"events": events, "report": report, "manifest": manifest}


def summarize_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(e["type"] for e in events)
    last = events[-1] if events else {}
    return {
        "total_events": len(events),
        "by_type": dict(counts),
        "last_seq": last.get("seq"),
        "last_sim_time_utc": last.get("sim_time_utc"),
        "run_id": last.get("run_id"),
    }


def main() -> None:  # pragma: no cover - streamlit entry point
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    args, _ = parser.parse_known_args()
    try:
        import streamlit as st
    except ImportError:  # streamlit is optional at runtime
        print(json.dumps(summarize_events(load_run(Path(args.run_dir))["events"]), indent=2))
        return

    st.set_page_config(page_title="SPX Research Run")
    run = load_run(Path(args.run_dir))
    summary = summarize_events(run["events"])
    st.title(f"Run {summary['run_id'] or args.run_dir}")
    c1, c2, c3 = st.columns(3)
    c1.metric("events", summary["total_events"])
    c2.metric("last seq", summary["last_seq"])
    c3.metric("cash", run["report"].get("final_cash", "n/a"))
    st.subheader("Event types")
    st.bar_chart(summary["by_type"])
    st.subheader("Report")
    st.json(run["report"])


if __name__ == "__main__":
    main()
