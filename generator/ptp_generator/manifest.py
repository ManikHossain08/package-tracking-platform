"""Ground-truth manifest (spec 02 §6, SC-0002 part B). Tests compare pipeline output to these files."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .oracle import expected_status_event
from .plan import Plan
from .render import format_time


def package_manifest(plan: Plan) -> list[dict[str, Any]]:
    faults: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    malformed: Counter[str] = Counter()
    for r in plan.records:
        if r.fault == "malformed":
            malformed[r.key] += 1
        elif r.fault:
            faults[r.key][r.fault].append(r.event.event_id)

    rows = []
    for package_id, events in sorted(plan.events_by_package().items()):
        winner = expected_status_event(events)
        f = faults[package_id]
        rows.append(
            {
                "package_id": package_id,
                "expected_final_status": winner.event_type,
                "expected_status_event_id": winner.event_id,
                "expected_status_event_time": format_time(winner.event_time),
                "expected_event_count": len(events),
                "unique_event_ids": sorted(e.event_id for e in events),
                "injected": {
                    "duplicates": len(f["duplicate"]),
                    "semantic_duplicates": len(f["semantic_duplicate"]),
                    "out_of_order": len(f["out_of_order"]),
                    "late": len(f["late"]),
                    "malformed": malformed[package_id],
                    "duplicate_event_ids": sorted(set(f["duplicate"])),
                    "semantic_duplicate_event_ids": sorted(f["semantic_duplicate"]),
                    "out_of_order_event_ids": sorted(f["out_of_order"]),
                    "late_event_ids": sorted(f["late"]),
                },
            }
        )
    return rows


def malformed_rows(plan: Plan) -> list[dict[str, Any]]:
    return [
        {
            "index": r.index,
            "key": r.key,
            "fault_type": r.malformed_type,
            "expected_error_code": r.expected_error_code,
        }
        for r in plan.records
        if r.fault == "malformed"
    ]


def summary(plan: Plan, manifest: list[dict[str, Any]]) -> dict[str, Any]:
    s = plan.scenario
    faults = Counter(r.fault for r in plan.records)
    return {
        "scenario": s.name,
        "seed": s.seed,
        "window_start": format_time(s.window_start),
        "window_end": format_time(s.window_end),
        "packages": len(manifest),
        "records_total": len(plan.records),
        "valid_records": sum(1 for r in plan.records if r.is_valid),
        "unique_valid_events": sum(m["expected_event_count"] for m in manifest),
        "duplicate_copies": faults["duplicate"],
        "semantic_duplicates": faults["semantic_duplicate"],
        "out_of_order": faults["out_of_order"],
        "late": faults["late"],
        "malformed_total": faults["malformed"],
        "malformed_by_error_code": dict(
            sorted(Counter(r.expected_error_code for r in plan.records if r.fault == "malformed").items())
        ),
        "expected_status_counts": dict(sorted(Counter(m["expected_final_status"] for m in manifest).items())),
    }


def write_manifest(plan: Plan, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = package_manifest(plan)
    with (out_dir / "manifest.jsonl").open("w") as f:
        f.writelines(json.dumps(row) + "\n" for row in manifest)
    with (out_dir / "malformed.jsonl").open("w") as f:
        f.writelines(json.dumps(row) + "\n" for row in malformed_rows(plan))
    result = summary(plan, manifest)
    (out_dir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result
