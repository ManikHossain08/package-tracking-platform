"""Compare the live stack's Postgres status with a generator manifest (KPI-P1, KPI-P2).

Usage: python3 scripts/reconcile.py data/generator/<scenario> [--wait-seconds 180]
Waits (bounded polling on ops.sink_batches) until the processor has consumed the run, then compares.
Standard library only; queries Postgres through `docker compose exec psql`.
"""

import argparse
import csv
import io
import json
import subprocess
import sys
import time
from pathlib import Path

COMPOSE = [
    "docker",
    "compose",
    "--project-directory",
    "infra",
    "-f",
    "infra/docker-compose.yml",
    "--env-file",
    ".env",
]


def query(sql: str) -> list[dict[str, str]]:
    cmd = [
        *COMPOSE,
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --csv -c "$1"',
        "_",
        sql,
    ]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return list(csv.DictReader(io.StringIO(out)))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--wait-seconds", type=float, default=180)
    args = parser.parse_args()

    summary = json.loads((args.run_dir / "summary.json").read_text())
    manifest = [json.loads(line) for line in (args.run_dir / "manifest.jsonl").read_text().splitlines()]
    expected_pkgs = {m["package_id"]: m for m in manifest}

    # Bounded polling on an observable condition: all of this run's packages are present.
    ids = ",".join(f"'{p}'" for p in expected_pkgs)
    deadline = time.monotonic() + args.wait_seconds
    while True:
        rows = query(
            "SELECT package_id, current_status, status_event_id, event_count FROM tracking.package_status "
            f"WHERE package_id IN ({ids})"
        )
        done = (
            len(rows) == len(expected_pkgs)
            and sum(int(r["event_count"]) for r in rows) == summary["unique_valid_events"]
        )
        if done or time.monotonic() > deadline:
            break
        time.sleep(2)  # polling interval of a bounded wait, not a synchronisation guess

    actual = {r["package_id"]: r for r in rows}
    mismatches = [
        f"{p}: expected {m['expected_final_status']}/{m['expected_status_event_id']}/"
        f"{m['expected_event_count']}, got {actual.get(p)}"
        for p, m in expected_pkgs.items()
        if p not in actual
        or actual[p]["current_status"] != m["expected_final_status"]
        or actual[p]["status_event_id"] != m["expected_status_event_id"]
        or int(actual[p]["event_count"]) != m["expected_event_count"]
    ]
    print(
        json.dumps(
            {
                "scenario": summary["scenario"],
                "packages_expected": len(expected_pkgs),
                "packages_found": len(actual),
                "status_mismatches": len(mismatches),
                "unique_events_expected": summary["unique_valid_events"],
                "unique_events_applied": sum(int(r["event_count"]) for r in rows),
                "malformed_expected": summary["malformed_total"],
            },
            indent=2,
        )
    )
    for m in mismatches[:10]:
        print("MISMATCH", m)
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
