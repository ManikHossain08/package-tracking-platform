"""Command line: python -m ptp_generator --scenario bench/scenarios/smoke.yaml --sink kafka."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .config import load_scenario
from .emit import emit
from .manifest import write_manifest
from .plan import build_plan
from .sinks import FileSink, KafkaSink

_STANDARD_ATTRS = set(vars(logging.makeLogRecord({})))


class JsonFormatter(logging.Formatter):
    """Structured logs with stable keys (FR-OBS-02)."""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "component": "generator",
            "run_id": self.run_id,
            "message": record.getMessage(),
        }
        entry.update({k: v for k, v in vars(record).items() if k not in _STANDARD_ATTRS})
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ptp_generator", description=__doc__)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--sink", choices=["kafka", "file"], default="file")
    parser.add_argument("--out-dir", type=Path, required=True, help="manifest and file-sink output")
    parser.add_argument(
        "--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    )
    parser.add_argument("--topic", default=os.environ.get("EVENTS_TOPIC", "package-events.v1"))
    parser.add_argument("--seed", type=int, help="override the scenario seed")
    parser.add_argument("--packages", type=int, help="override the scenario package count")
    parser.add_argument("--rate-eps", type=float, help="override the scenario rate (0 = unthrottled)")
    args = parser.parse_args(argv)

    run_id = str(uuid.uuid4())
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter(run_id))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    log = logging.getLogger("ptp_generator")

    scenario = load_scenario(
        args.scenario, {"seed": args.seed, "packages": args.packages, "rate_eps": args.rate_eps}
    )
    plan = build_plan(scenario)
    summary = write_manifest(plan, args.out_dir)
    log.info(
        "plan built",
        extra={
            "scenario": scenario.name,
            "seed": scenario.seed,
            "records_total": summary["records_total"],
            "out_dir": str(args.out_dir),
        },
    )

    if args.sink == "kafka":
        sink = KafkaSink(args.bootstrap_servers, args.topic, delivery_path=args.out_dir / "delivery.jsonl")
    else:
        sink = FileSink(args.out_dir / "records.jsonl")
    stats = emit(plan.records, sink, scenario.rate_eps, scenario.outage)

    run = {
        "run_id": run_id,
        "sink": args.sink,
        "topic": args.topic if args.sink == "kafka" else None,
        "records": stats.records,
        "duration_seconds": round(stats.duration_seconds, 3),
        "achieved_eps": round(stats.achieved_eps, 1),
    }
    (args.out_dir / "run.json").write_text(json.dumps(run, indent=2) + "\n")
    log.info("emission complete", extra=run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
