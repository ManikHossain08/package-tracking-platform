"""replay_from_bronze: re-apply an ingest-date range of bronze to Postgres (FR-REC-02), manual trigger only.

Parameters are validated before any Spark job starts (AC-7.10).
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from airflow.sdk import DAG, Param, task
from ptp_common import DEFAULT_ARGS, spark_job

MAX_RANGE_DAYS = 92  # bronze retention (90 d) plus slack

with DAG(
    dag_id="replay_from_bronze",
    description="Idempotent replay of bronze into tracking.* (as-ingested or backfill mode)",
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ptp", "recovery"],
    doc_md=__doc__,
    params={
        "ingest_date_from": Param(type="string", format="date", description="first ingest date (UTC)"),
        "ingest_date_to": Param(
            type="string", format="date", description="last ingest date (UTC), inclusive"
        ),
        "mode": Param("as-ingested", enum=["as-ingested", "backfill"]),
        "dry_run": Param(False, type="boolean"),
    },
):

    @task(retries=0)
    def validate(params: dict | None = None) -> None:
        """Rejects bad ranges before a Spark container is launched."""
        p = params or {}
        start, end = date.fromisoformat(p["ingest_date_from"]), date.fromisoformat(p["ingest_date_to"])
        today = datetime.now(UTC).date()
        if end < start:
            raise ValueError(f"ingest_date_to {end} is before ingest_date_from {start}")
        if end > today:
            raise ValueError(f"ingest_date_to {end} is in the future")
        if (end - start).days + 1 > MAX_RANGE_DAYS:
            raise ValueError(f"range of {(end - start).days + 1} days exceeds {MAX_RANGE_DAYS}; split it")
        if p["mode"] not in ("as-ingested", "backfill"):
            raise ValueError(f"unknown mode {p['mode']}")

    replay = spark_job(
        "replay",
        [
            "replay",
            "--from",
            "{{ params.ingest_date_from }}",
            "--to",
            "{{ params.ingest_date_to }}",
            "--mode",
            "{{ params.mode }}",
            "--dry-run",
            "{{ params.dry_run | lower }}",
        ],
        writer="stream",
        retries=0,
    )
    validate() >> replay
