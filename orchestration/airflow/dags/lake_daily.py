"""lake_daily: silver → gold → data quality, hourly (spec 03 §Batch and recovery workflows)."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.sdk import DAG
from ptp_common import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="lake_daily",
    description="Incremental silver build, 7-day gold window, DQ checks (fails on critical/high DQ failures)",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ptp", "lake"],
    doc_md=__doc__,
):
    silver = spark_job("silver", ["lake", "silver"])
    gold = spark_job("gold", ["lake", "gold", "--as-of", "{{ ds }}"])
    # A DQ failure is a finding about the data, not a transient error, so it is not retried.
    dq = spark_job("dq", ["lake", "dq", "--as-of", "{{ ds }}"], retries=0)
    silver >> gold >> dq
