"""maintenance: retention (NFR-08) for processed_events, bronze and silver, daily."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.sdk import DAG
from ptp_common import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="maintenance",
    description="Drop expired processed_events partitions; delete expired bronze/silver partitions",
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ptp", "maintenance"],
    doc_md=__doc__,
):
    spark_job("retention", ["lake", "maintenance", "--as-of", "{{ ds }}"])
