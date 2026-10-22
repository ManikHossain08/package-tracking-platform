# ADR-0008: Airflow for batch and recovery orchestration

- **Status:** Accepted (2026-10-21, decision D7); this record written 2026-10-22
- **Requirements:** FR-REC-02, FR-ANL-01, NFR-08, spec 03 §Batch and recovery workflows

## Context

The batch side (silver, gold, data quality, replay, retention) needs scheduling, retries, parameters,
history and a UI. The streaming queries are long-running and are **not** orchestrated by the scheduler.

## Options

| Option | Pros | Cons |
|---|---|---|
| **Airflow 3.x** | Industry default; matches the stated stack; Cloud Composer / MWAA map directly to production | Heavier locally; task-centric rather than asset-centric |
| Dagster | Asset lineage fits a medallion lake; the owner already uses it | Not the stated stack; less common in job postings for this stack |
| Cron + make | Trivial | No retries, history, params, backfills or UI |

## Decision

Airflow 3.x (D7). DAGs only *launch* jobs. All logic stays in the versioned Scala/Python code, so the same job
runs from `make` and from Airflow. Locally Airflow runs as one container (`airflow standalone`, LocalExecutor,
Postgres metadata in a separate database) to fit the memory budget (D8). Production would run the components
separately or use Cloud Composer / MWAA.

## Consequences

- How a DAG starts a Spark job is decided in ADR-0009.
- Stateful logic never lives in DAG files, which keeps DAGs thin and testable with import checks.
