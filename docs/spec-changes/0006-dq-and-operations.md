# SC-0006: Data-quality job semantics and operational details

- **Status:** Approved (2026-10-22)
- **Affects:** spec 05 (DQ-03, DQ-05, DQ-07, DQ-10 wording), spec 02 §4 (`tracking.status_anomalies`, `ops.dq_results`),
  spec 06 (roles), spec 04 NFR-08 (retention mechanics), spec 07 (AC-7.x)

Implementing the DQ job needs exact definitions for rules that spec 05 states loosely.

## A. Check definitions

| Check | Spec 05 wording | Proposed exact definition | Severity |
|---|---|---|---|
| DQ-02 | `event_id` unique in silver and in `processed_events` | Silver: no `event_id` appears twice anywhere in the window. `processed_events`: guaranteed by its PK, so the check reports 0 by construction and only silver is evaluated. | critical |
| DQ-03 | freshness "while the generator runs" | **Lag-aware:** fail if Kafka end offsets are ahead of the offsets last committed in `ops.sink_batches` (data is waiting) **and** the last commit is older than 15 min. An idle topic is healthy. | high (fail) |
| DQ-04 | `CREATED` within 24 h of a package's first event | Packages whose first event is more than 24 h old and that have no `CREATED`. Each becomes an `ORPHAN_EVENT` anomaly row. | warn |
| DQ-05 | rate of `POST_TERMINAL_EVENT` | Events whose ordering key is greater than the key of the package's status-defining terminal event (written as anomaly rows). Warn if the rate is > 1 % of the window's events. | warn |
| DQ-06 | count reconciliation for a closed ingest window | For ingest dates older than today: bronze rows = valid + invalid; valid bronze event IDs (distinct) = silver rows for those IDs; DLQ distinct `(partition, offset)` = bronze invalid rows (DLQ read from Kafka in batch) | critical |
| DQ-07 | status recomputed from silver, packages with no events in the open window | Packages whose `last_event_time` is older than the watermark (2 h): `package_status` (status, status event, event count) must equal `StatusRule` applied to their silver events | critical |
| DQ-08 | Σ `delivered_count` = distinct DELIVERED packages | Σ `delivered_count` = rows in `delivery_facts` = packages in silver whose status-defining event is `DELIVERED` and that have `CREATED` (SC-0005 C1/C2), for the gold window | critical |
| DQ-09 | dwell ∈ [0, 14 d]; delay ∈ [−30 d, +60 d] | Counted over the gold window's facts; any value outside → warn | warn |
| DQ-10 | hourly volume within ±50 % of the trailing 7-day same-hour median | `skipped` unless 7 full days of silver history exist; synthetic demo data normally skips it | warn |

Exit code: non-zero if any **critical or high** check fails. Warnings never fail the DAG.

## B. Tables and roles (migration V6)

- `tracking.status_anomalies(event_id, anomaly_code, package_id, detected_at, run_id)` with PK `(event_id, anomaly_code)`.
  The DQ run replaces its window's anomalies, so re-runs are idempotent.
- `ops.dq_results(run_id, check_id, window_from, window_to, status, severity, observed, threshold, sample, checked_at)`.
- `ptp_batch_writer` gets `SELECT` on `tracking`, as spec 06 already states, and write access to
  `status_anomalies` and `dq_results`, plus `EXECUTE` on a new SECURITY DEFINER function
  `tracking.drop_processed_events_partitions_before(date)` for retention.
- Airflow gets its own database `airflow` and role `ptp_airflow`, with no access to the application schemas.

## C. Retention mechanics (NFR-08)

- `processed_events`: the maintenance DAG calls the SECURITY DEFINER function, which drops daily partitions older than 30 days.
- Bronze: deleting `ingest_date` directories under a Spark file sink would leave the sink's `_spark_metadata` log
  pointing at missing files. Spark's file-sink **`retention`** option (90 days) drops expired entries from the log
  when it compacts, and readers use `spark.sql.files.ignoreMissingFiles=true` for the short gap before compaction.
  This exact mechanism is verified by AC-7.11 before it's relied on. If it doesn't hold, I'll come back with an alternative.
- Silver (400 days) is ordinary directories without a log: delete expired `event_date` partitions.

## D. Deferred

Prometheus metrics for DQ results (FR-OBS-01) come with the observability step (delivered by SC-0007 B). Results are queryable in `ops.dq_results` until then.

## Decision

Approved by the project owner on 2026-10-22. Recorded in spec 05, spec 02 §4, spec 06 and spec 04 (NFR-08).
Step 7 is split: 7a (DQ job, anomalies, jobs image) and 7b (Airflow, DAGs, retention).
