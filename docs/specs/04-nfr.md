# 04 — Non-functional requirements

| ID | Requirement | Verification |
|---|---|---|
| NFR-01 | **Idempotency:** applying any multiset of valid events, in any order, any number of times, yields the same `package_status` (excluding processing-time columns). | Property-based test (TS-U-05), replay test (TS-R-04) |
| NFR-02 | **Explicit semantics:** each sink's guarantee is documented (ADR-0002) and each has a test demonstrating it. | TS-R-01..05 |
| NFR-03 | **RPO / RTO** — see below. | TS-R-01, TS-R-06, benchmark |
| NFR-04 | **Schema evolution** follows the rules in spec 02 §2. v1 consumers tolerate unknown fields. | Contract test TS-C-03 |
| NFR-05 | **Bounded memory:** `maxOffsetsPerTrigger` (default 50 000), watermark-bounded state, no unbounded `collect()` on the driver. State rows are exposed as a metric. | Soak test: state rows and heap plateau under constant rate (TS-P-02) |
| NFR-06 | **Secure configuration:** no secrets in git; `.env.example` lists every variable; least-privilege DB roles. | gitleaks in CI; role test |
| NFR-07 | **Reproducible builds:** exact versions for sbt deps, Docker images (tag **and** digest), Python (locked requirements with hashes). | CI from a clean cache |
| NFR-08 | **Retention and duplicate cleanup** — see below. | Maintenance DAG test |
| NFR-09 | **Observability:** structured logs, metrics per §Metrics, actionable errors. | Log/metric contract tests |
| NFR-10 | **No invented performance claims:** targets are set only after a documented baseline (spec 09). | Review gate |
| NFR-11 | **Configurable batch sizes:** trigger interval, `maxOffsetsPerTrigger`, JDBC batch size, shuffle partitions. | Config test |

## NFR-03 — Recovery objectives

| Failure | RPO | RTO (target, to be measured) | Basis |
|---|---|---|---|
| Processor crash or restart | 0 events, if the outage < Kafka retention (7 d) | Resume < 2 min; catch-up time measured | Checkpoint + Kafka replay |
| Postgres outage | 0 | Postgres recovery time + catch-up | The query fails without committing offsets; resumes after restart |
| Postgres data loss | 0 for events in bronze | Rebuild time measured by benchmark | `replay_from_bronze` |
| Checkpoint lost or corrupt | 0 | Runbook (FR-REC-04) | Rebuild state from bronze, restart from explicit offsets |
| Kafka data loss (single broker locally) | Events not yet in bronze are lost | — | Accepted for local. Production needs replication factor 3 and `min.insync.replicas=2`. |

## NFR-08 — Retention

| Data | Retention (local default) | Mechanism |
|---|---|---|
| `package-events.v1` | 7 days | Kafka `retention.ms` |
| `package-events.dlq.v1` | 14 days | Kafka `retention.ms` (contains raw payloads, so it is limited) |
| `tracking.processed_events` | 30 days (must be > Kafka retention) | Drop daily partitions |
| Bronze Parquet | 90 days | Maintenance DAG deletes `ingest_date` partitions; the file sink's `retention` option drops their log entries at compaction, and `BronzeRow.read` (explicit schema + `ignoreMissingFiles`) tolerates the gap (SC-0006 C, verified by TS-O-03) |
| Silver Parquet | 400 days | Maintenance DAG |
| Gold / `package_status` | Indefinite (small) | — |
| Spark checkpoints | Spark-managed (`minBatchesToRetain`) | — |

## Metrics

Prefix `ptp_`. Labels: `query`, plus `error_code` / `reason` where relevant. No high-cardinality labels such as package or event IDs.

| Metric | Type | Status (SC-0007 A) |
|---|---|---|
| `ptp_input_rows_total{query}` | counter | ✅ step 8 |
| `ptp_events_valid_total`, `ptp_events_rejected_total{error_code}` | counter | ✅ step 8 |
| `ptp_events_applied_total` (was `ptp_processed_rows_total`) | counter | ✅ step 8 (SC-0007) |
| `ptp_events_duplicate_total{stage}` | counter | ✅ `stage="sink"`; `stage="stream"` with FR-DED-02 |
| `ptp_batch_duration_seconds{query}` | histogram | ✅ step 8 |
| `ptp_offsets_behind_latest{query}` | gauge (Kafka source progress, `maxOffsetsBehindLatest`) | ✅ step 8 |
| `ptp_last_progress_timestamp_seconds{query}` | gauge (stall detection) | ✅ step 8 |
| `ptp_sink_failures_total{sink}` | counter | ✅ step 8 (known stages pre-initialised at 0) |
| `ptp_trigger_active_seconds{query}`, `ptp_heartbeat_timestamp_seconds` | gauges (driver heartbeat, 10 s) | ✅ step 8: stall/hang detection (Spark posts no progress while idle) |
| `ptp_batch_last_success_timestamp_seconds`, `ptp_batch_duration_seconds`, `ptp_batch_success` (Pushgateway, `job`/`task`) | gauges | ✅ step 8 (SC-0007 B) |
| `ptp_dq_check_failed{check_id,severity}`, `ptp_status_anomalies{code}` (Pushgateway) | gauges | ✅ step 8 |
| `ptp_events_late_total` | counter | ⏳ with FR-LATE-01 |
| `ptp_events_semantic_duplicate_total` | counter | ⏳ with FR-DED-03 (stream) |
| `ptp_state_rows` | gauge | ⏳ with FR-DED-02 |
| `ptp_sink_retries_total{sink}` | counter | ⏳ with FR-REC-03 |

Note: Spark does not commit consumer-group offsets to Kafka, so Kafka-side lag exporters
cannot see this consumer. Lag comes from Spark's own progress metrics.
