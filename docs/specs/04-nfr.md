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
| Bronze Parquet | 90 days | Maintenance DAG deletes `ingest_date` partitions **and** compacts the file-sink metadata log accordingly |
| Silver Parquet | 400 days | Maintenance DAG |
| Gold / `package_status` | Indefinite (small) | — |
| Spark checkpoints | Spark-managed (`minBatchesToRetain`) | — |

## Metrics

Prefix `ptp_`. Labels: `query`, plus `error_code` / `reason` where relevant. No high-cardinality labels such as package or event IDs.

| Metric | Type |
|---|---|
| `ptp_input_rows_total`, `ptp_processed_rows_total` | counter |
| `ptp_events_valid_total`, `ptp_events_rejected_total{error_code}` | counter |
| `ptp_events_duplicate_total{stage=stream\|sink}`, `ptp_events_semantic_duplicate_total` | counter |
| `ptp_events_late_total`, `ptp_status_anomalies_total{code}` | counter |
| `ptp_batch_duration_seconds` | histogram |
| `ptp_offsets_behind_latest` | gauge (from `StreamingQueryProgress` source metrics) |
| `ptp_state_rows` | gauge |
| `ptp_sink_retries_total{sink}`, `ptp_sink_failures_total{sink}` | counter |
| `ptp_last_progress_timestamp_seconds` | gauge (stall detection) |

Note: Spark does not commit consumer-group offsets to Kafka, so Kafka-side lag exporters
cannot see this consumer. Lag comes from Spark's own progress metrics.
