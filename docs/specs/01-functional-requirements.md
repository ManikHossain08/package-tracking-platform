# 01 — Functional requirements

Priority: **P0** first vertical slice · **P1** MVP-complete · **P2** stretch.
The letters A–M refer to the original project brief.

## Event lifecycle (domain model)

| Event type | Lifecycle rank | Terminal | Required context |
|---|---|---|---|
| `CREATED` | 10 | no | `shipment` object |
| `PICKED_UP` | 20 | no | `vehicle_id` or `facility_id` |
| `ARRIVED_AT_FACILITY` | 30 | no | `facility_id` |
| `DEPARTED_FACILITY` | 40 | no | `facility_id` |
| `OUT_FOR_DELIVERY` | 50 | no | `vehicle_id` |
| `DELIVERY_ATTEMPTED` | 60 | no | `vehicle_id`, `details.attempt_number` |
| `EXCEPTION` | 70 | no | `details.exception_code` |
| `DELIVERED` | 90 | **yes** | `vehicle_id` |
| `RETURNED_TO_SENDER` | 90 | **yes** | `facility_id` |

Packages loop through facilities (ARRIVED → DEPARTED repeats per hub), so the rank is
**only** a tiebreaker for identical event times. It does not define allowed transitions.

### Status ordering rule (ADR-0004, amended by SC-0002)

- **Ordering key** = `(event_time_utc, lifecycle_rank, event_id)`, compared
  lexicographically. It is total and deterministic.
- The current status is the **most preferred** event seen so far, under this total
  preference order (most preferred first):
  1. any terminal event (`DELIVERED`, `RETURNED_TO_SENDER`) beats any non-terminal event;
  2. between two terminal events, the **smaller** ordering key wins (the first outcome is sticky);
  3. between two non-terminal events, the **larger** ordering key wins.

  An incoming event replaces the stored status iff it is preferred over it. Because the
  result is the maximum under a fixed total order, it does not depend on arrival order,
  duplicates or batch boundaries (NFR-01).
- An event that is not preferred is still recorded (`processed_events`, Parquet)
  and increments `event_count`, but it does not change the status.
- Any event whose ordering key is greater than the winning terminal event's key is recorded
  as anomaly `POST_TERMINAL_EVENT` (see DQ-05), regardless of arrival order.

  *Superseded rule (before SC-0002, 2026-10-21):* "an event becomes the current status iff its
  key > the stored key and the stored status is not terminal". It depended on arrival order.
- `CREATED` shipment attributes (`promised_delivery_date`, `service_level`, destination)
  are merged into the status row **whenever** `CREATED` arrives, even if it arrives last.

## Requirements

### A — Synthetic data generation

| ID | Pri | Requirement | Acceptance (summary) |
|---|---|---|---|
| FR-GEN-01 | P0 | The generator produces realistic package lifecycles: 1–4 hub hops, optional attempts and exceptions, terminal event. Seed, package count, events/sec, duration, and DC/vehicle/destination-region (`postal_prefixes`) pool sizes are configurable. | Same seed + config ⇒ byte-identical event stream (excluding `emitted_at`). |
| FR-GEN-02 | P0 | Fault injection with configurable rates: duplicate %, out-of-order %, late % (with a delay distribution), malformed % (by malformation type), and burst replay after a simulated outage. | Each injected fault is recorded in a ground-truth manifest that tests can read. |
| FR-GEN-03 | P1 | The generator can emit a ground-truth expected final state per package, used by reconciliation tests. | Manifest matches the spec 02 format. |

### B — Ingestion

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-ING-01 | P0 | Publish JSON events conforming to `contracts/package-event.v1.schema.json` to topic `package-events.v1`, keyed by `package_id`, with an idempotent producer (`acks=all`). | Contract test: 100% of generated *valid* events pass schema validation. |
| FR-ING-02 | P1 | Topics are created by an explicit, versioned init script (partitions, retention, cleanup policy). Auto-creation is disabled. | The broker starts with `auto.create.topics.enable=false`; the init script is idempotent. |

### C/D — Validation and dead-lettering

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-VAL-01 | P0 | Validate each record: UTF-8, valid JSON object, size ≤ 64 KiB, required fields, ID formats, `event_type` enum, conditional fields, RFC 3339 timestamps with offset, supported `schema_version`. | Every invalid sample in `contracts/samples/invalid/` yields its expected `error_code`. |
| FR-VAL-02 | P0 | Runtime time-bound rules, judged against the record's **ingest time** (its Kafka `LogAppendTime` timestamp; SC-0004): reject `event_time > ingest_time + 5 min` (`EVENT_TIME_IN_FUTURE`). In live mode, reject `event_time < ingest_time − 30 days` (`EVENT_TIME_TOO_OLD`). Backfill mode (`ProcessingMode.Replay`) disables the lower bound. Because the clock is stored with the record, re-runs and replays reproduce the original decision. | Boundary tests at ±1 s around each bound. |
| FR-VAL-03 | P0 | Collect all violations per record, not just the first. The primary `error_code` is chosen by a documented precedence order. | A record with 3 violations produces 3 entries in `errors[]`. |
| FR-DLQ-01 | P0 | Invalid records go to `package-events.dlq.v1` conforming to `contracts/dlq-record.v1.schema.json`. Each record includes the raw payload, errors, source topic/partition/offset/timestamp/key, and processor name/version/run_id/batch_id. | Every rejected input offset is present in the DLQ; DLQ records validate against the schema. |
| FR-DLQ-02 | P1 | Rejected records never affect `package_status` or silver/gold outputs. They are kept in bronze with `is_valid=false`. | Integration test: status is unchanged after invalid-only input. |

### E/F/G/H — Stream processing, deduplication, lateness, status

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-STR-01 | P0 | A Scala Spark Structured Streaming job consumes `package-events.v1`. Trigger interval and `maxOffsetsPerTrigger` are configurable. The checkpoint location is configurable and per-query. | The job starts from config only; no code changes between environments. |
| FR-DED-01 | P0 | A given `event_id` is applied to `package_status` at most once, across micro-batches, restarts and replays within the `processed_events` retention window. | Duplicate injection ⇒ status and `event_count` equal a duplicate-free run. |
| FR-DED-02 | P1 | In-stream dedup with `dropDuplicatesWithinWatermark(event_id)` bounds per-batch work. The durable guarantee comes from FR-DED-01, not from Spark state. | State store row count stays bounded under a constant-rate test. |
| FR-DED-03 | P1 | Semantic duplicates (different `event_id`, same content hash) are **counted and reported**, not dropped. | Metric `events_semantic_duplicate_total` equals the injected count. |
| FR-ORD-01 | P0 | `package_status` follows the status ordering rule above. An older event never overwrites a newer state, and terminal states are sticky. | Property test: any permutation of an event set yields the same final status. |
| FR-LATE-01 | P1 | An event-time watermark of 2 h is applied for stateful operators. Events behind the watermark are still applied to status and written to bronze, and counted in `events_late_total`. | A 3 h-late event updates status if its key is newer and is counted as late. |

### I/J — Storage and analytics

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-STO-01 | P1 | Bronze: every consumed record (valid and invalid) is written to Parquet, partitioned by `ingest_date`, using the Spark file sink with its own checkpoint. | Bronze row count = consumed offsets for the range (read through Spark). |
| FR-STO-02 | P1 | Silver: valid events, deduplicated by `event_id`, typed, partitioned by `event_date`, compacted to a configurable target file size (default 128 MiB). Rebuilt idempotently per partition. | Silver `event_id` uniqueness (DQ-02); rebuilding twice gives identical output. |
| FR-ANL-01 | P1 | Gold fact tables (`delivery_facts`, `facility_visits`) and daily aggregates (`daily_delivery_performance`, `daily_facility_performance`, `daily_network_activity`) are computed from silver and loaded into Postgres. The whole 7-day recompute window is replaced in one transaction, reading silver back 30 days for earlier `CREATED` events (SC-0005 B, C8). | Re-running the same window yields identical rows (excluding `computed_at` and `run_id`). |
| FR-ANL-02 | P1 | Documented SQL query pack in `sql/analytics/` covering on-time %, delay percentiles, DC dwell, DC throughput and exception rate over any date range (queries read the fact tables, so percentiles are exact), with expected results on a fixed fixture. | Each query has a fixture-based test. |

### K — Replay and recovery

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-REC-01 | P0 | After a crash or restart, each query resumes from its checkpoint with no lost status updates and no double application. | Kill-and-restart test equals the uninterrupted baseline. |
| FR-REC-02 | P1 | Controlled replay of an `ingest_date` (or offset) range from bronze, re-applied through the **same** validation and apply code as streaming. Replay is idempotent. | Replaying a range twice leaves Postgres unchanged. |
| FR-REC-03 | P1 | When Postgres is unavailable, the batch is retried with bounded exponential backoff (configurable attempts and maximum delay). The query then fails with an actionable error and does not advance its checkpoint. | Failure-injection test: no data loss after Postgres comes back and the job restarts. |
| FR-REC-04 | P2 | A documented runbook covers checkpoint loss or corruption: rebuild from bronze, then resume from explicit offsets. | Runbook dry run in a test. |

### L — Observability

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-OBS-01 | P1 | Prometheus metrics: input rows/s, processed rows/s, batch duration, offsets behind latest, state rows, and counters for valid/rejected (by `error_code`)/duplicate/late/anomaly events and sink retries. | Metric names and labels match spec 04 §Metrics. |
| FR-OBS-02 | P0 | Structured JSON logs with stable keys (`run_id`, `query_name`, `batch_id`, `topic`, `partition`, `offset_range`, `error_code`). Errors include enough context to act on. | Log schema test on the sink-failure path. |
| FR-OBS-03 | P2 | Grafana dashboard and Prometheus alert rules: growing lag, DLQ rate spike, stalled query (no progress for N triggers), sink retry exhaustion. | Dashboards are provisioned from files. |

### M — Developer experience and CI

| ID | Pri | Requirement | Acceptance |
|---|---|---|---|
| FR-DEV-01 | P0 | One-command local stack (`make up`) with Compose profiles `core`, `obs`, `orchestration`, and `make test` / `make test-it`. | Works on a clean checkout with only Docker, a JDK and Python installed. |
| FR-DEV-02 | P0 | GitHub Actions run lint, unit tests, contract tests, integration tests, the traceability check and secret scanning on every PR. | CI is green on main. |
| FR-DEV-03 | P1 | `scripts/check_traceability.py` fails if any P0/P1 requirement has no tagged test, and regenerates `docs/traceability.md`. | CI step fails on an untraced requirement. |
