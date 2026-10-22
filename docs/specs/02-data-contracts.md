# 02 — Data contracts

The machine-readable contracts in `contracts/` are authoritative. This document explains
them and specifies the storage contracts (Postgres, Parquet) that do not have a schema file yet.

## 1. Kafka topics

| Topic | Key | Value | Partitions | Retention | Cleanup | Writer |
|---|---|---|---|---|---|---|
| `package-events.v1` | `package_id` (UTF-8) | package-event v1 JSON | 6 | 7 days | delete | generator / producers |
| `package-events.dlq.v1` | original key or null | dlq-record v1 JSON | 3 | 14 days | delete | status-processor |

Six partitions allow up to 6 parallel Spark tasks locally. The partition count is part of
the contract: changing it re-maps keys and breaks per-key ordering for in-flight data.

## 2. Package event v1 — `contracts/package-event.v1.schema.json`

| Field | Type | Req | Rules |
|---|---|---|---|
| `event_id` | string (UUID) | yes | Producer-assigned, **stable across producer retries**. UUIDv7 recommended. The idempotency key. |
| `schema_version` | string | yes | `^1\.\d+$`. A consumer for v1 accepts any `1.x`. |
| `package_id` | string | yes | `^PKG-[A-Z0-9]{12}$` |
| `event_type` | enum | yes | One of the 9 types in spec 01 |
| `event_time` | string (RFC 3339) | yes | **Offset required** (`Z` or `±hh:mm`); millisecond or microsecond precision. When the event happened in the physical world. |
| `emitted_at` | string (RFC 3339) | yes | When the producer sent it. Used for latency measurement and lateness analysis. |
| `source.system` | enum | yes | `DC_SCANNER`, `VEHICLE_APP`, `ORDER_SYSTEM` |
| `source.device_id` | string | yes | 1–64 chars |
| `source.sequence` | integer ≥ 0 | yes | Monotonic per device. Used for gap/duplicate diagnostics, **not** for ordering. |
| `facility_id` | string \| null | cond. | `^DC-[A-Z]{3}[0-9]{2}$`. Required for `ARRIVED_AT_FACILITY`, `DEPARTED_FACILITY`, `RETURNED_TO_SENDER`. |
| `vehicle_id` | string \| null | cond. | `^VH-[0-9]{6}$`. Required for `OUT_FOR_DELIVERY`, `DELIVERY_ATTEMPTED`, `DELIVERED`. |
| `shipment` | object | cond. | Required for `CREATED`: `promised_delivery_date` (date), `service_level` (`ECONOMY`/`STANDARD`/`EXPRESS`), `origin_facility_id`, `destination_postal_prefix` (`^[A-Z0-9]{3}$`), `destination_timezone` (IANA name). |
| `details` | object | cond. | `exception_code` required for `EXCEPTION`; `attempt_number` (1–5) required for `DELIVERY_ATTEMPTED`. |
| `attributes` | map<string,string> | no | Free-form, ≤ 20 entries. Not used by core logic. |

**PII:** the contract carries no names, street addresses or phone numbers. Location is
limited to facility IDs and a 3-character postal prefix. Producers sending extra PII fields
are a contract violation, and those fields are not propagated beyond bronze (see spec 06).

**Unknown fields** are allowed (`additionalProperties: true`) and preserved in bronze
`raw_value`. This keeps v1 consumers forward-compatible with additive v1.x changes.

### Schema evolution rules (NFR-04)

| Change | Allowed in v1.x? | Process |
|---|---|---|
| Add an optional field | ✅ | Bump minor (`1.1`). Consumers ignore unknown fields. |
| Add an enum value (e.g. a new `event_type`) | ⚠️ Treated as breaking for v1 consumers (they would reject it as `UNKNOWN_EVENT_TYPE`) | Deploy consumers that accept the new value **before** producers emit it. Document it in a spec change. |
| Make an optional field required | ❌ | New major version. |
| Remove or rename a field, or change a type or meaning | ❌ | New major version: topic `package-events.v2`, dual-publish during migration. |
| Change the topic partition count or key | ❌ | New topic. |

### Validation error codes

Precedence (the first one listed is the primary `error_code`):

1. `INVALID_ENCODING` — value is not UTF-8
2. `PAYLOAD_TOO_LARGE` — value > 64 KiB
3. `INVALID_JSON` — not parseable
4. `NOT_JSON_OBJECT` — parseable, but not an object
5. `UNSUPPORTED_SCHEMA_VERSION`
6. `MISSING_REQUIRED_FIELD`
7. `INVALID_FIELD_TYPE`
8. `UNKNOWN_EVENT_TYPE`
9. `INVALID_FORMAT` — ID pattern, UUID, enum other than `event_type`, length
10. `INVALID_TIMESTAMP` — not RFC 3339
11. `TIMESTAMP_MISSING_OFFSET`
12. `MISSING_CONDITIONAL_FIELD` — e.g. `facility_id` on `ARRIVED_AT_FACILITY`
13. `EVENT_TIME_IN_FUTURE` — runtime rule FR-VAL-02
14. `EVENT_TIME_TOO_OLD` — runtime rule FR-VAL-02, live mode only

Codes 1–12 are structural and also expressible in JSON Schema. Codes 13–14 depend on
processing time and exist only in the processor's validation code.

## 3. DLQ record v1 — `contracts/dlq-record.v1.schema.json`

| Field | Description |
|---|---|
| `dlq_schema_version` | `"1.0"` |
| `error_code` | Primary code (by precedence) |
| `errors[]` | `{code, field (JSON pointer or null), message}`, all violations |
| `raw_value` | Original value as a string. If it is not UTF-8: base64, with `raw_value_encoding = "base64"`. |
| `raw_value_encoding` | `utf8` \| `base64` |
| `source` | `{topic, partition, offset, kafka_timestamp, key}`. `(topic, partition, offset)` is the DLQ dedup key. |
| `processor` | `{name, version, run_id, batch_id}` |
| `rejected_at` | RFC 3339 UTC |

The DLQ is **at-least-once**: a retried micro-batch can write the same rejection twice.
Consumers dedupe on `source.(topic, partition, offset)`.

## 4. PostgreSQL serving contracts

Schemas: `tracking` (written by the streaming processor), `analytics` (written by batch jobs),
`ops` (pipeline bookkeeping). Migrations live in `sql/postgres/migrations/` (Flyway-style
numbered files) and are the source of truth once implemented.

### `tracking.package_status` — grain: one row per `package_id`

| Column | Type | Notes |
|---|---|---|
| `package_id` | text PK | |
| `current_status` | text not null | `event_type` of the status-defining event |
| `status_event_id` | uuid not null | ┐ |
| `status_event_time` | timestamptz not null | ├ ordering key (spec 01) |
| `status_rank` | smallint not null | ┘ |
| `facility_id`, `vehicle_id` | text null | From the status-defining event |
| `promised_delivery_date` | date null | From `CREATED`, merged whenever `CREATED` arrives |
| `service_level`, `destination_postal_prefix`, `destination_timezone`, `origin_facility_id` | text null | From `CREATED` |
| `created_event_time`, `delivered_event_time` | timestamptz null | |
| `last_event_time` | timestamptz not null | Max `event_time` seen, regardless of whether it set the status |
| `event_count` | integer not null | Unique events applied |
| `anomaly_count` | integer not null | |
| `first_seen_at`, `updated_at` | timestamptz not null | Processing time |
| `updated_by_query`, `updated_batch_id` | text, bigint | Lineage |

**Comparison detail:** Postgres orders `uuid` by bytes, which equals ordering by the canonical
lowercase hex string. The Scala comparator MUST use the same ordering, so that the in-batch
reduction and the SQL guard agree.

### `tracking.processed_events` — grain: one row per unique `event_id`

`event_id uuid`, `event_date date` (UTC date of `event_time`), `package_id text`,
`content_hash bytea`, `first_batch_id bigint`, `first_processed_at timestamptz`.
**PK `(event_id, event_date)`**, `PARTITION BY RANGE (event_date)`, daily partitions.

`event_date` is derived deterministically from `event_time`, so every duplicate of an event
maps to the same partition and the composite PK still deduplicates. Partitions older than
the retention window (default 30 days, which must be longer than the Kafka retention of
7 days) are dropped. A duplicate arriving after that cannot change status (the ordering
guard rejects it) but could inflate `event_count`. That residual risk is accepted and documented.

### `tracking.status_anomalies` — grain: one row per (`event_id`, `anomaly_code`)

Codes: `POST_TERMINAL_EVENT`, `ORPHAN_EVENT` (no `CREATED` after 24 h, written by the DQ job),
`SEMANTIC_DUPLICATE`.

### `ops.sink_batches` — grain: one row per (`query_name`, `batch_id`)

`started_at`, `committed_at`, `input_rows`, `applied_rows`, `duplicate_rows`, `rejected_rows`,
`late_rows`, `min_offset`/`max_offset` (JSON per partition), `processor_version`, `run_id`.
Written by the driver **after** all partition writes for the batch succeed. It is used for
observability and latency measurement, **not** as the idempotency mechanism (see ADR-0002).

### Analytics (gold)

| Table | Grain / PK | Columns |
|---|---|---|
| `analytics.daily_delivery_performance` | (`delivery_date`, `service_level`, `destination_postal_prefix`) | `delivered_count`, `on_time_count`, `on_time_pct`, `delay_p50_min`, `delay_p95_min`, `computed_at`, `run_id` |
| `analytics.daily_facility_performance` | (`activity_date`, `facility_id`) | `arrivals`, `departures`, `dwell_p50_min`, `dwell_p95_min`, `exception_count`, `computed_at`, `run_id` |

`delivery_date` is the date in the destination's local time zone. `activity_date` is in UTC
(facility time zones are out of scope for the MVP).

## 5. Parquet lake layout

Root: `${LAKE_ROOT}` (local default `./data/lake`).

| Layer | Path | Partition | Writer | Content |
|---|---|---|---|---|
| Bronze | `bronze/package_events/ingest_date=YYYY-MM-DD/` | `ingest_date` (UTC date of Kafka timestamp) | bronze-writer stream (Spark file sink) | All consumed records: `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `kafka_key`, `raw_value`, `is_valid`, `error_codes[]`, best-effort parsed `event_id`, `package_id`, `event_type`, `event_time`, `processor_version` |
| Silver | `silver/package_events/event_date=YYYY-MM-DD/` | `event_date` (UTC) | Airflow batch job | Valid, typed, deduplicated by `event_id` (earliest `kafka_timestamp` wins), all contract fields flattened, plus `content_hash` |
| Gold | Postgres `analytics.*` | — | Airflow batch job | See §4 |

**Why bronze is partitioned by ingest date:** invalid records may have no usable `event_time`,
and late events must not rewrite old partitions. Partitioning by ingest date keeps bronze
append-only and makes "replay what arrived on day X" a partition read.
Silver uses `event_date` because analytics query by event time.

**Reader rule:** bronze is written by the Spark file sink. It is only consistent when read
through Spark, which honours `_spark_metadata`. Other engines globbing the directory may
see files from failed batches. Silver is rewritten atomically per partition by the batch
job, so any Parquet reader can use it.

## 6. Generator ground-truth manifest (FR-GEN-02/03)

JSON Lines, one record per package: `package_id`, `expected_final_status`,
`expected_status_event_id`, `unique_event_ids[]`, and `injected` (`duplicates`,
`out_of_order`, `late`, `malformed` counts and the event IDs or offsets involved).
Tests compare pipeline outputs against this manifest; they never re-derive the truth with the
same code under test.
