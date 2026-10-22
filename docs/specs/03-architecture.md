# 03 — Architecture

## Context

```
 ┌───────────────┐   package-events.v1    ┌────────────────────────────── Spark (local[*]) ───────────────────────────┐
 │ generator     │ ─────────────────────► │ Query A: status-processor   checkpoint: checkpoints/status-processor      │
 │ (Python)      │  key=package_id        │   parse → validate → dropDuplicatesWithinWatermark(event_id)              │
 │ + manifest    │  idempotent producer   │   foreachBatch:  invalid → Kafka DLQ                                      │
 └───────────────┘                        │                  valid   → Postgres tracking.* (idempotent apply)          │
                                          │                  driver  → ops.sink_batches, metrics                       │
                                          │ Query B: bronze-writer      checkpoint: checkpoints/bronze-writer         │
                                          │   parse → validate(flags only) → Parquet file sink (bronze, ingest_date)  │
                                          └────────────────────────────────────────────────────────────────────────────┘
                                                 │ /metrics (driver)                       │ bronze Parquet
                                          ┌──────▼──────┐  ┌─────────┐              ┌──────▼─────────────────────────────┐
                                          │ Prometheus  │─►│ Grafana │              │ Airflow (profile: orchestration)    │
                                          └─────────────┘  └─────────┘              │  silver_build → gold_load → dq_checks│
                                                                                    │  replay_from_bronze (manual trigger) │
                                                                                    └──────────────┬──────────────────────┘
                                                                                                   ▼
                                                                                   PostgreSQL: tracking · analytics · ops
```

## Components and responsibilities

| Component | Responsibility | Not responsible for |
|---|---|---|
| **generator** (Python) | Realistic lifecycles, fault injection, ground-truth manifest, configurable rate | Validation logic (it only produces malformed data on purpose) |
| **Kafka** (KRaft, single broker locally) | Durable, ordered-per-key log; replay within retention | Schema enforcement |
| **processor-core** (Scala library, no Spark) | Parsing, validation rules, error codes, ordering key, status transition rules, content hash | I/O |
| **status-processor** (Spark query A) | Validate, dedupe, DLQ routing, idempotent Postgres apply, metrics | Analytics |
| **bronze-writer** (Spark query B) | Immutable append of every consumed record to Parquet | Dedup, business rules beyond flagging validity |
| **batch jobs** (Spark batch, Scala) | Silver build, gold aggregates, replay, DQ checks, reconciliation | Real-time status |
| **Airflow** | Schedules batch jobs; manual replay DAG with parameters; retries; SLAs | Business logic (DAGs only call jobs) |
| **PostgreSQL** | Serving store for status and gold; pipeline bookkeeping | Long-term raw history (that is Parquet) |
| **Prometheus / Grafana** | Metrics scraping, dashboards, alert rules | Log storage (logs go to stdout as JSON) |

`processor-core` is shared by streaming, replay and batch. That is what makes FR-REC-02
("replay through the same code") true by construction.

## Status apply algorithm (query A, per micro-batch)

1. Parse and validate (`processor-core`). Split into `valid` and `invalid`.
2. `invalid` → DLQ producer (Kafka sink, at-least-once).
3. `valid` → `dropDuplicatesWithinWatermark(event_id)` with watermark `event_time − 2h`
   (bounds state and removes most duplicates cheaply).
4. Repartition `valid` by `package_id`, then for each Spark partition in **one JDBC transaction**:
   1. *(Implemented without a staging table: arrays of IDs/dates/packages are passed to `unnest(...)`.)*
   2. `INSERT INTO tracking.processed_events … SELECT … FROM unnest(…) ON CONFLICT DO NOTHING RETURNING event_id`.
      This gives the set of events **new** to the platform.
   3. For the new events only, aggregate per `package_id`: the most preferred event (spec 01
      status rule), the count, `CREATED` shipment attributes, and the max `event_time`.
   4. `INSERT INTO tracking.package_status … ON CONFLICT (package_id) DO UPDATE`. Status columns
      are updated via `CASE WHEN (new event is preferred over stored event)`, the same
      preference order as spec 01 (SC-0002).
      Shipment columns via `COALESCE`. Counters are incremented.
   5. Insert anomalies (`POST_TERMINAL_EVENT`, `SEMANTIC_DUPLICATE`).
   6. Commit.
5. The driver writes `ops.sink_batches` and publishes metrics.

**Partial-failure behaviour:** atomicity is per Spark partition, not per micro-batch. If
partition 3 commits and partition 4 fails, Spark re-runs the whole micro-batch after the
retry. Partition 3's events are then no-ops because they are already in `processed_events`.
Correctness relies on the idempotency of step 4, not on batch-level atomicity. See ADR-0002.

## Delivery and processing semantics (summary — ADR-0002 is authoritative)

| Hop | Guarantee | Mechanism |
|---|---|---|
| Producer → Kafka | At-least-once; no producer-session duplicates | `enable.idempotence=true`, `acks=all`. Application-level resends create duplicates with the same `event_id`. |
| Kafka → Spark | At-least-once replay of micro-batches | Offsets in the checkpoint, committed after the sink succeeds |
| Spark → Postgres status | **Effectively-once state** | `processed_events` dedup + ordering guard, transactional per partition |
| Spark → DLQ | At-least-once | Dedupe on source coordinates |
| Spark → bronze Parquet | Exactly-once **for `_spark_metadata`-aware readers** | File sink commit log |
| Bronze → silver → gold | Idempotent rebuild | Deterministic dedup, partition overwrite, transactional replace |

## Batch and recovery workflows (Airflow)

| DAG | Schedule | Steps |
|---|---|---|
| `lake_daily` | Hourly (local), parameterised window | `silver_build(ingest window → affected event_dates)` → `gold_load(last 7 days)` → `dq_checks` → `reconcile_status` |
| `replay_from_bronze` | Manual, params: `ingest_date_from`, `ingest_date_to`, `mode`, `dry_run` | Read bronze → `StatusApply` (the same classify/apply code as the stream) → Postgres → `ops.replay_runs`. Mode `as-ingested` (default) reproduces live decisions (SC-0004); `backfill` disables the age bound. Available now as `make replay`; the DAG wraps it when Airflow lands. |
| `maintenance` | Daily | Drop expired `processed_events` partitions; Parquet retention per spec 04 |

## Deployment view (local)

Docker Compose with profiles:

- `core`: `kafka`, `kafka-init`, `postgres`, `pg-migrate`, `spark-processor` (both queries in one driver JVM, `local[*]`), `generator` (run-once)
- `obs`: `prometheus`, `grafana`
- `orchestration`: `airflow` (LocalExecutor, Postgres metadata DB in a separate database)

Running both streaming queries in one driver keeps memory low locally. The trade-off is a
shared failure domain: a driver OOM stops both queries. In production they would be
separate applications. ADR-0006 discusses this.

## Alternatives considered

See ADR-0006 (sink topology), ADR-0007 (planned: JSON Schema vs schema registry) and
ADR-0008 (planned: Airflow vs Dagster). Also rejected for the MVP:
- Kafka Streams for status: it would drop the Spark requirement.
- Delta Lake / Iceberg instead of plain Parquet: they bring real ACID benefits but add
  scope. A strong P2 candidate.
- Debezium/CDC: there is no source database in this problem.
