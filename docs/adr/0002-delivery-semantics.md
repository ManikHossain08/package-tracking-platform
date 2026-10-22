# ADR-0002: Delivery and processing semantics

- **Status:** Accepted (2026-10-21)
- **Requirements:** NFR-01, NFR-02, FR-DED-01, FR-REC-01, FR-REC-03

## Context

Spark checkpoints record which offsets a micro-batch covered. After a failure, Spark
**re-executes** the last uncommitted micro-batch. Whether that leads to duplicates in the
outputs depends entirely on each sink. Checkpointing alone gives **at-least-once delivery
to sinks**, not exactly-once end to end.

## Options for the Postgres status sink

| Option | How | Pros | Cons |
|---|---|---|---|
| A. Plain upsert, last-write-wins | `ON CONFLICT DO UPDATE` | Simple | Older events overwrite newer ones; replays double-count |
| B. Batch-id idempotency | Store `(query, batch_id)`; skip the batch if present | Spark-documented pattern | Only covers whole-batch replays of the *same* query. Breaks for bronze replays, new checkpoints, or `processed_events` partial commits. Needs batch-level atomicity, which per-partition JDBC writes do not have. |
| C. Event-level idempotency + monotonic guard | `processed_events` PK on `event_id`; status updated only when the ordering key is newer | Correct for re-runs, replays, duplicates and reordering; independent of the checkpoint | Extra table and index; retention needed |
| D. Single driver-side transaction per batch | `collect()` to the driver | Batch atomicity | Unbounded driver memory; does not scale |

## Decision

**Option C.** `ops.sink_batches` is still written, for observability only. The status
sink is **effectively-once with respect to state**: re-applying any events any number of
times converges to the same rows. It is not "exactly-once delivery". The duplicate work
happens and is absorbed.

| Hop | Guarantee |
|---|---|
| Producer → Kafka | At-least-once; idempotent producer prevents session-level duplicates only |
| Kafka → Spark | At-least-once micro-batch execution |
| → Postgres status | Effectively-once state (option C) |
| → DLQ (Kafka) | At-least-once; consumers dedupe on source coordinates |
| → Bronze Parquet (file sink) | Exactly-once file commits for readers honouring `_spark_metadata` |
| Bronze → silver/gold | Deterministic, idempotent rebuild |

## Consequences

- `processed_events` retention bounds the dedup horizon (30 d). Beyond it, a duplicate
  cannot change status (guard) but could inflate `event_count`. This is accepted and documented.
- Atomicity is per Spark partition. Correctness under partial failure is guaranteed by
  idempotency and tested (TS-R-03).
- Kafka transactions (`read_committed`, transactional producer) were not chosen for the
  DLQ: Spark's Kafka sink does not offer transactional writes. At-least-once plus dedup
  is the honest guarantee.
