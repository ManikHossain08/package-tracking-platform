# ADR-0006: Streaming sink topology

- **Status:** Accepted (2026-10-21)
- **Requirements:** FR-STR-01, FR-STO-01, FR-DLQ-01, NFR-02

## Context

Three outputs come from the same input: Postgres status, DLQ, and bronze Parquet.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. One query, `foreachBatch` writes all three | Read Kafka once; one checkpoint | Loses the file sink's `_spark_metadata` commit log (exactly-once files), so bronze writes would need custom idempotency. A Postgres outage also stops bronze landing, coupling raw capture to the serving store. |
| **B. Two queries:** status-processor (Postgres + DLQ in `foreachBatch`) and bronze-writer (file sink) | Raw capture keeps running during a Postgres outage; native file-sink guarantees; independent checkpoints and restarts | Kafka read and parsed twice (≈2× consumer I/O); two checkpoints to operate |
| C. Three queries (DLQ separate too) | Maximum isolation | DLQ routing would duplicate validation, for little benefit |

## Decision

**Option B.** Both queries run in one driver JVM locally (spec 03) to save memory. In
production they would be separate applications with separate resource settings.

## Consequences

- Kafka consumer load doubles. This is negligible locally and should be measured in the benchmark.
- Bronze can be *ahead* of Postgres during an outage. This is useful: replay from bronze
  can restore Postgres (FR-REC-02).
- Validation code is shared (`processor-core`), so both queries classify records identically.
  A test asserts that bronze `is_valid=false` rows equal the DLQ source offsets (DQ-06).
