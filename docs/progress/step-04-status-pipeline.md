# Step 4 — status pipeline (first vertical slice): results (2026-10-22)

Generator → Kafka → Spark Structured Streaming (`status-processor`) → DLQ (Kafka) + status (Postgres).
Acceptance criteria: spec 07 AC-1.1 … AC-1.8 (AC-1.1/1.2/1.5 as amended by SC-0003, approved 2026-10-22).

## What was built

| Area | Files |
|---|---|
| Schema & roles | `sql/postgres/migrations/V1–V3` (Flyway 13.10.3 in Compose), `infra/postgres/bootstrap-roles.{sql,sh}` |
| Core logic (pure Scala) | `status/StatusRule.scala` (SC-0002 preference), `status/StatusAggregator.scala`, `status/ApplyEvent.scala`, `dlq/DlqRecord.scala` |
| Spark job | `streaming/StatusProcessor.scala`, `Classify.scala`, `PostgresStatusSink.scala`, `ProcessorConfig.scala`, `StructuredLog.scala`, `StatusProcessorApp.scala` |
| Tooling | `make process`, `make status`, `make reconcile`, `make test-it`; CI job `integration` |

## Acceptance results (integration tests: real Kafka + Postgres via Testcontainers, real Python generator)

| AC | Result | Evidence |
|---|---|---|
| AC-1.1 | ✅ | `clean` scenario: all 100 packages equal the manifest (status, status event, time, count); DLQ empty |
| AC-1.2 | ✅ | `it-duplicates`, 100 records/batch: state = manifest; `processed_events` = unique events; Σ duplicate_rows = duplicate_copies |
| AC-1.3 | ✅ | DELIVERED@T2 then OUT_FOR_DELIVERY@T1 in a later batch → DELIVERED; and the reverse arrival order → DELIVERED (SC-0002) |
| AC-1.4 | ✅ | 8 invalid samples + future/too-old runtime cases → DLQ with the expected code, schema-valid, correct source topic/offset; no status rows |
| (combined) | ✅ | `smoke` (dirty) scenario: status = manifest; DLQ (partition, offset) → code map **equals** generator `malformed.jsonl` × `delivery.jsonl`; Σ input/applied/duplicate/rejected equal `summary.json` (KPI-P2 = 0) |
| AC-1.5 | ✅ | `it-restart` (≥ 5 000 records, 500/batch): crash injected after the sink commit of batch 3; restart on the same checkpoint → state = manifest, `processed_events` = unique events, batch 3 `attempts` = 2, two run IDs |
| AC-1.6 | ✅ | `SinkPropertyIT`: 25 random event sets × 3 random orders/duplications/splits; SQL result = Scala `StatusRule` every time |
| AC-1.7 | ✅ locally / ⏳ CI | `make test` and `make test-it` pass; CI workflow has an `integration` job (no GitHub remote yet) |
| AC-1.8 | ✅ | Error line on the injected crash is one JSON object with ts, component, run_id, query_name, batch_id, error_code, offset_range, message, action |

Totals: 104 core + 7 streaming unit tests, 7 integration tests (~40 s), 47 Python tests, `make verify` 0 failures, pin check 0, no leaks.

## Do the tests catch bugs? (manual mutation check, reverted)

| Injected bug | Caught by |
|---|---|
| SQL terminal branch `<` → `>` (latest terminal wins) | `SinkPropertyIT` failed |
| Durable dedup ignored (every event treated as new) | 4 of 6 `StatusPipelineIT` tests failed |

## Live run on the Compose stack (`make up`, `make process`, `make generate`, `make reconcile`)

| Run | Records | Result |
|---|---|---|
| `smoke` (unthrottled) | 698 | 100/100 packages match the manifest; 635/635 unique events applied |
| `outage-burst` (200 events/s, 30 s outage) | 13 140 | 2 000/2 000 packages match; 12 626/12 626 unique events applied |
| Totals in `ops.sink_batches` | 13 838 | applied 13 261, duplicates 433, rejected 144; DLQ topic holds exactly 144 records |

This shows correctness on live data. It is **not** a benchmark (spec 09 is a later step).

## Least privilege check

As `ptp_stream_writer`: SELECT/INSERT on status tables and creating daily partitions through the
SECURITY DEFINER function work. `CREATE TABLE` (permission denied) and `DROP TABLE` (must be owner) fail.

## Known gaps (deliberately deferred, tracked in docs/traceability.md)

- P1 items not in slice 1: in-stream `dropDuplicatesWithinWatermark` and the late counter (FR-DED-02, FR-LATE-01),
  Postgres retry with backoff (FR-REC-03), anomalies (`POST_TERMINAL_EVENT`, `SEMANTIC_DUPLICATE`, FR-DED-03/DQ-05),
  Prometheus metrics (FR-OBS-01), bronze writer (FR-STO-01).
- `POST_TERMINAL_EVENT` must be order-independent too, so it is planned as a batch DQ check over full history
  rather than an in-stream insert (to be confirmed when DQ-05 is built).
- Packaging: the processor runs on the host via sbt. A container image (with the Kafka connector bundled) and clean
  SIGTERM handling come with packaging. Stopping `make process` with a signal currently exits with code 2.
- Two different `CREATED` events for one package (not produced by the generator) would keep the first-arrived shipment
  attributes across batches.
