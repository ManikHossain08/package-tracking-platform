# Step 5 — bronze Parquet + replay: results (2026-10-22)

Acceptance criteria: spec 07, AC-5.1 … AC-5.7. Spec change approved during this step:
**SC-0004** (time bounds judged at Kafka ingest time, `LogAppendTime`).

## What was built

| Area | Files |
|---|---|
| Validation clock (SC-0004) | `Classify.scala` uses the record's Kafka timestamp; `infra/kafka/create-topics.sh` sets `message.timestamp.type=LogAppendTime`; `make verify` checks it |
| Shared apply path | `StatusApply.scala`: classify → DLQ (optional) → Postgres, used by both the stream and replay |
| Bronze writer (query B, ADR-0006) | `BronzeWriter.scala` (`BronzeRow`), core `bronze/BestEffortFields.scala`; runs beside the status processor in one driver |
| Replay | `Replay.scala` (`ReplayApp`, modes `as-ingested` / `backfill`, dry run), migration `V4__ops_replay_runs.sql`, `make replay` |

## Acceptance results

| AC | Result | Evidence (integration tests, Testcontainers + real generator) |
|---|---|---|
| AC-5.1 | ✅ | Bronze `(partition, offset)` set = `delivery.jsonl`; row count = `records_total` (dirty `smoke` run) |
| AC-5.2 | ✅ | `kafka_key`/`raw_value` byte-identical to Kafka for every row; bronze invalid rows = DLQ offsets with the same primary code (DQ-06) |
| AC-5.3 | ✅ | `ingest_date` = UTC date of the Kafka timestamp for every row; one directory per date; a date filter shows `PartitionFilters` in the plan |
| AC-5.4 | ✅ | Deleting the last checkpoint commit (and its `.crc`) re-executes the batch; row count and distinct offsets unchanged |
| AC-5.5 | ✅ | Two replays over an already-applied range: `package_status` and `processed_events` snapshots identical; applied 0, duplicates = valid |
| AC-5.6 | ✅ | Tables emptied → replay rebuilds the manifest exactly; repeated with the clock +31 days → same result |
| AC-5.7 | ✅ | Dry run writes nothing; `ops.replay_runs` rows (mode, dry_run, status, counts); a 40-day-old event stays rejected in `as-ingested` and is applied only in `backfill`; the DLQ is never re-published |

Totals: integration suite 12/12 (step 4's 7 still pass after the refactor), 104 core + 10 streaming unit
tests, 47 Python unit tests, Python integration test 12/12 consecutive passes after the fix below.

## Mutation check (reverted)

| Injected bug | Caught by |
|---|---|
| `Classify` back to processing-time clock (undoing SC-0004) | `ClassifySpec` (2 tests) and `ReplayIT` AC-5.6 failed |

## Live run on the Compose stack (fresh stack)

| Step | Result |
|---|---|
| `make process` + `make generate` (`smoke`, 698 records) + `it-restart` (5 967 records) | live reconcile: 100/100 and 800/800 packages, 0 mismatches |
| Simulated Postgres data loss (`TRUNCATE` status tables), `make replay` | input 6 665, rejected 131, applied 6 164 = the generators' `records_total`, `malformed_total`, `unique_valid_events`; reconcile 0 mismatches for both runs |
| `make replay` again | applied 0, duplicates 6 534: idempotent |

## Issues found and fixed during this step

1. **Design: validation clock** (SC-0004). Processing-time bounds would have made replay drop valid data after 30 days, let
   outages reject valid events, and let the two queries disagree. Approved and fixed before any step 5 code was written.
2. **Test pollution of live data.** The Python integration test (TS-I-06) published to the live `package-events.v1`. Because the
   same seed gives the same package IDs, it merged a second history into the demo packages (a live reconcile showed 100
   mismatches, and the totals were off by exactly 698 records = one `smoke` run). Fixed: the test now creates and deletes its own
   topic with production settings.
3. **Flaky test.** The same test intermittently hit librdkafka `_DESTROY_BROKER` on `get_watermark_offsets` with a fresh
   consumer. Fixed with a bounded retry on the observable condition (and a wait for partition leaders), not a sleep: 12/12 passes.
4. Test-only issues: Spark `Row` values are `java.sql` types; Hadoop's checkpoint commit has a `.crc` sidecar that a
   crash simulation must also remove.

## Known gaps

- Bronze small files: one file per partition per micro-batch. Compaction belongs to the silver job (FR-STO-02).
- Bronze retention (90 days) and the file-sink metadata log compaction are maintenance-DAG work (NFR-08).
- Both queries share one driver JVM locally (ADR-0006). Stopping `make process` with a signal still exits with code 2 (packaging step).
