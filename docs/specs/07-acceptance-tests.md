# 07 — Acceptance criteria and test scenarios

## Test levels

| Prefix | Level | Runs in | Infrastructure |
|---|---|---|---|
| TS-U | Unit (Scala `processor-core`, Python generator) | `make test`, every PR | None |
| TS-C | Contract (schemas, samples, generator output) | every PR | None |
| TS-I | Integration (Spark streaming + Kafka + Postgres) | `make test-it`, every PR | Testcontainers |
| TS-R | Failure and recovery | `make test-it`, every PR (long ones nightly) | Testcontainers, fault injection |
| TS-D | Data quality and consistency | `make test-it` | Testcontainers + local lake |
| TS-P | Performance and soak | `make bench`, manual or nightly | Compose stack |

## Synchronisation rules (no arbitrary sleeps)

- Spark-level tests use `MemoryStream` / `processAllAvailable()`, or `Trigger.AvailableNow`.
- Cross-process assertions use `eventually(timeout = T, interval = I)` with an **observable
  condition**, e.g. "`ops.sink_batches` contains a batch whose `max_offset` ≥ the produced
  end offset", or "the DLQ end offset = expected count". The timeouts are named constants.
- Containers become ready via Testcontainers wait strategies (log line or health check).
- Fault injection is triggered by test hooks or by container stop/start, never by timing races.
  Example: a `FailingJdbcSink` fails on attempt N.

## First vertical slice — acceptance criteria (P0)

**Scope:** generator → Kafka → validation → DLQ, and status in Postgres with dedup and
ordering, restart-safe. Bronze, silver, gold, Airflow and Grafana are **out of scope** for slice 1.

| AC | Given | When | Then | Reqs | Tests |
|---|---|---|---|---|---|
| AC-1.1 | 1 000 valid events for 100 packages (seeded generator, no faults) | Processed until `sink_batches` covers the end offset | `package_status` has exactly 100 rows; each row's `current_status` and `status_event_id` equal the manifest's expected values; `event_count` = 10 per package (= manifest) | FR-GEN-01, FR-ING-01, FR-STR-01, FR-ORD-01 | TS-I-01 |
| AC-1.2 | The AC-1.1 input with 50 events re-sent 1–2 extra times (same `event_id`), some in later micro-batches | Processed | `processed_events` count = 1 000; `package_status` identical to the AC-1.1 result (excluding processing-time columns); `ptp_events_duplicate_total` = number of extra copies | FR-DED-01 | TS-I-03 |
| AC-1.3 | Package P with `DELIVERED@T2` processed in batch k and `OUT_FOR_DELIVERY@T1` (T1 < T2) in batch k+1 | Processed | P stays `DELIVERED`, `status_event_time = T2`, `event_count` includes both events | FR-ORD-01 | TS-I-04, TS-U-04 |
| AC-1.4 | One record per invalid sample in `contracts/samples/invalid/`, plus runtime cases (+5 min +1 s future, live-mode 30 d +1 s old) | Processed | Each appears in the DLQ exactly with its `expected.json` primary `error_code`, the correct `source.offset`, and a schema-valid DLQ record. `package_status` has no row for those packages. | FR-VAL-01..03, FR-DLQ-01 | TS-I-02, TS-U-01 |
| AC-1.5 | A stream of 5 000 events | The processor is stopped after ≥ 1 committed batch and before the end, then restarted on the same checkpoint | Final `package_status` equals the uninterrupted baseline; no `event_id` appears twice in `processed_events`; every input offset is either applied, a duplicate, or in the DLQ | FR-REC-01 | TS-R-01 |
| AC-1.6 | A random set of events for 1–20 packages (property generator) | Applied via `processor-core` + the SQL apply logic in random permutations, with random duplication and split points | Final status is identical across all runs | NFR-01, FR-ORD-01 | TS-U-05 |
| AC-1.7 | Clean checkout | `make test` and `make test-it` locally, and CI on a PR | All pass; test reports are published as CI artifacts | FR-DEV-01, FR-DEV-02 | CI |
| AC-1.8 | A processing error is injected | Logs are captured | Each error log line is valid JSON containing the stable keys from spec 06 | FR-OBS-02 | TS-U-08 |

## Test scenario catalogue

| ID | Scenario | Reqs | Pri |
|---|---|---|---|
| TS-U-01 | Every structural rule and its error code, including precedence and multiple errors | FR-VAL-01, FR-VAL-03 | P0 |
| TS-U-02 | Parser fuzzing: random bytes / truncated JSON never throw, always map to an error code | FR-VAL-01 | P0 |
| TS-U-03 | Time-bound rules at ±1 s boundaries, live vs replay mode | FR-VAL-02 | P0 |
| TS-U-04 | Ordering key comparison: same time different rank, same time and rank different `event_id`, terminal stickiness | FR-ORD-01 | P0 |
| TS-U-05 | Property test: permutation, duplication and batch-split invariance (AC-1.6) | NFR-01 | P0 |
| TS-U-06 | Generator determinism: same seed ⇒ same stream; manifest agrees with the injected faults | FR-GEN-01/02 | P0 |
| TS-U-07 | Late-arriving `CREATED` merges shipment attributes without changing status | FR-ORD-01 | P0 |
| TS-U-08 | Structured log format | FR-OBS-02 | P0 |
| TS-C-01 | All valid samples pass and all invalid samples fail the JSON Schema with the expected codes | FR-VAL-01 | P0 |
| TS-C-02 | 10 000 generated valid events pass the schema | FR-ING-01 | P0 |
| TS-C-03 | Schema evolution: a v1.1 event with an extra optional field is accepted and preserved in bronze; a `2.0` event goes to the DLQ as `UNSUPPORTED_SCHEMA_VERSION`; a new enum value is rejected until the consumer is upgraded | NFR-04 | P1 |
| TS-I-01 | Happy path end to end (AC-1.1) | FR-ING-01, FR-STR-01 | P0 |
| TS-I-02 | Invalid input → DLQ (AC-1.4) | FR-DLQ-01 | P0 |
| TS-I-03 | Duplicate delivery within and across micro-batches (AC-1.2) | FR-DED-01 | P0 |
| TS-I-04 | Out-of-order and late events, including a 3 h-late event behind the watermark (AC-1.3, FR-LATE-01) | FR-ORD-01, FR-LATE-01 | P0/P1 |
| TS-I-05 | Bronze completeness: bronze rows = consumed offsets (read via Spark) | FR-STO-01 | P1 |
| TS-R-01 | Kill and restart on the checkpoint (AC-1.5) | FR-REC-01 | P0 |
| TS-R-02 | Postgres down mid-stream: retries counted, the query fails after max attempts with an actionable error, the checkpoint does not advance; after Postgres returns and the processor restarts, the final state = baseline | FR-REC-03 | P1 |
| TS-R-03 | Sink fails after partial partition commit (injected on partition N): the re-run gives the baseline state, no double counting | FR-REC-01, NFR-02 | P1 |
| TS-R-04 | Replay a historical bronze range twice (and once overlapping live processing): Postgres unchanged after the second run | FR-REC-02 | P1 |
| TS-R-05 | Kafka broker restart during processing: resumes without loss | FR-REC-01 | P1 |
| TS-R-06 | Outage burst: the generator buffers 30 min of events and then floods; lag recovers; correctness holds; catch-up time recorded | NFR-03 | P1 |
| TS-R-07 | Offsets aged out: the query fails with an actionable error; runbook recovery from bronze | FR-REC-04 | P2 |
| TS-D-01 | Count reconciliation DQ-06 on a closed window | DQ-06 | P1 |
| TS-D-02 | State reconciliation DQ-07: streaming status = batch recomputation from silver | DQ-07, KPI-P1 | P1 |
| TS-D-03 | Gold consistency DQ-08, plus fixture-based expected results for the SQL query pack | DQ-08, FR-ANL-02 | P1 |
| TS-D-04 | Silver uniqueness DQ-02, and an idempotent rebuild of silver/gold | DQ-02, FR-STO-02, FR-ANL-01 | P1 |
| TS-P-01 | Throughput/latency benchmark per spec 09 | NFR-10 | P1 |
| TS-P-02 | 30-min soak at constant rate: heap and state rows plateau | NFR-05 | P1 |
