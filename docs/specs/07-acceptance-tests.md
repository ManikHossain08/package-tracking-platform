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

## Step 1 (setup) — acceptance criteria

**Scope:** reproducible local foundation. It covers the Compose `core` profile with Kafka and
Postgres only, the build skeletons, the contract tests and CI. No application logic.

| AC | Criterion | Reqs | Verified by |
|---|---|---|---|
| AC-0.1 | `make up` starts Kafka and Postgres, and both report healthy within a bounded wait (`docker compose up --wait`, timeout 120 s) | FR-DEV-01 | `make verify` |
| AC-0.2 | After `make up`, topics `package-events.v1` (6 partitions, retention 7 d) and `package-events.dlq.v1` (3 partitions, retention 14 d) exist with exactly these settings | FR-ING-02 | `make verify` |
| AC-0.3 | Topic auto-creation is disabled: producing to a non-existent topic fails, and the topic is not created | FR-ING-02 | `make verify` |
| AC-0.4 | The topic init is idempotent: running it again succeeds and changes nothing | FR-ING-02 | `make verify` |
| AC-0.5 | `make test` runs the Scala and Python unit/contract tests without Docker. It includes TS-C-01 (contract samples) and a Spark 4.1.3 local-session smoke test. | FR-DEV-01, FR-VAL-01 | `make test` output |
| AC-0.6 | Every Compose image is pinned by tag **and** `@sha256` digest. sbt, Scala and library versions are exact. Python dependencies are locked with hashes. | NFR-07 | `make check-pins` |
| AC-0.7 | No secrets are committed: `.env` is git-ignored, `.env.example` has placeholders only, and gitleaks finds nothing | NFR-06 | `make secrets-scan`, CI |
| AC-0.8 | The CI workflow runs pin checks, the secret scan, Python tests, Scala tests and the Compose verification on every push and PR | FR-DEV-02 | Workflow file. A green run requires a GitHub remote, which isn't set up yet. |

## Step 2 (generator) — acceptance criteria

**Scope:** `generator/` Python package, scenario files and manifest. The ground-truth rule
follows the spec 01 status rule (as amended by SC-0002).

| AC | Criterion | Reqs | Test |
|---|---|---|---|
| AC-2.1 | Same scenario and seed ⇒ identical records (keys, values, order), excluding `emitted_at` and values derived from the emit-time clock. A different seed ⇒ a different stream. | FR-GEN-01 | TS-U-06 |
| AC-2.2 | Realistic lifecycles: each package starts with `CREATED`; 1–4 facilities, each `ARRIVED` before `DEPARTED`; at most 3 delivery attempts, then `RETURNED_TO_SENDER`; the terminal event is last; true event times strictly increase per package; `promised_delivery_date` = created date (destination tz) + 1/2/4 days for EXPRESS/STANDARD/ECONOMY; no event after the window end | FR-GEN-01 | TS-U-09 |
| AC-2.3 | Every non-malformed record generated for a 10 000-event scenario passes `package-event.v1.schema.json` | FR-ING-01 | TS-C-02 |
| AC-2.4 | Every structural malformed record fails the schema. Every runtime malformed record passes the schema, but its `event_time` is beyond the documented bound. Each has the expected primary `error_code` in `malformed.jsonl`. | FR-GEN-02 | TS-C-02 |
| AC-2.5 | Injected faults are observable and exactly counted: duplicate copies share an `event_id` and appear later; semantic duplicates have a new `event_id` and identical content; out-of-order events are emitted after a later-keyed event of the same package; late events are emitted after the stream's max event time has passed `event_time + delay`. All counts equal `summary.json`. | FR-GEN-02 | TS-U-10 |
| AC-2.6 | The manifest's expected status per package equals the SC-0002 rule. The oracle is permutation-invariant (property test) and matches hand-written cases. | FR-GEN-03 | TS-U-11 |
| AC-2.7 | Pacing: at `rate_eps = R`, the send schedule offsets are `i / R`. During a configured outage, records are held and released together at the outage end. `rate_eps = 0` sends unthrottled. | FR-GEN-01, FR-GEN-02 | TS-U-12 |
| AC-2.8 | The Kafka sink publishes every record to `package-events.v1`, keyed by `package_id`, with `enable.idempotence=true` and `acks=all`. Topic end offsets grow by exactly the emitted count; `delivery.jsonl` has one row per record. | FR-ING-01 | TS-I-06 (needs `make up`) |

## Step 3 (validation in `processor-core`) — acceptance criteria

**Scope:** pure Scala validation of one Kafka record value into either a typed event or a
rejection with error codes. No Spark, Kafka or JDBC. Rules: spec 02 §2.

| AC | Criterion | Reqs | Test |
|---|---|---|---|
| AC-3.1 | Every `contracts/samples/valid/*` is accepted. Every `contracts/samples/invalid/*` is rejected with exactly the primary `error_code` in `expected.json`. | FR-VAL-01 | TS-C-01 (Scala) |
| AC-3.2 | Cross-language parity on a generator snapshot: every valid generated record is accepted, and every malformed one is rejected with the manifest's `expected_error_code` (all 10 malformed types present) | FR-VAL-01, FR-VAL-02, FR-GEN-02 | TS-C-04 |
| AC-3.3 | Every rule in spec 02 §2 has at least one accepting and one rejecting unit test. A record with 3 independent violations yields 3 errors, each with a JSON-pointer `field`, and the primary code follows the precedence list. | FR-VAL-01, FR-VAL-03 | TS-U-01 |
| AC-3.4 | Time bounds: `event_time = now + 5 min` is accepted and `+5 min +1 s` is rejected (`EVENT_TIME_IN_FUTURE`). In live mode, `now − 30 d` is accepted and `−30 d −1 s` is rejected (`EVENT_TIME_TOO_OLD`). Replay mode accepts `now − 31 d`. | FR-VAL-02 | TS-U-03 |
| AC-3.5 | Totality: for 10 000+ random inputs (random bytes, truncated JSON, field-level mutations of valid events), `validate` never throws. Every rejection has ≥ 1 error, and its primary code is the highest-precedence one. | FR-VAL-01, FR-VAL-03 | TS-U-02 |
| AC-3.6 | Accepted events are faithfully typed: `event_time` with any offset is normalised to the same UTC instant; microsecond precision is kept; optional and conditional fields map to `Option`s | FR-VAL-01 | TS-U-13 |
| AC-3.7 | `processor-core` has no Spark, Kafka or JDBC on its classpath (spec 03). Its JSON library version equals Spark 4.1.3's (Jackson 2.21.2) to avoid runtime conflicts. | spec 03, NFR-07 | TS-U-14 |

## Step 5 (bronze Parquet + replay) — acceptance criteria

**Scope:** query B of ADR-0006 (`bronze-writer`: Kafka → Parquet file sink) running beside the status
processor in the same driver, and a replay job (bronze → same validation and apply code → Postgres).
AC-5.2, AC-5.5 and AC-5.6 rely on **SC-0004** (approved): time bounds are judged at Kafka ingest time.

| AC | Criterion | Reqs | Test |
|---|---|---|---|
| AC-5.1 | **Completeness:** after a dirty generator run is consumed, bronze (read through Spark, which honours `_spark_metadata`) holds exactly one row per Kafka record, valid and invalid alike: its `(partition, offset)` set equals `delivery.jsonl` | FR-STO-01 | TS-I-05 |
| AC-5.2 | **Fidelity and agreement:** `raw_value` and `kafka_key` are byte-identical to what was produced (stored as binary, lossless). Bronze rows with `is_valid = false` are exactly the DLQ's source offsets, with the same primary error code (DQ-06). | FR-STO-01, DQ-06 | TS-I-05 |
| AC-5.3 | **Layout:** files live under `bronze/package_events/ingest_date=YYYY-MM-DD/`, where the date is the UTC date of the Kafka timestamp. A read filtered on `ingest_date` prunes partitions (the plan shows a partition filter). | FR-STO-01 | TS-I-05 |
| AC-5.4 | **Exactly-once files under re-run:** remove the checkpoint commit of the last batch (simulating a crash after the files were written but before the checkpoint commit), then restart → the batch is re-executed and bronze row count and content are unchanged | FR-STO-01, NFR-02 | TS-R-08 |
| AC-5.5 | **Replay is idempotent:** after the stream has applied a dirty run, replaying its ingest-date range (once, then again) leaves `tracking.package_status` and `tracking.processed_events` byte-for-byte unchanged | FR-REC-02, NFR-01 | TS-R-04 |
| AC-5.6 | **Replay restores state:** after the tracking tables are emptied (simulated Postgres data loss), replaying bronze rebuilds `package_status` equal to the generator manifest (RPO 0 for data in bronze, NFR-03). Replaying 31 days later (simulated clock) gives the same result. | FR-REC-02, NFR-03 | TS-R-04 |
| AC-5.7 | **Replay is observable and safe:** each run is recorded in `ops.replay_runs` (range, mode, counts, timings); `--dry-run` validates and counts but writes nothing; invalid records are counted, not re-sent to the DLQ | FR-REC-02, FR-OBS-02 | TS-R-04 |

## Step 6 (silver + gold analytics) — acceptance criteria

**Scope:** batch jobs `silver-build` (bronze → silver Parquet) and `gold-load` (silver → Postgres `analytics`),
plus the SQL query pack. They rely on **SC-0005** (approved): fact tables, metric definitions C1–C8,
and the generator's expected-gold files. Silver comparisons ignore `silver_run_id`; gold comparisons ignore `computed_at` and `run_id`.

| AC | Criterion | Reqs | Test |
|---|---|---|---|
| AC-6.1 | **Silver content:** for a dirty generator run, silver holds exactly the unique valid events (`count = unique_valid_events`); `event_id` is unique (DQ-02); every row is in `event_date` = UTC date of `event_time`; no invalid record is present | FR-STO-02, DQ-02 | TS-D-04 |
| AC-6.2 | **Dedup rule:** for an `event_id` seen more than once, silver keeps the copy with the smallest `(kafka_timestamp, kafka_partition, kafka_offset)` | FR-STO-02 | TS-D-04 |
| AC-6.3 | **Typed with lineage:** all contract fields are typed columns (shipment and details flattened), plus `content_hash` (ADR-0003 fields) and lineage (`kafka_partition`, `kafka_offset`, `kafka_timestamp`, `silver_run_id`) | FR-STO-02 | TS-D-04 |
| AC-6.4 | **Idempotent, targeted rebuild:** building the same ingest window twice gives identical silver rows. A late event touches only its own `event_date` partition; other partitions' files are not rewritten. A build that fails after staging but before the swap leaves the live partitions unchanged, and the next build succeeds. | FR-STO-02 | TS-D-04 |
| AC-6.5 | **Compaction:** each silver partition has at most `max(1, ⌈estimated bytes / target⌉)` files (target configurable, default 128 MiB); bronze's many small files become one file per partition locally | FR-STO-02 | TS-D-04 |
| AC-6.6 | **Gold equals an independent oracle:** `delivery_facts`, `facility_visits` and `daily_network_activity` equal the generator's expected files (C1–C7). The daily aggregates equal those facts aggregated (counts, on-time %, exact p50/p95). | FR-ANL-01, FR-GEN-03 | TS-D-03 |
| AC-6.7 | **Consistency (DQ-08):** Σ `delivered_count` = rows in `delivery_facts` = packages whose `package_status` is `DELIVERED` and that have a `CREATED` event, for the window | FR-ANL-01, DQ-08 | TS-D-03 |
| AC-6.8 | **Atomic, idempotent load:** re-running the same window gives identical rows (excluding `computed_at`, `run_id`). A load failing mid-way leaves the previous gold rows intact. Every run is logged in `ops.gold_runs`. | FR-ANL-01, NFR-01 | TS-D-03 |
| AC-6.9 | **Recompute window:** a late `DELIVERED` whose delivery date is inside the 7-day window changes gold on the next run; facts older than the window are unchanged | FR-ANL-01, FR-LATE-01 | TS-D-03 |
| AC-6.10 | **SQL query pack:** every query in `sql/analytics/` (on-time %, delay percentiles, dwell, throughput, exception rate) runs as `ptp_reader` and returns hand-computed expected results on a small fixed fixture. A window with no data returns empty results or NULL rates, never an error (SC-0008 B) | FR-ANL-02 | TS-D-05 |

## Step 7 (data-quality job + orchestration) — acceptance criteria

**Scope:** a scheduled DQ job writing `ops.dq_results`; a `ptp-jobs` container image; Airflow running the
`lake_daily`, `replay_from_bronze` and `maintenance` DAGs. These criteria rely on **ADR-0009** (option A) and **SC-0006**, both approved. Delivered in two parts: 7a = AC-7.1…7.6, 7b = AC-7.7…7.11.

| AC | Criterion | Reqs | Test |
|---|---|---|---|
| AC-7.1 | **One result per check:** a DQ run evaluates DQ-02 … DQ-10 for a window and writes one `ops.dq_results` row per check: `status` ∈ {pass, warn, fail, skipped}, `observed`, `threshold`, `sample` (≤ 10 ids), `run_id`, `window` | DQ-02…10 | TS-D-06 |
| AC-7.2 | **Clean data passes:** after a dirty generator run through stream → bronze → silver → gold, every check is pass or warn. DQ-10 is `skipped` (with a reason) when there are fewer than 7 days of history. DQ-05's observed count equals the post-terminal events the generator's oracle predicts. | DQ-02…10 | TS-D-06 |
| AC-7.3 | **Each check catches its fault:** for every check, one planted fault (e.g. a duplicate silver row, a tampered `package_status` row, a gold count off by one, an orphan package, an implausible dwell) makes that check fail or warn, with the planted id in `sample`. The other checks are unaffected. | DQ-02…10 | TS-D-07 |
| AC-7.4 | **Severity drives the exit code:** any critical `fail` makes the DQ job exit non-zero (so the DAG task fails); warnings alone exit 0 | DQ-02…10 | TS-D-07 |
| AC-7.5 | **Anomalies:** `ORPHAN_EVENT` and `POST_TERMINAL_EVENT` rows are written to `tracking.status_anomalies`, idempotently (re-running gives the same rows) | DQ-04, DQ-05, FR-DED-03 | TS-D-06 |
| AC-7.6 | **Packaged jobs:** `make image` builds `ptp-jobs` (pinned Spark base + assembly jar). The status processor runs as the Compose service `spark-processor`, and `docker compose stop` stops it **cleanly** (exit 0, queries stopped), closing the step 4 gap. | FR-DEV-01, NFR-07 | TS-I-07 |
| AC-7.7 | **Airflow up:** `make up-all` starts Airflow healthy within a bounded wait. Its UI binds to 127.0.0.1 with the admin password from `.env`, and its metadata lives in its own Postgres database and role. | FR-DEV-01, NFR-06 | `make verify-airflow` |
| AC-7.8 | **DAG integrity:** no import errors. `lake_daily` = silver → gold → dq; `replay_from_bronze` validates its params (dates, mode, dry_run) before launching Spark; `maintenance` has its retention tasks. Every task has retries, a timeout and run-scoped arguments. | FR-DEV-01 | TS-O-01 |
| AC-7.9 | **End to end:** triggering `lake_daily` on the running stack produces new `ops.gold_runs` and `ops.dq_results` rows and succeeds. With a planted critical fault, the run fails at the dq task. | FR-ANL-01, DQ-* | TS-O-02 |
| AC-7.10 | **Replay via Airflow:** a `replay_from_bronze` run with params writes an `ops.replay_runs` row; invalid params fail before any Spark job starts | FR-REC-02 | TS-O-02 |
| AC-7.11 | **Retention (NFR-08):** `maintenance` drops `processed_events` partitions older than 30 days (through a SECURITY DEFINER function) and bronze `ingest_date` partitions older than 90 days. Afterwards bronze stays readable by Spark, the status processor keeps running, and data inside retention is untouched. | NFR-08 | TS-O-03 |

## Step 8 (observability) — acceptance criteria

**Scope:** Prometheus metrics from the streaming processor and the batch jobs, alert rules, and Grafana dashboards
(`obs` profile). These criteria rely on **SC-0007** (approved).

| AC | Criterion | Reqs | Test |
|---|---|---|---|
| AC-8.1 | **Endpoint:** the processor serves `/metrics` on `METRICS_PORT` with the SC-0007 metric set (HELP/TYPE present). Labels are bounded: `query`, `error_code`, `stage`, `sink` only; never a package or event id. | FR-OBS-01 | TS-M-01 |
| AC-8.2 | **Counters are exact:** after a generator run, `ptp_input_rows_total` = `records_total`, `ptp_events_rejected_total` by `error_code` = `malformed_by_error_code`, `ptp_events_applied_total` = `unique_valid_events`, `ptp_events_duplicate_total` = `duplicate_copies` (and equal the `ops.sink_batches` sums) | FR-OBS-01, KPI-P2 | TS-M-01 |
| AC-8.3 | **Progress metrics:** `ptp_batch_duration_seconds` count = committed batches; `ptp_offsets_behind_latest` is 0 once caught up; `ptp_last_progress_timestamp_seconds` is recent for both queries | FR-OBS-01 | TS-M-01 |
| AC-8.4 | **Batch jobs:** silver, gold, dq, replay and maintenance push last-success timestamp, duration and outcome to the Pushgateway; DQ pushes one series per check with its status | FR-OBS-01, DQ-* | TS-M-02 |
| AC-8.5 | **Prometheus:** scrapes the processor and the Pushgateway (targets up); rule files pass `promtool check rules` | FR-OBS-03 | `make verify-obs` |
| AC-8.6 | **Alert rules are tested:** `promtool test rules` shows each alert firing on synthetic series built for it and staying silent on a healthy series | FR-OBS-03 | TS-M-03 |
| AC-8.7 | **Live alerts:** stopping the processor fires `PtpProcessorDown`; a planted critical DQ failure fires `PtpDqCriticalFailure` (observed through the Prometheus API with a bounded wait) | FR-OBS-03 | `make verify-obs` |
| AC-8.8 | **Grafana:** provisioned datasources and dashboards load without errors (by uid, through the API). Every `ptp_*` metric referenced by a dashboard or rule exists in Prometheus. | FR-OBS-03 | `make verify-obs` |
| AC-8.9 | **Security:** Prometheus, Pushgateway and Grafana bind to 127.0.0.1 only; Grafana's admin password comes from `.env`; its Postgres datasource uses the read-only `ptp_reader` | NFR-06 | `make verify-obs` |

## First vertical slice — acceptance criteria (P0)

**Scope:** generator → Kafka → validation → DLQ, and status in Postgres with dedup and
ordering, restart-safe. Bronze, silver, gold, Airflow and Grafana are **out of scope** for slice 1.

| AC | Given | When | Then | Reqs | Tests |
|---|---|---|---|---|---|
| AC-1.1 | The `clean` scenario: 100 packages, no faults (seeded generator) | Processed to the end of the topic | `package_status` has exactly 100 rows; each row's `current_status`, `status_event_id`, `status_event_time` and `event_count` equal the manifest; `processed_events` = `unique_valid_events`; DLQ empty *(amended by SC-0003)* | FR-GEN-01, FR-ING-01, FR-STR-01, FR-ORD-01 | TS-I-01 |
| AC-1.2 | The `it-duplicates` scenario: 8 % of events re-sent 1–2 extra times (same `event_id`) | Processed in ≥ 4 micro-batches, so copies also land in later batches | `package_status` equals the manifest; `processed_events` = `unique_valid_events`; Σ `ops.sink_batches.duplicate_rows` = `duplicate_copies` *(amended by SC-0003)* | FR-DED-01 | TS-I-03 |
| AC-1.3 | Package P with `DELIVERED@T2` processed in batch k and `OUT_FOR_DELIVERY@T1` (T1 < T2) in batch k+1 | Processed | P stays `DELIVERED`, `status_event_time = T2`, `event_count` includes both events | FR-ORD-01 | TS-I-04, TS-U-04 |
| AC-1.4 | One record per invalid sample in `contracts/samples/invalid/`, plus runtime cases (+5 min +1 s future, live-mode 30 d +1 s old) | Processed | Each appears in the DLQ exactly with its `expected.json` primary `error_code`, the correct `source.offset`, and a schema-valid DLQ record. `package_status` has no row for those packages. | FR-VAL-01..03, FR-DLQ-01 | TS-I-02, TS-U-01 |
| AC-1.5 | The `it-restart` scenario: ≥ 5 000 records, all fault types, 500 records per micro-batch | A crash is injected after the sink commit of batch 3 (before Spark commits its offsets), then the processor restarts on the same checkpoint *(amended by SC-0003)* | Final `package_status` equals the uninterrupted baseline; no `event_id` appears twice in `processed_events`; every input offset is either applied, a duplicate, or in the DLQ | FR-REC-01 | TS-R-01 |
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
| TS-U-09 | Generator lifecycle realism (AC-2.2) | FR-GEN-01 | P0 |
| TS-U-10 | Generator fault injection is real and exactly counted (AC-2.5) | FR-GEN-02 | P0 |
| TS-U-11 | Ground-truth oracle: SC-0002 rule, permutation invariance (AC-2.6) | FR-GEN-03 | P0 |
| TS-U-12 | Pacing and outage schedule; Kafka sink failure handling (AC-2.7) | FR-GEN-01/02, FR-ING-01 | P0 |
| TS-U-13 | Validated events are typed faithfully: UTC normalisation, precision, Options (AC-3.6) | FR-VAL-01 | P0 |
| TS-U-14 | `processor-core` isolation and Jackson version alignment (AC-3.7) | FR-DEV-01, NFR-07 | P0 |
| TS-C-01 | All valid samples pass and all invalid samples fail the JSON Schema with the expected codes | FR-VAL-01 | P0 |
| TS-C-02 | 10 000 generated valid events pass the schema | FR-ING-01 | P0 |
| TS-C-04 | Scala validator vs Python generator labels on a committed snapshot (AC-3.2) | FR-VAL-01, FR-VAL-02 | P0 |
| TS-C-03 | Schema evolution: a v1.1 event with an extra optional field is accepted and preserved in bronze; a `2.0` event goes to the DLQ as `UNSUPPORTED_SCHEMA_VERSION`; a new enum value is rejected until the consumer is upgraded | NFR-04 | P1 |
| TS-I-01 | Happy path end to end (AC-1.1) | FR-ING-01, FR-STR-01 | P0 |
| TS-I-02 | Invalid input → DLQ (AC-1.4) | FR-DLQ-01 | P0 |
| TS-I-03 | Duplicate delivery within and across micro-batches (AC-1.2) | FR-DED-01 | P0 |
| TS-I-04 | Out-of-order and late events, including a 3 h-late event behind the watermark (AC-1.3, FR-LATE-01) | FR-ORD-01, FR-LATE-01 | P0/P1 |
| TS-I-06 | Generator → Kafka: all records published once, keyed by `package_id` (AC-2.8) | FR-ING-01 | P0 |
| TS-I-05 | Bronze completeness: bronze rows = consumed offsets (read via Spark) | FR-STO-01 | P1 |
| TS-R-01 | Kill and restart on the checkpoint (AC-1.5) | FR-REC-01 | P0 |
| TS-R-02 | Postgres down mid-stream: retries counted, the query fails after max attempts with an actionable error, the checkpoint does not advance; after Postgres returns and the processor restarts, the final state = baseline | FR-REC-03 | P1 |
| TS-R-03 | Sink fails after partial partition commit (injected on partition N): the re-run gives the baseline state, no double counting | FR-REC-01, NFR-02 | P1 |
| TS-R-04 | Replay a historical bronze range twice (and once overlapping live processing): Postgres unchanged after the second run | FR-REC-02 | P1 |
| TS-R-05 | Kafka broker restart during processing: resumes without loss | FR-REC-01 | P1 |
| TS-R-06 | Outage burst: the generator buffers 30 min of events and then floods; lag recovers; correctness holds; catch-up time recorded | NFR-03 | P1 |
| TS-R-08 | Bronze file sink: a batch re-executed after a lost checkpoint commit writes no duplicate rows (AC-5.4) | FR-STO-01, NFR-02 | P1 |
| TS-R-07 | Offsets aged out: the query fails with an actionable error; runbook recovery from bronze | FR-REC-04 | P2 |
| TS-D-01 | Count reconciliation DQ-06 on a closed window | DQ-06 | P1 |
| TS-D-02 | State reconciliation DQ-07: streaming status = batch recomputation from silver | DQ-07, KPI-P1 | P1 |
| TS-D-03 | Gold equals the generator's independent oracle (facts and daily tables), DQ-08 consistency, idempotent/atomic load, recompute window (AC-6.6–6.9) | FR-ANL-01, DQ-08 | P1 |
| TS-D-04 | Silver content, dedup rule, typing/lineage, idempotent targeted rebuild, crash before swap, compaction (AC-6.1–6.5) | DQ-02, FR-STO-02 | P1 |
| TS-D-05 | SQL query pack on a hand-computed fixture, as `ptp_reader` (AC-6.10) | FR-ANL-02 | P1 |
| TS-D-06 | DQ job on clean data: one result per check, all pass/warn, DQ-10 skipped, anomalies = oracle, idempotent (AC-7.1/7.2/7.5) | DQ-02…10 | P1 |
| TS-D-07 | DQ fault sequence: each check catches its planted fault, others unchanged; exit codes (AC-7.3/7.4) | DQ-02…09 | P1 |
| TS-M-01 | `/metrics`: exact counters vs the generator, bounded labels, progress gauges; heartbeat: idle = 0 s, hung batch grows (AC-8.1–8.3) | FR-OBS-01 | P1 |
| TS-M-02 | Pushgateway: success/failure/DQ series; an unreachable gateway never fails a job (AC-8.4) | FR-OBS-01 | P1 |
| TS-M-03 | `promtool test rules`: each of the 8 alerts fires on its synthetic series and stays silent on healthy ones (AC-8.6) | FR-OBS-03 | P1 |
| TS-O-01 | DAG integrity inside Airflow: imports, task graph, retries, timeouts, run-scoped commands, secrets kept out of templated env (AC-7.8) | FR-DEV-01 | P1 |
| TS-O-02 | `make verify-airflow`: health/auth/DB isolation, lake_daily end to end and with a planted fault, replay valid/invalid, maintenance (AC-7.7–7.11) | FR-ANL-01, FR-REC-02, NFR-08 | P1 |
| TS-O-03 | Retention: expiry in all three stores; deleting a log-referenced bronze partition keeps bronze readable and writable (AC-7.11) | NFR-08 | P1 |
| TS-I-07 | Containerised processor: reconcile a run, graceful stop with exit 0 (`make verify-processor`, AC-7.6) | FR-DEV-01 | P1 |
| TS-P-01 | Throughput/latency benchmark per spec 09 | NFR-10 | P1 |
| TS-P-02 | 30-min soak at constant rate: heap and state rows plateau | NFR-05 | P1 |
