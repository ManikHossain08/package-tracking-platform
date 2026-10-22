# Traceability matrix

> Maintained by hand until `scripts/check_traceability.py` (FR-DEV-03) exists, which will
> generate this file from requirement tags in tests. Implementation and test columns are
> empty until the step that implements them. Steps 1–4 are done; step 4 completed the first vertical slice.

| Requirement | Pri | Spec | ADR | Implementation | Planned tests | Status |
|---|---|---|---|---|---|---|
| FR-GEN-01 | P0 | 01 | — | `generator/ptp_generator/{lifecycle,config,emit}.py` | TS-U-06 ✅, TS-U-09 ✅, TS-U-12 ✅, TS-I-06 ✅ | ✅ Verified |
| FR-GEN-02 | P0 | 01, 02 §6 | — | `generator/ptp_generator/{plan,render,emit}.py` | TS-U-10 ✅, TS-U-12 ✅, TS-C-02 ✅ | ✅ Verified |
| FR-GEN-03 | P1 | 01, 02 §6 | 0004 (SC-0002) | `generator/ptp_generator/{oracle,manifest}.py` | TS-U-11 ✅; TS-D-02 planned | ✅ Verified |
| FR-ING-01 | P0 | 01, 02 §1–2 | — | `generator/ptp_generator/sinks.py`; Kafka source in `StatusProcessor` | TS-C-02 ✅, TS-I-06 ✅, TS-I-01 ✅ | ✅ Verified |
| FR-ING-02 | P1 | 01, 02 §1 | — | `infra/kafka/create-topics.sh`, `infra/docker-compose.yml` | `make verify` (AC-0.2–0.4) | ✅ Verified |
| FR-VAL-01 | P0 | 01, 02 §2 | — | `EventValidator.scala`, `Classify.scala` | TS-C-01 ✅, TS-C-04 ✅, TS-U-01 ✅, TS-U-02 ✅, TS-U-13 ✅, TS-I-02 ✅ | ✅ Verified |
| FR-VAL-02 | P0 | 01 | 0005 | `EventValidator.timeBounds`; batch clock fixed on the driver (`StatusProcessor`) | TS-U-03 ✅, TS-C-04 ✅, TS-I-02 ✅ | ✅ Verified |
| FR-VAL-03 | P0 | 01, 02 §2 | — | `Rejection.primary`, `ErrorCode.precedence` | TS-U-01 ✅, TS-U-02 ✅ | ✅ Verified |
| FR-DLQ-01 | P0 | 01, 02 §3 | 0002 | `DlqRecord.scala`, DLQ write in `StatusProcessor` | DlqRecordSpec ✅, TS-I-02 ✅ (schema-valid, offsets), dirty-stream offset reconciliation ✅ | ✅ Verified |
| FR-DLQ-02 | P1 | 01 | — | invalid rows never reach `PostgresStatusSink` | TS-I-02 ✅ (status empty after invalid-only input); bronze part pending | Partial |
| FR-STR-01 | P0 | 01, 03 | 0006 | `StatusProcessor.scala`, `ProcessorConfig.scala`, `StatusProcessorApp.scala` | TS-I-01 ✅, ProcessorConfigSpec ✅, live run on Compose ✅ | ✅ Verified |
| FR-DED-01 | P0 | 01, 02 §4 | 0002, 0003 | `tracking.processed_events`, `PostgresStatusSink.insertProcessed` | TS-I-03 ✅, TS-U-05 ✅ (SinkPropertyIT), TS-R-01 ✅ | ✅ Verified |
| FR-DED-02 | P1 | 01 | 0005 | — | TS-P-02 | Specified |
| FR-DED-03 | P1 | 01 | 0003 | — | TS-I-03 | Specified |
| FR-ORD-01 | P0 | 01 | 0004 | `StatusRule.scala`, SQL `Preferred` in `PostgresStatusSink` | TS-U-04 ✅, TS-U-05 ✅, TS-I-04 ✅, manifest equality in every IT ✅ | ✅ Verified |
| FR-LATE-01 | P1 | 01 | 0005 | late events applied by the preference rule | dirty-stream IT ✅ (late events reconcile); watermark + `late` counter pending | Partial |
| FR-STO-01 | P1 | 01, 02 §5 | 0006 | — | TS-I-05 | Specified |
| FR-STO-02 | P1 | 01, 02 §5 | — | — | TS-D-04 | Specified |
| FR-ANL-01 | P1 | 01, 02 §4 | 0005 | — | TS-D-03, TS-D-04 | Specified |
| FR-ANL-02 | P1 | 01 | — | — | TS-D-03 | Specified |
| FR-REC-01 | P0 | 01, 06 | 0002 | checkpoint per query; idempotent sink | TS-R-01 ✅ (crash after sink commit, re-run batch 3, attempts = 2); TS-R-05 pending | ✅ Verified (TS-R-05 pending) |
| FR-REC-02 | P1 | 01, 03 | 0006 | — | TS-R-04 | Specified |
| FR-REC-03 | P1 | 01, 06 | 0002 | — | TS-R-02 | Specified |
| FR-REC-04 | P2 | 01, 06 | — | — | TS-R-07 | Specified |
| FR-OBS-01 | P1 | 01, 04 | — | — | metric contract test | Specified |
| FR-OBS-02 | P0 | 01, 06 | — | `StructuredLog.scala`, `ptp.events` logger | TS-U-08 ✅, AC-1.8 in TS-R-01 ✅ | ✅ Verified |
| FR-OBS-03 | P2 | 01 | — | — | — | Specified |
| FR-DEV-01 | P0 | 01, 08 | 0001 | `Makefile`, `infra/`, `processor/build.sbt` | `SparkRuntimeSmokeSpec` ✅, `make verify` ✅ | ✅ Verified (core profile) |
| FR-DEV-02 | P0 | 01 | — | `.github/workflows/ci.yml` (checks, scala, integration, stack jobs) | CI run pending: no GitHub remote yet | Implemented, not yet run |
| FR-DEV-03 | P1 | 01 | — | — | CI | Specified |
| NFR-01 | — | 04 | 0002 | `StatusRule` + SQL mirror | TS-U-05 ✅ (Scala property + SQL property); TS-R-04 pending | ✅ Verified (replay pending) |
| NFR-02 | — | 04 | 0002 | per-sink semantics as in ADR-0002 | TS-R-01 ✅, DLQ/status reconciliation ✅; TS-R-02..05 pending | Partial |
| NFR-03 | — | 04 | — | — | TS-R-01, TS-R-06 | Specified |
| NFR-04 | — | 02, 04 | — | `additionalProperties: true` in event schema | unknown-field contract test ✅; TS-C-03 planned | Partial |
| NFR-05 | — | 04 | 0005 | — | TS-P-02 | Specified |
| NFR-06 | — | 04, 06 | — | `.env.example`, `make env`, `.gitleaks.toml` | `make secrets-scan` ✅ | ✅ Verified |
| NFR-07 | — | 04 | 0001 | digests in compose, `build.sbt` vals, hashed `requirements-dev.txt`, SHA-pinned actions | `make check-pins` ✅ | ✅ Verified |
| NFR-08 | — | 04 | — | — | maintenance DAG test | Specified |
| NFR-09 | — | 04, 06 | — | — | TS-U-08 | Specified |
| NFR-10 | — | 04, 09 | — | — | TS-P-01 | Specified |
| NFR-11 | — | 04, 08 | — | — | config test | Specified |
| DQ-01..10 | — | 05 | — | — | TS-D-01..04 | Specified |
