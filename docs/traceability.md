# Traceability matrix

> Maintained by hand until `scripts/check_traceability.py` (FR-DEV-03) exists, which will
> generate this file from requirement tags in tests. Implementation and test columns are
> empty until the step that implements them. Steps 1–8 are done (4 = first vertical slice; 5 = bronze + replay; 6 = silver + gold; 7 = DQ, jobs image, Airflow; 8 = observability).

| Requirement | Pri | Spec | ADR | Implementation | Planned tests | Status |
|---|---|---|---|---|---|---|
| FR-GEN-01 | P0 | 01 | — | `generator/ptp_generator/{lifecycle,config,emit}.py` | TS-U-06 ✅, TS-U-09 ✅, TS-U-12 ✅, TS-I-06 ✅ | ✅ Verified |
| FR-GEN-02 | P0 | 01, 02 §6 | — | `generator/ptp_generator/{plan,render,emit}.py` | TS-U-10 ✅, TS-U-12 ✅, TS-C-02 ✅ | ✅ Verified |
| FR-GEN-03 | P1 | 01, 02 §6 | 0004 (SC-0002), SC-0005 D | `generator/ptp_generator/{oracle,gold,manifest}.py` | TS-U-11 ✅, gold oracle tests ✅, TS-D-03 ✅ (gold = oracle); TS-D-02 planned | ✅ Verified |
| FR-ING-01 | P0 | 01, 02 §1–2 | — | `generator/ptp_generator/sinks.py`; Kafka source in `StatusProcessor` | TS-C-02 ✅, TS-I-06 ✅, TS-I-01 ✅ | ✅ Verified |
| FR-ING-02 | P1 | 01, 02 §1 | — | `infra/kafka/create-topics.sh`, `infra/docker-compose.yml` | `make verify` (AC-0.2–0.4) | ✅ Verified |
| FR-VAL-01 | P0 | 01, 02 §2 | — | `EventValidator.scala`, `Classify.scala` | TS-C-01 ✅, TS-C-04 ✅, TS-U-01 ✅, TS-U-02 ✅, TS-U-13 ✅, TS-I-02 ✅ | ✅ Verified |
| FR-VAL-02 | P0 | 01 | 0005 | `EventValidator.timeBounds`; batch clock fixed on the driver (`StatusProcessor`) | TS-U-03 ✅, TS-C-04 ✅, TS-I-02 ✅ | ✅ Verified |
| FR-VAL-03 | P0 | 01, 02 §2 | — | `Rejection.primary`, `ErrorCode.precedence` | TS-U-01 ✅, TS-U-02 ✅ | ✅ Verified |
| FR-DLQ-01 | P0 | 01, 02 §3 | 0002 | `DlqRecord.scala`, DLQ write in `StatusProcessor` | DlqRecordSpec ✅, TS-I-02 ✅ (schema-valid, offsets), dirty-stream offset reconciliation ✅ | ✅ Verified |
| FR-DLQ-02 | P1 | 01 | — | invalid rows never reach `PostgresStatusSink`; kept in bronze with `is_valid = false` | TS-I-02 ✅, TS-I-05 ✅ | ✅ Verified (silver/gold part when built) |
| FR-STR-01 | P0 | 01, 03 | 0006 | `StatusProcessor.scala`, `ProcessorConfig.scala`, `StatusProcessorApp.scala` | TS-I-01 ✅, ProcessorConfigSpec ✅, live run on Compose ✅ | ✅ Verified |
| FR-DED-01 | P0 | 01, 02 §4 | 0002, 0003 | `tracking.processed_events`, `PostgresStatusSink.insertProcessed` | TS-I-03 ✅, TS-U-05 ✅ (SinkPropertyIT), TS-R-01 ✅ | ✅ Verified |
| FR-DED-02 | P1 | 01 | 0005 | — | TS-P-02 | Specified |
| FR-DED-03 | P1 | 01 | 0003 | — | TS-I-03 | Specified |
| FR-ORD-01 | P0 | 01 | 0004 | `StatusRule.scala`, SQL `Preferred` in `PostgresStatusSink` | TS-U-04 ✅, TS-U-05 ✅, TS-I-04 ✅, manifest equality in every IT ✅ | ✅ Verified |
| FR-LATE-01 | P1 | 01 | 0005 | late events applied by the preference rule | dirty-stream IT ✅ (late events reconcile); watermark + `late` counter pending | Partial |
| FR-STO-01 | P1 | 01, 02 §5 | 0006 | `BronzeWriter.scala` (`BronzeRow`), `BestEffortFields.scala` | TS-I-05 ✅ (AC-5.1–5.3), TS-R-08 ✅ (AC-5.4), live run ✅ | ✅ Verified |
| FR-STO-02 | P1 | 01, 02 §5 | SC-0005 A | `ptp/lake/SilverBuild.scala`, `core/gold/ContentHash.scala` | TS-D-04 ✅ (AC-6.1–6.5), live run ✅ | ✅ Verified |
| FR-ANL-01 | P1 | 01, 02 §4 | 0005, SC-0005 B/C | `ptp/lake/GoldLoad.scala`, `core/gold/GoldRules.scala`, migration V5 | TS-D-03 ✅ (AC-6.6–6.9), GoldRulesSpec ✅, live run ✅ | ✅ Verified |
| FR-ANL-02 | P1 | 01 | SC-0005 B | `sql/analytics/01–05*.sql`, `make query` | TS-D-05 ✅ (AC-6.10, as `ptp_reader`) | ✅ Verified |
| FR-REC-01 | P0 | 01, 06 | 0002 | checkpoint per query; idempotent sink | TS-R-01 ✅ (crash after sink commit, re-run batch 3, attempts = 2); TS-R-05 pending | ✅ Verified (TS-R-05 pending) |
| FR-REC-02 | P1 | 01, 03 | 0006, 0005 (SC-0004) | `Replay.scala`, `StatusApply.scala`, `ops.replay_runs` (V4) | TS-R-04 ✅ (AC-5.5–5.7), ClassifySpec ✅, live data-loss rebuild ✅, `replay_from_bronze` DAG ✅ (TS-O-02) | ✅ Verified |
| FR-REC-03 | P1 | 01, 06 | 0002 | — | TS-R-02 | Specified |
| FR-REC-04 | P2 | 01, 06 | — | — | TS-R-07 | Specified |
| FR-OBS-01 | P1 | 01, 04 | SC-0007 | `ProcessorMetrics.scala` (endpoint, progress listener, heartbeat), `lake/BatchMetrics.scala` (Pushgateway) | TS-M-01 ✅ (exact counters vs generator), heartbeat test ✅ (idle vs hung batch), TS-M-02 ✅ (pushes), `make verify-obs` ✅ | ✅ Verified (late/semantic-dup/state/retry metrics arrive with their features) |
| FR-OBS-02 | P0 | 01, 06 | — | `StructuredLog.scala`, `ptp.events` logger | TS-U-08 ✅, AC-1.8 in TS-R-01 ✅ | ✅ Verified |
| FR-OBS-03 | P2 | 01 | SC-0007 | `infra/observability/` (Prometheus config, 8 alert rules, Grafana provisioning + 2 dashboards) | TS-M-03 ✅ (`promtool test rules`), live alerts ✅ (processor down, DQ failure, no false stall on idle), 22 panel queries ✅ | ✅ Verified (no Alertmanager by decision) |
| FR-DEV-01 | P0 | 01, 08 | 0001, 0009 | `Makefile`, `infra/`, `processor/build.sbt`, `infra/jobs/` (ptp-jobs image) | `SparkRuntimeSmokeSpec` ✅, `make verify` ✅, `make verify-processor` ✅ (AC-7.6: reconcile + graceful stop, exit 0) | ✅ Verified (core, stream, orchestration profiles) |
| FR-DEV-02 | P0 | 01 | — | `.github/workflows/ci.yml` (checks, scala, integration, stack jobs) | CI run pending: no GitHub remote yet | Implemented, not yet run |
| FR-DEV-03 | P1 | 01 | — | — | CI | Specified |
| NFR-01 | — | 04 | 0002 | `StatusRule` + SQL mirror | TS-U-05 ✅ (Scala property + SQL property); TS-R-04 pending | ✅ Verified (replay pending) |
| NFR-02 | — | 04 | 0002 | per-sink semantics as in ADR-0002 | TS-R-01 ✅, DLQ/status reconciliation ✅; TS-R-02..05 pending | Partial |
| NFR-03 | — | 04 | — | checkpoint restart; rebuild from bronze | TS-R-01 ✅, TS-R-04 ✅ (RPO 0 for data in bronze); TS-R-06 + RTO measurement pending | Partial |
| NFR-04 | — | 02, 04 | — | `additionalProperties: true` in event schema | unknown-field contract test ✅; TS-C-03 planned | Partial |
| NFR-05 | — | 04 | 0005 | — | TS-P-02 | Specified |
| NFR-06 | — | 04, 06 | — | `.env.example`, `make env`, `.gitleaks.toml` | `make secrets-scan` ✅ | ✅ Verified |
| NFR-07 | — | 04 | 0001 | digests in compose, `build.sbt` vals, hashed `requirements-dev.txt`, SHA-pinned actions | `make check-pins` ✅ | ✅ Verified |
| NFR-08 | — | 04 | SC-0006 C | `ptp/lake/Retention.scala`, V6 `drop_processed_events_partitions_before`, bronze sink `retention` + `BronzeRow.read`, `maintenance` DAG | TS-O-03 ✅ (RetentionIT: expiry + log-referenced deletion), maintenance DAG run ✅ | ✅ Verified |
| NFR-09 | — | 04, 06 | — | — | TS-U-08 | Specified |
| NFR-10 | — | 04, 09 | — | — | TS-P-01 | Specified |
| NFR-11 | — | 04, 08 | — | — | config test | Specified |
| DQ-01..10 | — | 05 | SC-0006 | `ptp/lake/DqJob.scala`, `core/dq/DqRules.scala`, migration V6 (`ops.dq_results`, `tracking.status_anomalies`) | TS-D-06 ✅ (clean data passes, anomalies = oracle, idempotent), TS-D-07 ✅ (each of DQ-02…09 catches a planted fault; exit codes), DqRulesSpec ✅ (DQ-10 logic); scheduling via Airflow in 7b | ✅ Verified (DQ-10 only by unit test: needs 7 days of history) |
| ADR-0008/0009 orchestration | — | 03, 08 | 0008, 0009 | `orchestration/airflow/dags/`, `infra/docker-compose.yml` (airflow, docker-proxy) | TS-O-01 ✅ (check_dags.py, 43 checks), TS-O-02 ✅ (verify_airflow.sh: lake_daily success + planted-fault failure at dq, replay valid/invalid, maintenance) | ✅ Verified |
