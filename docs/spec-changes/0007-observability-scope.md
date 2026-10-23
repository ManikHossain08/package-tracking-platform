# SC-0007: Observability scope, batch-job metrics and alert delivery

- **Status:** Approved (2026-10-22): A, B, C (rules only, no Alertmanager), D (read-only Postgres for Grafana)
- **Affects:** spec 04 §Metrics (FR-OBS-01), FR-OBS-03, spec 03 (components), spec 06 (`ptp_reader` grants), SC-0006 D

## A. Metric set: export what exists now; the rest arrives with its feature

Spec 04 lists metrics for features that aren't built yet (they're open P1 items). Exporting placeholder zeros
would mislead dashboards and alerts, so I propose:

| Spec 04 metric | Step 8 | Why |
|---|---|---|
| `ptp_input_rows_total`, `ptp_events_valid_total`, `ptp_events_rejected_total{error_code}` | ✅ | from each micro-batch |
| `ptp_events_duplicate_total{stage}` | ✅ `stage="sink"` only | in-stream dedup (`stage="stream"`, FR-DED-02) isn't built |
| `ptp_processed_rows_total` | ✅ as `ptp_events_applied_total` (renamed: "processed" was ambiguous) | applied = new to the platform |
| `ptp_batch_duration_seconds` (histogram) | ✅ | driver timing per micro-batch |
| `ptp_offsets_behind_latest` (gauge) | ✅ | Spark Kafka source progress (`maxOffsetsBehindLatest`) |
| `ptp_last_progress_timestamp_seconds` | ✅ per `query` | stall detection, both queries |
| `ptp_sink_failures_total{sink}` | ✅ | failure stage of a micro-batch |
| `ptp_events_late_total` | ⏳ with FR-LATE-01 | lateness counting isn't implemented |
| `ptp_events_semantic_duplicate_total` | ⏳ with FR-DED-03 (stream) | detected only in batch today (DQ/anomalies) |
| `ptp_state_rows` | ⏳ with FR-DED-02 | no stateful operator exists yet |
| `ptp_sink_retries_total{sink}` | ⏳ with FR-REC-03 | no retry logic yet |
| `ptp_status_anomalies_total{code}` | ✅ as a **batch** metric from DQ (B) | anomalies are computed by the DQ job |

## B. Batch jobs → Prometheus Pushgateway (new local service)

Silver, gold, dq, replay and maintenance are short-lived containers, so Prometheus can't scrape them. They push at the end of
each run (the standard pattern for batch jobs), grouped by `job` + `task`:
`ptp_batch_last_success_timestamp_seconds`, `ptp_batch_duration_seconds`, `ptp_batch_success` (1/0),
`ptp_dq_check_failed{check_id,severity}` (1/0), `ptp_status_anomalies{code}`. A push failure is logged and **never fails the job**.
This replaces the "deferred" note in SC-0006 D.

## C. Alerts: Prometheus rules, visible in Prometheus and Grafana; no notification routing

Rules: `PtpProcessorDown`, `PtpQueryStalled`, `PtpConsumerLagGrowing`, `PtpDlqRateHigh`, `PtpSinkFailures`,
`PtpDqCriticalFailure`, `PtpLakeStale`. Each is unit-tested with `promtool test rules`.
**No Alertmanager:** routing to email/Slack/PagerDuty needs an external service (excluded by the brief). The rules and their
firing state are the deliverable, and Alertmanager is the documented production step.

## D. Grafana reads run history from Postgres (read-only)

Dashboards: **Streaming** (Prometheus) and **Lake & DQ** (Prometheus + Postgres tables `ops.dq_results`, `ops.gold_runs`,
`ops.replay_runs`, `ops.sink_batches`). Migration V7 grants `ptp_reader` `SELECT` on those four run-log tables only (not
staging, not bronze/DLQ). The Grafana Postgres datasource uses `ptp_reader`.

## Versions (verified 2026-10-22)

`io.prometheus:prometheus-metrics-{core,exporter-httpserver,exporter-pushgateway}` 1.9.0 (1.10.0 was six days old); `prom/pushgateway:v1.11.4`
(v1.11.5 is one day old); Prometheus v3.15.1 and Grafana 13.2.4 as already pinned in ADR-0001.

**Implementation note (step 8): stall signal corrected.** Spark posts query progress only when a micro-batch runs. "Time since
last progress" therefore can't tell an idle query from a hung one: it raises false alarms when idle and has no series at all after
a restart without data. Found during live verification. `PtpQueryStalled` now uses `ptp_trigger_active_seconds`, sampled by a driver
heartbeat (a trigger active for more than 5 min = a hung batch), and a new `PtpDriverUnresponsive` covers a stale heartbeat while the
endpoint is up. The alert intent and names from part C are unchanged; there are now 8 rules.

## Decision

Approved by the project owner on 2026-10-22, all parts as proposed. Recorded in spec 04 §Metrics, spec 03, spec 06 and spec 07.
