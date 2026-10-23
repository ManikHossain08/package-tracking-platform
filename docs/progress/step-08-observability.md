# Step 8 — observability: results (2026-10-23)

Acceptance criteria: spec 07, AC-8.1 … AC-8.9. Spec change approved for this step: **SC-0007** (metric scope, Pushgateway for
batch jobs, rules-only alerting without Alertmanager, read-only Postgres for Grafana), with an implementation note on the stall signal.

## What was built

| Area | Files |
|---|---|
| Processor metrics | `ProcessorMetrics.scala`: `/metrics` (prometheus-metrics 1.9.0), counters per micro-batch, Spark progress listener, **driver heartbeat** (`ptp_trigger_active_seconds`, `ptp_heartbeat_timestamp_seconds`) |
| Batch metrics | `lake/BatchMetrics.scala`: Pushgateway `pushAdd` per task (silver, gold, dq, replay, maintenance), DQ check status, anomaly counts; best effort |
| Prometheus | `infra/observability/prometheus.yml`, `rules/ptp-alerts.yml` (8 rules), `rules/ptp-alerts.test.yml` (`make test-alerts`) |
| Grafana | Provisioned datasources (Prometheus; Postgres as `ptp_reader`) and dashboards **PTP — Streaming**, **PTP — Lake & Data Quality** |
| Schema | `V7__reader_run_history.sql`: `ptp_reader` may read the four `ops` run logs (not staging) |
| Checks | `MetricsIT` (3 tests), `scripts/verify_obs.sh`, `scripts/check_dashboards.py`; CI runs `test-alerts` and `verify-obs` |

## Acceptance results

| AC | Result | Evidence |
|---|---|---|
| AC-8.1 | ✅ | HELP/TYPE for every family; label names ⊆ {query, error_code, stage, sink, le} |
| AC-8.2 | ✅ | Over a dirty generator run, counter deltas equal `records_total`, `valid_records`, `unique_valid_events`, `duplicate_copies`, `malformed_by_error_code` (per code) and the `ops.sink_batches` sums |
| AC-8.3 | ✅ | Histogram count = committed batches; offsets behind = 0 when caught up; progress recent for both queries. Heartbeat (real Spark): an idle query reads 0 s active; a deliberately hung batch reads > 2 s and returns to ~0 when released. |
| AC-8.4 | ✅ | Real Pushgateway: success series, a failed run without a false last-success time, DQ-07 critical = 1, DQ-09 warning = 0, anomaly counts; an unreachable gateway doesn't fail the job |
| AC-8.5 | ✅ | Both targets up; `promtool check config` including rules |
| AC-8.6 | ✅ | `promtool test rules`: all 8 alerts fire on their series; idle, draining, 2 % DLQ and warning-only cases stay silent |
| AC-8.7 | ✅ | Live: a planted duplicate silver file → `PtpDqCriticalFailure` firing; processor stopped → `PtpProcessorDown` firing; after 150 s idle **no** stall alert |
| AC-8.8 | ✅ | Grafana healthy; both dashboards provisioned by uid; both datasources healthy; **all 22 panel queries execute** through Grafana's query API; every `ptp_*` metric referenced by dashboards and rules exists in Prometheus |
| AC-8.9 | ✅ | Prometheus/Pushgateway/Grafana bound to 127.0.0.1; anonymous Grafana API → 401; the Postgres datasource user is `ptp_reader` |

Totals: integration 27/27, unit 110 core + 10 streaming, Python 54 + 1 integration, `make verify-obs` 21/21, `make test-alerts` 8 rules,
lint/pins/secrets clean.

## Issues found and fixed

1. **The stall alert was wrong, and only a live run could show it.** Spark posts progress only when a micro-batch runs. "Time since last
   progress > 2 min" would fire for every healthy idle stream, and has no series at all after a restart without new data, so a hang
   then would go unnoticed. The `promtool` tests passed because synthetic series can't model that runtime behaviour. Fixed with a
   driver heartbeat that samples each query's status (`ptp_trigger_active_seconds` = how long the current trigger has run) plus
   `PtpDriverUnresponsive`. Verified with real Spark (idle vs hung batch) and live (150 s idle → no alert).
2. **Labelled counters have no series until their first increment**, so `ptp_sink_failures_total` didn't exist on a healthy processor,
   which breaks `rate()`/`increase()` and the "referenced metrics exist" check. Known label values are now pre-initialised at 0.
3. **Race in my own check:** pushed batch metrics appear only after the next Pushgateway scrape (10 s). The check now waits for the
   condition (bounded) instead of reading once.
4. **Empty result frames** (a valid "nothing failing" panel) crashed the dashboard checker. It now handles them.

## Notes

- No Alertmanager (decision C): rule state is visible in Prometheus and Grafana; routing to email/Slack/PagerDuty is the production step.
- Spec 04 metrics for unbuilt features (late events, stream semantic duplicates, state rows, sink retries) arrive with those features
  (SC-0007 A).
- The host-run processor (`make process`) binds its endpoint to 127.0.0.1; Prometheus scrapes it through `host.docker.internal` on a
  best-effort basis. The container is the primary target.
- Memory: Airflow was stopped while the observability stack and the processor ran (Docker Desktop still at 7.7 GiB).
