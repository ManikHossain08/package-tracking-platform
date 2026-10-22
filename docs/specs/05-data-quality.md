# 05 — Data quality rules

Two layers:
- **Gate rules** run in-stream and decide valid vs DLQ (FR-VAL-*).
- **Monitor rules** run in batch (`dq_checks` task). They report, alert and can fail the DAG, but never mutate data.

| ID | Dimension | Rule | Layer | Severity | Action on breach |
|---|---|---|---|---|---|
| DQ-01 | Completeness / validity | All structural rules in spec 02 §2 | Gate | Reject | DLQ with error codes |
| DQ-02 | Uniqueness | `event_id` is unique in silver per `event_date`, and in `processed_events` | Monitor | Critical | Fail `lake_daily` |
| DQ-03 | Freshness | Lag-aware (SC-0006): fail if Kafka end offsets are ahead of the offsets last committed in `ops.sink_batches` **and** the last commit is older than 15 min. An idle topic is healthy. | Monitor + alert | High | Fail DAG + alert |
| DQ-04 | Referential integrity | Every package with non-`CREATED` events has a `CREATED` event within 24 h of its first event | Monitor | Warn | `ORPHAN_EVENT` anomaly rows |
| DQ-05 | Validity (business) | Events whose ordering key is greater than the package's status-defining terminal event (`POST_TERMINAL_EVENT` rows); warn if > 1 % of the window's events | Monitor | Warn above threshold | Alert |
| DQ-06 | Reconciliation (counts) | For a closed ingest window: bronze rows = valid + invalid; valid = silver new rows + duplicates removed; DLQ distinct source offsets = bronze invalid rows | Monitor | Critical | Fail DAG |
| DQ-07 | Reconciliation (state) | For packages whose `last_event_time` is older than the 2 h watermark: `package_status` (status, status event, event count) = `StatusRule` over their silver events | Monitor | Critical | Fail DAG, list mismatching `package_id`s |
| DQ-08 | Consistency (gold) | Sum of `delivered_count` in gold = count of distinct `DELIVERED` packages in silver for the window | Monitor | Critical | Fail DAG |
| DQ-09 | Plausibility | DC dwell time ∈ [0, 14 days]; delay ∈ [−30 d, +60 d] | Monitor | Warn | Count + sample IDs |
| DQ-10 | Volume anomaly | Hourly valid-event count is within ±50% of the trailing 7-day same-hour median; `skipped` unless 7 full days of silver history exist | Monitor | Warn | Alert |

"Open window" means data still within the watermark or the batch recompute window. DQ-07
only checks closed windows, so late data does not produce false mismatches.

Exact definitions: SC-0006 A. The DQ job exits non-zero if any **critical or high** check fails; warnings never fail the DAG.

DQ results are written to `ops.dq_results` (`check_id`, `window`, `status`, `observed`,
`threshold`, `sample`, `run_id`, `checked_at`) and exported as metrics.
