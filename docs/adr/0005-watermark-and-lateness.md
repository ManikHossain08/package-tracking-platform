# ADR-0005: Watermark and lateness policy

- **Status:** Accepted (2026-10-21); clock amended by SC-0004 (2026-10-22)
- **Requirements:** FR-LATE-01, FR-DED-02, FR-VAL-02, FR-ANL-01, NFR-05

## Context

In Spark, the watermark controls **when state can be evicted** for stateful operators. If
the watermark were the *correctness* boundary, every event later than it would be dropped,
and buffered vehicle scans after a long offline period would be lost.

## Decision

Separate the concerns:

| Concern | Mechanism | Value (default) |
|---|---|---|
| Bound streaming state memory | `withWatermark(event_time, 2 hours)` + `dropDuplicatesWithinWatermark(event_id)` | 2 h |
| Status correctness for late events | Postgres ordering guard; no watermark involved | Any lateness |
| Durable dedup horizon | `processed_events` retention | 30 d |
| Gold correctness for late events | Batch recompute of the last N event-dates | 7 d |
| Reject implausible clocks | `event_time > ingest_time + skew` | 5 min |
| Reject stale garbage in live mode | `event_time < ingest_time − max_age` (disabled in backfill) | 30 d |

Events behind the watermark are counted (`ptp_events_late_total`) and still applied.
`dropDuplicatesWithinWatermark` may let duplicates of very late events through to the sink,
where `processed_events` absorbs them.

## Alternatives

- **Watermark-only dropping (Spark default for aggregates):** rejected, because it causes data loss.
- **Streaming windowed gold aggregates with a large watermark (e.g. 7 d):** rejected for
  the MVP. State size scales with the watermark, and batch recompute is simpler to reason
  about and to backfill.

## Consequences

- Gold metrics for the most recent day can change for up to 7 days. Dashboards label them
  "provisional" until the window closes.
- The values are configuration (spec 08) and are revisited after the benchmark's lateness
  distribution analysis.

## Amendment 1 — validation clock = ingest time (SC-0004, accepted 2026-10-22)

The time bounds are judged against the record's Kafka `LogAppendTime` (ingest time), not the consumer's
processing time. That makes every query, re-run and replay take the same decision for a record, and a
consumer outage or lag can no longer turn valid events into `EVENT_TIME_TOO_OLD`. Replay from bronze
reproduces live decisions by default. Backfill mode (lower bound off) is opt-in.
Details: `docs/spec-changes/0004-validation-clock-ingest-time.md`.
