# ADR-0005: Watermark and lateness policy

- **Status:** Accepted (2026-10-21)
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
| Reject implausible clocks | `event_time > now + skew` | 5 min |
| Reject stale garbage in live mode | `event_time < now − max_age` (disabled in replay) | 30 d |

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
