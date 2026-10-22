# ADR-0004: Ordering and status policy

- **Status:** Accepted (2026-10-21)
- **Requirements:** FR-ORD-01, NFR-01

## Context

Events arrive out of order. "Do not let an older event overwrite a newer state" requires a
precise definition of "older", and a policy for nonsensical sequences.

## Options for the ordering basis

| Option | Problem |
|---|---|
| Arrival order (Kafka offset / processing time) | Wrong by definition for late events |
| `emitted_at` | Device buffering makes it unrelated to when the scan happened |
| `source.sequence` | Only comparable within one device; packages move between devices |
| **`event_time`** + deterministic tiebreakers | Reflects the physical world; ties need resolving |

## Decision

1. Ordering key = `(event_time_utc, lifecycle_rank, event_id)`. Ranks are in spec 01.
   Using the rank before `event_id` makes simultaneous events resolve meaningfully
   (`DELIVERED` beats `OUT_FOR_DELIVERY` at the same instant). `event_id` makes the key total.
2. Terminal states (`DELIVERED`, `RETURNED_TO_SENDER`) are **sticky**. Later events are
   recorded and flagged `POST_TERMINAL_EVENT`, but do not change status.
3. Transition validity (e.g. `DELIVERED` without `OUT_FOR_DELIVERY`) is **not** enforced
   at apply time. Missing intermediate scans are common in reality and rejecting them would
   lose real deliveries. Plausibility is monitored in batch (DQ-05, DQ-09).
4. `CREATED` attributes merge whenever `CREATED` arrives, independent of the ordering result.

**Refinement vs Phase 1:** the Phase 1 proposal used `source.sequence` as a tiebreaker.
Sequence numbers are per device and not comparable across devices, so the lifecycle rank
replaces them. `source.sequence` remains for diagnostics.

## Consequences

- A clock-skewed device with timestamps far in the future could "win" incorrectly. This is
  mitigated by the +5 min future-skew rejection (FR-VAL-02).
- Sticky terminal states mean a genuine correction (e.g. delivered to the wrong address,
  then re-delivered) needs an explicit correction event type in a future v1.x (open question Q1).
- The ordering function lives in `processor-core` and is mirrored in SQL. A test asserts
  that the Scala comparator and the Postgres row comparison agree on random inputs.
