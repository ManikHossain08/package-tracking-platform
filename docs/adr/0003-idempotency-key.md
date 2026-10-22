# ADR-0003: Idempotency key

- **Status:** Accepted (2026-10-21)
- **Requirements:** FR-DED-01, FR-DED-03

## Context

Duplicates arise from producer retries, device resends after reconnect, upstream replays
and our own micro-batch re-execution. We need a key that is identical for "the same
real-world event" and different otherwise.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Producer `event_id` (UUID) | Explicit, cheap, a standard contract | Relies on producers keeping it stable across retries; a buggy producer resends with a new ID |
| B. Content hash of `(package_id, event_type, event_time, facility_id, vehicle_id)` | Catches resends with new IDs | Two genuinely distinct events can collide (e.g. two scans in the same millisecond); collisions silently drop real data |
| C. Kafka coordinates `(topic, partition, offset)` | Always unique per record | Identifies the *record*, not the *event*: upstream duplicates get different offsets |

## Decision

**A is authoritative. B is computed and stored (`content_hash`) for detection and reporting
only** (`SEMANTIC_DUPLICATE` anomaly, `ptp_events_semantic_duplicate_total`). C is used for
DLQ dedup and lineage.

Dropping on B would turn a data-quality *signal* into silent data loss. If monitoring shows
semantic duplicates are common and always spurious, the policy can be tightened by a spec
change with evidence.

## Consequences

- The producer contract states that `event_id` MUST be stable across retries (spec 02).
- The generator injects both ID-stable duplicates and new-ID semantic duplicates, so both
  paths are tested.
