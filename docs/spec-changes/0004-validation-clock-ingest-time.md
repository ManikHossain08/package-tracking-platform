# SC-0004: Judge time bounds against the record's Kafka ingest time, not consumer processing time

- **Status:** Approved (2026-10-22)
- **Affects:** FR-VAL-02 (spec 01), spec 02 §1 (topic config) and §2 (codes 13–14), ADR-0005, step 4 code
  (`StatusProcessor`, `Classify`), step 5 design (bronze writer, replay)

## 1. Change

| | Before (approved) | After (proposed) |
|---|---|---|
| Clock for `EVENT_TIME_IN_FUTURE` / `EVENT_TIME_TOO_OLD` | `processing_time`: when the consumer validates the record (the micro-batch start) | `ingest_time`: the record's **Kafka timestamp**, stamped by the broker |
| Kafka topic timestamps | default (`CreateTime`, set by the producer) | `message.timestamp.type=LogAppendTime` on `package-events.v1`, so the broker's clock is used and producers can't skew it |
| Live mode | `now − 30 d ≤ event_time ≤ now + 5 min` | `ingest_time − 30 d ≤ event_time ≤ ingest_time + 5 min` |
| Replay of bronze (FR-REC-02) | not specified | Default: **same rule, same clock** (stored `kafka_timestamp`), so it reproduces the live decisions exactly. Opt-in `backfill` mode = replay mode of FR-VAL-02 (lower bound off). |

## 2. Reason

With processing time as the clock:

1. **Replay can't reproduce live.** Re-validating bronze days later gives different answers near the
   bounds, and after 30 days every valid event becomes `EVENT_TIME_TOO_OLD`. Rebuilding Postgres from
   bronze (the RPO promise in NFR-03) would silently drop valid data.
2. **A consumer outage changes the data.** If the processor is down longer than the bound, valid events
   that were on time when they arrived are rejected purely because processing was late. Lag must not
   change the result.
3. **Two queries, two clocks.** ADR-0006 runs the bronze writer and the status processor as separate
   queries with separate micro-batches. Their `processing_time` differs, so `bronze.is_valid` and the
   DLQ can disagree at the boundary, which breaks the DQ-06 reconciliation.
4. **Re-runs aren't deterministic.** A re-executed micro-batch (AC-1.5) uses a new start time, so a
   boundary record could flip between DLQ and status.

The ingest timestamp is fixed once per record by the broker, so every query, re-run and replay sees
the same value. All four problems disappear.

## 3. Impact

- **Code:** `Classify` uses the record's `timestamp` instead of the batch start (a one-line change),
  and the topic init script adds `message.timestamp.type=LogAppendTime` (idempotent, applied to the existing topic).
- **Tests:** AC-3.4 unit tests are unchanged (the validator takes the clock as a parameter). AC-1.4 runtime
  cases already use `now ± margin`, which is equivalent to ingest time ± margin. A new test is added: a record
  whose event is valid at ingest stays valid when replayed 31 days later (simulated clock).
- **Generator:** unchanged. Its runtime-malformed records are relative to emit time ≈ ingest time.
- **Data already stored:** none of value (local demo data only).
- **Semantics note:** "too old" now means "the event was more than 30 days old *when it reached Kafka*".
  This is the business meaning we want (late data from devices), not an artefact of our own lag.

## 4. Alternatives considered

| Option | Why not |
|---|---|
| Keep processing time; store the live decision in bronze and have replay trust it | Two queries still disagree (#3); outages still reject valid data (#2) |
| Producer `CreateTime` as the clock | A device or producer with a wrong clock could make its own records pass or fail the bounds |
| Remove the time bounds | Loses protection against clock-skewed devices (ADR-0004 consequence) |

## Decision

Approved by the project owner on 2026-10-22. Recorded in spec 01 (FR-VAL-02), spec 02 §1–2 and ADR-0005 Amendment 1.
