# SC-0003: Make slice 1 acceptance criteria manifest-based instead of fixed counts

- **Status:** Approved (2026-10-22)
- **Affects:** spec 07 AC-1.1, AC-1.2, AC-1.5 (wording only; no requirement changes)

## 1. Change

| AC | Before | After |
|---|---|---|
| AC-1.1 | "Given 1 000 valid events for 100 packages … `event_count` = 10 per package (= manifest)" | "Given the `clean` scenario (100 packages, no faults) … every package's `current_status`, `status_event_id`, `status_event_time` and `event_count` equal the manifest; `processed_events` = `unique_valid_events`; DLQ empty" |
| AC-1.2 | "the AC-1.1 input with 50 events re-sent 1–2 extra times" | "the `it-duplicates` scenario (8 % of events re-sent 1–2 extra times), processed in ≥ 4 micro-batches … state equals the manifest; `processed_events` = `unique_valid_events`; Σ `duplicate_rows` = `duplicate_copies`" |
| AC-1.5 | "a stream of 5 000 events" | "the `it-restart` scenario (≥ 5 000 records, all fault types), 500 records per micro-batch, crash injected after the sink commit of batch 3 …" (the rest unchanged) |

## 2. Reason

FR-GEN-01 requires realistic lifecycles, which have 1–14 events per package depending on hops,
attempts and in-transit truncation. A fixed "10 per package" can't be true for realistic data.
Equality with the generator's independent oracle (FR-GEN-03) is the stronger criterion: it checks
every package's exact status, status event and count.

## 3. Impact

- **Tests:** already implemented this way (`StatusPipelineIT`); no code change.
- **Requirements, contracts, data:** none.

## Decision

Approved by the project owner on 2026-10-22. Recorded in spec 07 (AC-1.1, AC-1.2, AC-1.5).
