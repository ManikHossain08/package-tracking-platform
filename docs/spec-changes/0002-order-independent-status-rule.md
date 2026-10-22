# SC-0002: Make the status rule order-independent; add generator manifest files

- **Status:** Approved (2026-10-21) — parts A and B
- **Affects:** spec 01 §Status ordering rule, ADR-0004 (Decision 1–2), spec 02 §6, NFR-01, AC-1.6

## Part A — status rule

### 1. Change

**Before (spec 01, ADR-0004):** an event becomes the current status iff
`key(new) > key(stored)` **and** the stored status is not terminal.

**After:** the current status is the **most preferred** event seen so far, using this
total preference order (most preferred first):

1. Any terminal event (`DELIVERED`, `RETURNED_TO_SENDER`) is preferred over any non-terminal event.
2. Between two terminal events, the one with the **smaller** ordering key wins.
   The *first* terminal outcome in event time is sticky.
3. Between two non-terminal events, the one with the **larger** ordering key wins.
   This is the latest known position.

The ordering key is unchanged: `(event_time_utc, lifecycle_rank, event_id)`.

### 2. Reason

While building the generator's independent ground truth (FR-GEN-03), I found the
approved rule gives **different final states for different arrival orders**. That
contradicts NFR-01 and makes AC-1.6 impossible to pass:

| Arrival order | `DELIVERED@10:00` then `OUT_FOR_DELIVERY@11:00` | `OUT_FOR_DELIVERY@11:00` then `DELIVERED@10:00` |
|---|---|---|
| Approved rule | `DELIVERED` (sticky) | `OUT_FOR_DELIVERY`: the later-arriving `DELIVERED` has a smaller key, so it is rejected |
| Proposed rule | `DELIVERED` | `DELIVERED` |

Taking the maximum under a fixed total order never depends on arrival order, duplicates or
batch boundaries. This is the property that makes replay and recovery safe (ADR-0002).

### 3. Impact

- **Code:** none yet. The processor is implemented in steps 3–4 directly against the new rule.
  The generator oracle (step 2) uses it.
- **SQL guard (spec 03 step 4.4):** the `CASE` condition becomes "new event is preferred
  over stored" instead of "new key > stored key and not terminal". Same cost.
- **`POST_TERMINAL_EVENT` anomaly:** unchanged in meaning. It applies to any event whose key
  is greater than the winning terminal's key, regardless of arrival order.
- **Q1 business assumption:** unchanged. The first terminal still wins and later events are flagged.
- **Data already stored:** none.

## Part B — generator manifest files (spec 02 §6)

### 1. Change

Additive. Keep the per-package `manifest.jsonl` as specified, and add:
- `summary.json`: run totals, used for exact count reconciliation (KPI-P2);
- `malformed.jsonl`: one row per malformed record, with its emission index, fault type
  and expected primary `error_code`;
- `delivery.jsonl` (Kafka sink only): the emission index → Kafka partition/offset, so later
  tests can match DLQ `source.offset` (AC-1.4).

### 2. Reason

Malformed records may have no valid `package_id`, so they cannot live in a per-package
manifest. Exact offsets are only known after Kafka acknowledges the write.

### 3. Impact

Additive only. No existing field changes.

## Decision

Approved by the project owner on 2026-10-21 (both parts). Recorded in spec 01, spec 02 §6,
spec 03 (apply algorithm step 4) and ADR-0004 Amendment 1.
