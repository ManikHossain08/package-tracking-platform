# Step 2 — generator: results (2026-10-21)

Acceptance criteria: spec 07, AC-2.1 … AC-2.8. Same dev machine as step 1.

| AC | Result | Evidence |
|---|---|---|
| AC-2.1 | ✅ | TS-U-06: same seed ⇒ byte-identical stream; different seed differs |
| AC-2.2 | ✅ | TS-U-09: 7 lifecycle checks over 300 packages (CREATED first, strictly increasing times, ≤4 facilities, ≤3 attempts then return, promised dates per service level and tz, in-transit truncation, per-device sequences) |
| AC-2.3 | ✅ | TS-C-02: all ≥10 000 valid generated records pass the v1 schema |
| AC-2.4 | ✅ | TS-C-02: every structural malformed record fails the schema; runtime ones pass it but exceed the time bound |
| AC-2.5 | ✅ | TS-U-10: duplicates, semantic duplicates, out-of-order and late placements verified record by record; counts equal `summary.json` |
| AC-2.6 | ✅ | TS-U-11: hand cases + 2 000 shuffled/duplicated permutations give the same expected status. Rule per SC-0002 (approved 2026-10-21). |
| AC-2.7 | ✅ | TS-U-12: schedule `i/R`, outage hold-and-release, unthrottled mode, emitter waits via an injected clock |
| AC-2.8 | ✅ | TS-I-06 against the live stack: end offsets grew by exactly `records_total`; every record read back; keys = manifest package IDs; one partition per key; `delivery.jsonl` complete |

Totals: `make ci` → 47 Python tests passed, 3 Scala tests passed, 0 pin problems, lint clean.
`make test-it-py` → 1 passed. `make secrets-scan` → no leaks.

## Observed run (not a benchmark)

`make generate SCENARIO=bench/scenarios/outage-burst.yaml` (2 000 packages, `rate_eps: 200`, 30 s outage):
13 140 records (12 626 unique valid events, 384 duplicate copies, 631 out-of-order, 241 late,
130 malformed) published in 65.7 s at 199.9 events/s. This shows that pacing works. It is
**not** a throughput measurement; that waits for the spec 09 benchmark.

## Spec issue found

The approved status rule gave different results depending on arrival order (see SC-0002).
The fix was approved on 2026-10-21 and recorded in spec 01, spec 02 §6 and ADR-0004 (Amendment 1).
