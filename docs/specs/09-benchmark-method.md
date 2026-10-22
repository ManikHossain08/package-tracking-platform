# 09 — Benchmark method

**No performance targets exist yet** (NFR-10). This spec defines how a baseline is measured.
Targets will be proposed as a spec change once the baseline results exist.

## Environment recording

Every result file records:
- the host: CPU model, cores, RAM (dev machine: Apple M4, 10 cores, 16 GB);
- OS and Docker versions, and the Docker memory/CPU limits;
- container limits per service and JVM flags;
- the git commit and the full scenario file.

## Scenarios (`bench/scenarios/*.yaml`)

Parameters: `packages`, `events_per_package` (derived from the lifecycle model), `rate_eps`
(fixed or step function), `duration`, `duplicate_pct`, `out_of_order_pct`, `late_pct` and
its delay distribution, `malformed_pct`, `burst` (outage simulation), `max_offsets_per_trigger`,
`trigger_interval`, and `seed`.

Initial scenario set:

| Name | Purpose |
|---|---|
| `smoke` | 1 k events, all fault types; correctness only |
| `steady-ramp` | Step `rate_eps` up until lag grows without bound; finds the sustainable rate |
| `dirty-steady` | 70% of the sustainable rate, 5% duplicates, 10% out-of-order, 2% late, 1% malformed |
| `outage-burst` | 30 min buffered, then a flood; measures catch-up (TS-R-06) |
| `soak` | Constant 70% of the sustainable rate for 30 min (TS-P-02) |
| `failure-injection` | Kill the processor and stop Postgres at fixed offsets; recovery behaviour |

## Metrics and how each is measured

| Metric | Measurement |
|---|---|
| Input events/s | Producer-side counter (generator), plus Kafka end-offset delta over time |
| Processing throughput | `ptp_processed_rows_total` rate; `numInputRows / batchDuration` from query progress |
| End-to-end latency | Per batch: `sink_batches.committed_at − emitted_at` of events in that batch, sampled into a histogram, reported as p50/p95/p99. Producer and processor share the host clock locally, so skew is negligible. This assumption is stated in results. |
| Duplicate / rejected counts | Pipeline counters vs the generator manifest (they must match exactly) |
| Memory / CPU | `docker stats` sampler at 1 s, plus JVM heap from metrics |
| Recovery behaviour | Time from restart to the first committed batch; time until `offsets_behind_latest` returns to the pre-failure baseline; correctness check vs the manifest |

## Method

- Warm-up 2 min, excluded from results. The measurement window is fixed per scenario.
  Each scenario runs 3 times; report the median and the range.
- A result is **invalid** if the correctness checks (manifest reconciliation) fail. A fast
  but wrong run is not a result.
- Results go to `bench/results/<date>-<commit>-<scenario>.json`, with a human summary in
  `bench/RESULTS.md`.
