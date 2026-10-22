# Step 6 — silver + gold analytics: results (2026-10-22)

Acceptance criteria: spec 07, AC-6.1 … AC-6.10. Spec change approved for this step: **SC-0005**
(honest silver swap guarantee, gold fact tables, metric definitions C1–C8, generator gold oracle).

## What was built

| Area | Files |
|---|---|
| Silver | `ptp/lake/SilverBuild.scala`: incremental by per-partition Kafka offset watermark, rebuilds whole `event_date` partitions from bronze, dedup by earliest lineage, typed rows + `content_hash`, compaction to a target size, staging-directory swap |
| Gold rules (pure) | `core/gold/GoldRules.scala` (C1–C6, reusing `StatusRule`), `core/gold/ContentHash.scala` |
| Gold load | `ptp/lake/GoldLoad.scala`: Spark facts → `ops.gold_stage_*` (JDBC) → one Postgres transaction replaces the window and computes the daily aggregates with `percentile_cont`; `ops.gold_runs` |
| Schema | `V5__gold_analytics.sql`: 2 fact tables, 3 daily tables, staging, run log, `ptp_batch_writer` grants |
| SQL pack | `sql/analytics/01–05*.sql` + README; `make query` |
| Oracle | `generator/ptp_generator/gold.py`: an independent Python implementation of C1–C7, writing 5 `expected_*.jsonl` files |
| Tooling | `make silver`, `make gold`, `make lake`, `make query`; scenario `it-gold.yaml`; generator `postal_prefixes` pool |

## Acceptance results

| AC | Result | Evidence |
|---|---|---|
| AC-6.1 | ✅ | Silver event IDs = the manifest's unique valid events, each once; `event_date` = UTC date of `event_time` |
| AC-6.2 | ✅ | For every event, the kept copy = the minimum `(kafka_timestamp, partition, offset)` over all valid bronze copies |
| AC-6.3 | ✅ | Shipment fields typed on CREATED; 64-hex `content_hash`; semantic duplicates share a hash; `silver_run_id` set |
| AC-6.4 | ✅ | Two full rebuilds give identical rows; a late event rebuilds only its date (other partitions' files and mtimes untouched); a crash after staging leaves live files and rows unchanged, and the next build catches up |
| AC-6.5 | ✅ | Bronze's many small files → 1 file per date; with a 100 KiB target, several files per date, equal to the reported count |
| AC-6.6 | ✅ | Postgres facts, visits and all 3 daily tables equal the Python oracle (`it-gold`, 500 packages, groups large enough to separate p50/p90/p95) |
| AC-6.7 | ✅ | Σ `delivered_count` = `delivery_facts` rows = DELIVERED packages with CREATED in `package_status` |
| AC-6.8 | ✅ | Re-run gives identical rows; failures after staging or inside the transaction leave gold intact; `ops.gold_runs` 2 succeeded / 2 failed; staging empty |
| AC-6.9 | ✅ | A late DELIVERED inside the window creates its fact; a planted fact 20 days old keeps its `run_id` |
| AC-6.10 | ✅ | All 5 queries return hand-computed results on the fixture as `ptp_reader`; the reader can't delete analytics or read staging |

Totals: integration suite 20/20 (steps 4–6), 107 core + 10 streaming unit tests, 53 Python tests.

## Bugs found by the tests (fixed)

1. **`facility_visits` key was wrong.** The original key `(package, facility, arrived_at)` collided when an arrival had a semantic
   duplicate: two event IDs with the same timestamp, which C5 explicitly treats as two visits. The oracle comparison failed
   with a duplicate-key error. Fixed by keying visits by `arrival_event_id`, which is the approved "one row per arrival" grain.
   V5 wasn't committed yet, so it was corrected in place and the local database recreated.
2. **A test that couldn't fail.** A mutation that computed delay p95 as p90 **survived**: with random postal prefixes almost every daily group held
   one delivery, where all percentiles are equal. Added the generator's `postal_prefixes` pool (off by default; existing seeds and streams
   unchanged, proven by a unit test) and the `it-gold` scenario. The same mutation, and a dwell p95→p50 mutation, are now caught.

## Mutation checks (reverted)

| Injected bug | Caught by |
|---|---|
| Silver keeps the *latest* duplicate | AC-6.1/6.2 |
| Daily delay p95 computed as p90 | AC-6.6 (after the `it-gold` fix; it survived before) |
| Daily dwell p95 computed as p50 | AC-6.6 |

## Live run (Compose stack)

`it-gold` (3 808 records) through the live streams → `make lake` → silver 3 569 rows (= `unique_valid_events`) in 1 file
per date; gold 304 delivery facts and 1 125 visits (= the oracle's expected files); `make query` for all KPIs as `ptp_reader`.

Observed: EXPRESS 50.6 % on time vs ECONOMY 100 %. This is **a generator simplification, not a pipeline result**: transit times don't depend
on the service level, so 1-day promises are often missed. Worth fixing before a portfolio demo (follow-up).

## Known gaps

- Silver selection scans bronze (filtering on offsets), and the per-date rebuild scans all bronze for affected dates. That's fine for 90-day
  local retention; at scale, bound it with partition pruning (ingest-date lookback = maximum lateness).
- The silver swap isn't snapshot-isolated (SC-0005 A); readers are serialised with the build.
- The scheduled DQ job (`ops.dq_results`, DQ-03/04/05/09/10) and the Airflow `lake_daily` DAG are not built yet.
