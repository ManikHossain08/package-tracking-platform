# SC-0005: Silver/gold design details, metric definitions and an independent gold oracle

- **Status:** Approved (2026-10-22) — all parts, C1–C8
- **Affects:** spec 00 (KPI-B1…B4 wording), spec 01 (FR-STO-02, FR-ANL-01/02), spec 02 §4 (analytics tables),
  §5 (silver), §6 (generator outputs), spec 06 (roles), spec 07 (AC-6.x)

Implementing silver and gold showed that the specs leave business definitions open and overclaim one
technical guarantee. Each part below can be approved or changed independently.

## Part A — silver partition replace is a safe swap, not strictly atomic (correction)

**Before (spec 02 §5):** "Silver is rewritten atomically per partition by the batch job, so any Parquet reader can use it."

**After:** each `event_date` partition is written to a staging directory and swapped in with two directory
renames. A reader never sees **duplicate or partial** data, but during the swap (microseconds) the partition
can briefly appear **empty**. Readers are therefore serialised with the build (the `lake_daily` DAG runs
silver → gold in sequence). If concurrent readers ever need snapshot isolation, that's the trigger for
Delta Lake / Iceberg (already listed as a P2 candidate in spec 03).

**Why:** a plain Parquet directory on a filesystem can't do a multi-file atomic replace. Claiming it would be false.

## Part B — gold = fact tables + daily aggregates (additive)

The spec only defines two daily aggregate tables. Percentiles and rates can't be re-aggregated from daily
values: a weekly p95 is **not** derivable from seven daily p95s. So I propose loading the per-entity facts too:

| Table (schema `analytics`) | Grain | Purpose |
|---|---|---|
| `delivery_facts` *(new)* | one row per delivered package | `package_id`, `delivery_date` (destination-local), `delivered_at`, `promised_delivery_date`, `service_level`, `destination_postal_prefix`, `destination_timezone`, `delay_minutes`, `on_time` |
| `facility_visits` *(new)* | one row per (package, facility, arrival) | `package_id`, `facility_id`, `arrived_at`, `departed_at` (nullable), `dwell_minutes`, `activity_date` |
| `daily_network_activity` *(new)* | one row per UTC day | `active_packages`, `packages_with_exception` (for KPI-B4) |
| `daily_delivery_performance` | as specified | derived from `delivery_facts` |
| `daily_facility_performance` | as specified | derived from `facility_visits` and EXCEPTION events |
| `ops.gold_runs` *(new)* | one row per gold run | window, status, row counts, timings |

The FR-ANL-02 SQL query pack queries the fact tables, so any date range gives exact percentiles.

A new role, **`ptp_batch_writer`** (already named in spec 06), gets DML on `analytics` and `ops.gold_runs` only.
The gold load replaces the whole recompute window **in one Postgres transaction**: delete the window, insert
from a staging table, commit. That is stronger than "per date partition".

## Part C — metric definitions (please confirm each)

| # | Question the spec leaves open | Proposed definition |
|---|---|---|
| C1 | Which delivery counts? | The package's **status-defining terminal event** (spec 01 rule: the first terminal). If that is `RETURNED_TO_SENDER`, the package is not delivered, even if a `DELIVERED` scan exists. Gold then always agrees with `package_status`. |
| C2 | Delivered but `CREATED` missing (no promised date) | Excluded from delivery facts; counted as `ORPHAN_EVENT` by DQ-04 (later step) |
| C3 | "End of the promised day" and on time | End of day = start of `promised_delivery_date + 1` in the destination time zone. **On time ⇔ delivered_at < end of day.** `delay_minutes = (delivered_at − end of day)` in minutes; it is negative when early. |
| C4 | Delay percentiles | Over the **signed** `delay_minutes` of delivered packages (early deliveries pull the median down). Exact, linear interpolation (Spark `percentile`, Postgres `percentile_cont`). |
| C5 | Dwell time pairing | Each `ARRIVED_AT_FACILITY` is paired with the **next** `DEPARTED_FACILITY` for the same package and facility (by event time). A visit with no departure yet has no dwell. |
| C6 | Which day a facility metric belongs to | Arrivals → UTC date of arrival; departures and dwell → UTC date of departure (when the dwell completes); exceptions → UTC date of the exception |
| C7 | KPI-B4 "packages active that day" | A package is active on UTC day D if it has **any valid event on D**. Exception rate = packages with ≥ 1 `EXCEPTION` on D ÷ active packages on D. |
| C8 | Recompute window | Gold recomputes the 7 days ending at the run's `as_of` date, reading silver from `window_start − 30 days`, so a package's earlier `CREATED` is found. Facts older than the window are left as they were (provisional data, ADR-0005). |

## Part D — the generator produces expected gold (additive, spec 02 §6)

The generator already has an independent Python oracle for package status. Extend it with the same
definitions (C1–C7) to write `expected_delivery_facts.jsonl`, `expected_facility_visits.jsonl` and
`expected_network_activity.jsonl`. The Scala/Spark gold job is then checked against a second,
independent implementation, as status is today. Without this, the tests could only check the gold job
against itself.

## Impact

- **Code:** new silver and gold batch jobs; generator oracle extension; migrations V5 (analytics tables,
  `ops.gold_runs`, grants) and a `ptp_batch_writer` role in the bootstrap; `make silver`, `make gold`, `make lake`.
- **Existing behaviour:** none changes. Status, DLQ, bronze and replay are untouched.
- **Spec text:** spec 00 KPI-B1…B4 get the precise definitions; spec 02 §4–§6 get the new tables and files;
  spec 02 §5 gets the Part A wording.

## Decision

Approved by the project owner on 2026-10-22 (parts A–D, definitions C1–C8). Recorded in spec 00 (KPI-B1…B4),
spec 01 (FR-ANL-01/02), spec 02 §4–§6 and spec 07.

*Clarification while implementing C5:* "next departure" means the departure with the smallest ordering key greater than
the arrival's. Several arrivals may pair with the same departure.
