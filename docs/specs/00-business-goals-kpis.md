# 00 — Business goals and KPIs

## Problem statement

A parcel delivery company receives tracking events from distribution centres (DCs) and
delivery vehicles. The events arrive **duplicated, delayed, out of order, malformed, or in
bursts after an outage**. Operations, customer service and network planning need:

1. A **trustworthy current status** for each package that never goes backwards when an
   older event arrives late.
2. **Delivery-delay metrics:** actual delivery compared with the promised delivery date.
3. **DC performance analytics:** throughput, dwell time and exception rates.

All three need low processing latency, and they must be auditable and replayable.

## Stakeholders (assumed)

| Stakeholder | Needs | Consumes |
|---|---|---|
| Customer service | Current status per package, freshness | `tracking.package_status` |
| Operations / DC managers | Dwell time, throughput, exceptions per DC per day | `analytics.daily_facility_performance` |
| Network planning | On-time %, delay distribution by service level and region | `analytics.daily_delivery_performance` |
| Data platform team | Pipeline health, DLQ volume, recovery | Prometheus / Grafana, DLQ topic |

## Business goals

| ID | Goal |
|---|---|
| BG-01 | Customer-facing status is correct even when the feed is imperfect. |
| BG-02 | Delay and DC metrics are trustworthy enough to drive operational decisions. |
| BG-03 | Bad data is never silently lost: it is quarantined with a reason and can be reprocessed. |
| BG-04 | The platform recovers from outages without manual data repair. |

## KPIs

Business KPIs are produced **by** the platform. Platform KPIs measure the platform itself.
Numeric targets for latency and throughput are deliberately left as **TBD** until the
benchmark in spec 09 has produced measurements (see NFR-10).

### Business KPIs (outputs)

| ID | KPI | Definition |
|---|---|---|
| KPI-B1 | On-time delivery rate | delivered on or before the end of the promised day ÷ delivered, per day / service level / region |
| KPI-B2 | Delivery delay p50 / p95 | minutes from the end of the promised day to `DELIVERED.event_time`; values ≤ 0 count as on time |
| KPI-B3 | DC dwell time p50 / p95 | `DEPARTED_FACILITY` − `ARRIVED_AT_FACILITY` for the same package and facility |
| KPI-B4 | Exception rate | packages with ≥ 1 `EXCEPTION` ÷ packages active that day |

### Platform KPIs (quality of the platform)

| ID | KPI | Target |
|---|---|---|
| KPI-P1 | Status correctness: reconciliation mismatches between `package_status` and a batch recomputation from silver | **0** |
| KPI-P2 | Unexplained event loss: input events − (applied + duplicates + rejected) | **0** |
| KPI-P3 | Status freshness: p95 of `status committed_at − emitted_at` | TBD after benchmark |
| KPI-P4 | Sustained throughput without growing lag | TBD after benchmark |
| KPI-P5 | Recovery time after a processor crash, until lag is back to steady state | TBD after benchmark |
| KPI-P6 | DLQ rate (rejected ÷ input), broken down by `error_code` | Observed. An alert fires on deviation from the injected rate in tests. |
