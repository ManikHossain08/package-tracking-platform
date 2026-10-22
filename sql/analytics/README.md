# Analytics SQL query pack (FR-ANL-02)

Queries over the gold fact and daily tables (SC-0005). Every query takes an inclusive date range through
psql variables and runs as the read-only `ptp_reader` role:

```bash
make query Q=01_on_time_rate FROM=2026-10-20 TO=2026-10-26
```

| File | KPI | Reads | Date column |
|---|---|---|---|
| `01_on_time_rate.sql` | KPI-B1 on-time % by service level, plus a total row | `delivery_facts` | `delivery_date` (destination-local) |
| `02_delay_percentiles.sql` | KPI-B2 signed delay p50/p90/p95/max by service level | `delivery_facts` | `delivery_date` |
| `03_facility_dwell.sql` | KPI-B3 dwell p50/p95 per facility | `facility_visits` | `departure_date` (UTC) |
| `04_facility_throughput.sql` | arrivals, departures, exceptions and average arrivals per day per facility | `daily_facility_performance` | `activity_date` (UTC) |
| `05_exception_rate.sql` | KPI-B4 exception rate per day, plus the whole range | `daily_network_activity` | `activity_date` (UTC) |

Percentiles are computed from the fact rows (`percentile_cont`, exact with linear interpolation), so any range is
correct. They are never averaged from daily percentiles. Expected results on a fixed fixture are tested by
`AnalyticsQueryPackIT` (TS-D-05).
