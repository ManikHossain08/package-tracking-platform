# SC-0008: `make down` stops every profile; the query pack tolerates empty windows

- **Status:** Approved (2026-10-24) by the project owner, who asked for both fixes after running the stack locally
- **Affects:** spec 08 (`make down`), spec 07 AC-6.10 / TS-D-05 (FR-ANL-02), `Makefile`, `sql/analytics/01, 04, 05`

## A. `make down` stops all profiles

**Change:** spec 08, `make down`: "stop, keep volumes" (implemented as `--profile core down`) → stop every service in
all profiles (`core`, `stream`, `orchestration`, `obs`), keep volumes.

**Reason:** after `make stream-up`, `make airflow-up` or `make obs-up`, `make down` stopped only Kafka and Postgres.
Airflow, the docker proxy, Prometheus, the Pushgateway and Grafana kept running, and Compose could not remove the
shared network ("Resource is still in use"). Observed on 2026-10-24.

**Impact:** `make down` now also stops `spark-processor`, `airflow`, `docker-proxy`, `prometheus`, `pushgateway` and
`grafana`. Volumes are kept, as before. The per-profile targets (`stream-down`, `airflow-down`, `obs-down`) are unchanged.
`make reset` is unchanged: it still deletes only the Kafka and Postgres volumes, as its prompt says.

## B. Query pack: empty windows return NULL rates, not an error

**Change:** AC-6.10 gains: a window with no data returns empty results or NULL rates, never an error.

**Reason:** `make query Q=01_on_time_rate` with the default window (today, UTC) failed with `division by zero`
when no package had been delivered that day: `GROUP BY ROLLUP` emits its total row on empty input, with `count(*) = 0`.

**Fix:** every division in the pack uses `NULLIF(denominator, 0)`: `01` (`count(*)`), `04` (days in the range, zero only
for a reversed range) and `05` (`sum(active_packages)`). `02` and `03` do not divide.

**Expected empty-window results:** `01` returns only the total row `NULL | 0 | 0 | NULL`; `02`, `03` and `04` return no rows;
`05` returns only the range row with NULL sums and a NULL rate. Results for windows with data are unchanged
(the existing fixture assertions still pass).

**Compatibility:** no schema or contract change; consumers that previously got an error now get NULL.
