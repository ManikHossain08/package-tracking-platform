# Step 7b — Airflow orchestration + retention: results (2026-10-22)

Acceptance criteria: spec 07, AC-7.7 … AC-7.11. Decisions: ADR-0008 (Airflow, D7), ADR-0009 option A (jobs image +
socket proxy), SC-0006 C (retention mechanics).

## What was built

| Area | Files |
|---|---|
| Airflow | Compose `airflow` (`apache/airflow:3.3.2`, pinned digest; `standalone`, LocalExecutor; UI on 127.0.0.1:8080; simple-auth password from `.env`; DAGs start paused); metadata in database `airflow` owned by `ptp_airflow`, which can't connect to the application database |
| Job launch | `docker-proxy` (`tecnativa/docker-socket-proxy:v0.5.0`, pinned): CONTAINERS + IMAGES + POST only; never published |
| DAGs | `orchestration/airflow/dags/`: `lake_daily` (silver → gold → dq; dq not retried), `replay_from_bronze` (typed params + validate task before Spark), `maintenance` (retention); `ptp_common.py` (DockerOperator per task, secrets via `private_environment`) |
| Retention | `ptp/lake/Retention.scala` (`LakeApp maintenance`); bronze sink `retention` option; `BronzeRow.read` for all bronze readers |
| Checks | `orchestration/airflow/checks/check_dags.py` (43 checks in the container), `scripts/verify_airflow.sh`, `RetentionIT`; CI job `orchestration` |

## Acceptance results

| AC | Result | Evidence |
|---|---|---|
| AC-7.7 | ✅ | Healthy in ~27 s; port bound to 127.0.0.1 only; admin login succeeds with the `.env` password and fails with a wrong one; `ptp_airflow` can't connect to `ptp`; Airflow metadata in database `airflow` |
| AC-7.8 | ✅ | `check_dags.py`: no import errors, exact task graphs, timeouts and retries on every task, run-scoped commands, no secrets in templated env, replay params, dq not retried. Airflow's own `dags list-import-errors`: none. |
| AC-7.9 | ✅ | `airflow dags test lake_daily`: succeeds and writes new `ops.gold_runs` / `ops.dq_results`. With a duplicated silver file, the run fails at `dq` (DQ-02 = fail). |
| AC-7.10 | ✅ | Dry-run replay through Airflow writes an `ops.replay_runs` row; a reversed date range fails in `validate` and no replay row appears (Spark never started) |
| AC-7.11 | ✅ | `RetentionIT`: expired partitions in processed_events, bronze and silver are removed and data inside retention is untouched; deleting a bronze partition still listed in `_spark_metadata` leaves bronze readable (empty, not an error) and the writer keeps working. `maintenance` runs through Airflow. |

Totals: integration 24/24, unit 110 core + 10 streaming, Python 54 + 1 integration, `make verify-airflow` 12/12, lint/pins/secrets clean.

## Issues found and fixed

1. **SC-0006 C's bronze mechanism didn't hold as written.** `ignoreMissingFiles` covers the data scan, but footer-based **schema
   inference** still opened deleted files and failed. Fixed by reading bronze with its explicit schema (`BronzeRow.read`); verified by
   `RetentionIT`, as SC-0006 required before relying on it.
2. **A lake that wasn't relocatable.** The bronze sink log stores **absolute** paths. Data written on the host (`/Users/…`) and in a
   container (`/data/…`) made Spark's partition discovery fail in the first Airflow run. Fixed: every runtime uses the same absolute
   path (`PTP_HOST_DATA_DIR` in `.env`; containers mount `./data` at that path). Recorded in ADR-0009.
3. **The DAG failed at the wrong task on corrupt silver.** A duplicated silver file crashed **gold** on the `facility_visits` key (safely,
   inside the transaction, but after 2 retries), so `dq` never ran to explain why. Gold now deduplicates its input by `event_id`, and the
   run fails at `dq` with DQ-02. A regression assertion was added to `DqIT`.
4. **DockerOperator needs the proxy's `IMAGES` section** (it checks that the image exists). ADR-0009 listed only CONTAINERS + POST; recorded as
   an implementation note. With POST, image pulls are technically possible too; still a local-dev-only posture.
5. Smaller: Airflow 3 moved `DagBag` (`airflow.dag_processing.dagbag`) and dropped `include_examples`; standalone scripts must add the
   dags folder to `sys.path` as the DAG processor does; `PTP_HOST_DATA_DIR` lives in `.env`, because Compose interpolates the whole file.

## Notes and limits

- `verify-airflow` uses `airflow dags test` (synchronous, real task execution through the proxy). Scheduled runs work the same way once a
  DAG is unpaused in the UI.
- Docker Desktop still had 7.7 GiB. Everything was run with the streaming container stopped while Airflow tests ran. Raise it to 10 GB (D8)
  to run all profiles at once.
- Airflow `standalone` is a development deployment; production maps to separate components or Cloud Composer / MWAA (ADR-0008).
