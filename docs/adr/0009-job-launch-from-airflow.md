# ADR-0009: How Airflow launches Spark jobs

- **Status:** Accepted (2026-10-22): option A
- **Requirements:** FR-DEV-01, NFR-06, NFR-07, AC-7.6 to AC-7.11

## Context

Today the Spark jobs run on the host through sbt. Airflow runs in a container, so it needs a packaged job and
a way to start it. The step 4 gap (no container image, unclean shutdown) is closed by the same packaging.

## Options

| Option | How | Pros | Cons |
|---|---|---|---|
| **A. Jobs image + DockerOperator via a socket proxy** | `ptp-jobs` image (Spark 4.1.3 base + assembly jar). Airflow's DockerOperator starts one short-lived container per task on the Compose network. Docker API access goes through `docker-socket-proxy`, which allows only container create/start/inspect/logs/remove. | Each job gets its own container and memory limit, like KubernetesPodOperator in production; the Airflow image stays small; the same image runs `spark-processor` | The proxy is a third-party image (pinned by digest). Even restricted, container-create access is powerful on a host, so it's acceptable **only** for local dev. |
| B. Spark inside the Airflow image (BashOperator `spark-submit`) | Custom Airflow image with Java 17 + Spark + the jar | No Docker API access at all | A heavy Airflow image coupled to Spark versions; jobs compete for the scheduler's memory; doesn't mirror production |
| C. Raw Docker socket mount | DockerOperator with `/var/run/docker.sock` | Simplest | Gives Airflow full root-equivalent control of the host; rejected |

## Decision

**A.** `docker-socket-proxy` exposes only `CONTAINERS` and `POST`; everything else, including images, volumes,
exec and swarm, is denied. Jobs run with explicit memory limits, on the Compose network, with the lake
mounted. Production mapping: KubernetesPodOperator, Dataproc or EMR operators; no Docker API at all.

## Consequences

- `make image` builds the assembly jar and `ptp-jobs`. The image tag is the git commit, so a DAG run is traceable to code.
- Memory: Airflow standalone (~1.5 GB) + Kafka (1 GB) + Postgres (0.5 GB) + `spark-processor` (≈2.5 GB) + one job
  container (≈2 GB) ≈ 7.5 GB. That is too close to the current Docker Desktop limit (7.7 GiB), so raise it to the
  10 GB in D8, or run `spark-processor` on the host (`make process`) while Airflow is up.
