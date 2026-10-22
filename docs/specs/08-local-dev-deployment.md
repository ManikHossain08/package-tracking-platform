# 08 — Local development and deployment

> **Status:** planned interface. Nothing here is implemented yet. Commands will be verified
> and this note removed when slice 1 lands.

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop / Engine + Compose v2 | 28.x (verified on dev machine) | Allocate ~10 GB RAM |
| JDK | 17 | Temurin or OpenJDK |
| sbt | pinned in `project/build.properties` | |
| Python | 3.12 | Generator and test utilities, in a venv |
| make | any | |

No cloud account and no paid service are needed.

## Compose profiles and memory budget (planned)

| Profile | Services | Approx. memory (to be measured) |
|---|---|---|
| `core` | kafka, kafka-init, postgres, pg-migrate, spark-processor, generator | Kafka 1 GB, Postgres 0.5 GB, Spark driver 2–3 GB |
| `obs` | prometheus, grafana | ~0.5 GB |
| `orchestration` | airflow (LocalExecutor) | ~2 GB |

## Make targets (planned)

| Target | Purpose |
|---|---|
| `make up` / `make up-all` / `make down` | Start `core` / all profiles; stop and keep volumes |
| `make reset` | Stop and delete volumes, lake and checkpoints (asks for confirmation) |
| `make generate SCENARIO=bench/scenarios/<name>.yaml` | Run the generator |
| `make test` | Unit + contract tests (no Docker) |
| `make test-it` | Integration, recovery and DQ tests (Testcontainers) |
| `make bench SCENARIO=…` | Benchmark per spec 09 |
| `make trace` | Regenerate `docs/traceability.md` |

## Environment variables (planned — mirrored in `.env.example`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | |
| `EVENTS_TOPIC` / `DLQ_TOPIC` | `package-events.v1` / `package-events.dlq.v1` | |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` | `postgres` / `5432` / `ptp` | |
| `PTP_STREAM_WRITER_PASSWORD` etc. | *(no default; required)* | Secrets: set in `.env` only |
| `LAKE_ROOT` | `/data/lake` | |
| `CHECKPOINT_ROOT` | `/data/checkpoints` | |
| `TRIGGER_INTERVAL` | `5 seconds` | |
| `MAX_OFFSETS_PER_TRIGGER` | `50000` | |
| `WATERMARK_DELAY` | `2 hours` | |
| `FUTURE_SKEW_TOLERANCE` | `5 minutes` | |
| `LIVE_MAX_EVENT_AGE` | `30 days` | |
| `PROCESSING_MODE` | `live` | `live` \| `replay` |
| `PG_RETRY_MAX_ATTEMPTS` / `PG_RETRY_MAX_DELAY` | `5` / `30s` | |
| `METRICS_PORT` | `9464` | |

## Deployment

The deliverable is a local deployment. Spec 03 §Deployment and ADR-0006 describe the
production mapping: separate applications per query, a replicated Kafka cluster, managed
Postgres, and object storage for the lake. No cloud resources are created by this project.
