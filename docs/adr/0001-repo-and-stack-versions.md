# ADR-0001: Repository location and stack version lines

- **Status:** Accepted. Repo location 2026-10-20; version lines 2026-10-21 (exact patch pins: see "Pinned versions" below).
- **Requirements:** NFR-07, FR-DEV-01

## Context

The project must be reproducible on a laptop (Apple M4, 16 GB, Java 17, Docker 28) with no
cloud or paid services. The first candidate folder (`~/Downloads/ORCHESTRATOR-2`) is an
unrelated Dagster/Snowflake codebase, and the `~/Downloads/CLAUDE.md` instructions there
assume GCP/BigQuery.

## Decision 1 — repository (accepted)

A standalone git repo at `~/projects/package-tracking-platform`, with its own `CLAUDE.md`.
It is outside `~/Downloads`, so parent-directory instructions do not apply.

## Decision 2 — version lines (accepted)

| Component | Chosen line | Alternative | Rationale |
|---|---|---|---|
| Spark | **4.1.x** (changed from 4.0.x by SC-0001) | 4.2.x, 3.5.x | 4.0.x maintenance ends ~Nov 2026; 4.1.x is mature and maintained into 2027. See SC-0001. |
| Scala | **2.13** | 2.12 | Spark 4 supports only 2.13. |
| JDK | **17** | 21 | Already installed; supported by Spark 4 and Kafka. |
| Kafka (broker image `apache/kafka`) | **3.9.x or 4.x, KRaft** | Confluent images | Official Apache image, no ZooKeeper. The client version must match what the Spark Kafka connector ships with. |
| PostgreSQL | **17** | 16 | Current stable line. |
| Airflow | **3.x** | 2.10.x | Current major. Optional profile. |
| Python | **3.12** | 3.11 | Matches the local install. |
| Test | ScalaTest + ScalaCheck, Testcontainers (Java/Scala), pytest | — | |

**Pinning rule:** the exact patch version and Docker image digest for each component are
verified against Maven Central / Docker Hub, compatibility-checked
(Spark ↔ Kafka client ↔ Scala binary version), and recorded here. No version is pinned
from memory.

## Pinned versions (runtime components)

Verified 2026-10-21 against Maven Central `maven-metadata.xml`, the Apache archive and
Docker Hub tags. Image digests live in `infra/docker-compose.yml` and the `Makefile`.
`make check-pins` enforces that nothing floats.

| Component | Pin | Notes |
|---|---|---|
| Spark | **4.1.3** | Approved via SC-0001 |
| Scala | **2.13.17** | Declared by Spark 4.1.3's parent POM | Must match Spark's binary version |
| Kafka client | **3.9.1** (transitive from `spark-sql-kafka-0-10` 4.1.3) | Not overridden |
| Kafka broker image | `apache/kafka:4.3.2` | KRaft. 4.x brokers accept 3.9 clients. |
| Spark runtime image | `apache/spark:4.1.3-scala2.13-java17-ubuntu` | |
| PostgreSQL image | `postgres:17.11` | 18.x exists. 17 is kept per D2 and is supported for years. |
| Postgres JDBC | `org.postgresql:postgresql:42.7.14` | |
| Airflow image | `apache/airflow:3.3.2-python3.12` | 3.4 is still beta |
| Prometheus image | `prom/prometheus:v3.15.1` | |
| Grafana image | `grafana/grafana:13.2.4` | |
| JDK (local) | 17 (OpenJDK 17.0.15 on dev machine) | |
| Python (local) | 3.12 (3.12.7 on dev machine) | |

## Consequences

- Spark 4 is newer, so some examples online target 3.x APIs. Differences are noted where relevant.
- Bitnami images are intentionally avoided (their free catalogue changed in 2025). Official
  images are used throughout.

## Pinned versions (build and test tooling, step 1)

| Component | Pin | Where |
|---|---|---|
| sbt | 1.12.15 | `processor/project/build.properties` |
| Jackson (`jackson-databind`, processor-core) | 2.21.2, equal to Spark 4.1.3's `fasterxml.jackson.version`, so the Spark runtime and core agree (asserted by TS-U-14) | `processor/build.sbt` |
| ScalaTest / scalatestplus-scalacheck | 3.2.20 / 3.2.20.0 (`scalacheck-1-19`) | `processor/build.sbt` |
| Python dev deps | pytest 9.1.1, jsonschema 4.26.0, ruff 0.16.10 (+ transitive), all with sha256 hashes, resolved with `--exclude-newer 2026-10-07` | `requirements-dev.txt` |
| gitleaks image | `zricethezav/gitleaks:v8.30.1@sha256:c00b6bd0…` | `Makefile` |
| GitHub Actions | checkout v7.0.1, setup-java v6.1.0, setup-python v7.0.0, sbt/setup-sbt v1.5.12, each pinned to its commit SHA | `.github/workflows/ci.yml` |

Testcontainers is pinned when the integration tests are added (step 4).
