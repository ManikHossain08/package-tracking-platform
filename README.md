# Real-Time Package Tracking and Analytics Platform

A local, production-style portfolio project: package tracking events flow through Kafka
into Spark Structured Streaming. The pipeline keeps a trustworthy current package status
in PostgreSQL, lands analytical history in partitioned Parquet, and exposes delivery-delay
and distribution-centre analytics. It is built to tolerate duplicated, late, out-of-order
and malformed events, and to recover after outages.

> **Status: Phase 2 complete — specifications and ADRs 0001–0006 accepted.** No application code yet. Spark line changed to 4.1.3 by approved [SC-0001](docs/spec-changes/0001-spark-version-line.md).

## Quick start

Prerequisites: Docker (with ~10 GB memory allocated), JDK 17, sbt, Python 3.12, make.

```bash
make up        # creates .env with a generated password, starts Kafka + Postgres, creates topics
make verify    # checks topics, configs, disabled auto-create, init idempotency
make test      # Python contract tests + Scala/Spark tests (no Docker needed)
make process   # run the Spark status processor (separate terminal; Ctrl-C to stop)
make generate  # publish the smoke scenario to Kafka; manifest in data/generator/smoke/
make reconcile # compare Postgres with the generator's manifest
make replay FROM=2026-10-22   # rebuild/repair Postgres from bronze Parquet (idempotent)
make lake      # silver (deduped, compacted Parquet) + gold analytics in Postgres
make query Q=01_on_time_rate FROM=2026-10-16 TO=2026-10-22   # KPI queries (sql/analytics/)
make status    # status distribution and batch totals
make test-it   # integration tests (Docker): Kafka + Postgres + Spark + generator
make down      # stop (volumes kept)
```

`make help` lists all targets.

## Where to start reading

| Document | Purpose |
|---|---|
| [docs/specs/README.md](docs/specs/README.md) | Spec index, status, open decisions |
| [docs/specs/01-functional-requirements.md](docs/specs/01-functional-requirements.md) | Requirements with stable IDs |
| [docs/specs/02-data-contracts.md](docs/specs/02-data-contracts.md) | Event, DLQ, Postgres and Parquet contracts |
| [docs/specs/03-architecture.md](docs/specs/03-architecture.md) | Components and data flow |
| [docs/specs/07-acceptance-tests.md](docs/specs/07-acceptance-tests.md) | Acceptance criteria and test scenarios |
| [docs/adr/](docs/adr/) | Architecture decision records |
| [contracts/](contracts/) | JSON Schemas and sample payloads |
| [docs/traceability.md](docs/traceability.md) | Requirement → implementation → test matrix |

## Development phases

1. Repository inspection and plan ✅
2. Specifications ✅
3. Architecture and ADRs ✅ (0001–0006 accepted; 0007–0008 planned)
4. Schemas and acceptance tests
5. Incremental implementation, one vertical slice at a time — step 1 (setup) ✅, step 2 (generator) ✅, step 3 (validation) ✅, step 4 (status pipeline, first slice) ✅, step 5 (bronze + replay) ✅, step 6 (silver + gold analytics) ✅
6. Test runs with actual results
7. Security, reliability and performance review
8. Documentation and portfolio demo
