# Specification index

| # | Spec | Status |
|---|---|---|
| 00 | [Business goals and KPIs](00-business-goals-kpis.md) | Draft for review |
| 01 | [Functional requirements](01-functional-requirements.md) | Draft for review |
| 02 | [Data contracts](02-data-contracts.md) | Draft for review |
| 03 | [Architecture](03-architecture.md) | Draft for review |
| 04 | [Non-functional requirements](04-nfr.md) | Draft for review |
| 05 | [Data quality](05-data-quality.md) | Draft for review |
| 06 | [Security and failure handling](06-security-failure-handling.md) | Draft for review |
| 07 | [Acceptance criteria and test scenarios](07-acceptance-tests.md) | Draft for review |
| 08 | [Local development and deployment](08-local-dev-deployment.md) | Draft (planned, not implemented) |
| 09 | [Benchmark method](09-benchmark-method.md) | Draft for review |

## Conventions

- **Requirement IDs** are stable and never reused: `FR-<AREA>-NN`, `NFR-NN`, `DQ-NN`.
  If a requirement is removed, it is marked *Withdrawn*, not deleted.
- **Priority:** P0 = first vertical slice, P1 = MVP-complete, P2 = stretch.
- **Spec changes** after approval go through `docs/spec-changes/` (see the README there).
- "MUST / SHOULD / MAY" follow RFC 2119 meaning.

## Decisions (from Phase 1)

All recommended defaults were accepted by the project owner. Changing any of them now
requires a proposal in `docs/spec-changes/`.

| ID | Decision | Accepted value | Accepted | Where |
|---|---|---|---|---|
| D1 | Repo location | `~/projects/package-tracking-platform` | 2026-10-20 | ADR-0001 |
| D2 | Version lines | Spark 4.1.x (4.1.3, via SC-0001), Scala 2.13, Java 17; exact pins in ADR-0001 | 2026-10-21 | ADR-0001 |
| D3 | Status ordering policy | Event-time order, lifecycle-rank tiebreak, sticky terminal states | 2026-10-21 | ADR-0004, spec 01 |
| D4 | Idempotency key | Producer `event_id`; content hash for detection only | 2026-10-21 | ADR-0003 |
| D5 | Lateness values | 2 h watermark, 7-day gold recompute, +5 min future skew, 30-day live lower bound | 2026-10-21 | ADR-0005 |
| D6 | Sink topology | Separate streaming query per sink | 2026-10-21 | ADR-0006 |
| D7 | Orchestration | Airflow 3.x, optional Compose profile | 2026-10-21 | spec 03 (ADR-0008 planned) |
| D8 | Resource budget | ~10 GB Docker memory; Airflow and observability off by default | 2026-10-21 | spec 08 |

## Business assumptions (accepted 2026-10-21 with the defaults)

These were open questions. The assumed answers are now part of the spec and can be
revisited through a spec change if real business input differs.

- **Q1:** Terminal states are sticky. A later event after `DELIVERED` / `RETURNED_TO_SENDER`
  is recorded and flagged `POST_TERMINAL_EVENT`, but never changes status. Corrections would
  need a future explicit correction event type.
- **Q2:** "On time" = delivered on or before the end of the promised day in the destination's
  local time zone.
- **Q3:** In live mode, events with `event_time` older than 30 days go to the DLQ
  (`EVENT_TIME_TOO_OLD`). Replay mode accepts them.
