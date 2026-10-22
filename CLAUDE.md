# CLAUDE.md — Package Tracking Platform

Project-specific rules for AI-assisted, spec-driven development in this repository.
These rules override generic cloud/warehouse defaults: this project is **local-only**
(Docker Compose), with no cloud account or paid service.

## Stack
Scala + Spark Structured Streaming, Kafka (KRaft), PostgreSQL, Parquet, Airflow,
Python (generator, test utilities), Prometheus, Grafana, GitHub Actions.
Exact versions are pinned in `docs/adr/0001-repo-and-stack-versions.md`; never use
floating versions (`latest`, `+`, unpinned pip requirements).

## Spec-driven workflow
1. Specs live in `docs/specs/`, decisions in `docs/adr/`, contracts in `contracts/`.
2. Every feature starts from a requirement ID (`FR-*`, `NFR-*`, `DQ-*`) with acceptance
   criteria in `docs/specs/07-acceptance-tests.md` **before** code is written.
3. Implement one small vertical slice at a time; run the tests and report actual output.
4. **Never silently change a spec to make an implementation pass.** Write a proposal in
   `docs/spec-changes/NNNN-<slug>.md` (what, why, impact on tests/data/consumers) and
   wait for approval.
5. Keep `docs/traceability.md` current: requirement → implementation → tests.

## Testing rules
- Tag every test with the requirement IDs it verifies
  (ScalaTest `Tag("FR-DED-01")`, pytest `@pytest.mark.req("FR-DED-01")`).
- No arbitrary sleeps for synchronisation. Use `StreamingQuery.processAllAvailable()`,
  bounded polling (`eventually` with explicit timeout/interval) on observable conditions,
  or Testcontainers wait strategies.
- Unit tests must not need Docker. Integration tests use Testcontainers / Compose.

## Engineering rules
- Do not claim exactly-once end-to-end. State the guarantee per sink (see ADR-0002).
- Business logic (validation, ordering, state transitions) is pure and independent of
  Spark/Kafka/JDBC wrappers so it can be unit tested and reused for replay.
- Structured JSON logs with stable keys: `run_id`, `query_name`, `batch_id`, `topic`,
  `partition`, `offset_range`, `error_code`.
- No secrets in the repo. Configuration via env vars; document each in `.env.example`
  and `docs/specs/08-local-dev-deployment.md`.
- Do not invent performance numbers. Only report results produced by `bench/`.
- Prefer small, reviewable changes; explain trade-offs before architectural changes.
