# 06 — Security and failure handling

## Security

| Area | Local MVP | Production note (documented, not implemented) |
|---|---|---|
| Secrets | `.env` (git-ignored), `.env.example` committed with placeholders; gitleaks in CI | Secret manager; short-lived credentials |
| Postgres roles | `ptp_migrator` (DDL), `ptp_stream_writer` (DML on `tracking`, `ops`), `ptp_batch_writer` (DML on `analytics`, `ops`, read on `tracking`), `ptp_reader` (SELECT on `analytics`, `tracking`), `ptp_airflow` (own `airflow` database only; SC-0006 B) | Same, plus TLS |
| Kafka | PLAINTEXT on the Docker network only; **not** exposed beyond localhost | SASL_SSL, ACLs per principal (producer: write `v1`; processor: read `v1`, write DLQ) |
| Grafana / Airflow | Bound to `127.0.0.1`; admin password from `.env` | SSO |
| PII | No PII in the contract. The DLQ and bronze hold raw payloads and could contain PII if a producer misbehaves, so their retention is limited (14 d / 90 d) and they are excluded from the `ptp_reader` role. | Field-level scanning of the DLQ |
| Supply chain | Pinned versions and image digests; Dependabot proposals reviewed manually | SBOM, image signing |
| Logs | Never log full payloads at INFO. Log `event_id`, `package_id` and coordinates; the raw payload lives only in the DLQ. | — |

## Failure handling matrix

| Failure | Detection | Behaviour | Recovery | Test |
|---|---|---|---|---|
| Malformed record | Validation | DLQ; bronze `is_valid=false`; counter by code | Fix the producer; optionally replay the DLQ after a contract fix | TS-I-02 |
| Poison record that crashes the parser | Exception inside parse | Parsing is total: every exception becomes `INVALID_JSON`/`INVALID_FIELD_TYPE`. No record may crash the query. | — | TS-U-02 fuzz |
| Duplicate event | `processed_events` conflict | No-op; counter | — | TS-I-03 |
| Late / out-of-order event | Ordering guard | Recorded; status changes only if the key is newer | — | TS-I-04 |
| Postgres unavailable or erroring | JDBC exception | Retry with exponential backoff and jitter (`PG_RETRY_MAX_ATTEMPTS=5`, `PG_RETRY_MAX_DELAY=30s`), then the query fails. Error log includes query, batch_id, offset ranges and SQLSTATE. | Restart (supervisor or Compose `restart: on-failure`). Resumes from the last committed batch. | TS-R-02 |
| Postgres constraint violation (bug) | SQLSTATE 23xxx | **No retry**: fail fast with context | Code fix, then restart | TS-R-03 |
| Kafka broker unavailable | Consumer timeout | Spark retries; the query fails after the timeout | Restart after the broker returns | TS-R-05 |
| DLQ produce fails | Producer exception | The batch fails. The status write may already be committed, but the re-run is idempotent. | Restart | TS-R-03 |
| Offsets aged out of Kafka (outage > retention) | `failOnDataLoss=true` → query error | Stop with an actionable message | Runbook: replay from bronze, then restart with explicit `startingOffsets` | TS-R-07 (P2) |
| Checkpoint corrupt or lost | Startup failure | Stop | Runbook FR-REC-04 | P2 |
| Driver OOM | Container exit 137 | Both queries stop | Restart. Tune `maxOffsetsPerTrigger`. | Soak test |
| Disk full (lake or checkpoint) | Write exception | Query fails | Free space, then restart | Manual |
| Unknown `schema_version` major | Validation | DLQ `UNSUPPORTED_SCHEMA_VERSION` | Deploy a v2 consumer and replay | TS-C-03 |

## Error message standard

Every error log entry has `level`, `ts`, `run_id`, `query_name`, `batch_id`, `component`,
`error_code`, `message`, and where relevant `topic`, `partition`, `offset_range`, `table`,
`sqlstate`, `attempt`, `max_attempts`, plus a short `action` hint
(e.g. `"Check Postgres availability; the query will resume from batch 42 on restart"`).
