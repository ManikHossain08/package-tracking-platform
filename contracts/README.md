# Contracts

Machine-readable data contracts. These are authoritative; `docs/specs/02-data-contracts.md`
explains them.

| File | Contract |
|---|---|
| `package-event.v1.schema.json` | Value of topic `package-events.v1` (JSON Schema 2020-12) |
| `dlq-record.v1.schema.json` | Value of topic `package-events.dlq.v1` |
| `samples/valid/` | Events that MUST pass, including a v1.1 event with an unknown extra field |
| `samples/invalid/` | Events that MUST fail; `expected.json` gives the expected primary `error_code` |
| `samples/dlq/` | Example DLQ records that MUST validate |

The JSON Schema covers **structural** rules only. Runtime rules that depend on processing
time (`EVENT_TIME_IN_FUTURE`, `EVENT_TIME_TOO_OLD`) and the mapping of violations to error
codes live in the processor's validation code. They are tested against `expected.json`.

Changing a contract after approval requires a proposal in `docs/spec-changes/`.
