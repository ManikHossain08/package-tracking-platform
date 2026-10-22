# Step 3 — validation in `processor-core`: results (2026-10-22)

Acceptance criteria: spec 07, AC-3.1 … AC-3.7. Code: `processor/core/src/main/scala/ptp/core/`
(`model/PackageEvent.scala`, `validation/ErrorCode.scala`, `validation/EventValidator.scala`).

| AC | Result | Evidence |
|---|---|---|
| AC-3.1 | ✅ | `ContractSamplesSpec`: 5 valid samples accepted, 8 invalid samples rejected with the code in `expected.json`; the DLQ schema's code list equals `ErrorCode.all` |
| AC-3.2 | ✅ | `GeneratorParitySpec`: all 513 records of the committed generator snapshot (all 10 malformed types) classified exactly as the Python generator labelled them |
| AC-3.3 | ✅ | `RulesSpec`: 43 rejection cases, 11 acceptance cases, whole-record cases, the unsupported-major rule, and a 3-violation record (3 errors with JSON pointers, primary by precedence) |
| AC-3.4 | ✅ | `TimeBoundsSpec`: +5 min accepted / +5 min +1 s rejected; −30 d accepted / −30 d −1 s rejected (live); replay accepts −31 d and −400 d; bounds configurable |
| AC-3.5 | ✅ | `TotalitySpec`: 12 000 generated inputs (random bytes, truncations, field mutations); no exception, all rejections well-formed |
| AC-3.6 | ✅ | `TypingSpec`: `-04:00` offset normalised to UTC, microseconds kept, shipment/details/attributes typed, event IDs lowercased |
| AC-3.7 | ✅ | `CoreIsolationSpec`: no Spark/Kafka/JDBC classes on the core classpath; Jackson 2.21.2 = Spark 4.1.3's version |

Totals: `make ci` → 88 core tests + 3 Spark smoke tests passed, 47 Python tests passed, 0 pin
problems, lint clean. Fixture secret scan: no leaks.

## Do the tests catch bugs? (manual mutation check)

| Injected bug | Caught by |
|---|---|
| Future bound made exclusive (`isAfter` → `!isBefore`) | `TimeBoundsSpec` "future skew" failed |
| `INVALID_FORMAT` precedence 9 → 7 | `RulesSpec` "collects every violation…" failed |

Both bugs were reverted after the check.

## Issues found while testing (tests fixed, not the validator)

- Scala inferred Jackson's generic `ObjectNode.set` as returning `Nothing`, which caused a `ClassCastException`. Fixed with explicit `set[ObjectNode]`.
- The truncation property cut only a sample's trailing newline, which leaves valid JSON. Fixed by trimming samples before truncating.

## Spec clarifications added (spec 02 §2 "Validation details")

These cover behaviour the spec left open. None changes an approved error code: tombstone → `INVALID_JSON`;
duplicate keys / trailing content → `INVALID_JSON`; `null` in a required field → `INVALID_FIELD_TYPE`;
another major → only `UNSUPPORTED_SCHEMA_VERSION`; impossible dates and non-IANA zones → `INVALID_FORMAT`;
time bounds inclusive.
