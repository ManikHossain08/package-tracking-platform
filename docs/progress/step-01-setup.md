# Step 1 — setup: results (2026-10-21)

Acceptance criteria: spec 07, AC-0.1 … AC-0.8. Commands were run on the dev machine
(Apple M4, 16 GB, Docker 28.1.1, OpenJDK 17.0.15, Python 3.12.7).

| AC | Result | Evidence |
|---|---|---|
| AC-0.1 | ✅ | `make up`: Kafka and Postgres healthy within the 120 s bound; `make verify`: Postgres accepts queries |
| AC-0.2 | ✅ | `package-events.v1` partitions=6 retention.ms=604800000; `package-events.dlq.v1` partitions=3 retention.ms=1209600000 |
| AC-0.3 | ✅ | Producing to `ptp-autocreate-probe` failed; the topic was not created |
| AC-0.4 | ✅ | kafka-init re-run succeeded with no change. Negative check: after injecting `retention.ms=1000`, `make verify` reported 2 failures, and kafka-init restored the config (next run: 0 failures). |
| AC-0.5 | ✅ | `make test`: 17 Python contract tests passed; 3 Scala tests passed (Spark 4.1.3 version check, Kafka connector on classpath, streaming query via `processAllAvailable()`) |
| AC-0.6 | ✅ | `make check-pins`: 0 problems. Negative check: replacing the Postgres image with `postgres:17` was reported. |
| AC-0.7 | ✅ | `make secrets-scan`: no leaks. One false positive (a synthetic `PKG-…` Kafka key in a DLQ sample) was allowlisted narrowly in `.gitleaks.toml`. |
| AC-0.8 | ⏳ | Workflow written. It has not run yet, because the repo has no GitHub remote. |

Idle memory with the `core` profile: Kafka ≈410 MiB (limit 1 GiB), Postgres ≈22 MiB (limit 512 MiB).

## Issues found and fixed while verifying

- `verify_stack.sh` exited early: `cmd | head -1` under `set -o pipefail` killed the producer with SIGPIPE. Replaced with `sed -n 1p`.
- The pin checker flagged `"scalatest"` as floating because it contains the substring "latest". It now checks the version declarations themselves.
