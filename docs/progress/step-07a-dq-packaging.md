# Step 7a — data-quality job + jobs image: results (2026-10-22)

Acceptance criteria: spec 07, AC-7.1 … AC-7.6 (7b = Airflow, AC-7.7 … 7.11). Decisions approved for this step:
**ADR-0009** option A (jobs image; Airflow launches it through a Docker socket proxy in 7b), **SC-0006** (exact DQ
definitions, tables, roles, retention mechanics), step split 7a/7b.

## What was built

| Area | Files |
|---|---|
| Schema | `V6__dq_and_anomalies.sql`: `tracking.status_anomalies`, `ops.dq_results`, retention function `drop_processed_events_partitions_before`, grants for `ptp_batch_writer` |
| DQ rules (pure) | `core/dq/DqRules.scala`: post-terminal (DQ-05), orphan (DQ-04), volume (DQ-10) |
| DQ job | `ptp/lake/DqJob.scala`: DQ-02 … DQ-10 per SC-0006; anomalies replaced per evaluated package; results to `ops.dq_results`; `LakeApp dq` exits 1 on a critical/high failure; `make lake` = silver → gold → dq |
| Oracle | `generator/ptp_generator/gold.py`: predicts post-terminal events (`expected_anomalies.jsonl`) |
| Packaging | sbt-assembly (`make jar`, 12 MB: excludes all 285 artifacts the Spark image ships), `infra/jobs/` (Dockerfile on the pinned Spark image, `run.sh`), `make image` → `ptp-jobs:<git sha>` |
| Processor container | Compose service `spark-processor` (profile `stream`, `init: true`); SIGTERM/SIGINT handler stops both queries and exits 0; `make stream-up/stream-down/verify-processor` |

## Acceptance results

| AC | Result | Evidence |
|---|---|---|
| AC-7.1 | ✅ | 9 results per run (DQ-02 … DQ-10), each a row in `ops.dq_results` |
| AC-7.2 | ✅ | `it-gold` run: DQ-02/03/04/06/07/08/09 pass (DQ-07 compared > 0 packages), DQ-05 pass/warn, DQ-10 `skipped` (history 0/7); DQ-05 observed = the oracle's predicted post-terminal count |
| AC-7.3 | ✅ | Planted faults, each detected with the planted id in `sample`, and the **full status map** of all other checks asserted unchanged: duplicated silver file → DQ-02 (+ DQ-07, expected); tampered status → DQ-07; gold count +1 → DQ-08; 30-day dwell → DQ-09; orphan + 60 post-terminal scans → DQ-04/DQ-05; DLQ record without a bronze row → DQ-06; waiting records with a stale commit → DQ-03 |
| AC-7.4 | ✅ | Exit code 1 at every critical/high failure stage; 0 for warning-only stages (DQ-09, DQ-04/05) |
| AC-7.5 | ✅ | `tracking.status_anomalies` = the oracle's rows; identical after a second run |
| AC-7.6 | ✅ | `make verify-processor`: the container reconciles a fresh run (100/100, 0 mismatches), and `docker compose stop` gives exit code 0 with "stop requested" → "queries stopped" (graceful=true) |

Totals: integration 22/22, unit 110 core + 10 streaming, Python 54 + 1 integration; pins, lint, secrets clean.

## Bugs found and fixed

1. **The DQ job crashed on the fault it exists to detect.** A duplicated silver file made the same post-terminal anomaly appear twice,
   and the anomaly insert hit its primary key, which aborted the whole run before any check reported. Fixed: anomaly rows are
   deduplicated and the insert is conflict-tolerant. Found by the AC-7.3 fault sequence.
2. **The jobs jar bundled Hadoop, Scala and compression codecs** (73 MB) through the Kafka connector's transitive dependencies, all already
   in the Spark image (duplicate classes on the classpath). Fixed by excluding every artifact listed in `infra/jobs/spark-image-jars.txt`,
   which is generated from the pinned image (`make spark-jars`): 12 MB.
3. **The pin checker flagged the Makefile target `image:`** as a Compose image key. Fixed: one pattern per file type; Dockerfile `FROM` lines are now checked too.
4. **`--profile stream` alone made the Compose project invalid** (`spark-processor` depends on `core` services), so `stop` silently didn't run.
   Fixed: stream commands activate both profiles; the verification found it.

## Notes

- DQ-10 is verified by unit tests only. An integration run would need ≥ 7 days of silver history, which demo data doesn't have.
- DQ-07 skips packages whose silver history starts before the read range (documented in SC-0006); at scale, extend the lookback.
- Docker Desktop memory: the owner will raise it to 10 GB (D8) before 7b adds Airflow.
- Prometheus export of DQ results is deferred to the observability step (SC-0006 D).
