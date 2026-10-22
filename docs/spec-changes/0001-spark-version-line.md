# SC-0001: Change the Spark version line from 4.0.x to 4.1.x

- **Status:** Approved (2026-10-21) — option A, Spark 4.1.3
- **Affects:** ADR-0001 (Decision 2), D2

## 1. Change

ADR-0001 Spark line: **4.0.x → 4.1.x**, pinned at **4.1.3**.

## 2. Reason (evidence gathered 2026-10-21 while pinning versions)

| Line | Latest patch | Notes |
|---|---|---|
| 4.0.x | 4.0.4 | Accepted in D2. Spark feature releases get ~18 months of maintenance. 4.0.0 shipped in May 2025, so 4.0.x support ends around **Nov 2026**, during this project. |
| **4.1.x** | **4.1.3** | Several patch releases in, so mature. Maintained well into 2027. |
| 4.2.x | 4.2.0 | Newest. The Spark versioning policy confirms an 18-month maintenance window for 4.2.x. It's a `.0` release, so early-bug risk is highest. |

Sources: Maven Central `maven-metadata.xml` for `spark-sql_2.13`, the `archive.apache.org/dist/spark/`
listing, Docker Hub `apache/spark` tags, and spark.apache.org/versioning-policy.html.

D2 was proposed on the assumption that 4.0 was the current line. That assumption was out of date.

## 3. Impact

- **Code:** none yet; no code exists.
- **Compatibility:** all three lines use Scala 2.13, Java 17 and Kafka client 3.9.x
  (4.1.3 → Kafka 3.9.1, Scala 2.13.17). The other pins in ADR-0001 are unchanged.
- **Docs:** ADR-0001 rationale row for Spark.
- **Risk:** low. The APIs used here (Structured Streaming, Kafka source, file sink,
  `foreachBatch`, `dropDuplicatesWithinWatermark`) are stable across 4.x.

## 4. Options

| Option | Recommendation |
|---|---|
| **A. 4.1.3** | ✅ Recommended: mature patch level, support outlasts the project build-out |
| B. 4.2.0 | Longest support window, but a fresh `.0`. Revisit at 4.2.1+. |
| C. Keep 4.0.4 | Not recommended: maintenance ends imminently |

## 5. Decision

Approved by the project owner on 2026-10-21: **option A, Spark 4.1.3**. ADR-0001 updated in the same change.
