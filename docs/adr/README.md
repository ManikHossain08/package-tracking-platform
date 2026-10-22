# Architecture decision records

| ADR | Title | Status |
|---|---|---|
| [0001](0001-repo-and-stack-versions.md) | Repository location and stack version lines | Accepted |
| [0002](0002-delivery-semantics.md) | Delivery and processing semantics | Accepted |
| [0003](0003-idempotency-key.md) | Idempotency key | Accepted |
| [0004](0004-ordering-and-status-policy.md) | Ordering and status policy | Accepted (amended by SC-0002) |
| [0005](0005-watermark-and-lateness.md) | Watermark and lateness policy | Accepted |
| [0006](0006-sink-topology.md) | Streaming sink topology | Accepted |
| 0007 | JSON Schema in repo vs schema registry | Planned |
| 0008 | Airflow vs Dagster for batch and recovery | Planned |

Statuses: Proposed → Accepted → (Superseded by NNNN | Deprecated). Accepted ADRs are not
edited; they are superseded by a new ADR. New ADRs use [template.md](template.md).
