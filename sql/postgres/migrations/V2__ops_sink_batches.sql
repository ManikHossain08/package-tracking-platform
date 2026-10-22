-- Grain: one row per (query_name, batch_id). Observability and latency measurement only;
-- not the idempotency mechanism (ADR-0002). attempts > 1 means the micro-batch was re-run.
CREATE TABLE ops.sink_batches (
    query_name         text        NOT NULL,
    batch_id           bigint      NOT NULL,
    run_id             text        NOT NULL,
    processor_version  text        NOT NULL,
    started_at         timestamptz NOT NULL,
    committed_at       timestamptz NOT NULL,
    input_rows         bigint      NOT NULL,
    valid_rows         bigint      NOT NULL,
    rejected_rows      bigint      NOT NULL,
    applied_rows       bigint      NOT NULL,
    duplicate_rows     bigint      NOT NULL,
    offset_ranges      jsonb       NOT NULL,
    attempts           integer     NOT NULL DEFAULT 1,
    PRIMARY KEY (query_name, batch_id)
);
