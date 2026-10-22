-- Grain: one row per replay run (FR-REC-02, AC-5.7). Written at start (running) and at the end.
CREATE TABLE ops.replay_runs (
    run_id           text        PRIMARY KEY,
    ingest_date_from date        NOT NULL,
    ingest_date_to   date        NOT NULL,
    mode             text        NOT NULL CHECK (mode IN ('as-ingested', 'backfill')),
    dry_run          boolean     NOT NULL,
    status           text        NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz,
    input_rows       bigint,
    valid_rows       bigint,
    rejected_rows    bigint,
    applied_rows     bigint,
    duplicate_rows   bigint,
    error            text
);
GRANT SELECT, INSERT, UPDATE ON ops.replay_runs TO ptp_stream_writer;
