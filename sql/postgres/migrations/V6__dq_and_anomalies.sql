-- Data-quality results and anomalies (SC-0006 B).

-- Grain: one row per (event_id, anomaly_code). Rebuilt by each DQ run for the packages it evaluates.
CREATE TABLE tracking.status_anomalies (
    event_id     text        NOT NULL,
    anomaly_code text        NOT NULL CHECK (anomaly_code IN ('ORPHAN_EVENT', 'POST_TERMINAL_EVENT', 'SEMANTIC_DUPLICATE')),
    package_id   text        NOT NULL,
    detected_at  timestamptz NOT NULL,
    run_id       text        NOT NULL,
    PRIMARY KEY (event_id, anomaly_code)
);
CREATE INDEX status_anomalies_package ON tracking.status_anomalies (package_id);

-- Grain: one row per (run_id, check_id).
CREATE TABLE ops.dq_results (
    run_id      text             NOT NULL,
    check_id    text             NOT NULL,
    window_from date             NOT NULL,
    window_to   date             NOT NULL,
    status      text             NOT NULL CHECK (status IN ('pass', 'warn', 'fail', 'skipped')),
    severity    text             NOT NULL CHECK (severity IN ('critical', 'high', 'warn')),
    observed    double precision,
    threshold   text             NOT NULL,
    sample      text[]           NOT NULL DEFAULT '{}',
    detail      text,
    checked_at  timestamptz      NOT NULL,
    PRIMARY KEY (run_id, check_id)
);

-- Retention (NFR-08): drop processed_events partitions entirely older than `before`. SECURITY DEFINER, so callers
-- need no DDL rights. Returns the dropped partition names.
CREATE FUNCTION tracking.drop_processed_events_partitions_before(before date) RETURNS SETOF text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, tracking AS $$
DECLARE
    part record;
BEGIN
    FOR part IN
        SELECT c.relname AS name
        FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = p.relnamespace
        WHERE n.nspname = 'tracking' AND p.relname = 'processed_events'
          AND c.relname ~ '^processed_events_[0-9]{8}$'
          AND to_date(substring(c.relname from '[0-9]{8}$'), 'YYYYMMDD') < before
    LOOP
        EXECUTE format('DROP TABLE tracking.%I', part.name);
        RETURN NEXT part.name;
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION tracking.drop_processed_events_partitions_before(date) FROM PUBLIC;

-- The DQ job and maintenance run as ptp_batch_writer (spec 06: read on tracking).
GRANT USAGE ON SCHEMA tracking TO ptp_batch_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA tracking TO ptp_batch_writer;
GRANT SELECT ON ops.sink_batches TO ptp_batch_writer;
GRANT SELECT, INSERT, DELETE ON tracking.status_anomalies TO ptp_batch_writer;
GRANT SELECT, INSERT ON ops.dq_results TO ptp_batch_writer;
GRANT EXECUTE ON FUNCTION tracking.drop_processed_events_partitions_before(date) TO ptp_batch_writer;
