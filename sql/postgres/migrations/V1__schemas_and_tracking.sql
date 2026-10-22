-- Serving schemas and the status tables (spec 02 §4). Applied by Flyway as ptp_migrator.
CREATE SCHEMA IF NOT EXISTS tracking;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grain: one row per package. Status columns follow the spec 01 preference rule (SC-0002).
CREATE TABLE tracking.package_status (
    package_id                 text        PRIMARY KEY,
    current_status             text        NOT NULL,
    status_event_id            uuid        NOT NULL,
    status_event_time          timestamptz NOT NULL,
    status_rank                smallint    NOT NULL,
    status_terminal            boolean     NOT NULL,
    facility_id                text,
    vehicle_id                 text,
    promised_delivery_date     date,
    service_level              text,
    origin_facility_id         text,
    destination_postal_prefix  text,
    destination_timezone       text,
    created_event_time         timestamptz,
    delivered_event_time       timestamptz,
    last_event_time            timestamptz NOT NULL,
    event_count                integer     NOT NULL CHECK (event_count > 0),
    anomaly_count              integer     NOT NULL DEFAULT 0,
    first_seen_at              timestamptz NOT NULL,
    updated_at                 timestamptz NOT NULL,
    updated_by_query           text        NOT NULL,
    updated_batch_id           bigint      NOT NULL
);
COMMENT ON TABLE tracking.package_status IS
    'Grain: one row per package_id. Status = most preferred event seen (spec 01, ADR-0004 Amendment 1).';

-- Grain: one row per unique event_id. Durable dedup horizon (FR-DED-01, ADR-0002/0003).
-- event_date is derived from event_time, so every duplicate lands in the same partition.
CREATE TABLE tracking.processed_events (
    event_id            uuid        NOT NULL,
    event_date          date        NOT NULL,
    package_id          text        NOT NULL,
    first_batch_id      bigint      NOT NULL,
    first_processed_at  timestamptz NOT NULL,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);
COMMENT ON TABLE tracking.processed_events IS
    'Grain: one row per event_id. Daily partitions; retention by dropping partitions (NFR-08).';

-- Creates the daily partition on demand. SECURITY DEFINER so the stream writer needs no DDL rights.
CREATE FUNCTION tracking.ensure_processed_events_partition(d date) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, tracking AS $$
DECLARE
    part text := format('processed_events_%s', to_char(d, 'YYYYMMDD'));
BEGIN
    IF to_regclass(format('tracking.%I', part)) IS NULL THEN
        BEGIN
            EXECUTE format('CREATE TABLE tracking.%I PARTITION OF tracking.processed_events FOR VALUES FROM (%L) TO (%L)',
                           part, d, d + 1);
        EXCEPTION WHEN duplicate_table OR unique_violation THEN
            NULL; -- created concurrently by another session
        END;
    END IF;
END
$$;
REVOKE ALL ON FUNCTION tracking.ensure_processed_events_partition(date) FROM PUBLIC;
