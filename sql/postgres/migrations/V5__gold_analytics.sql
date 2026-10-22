-- Gold layer (FR-ANL-01, SC-0005 part B). Facts + daily aggregates; loaded by ptp_batch_writer.

-- Grain: one row per delivered package (C1–C3).
CREATE TABLE analytics.delivery_facts (
    package_id                text             PRIMARY KEY,
    delivery_date             date             NOT NULL,  -- destination-local
    delivered_at              timestamptz      NOT NULL,
    promised_delivery_date    date             NOT NULL,
    service_level             text             NOT NULL,
    destination_postal_prefix text             NOT NULL,
    destination_timezone      text             NOT NULL,
    delay_minutes             double precision NOT NULL,  -- signed: negative = early
    on_time                   boolean          NOT NULL,
    computed_at               timestamptz      NOT NULL,
    run_id                    text             NOT NULL
);
CREATE INDEX delivery_facts_delivery_date ON analytics.delivery_facts (delivery_date);

-- Grain: one row per arrival event (C5–C6). Keyed by the arrival's event_id: semantic duplicates of an arrival
-- are distinct events with the same timestamp, and each is its own visit.
CREATE TABLE analytics.facility_visits (
    arrival_event_id text           PRIMARY KEY,
    package_id     text             NOT NULL,
    facility_id    text             NOT NULL,
    arrived_at     timestamptz      NOT NULL,
    arrival_date   date             NOT NULL,  -- UTC
    departed_at    timestamptz,
    departure_date date,                       -- UTC
    dwell_minutes  double precision,
    computed_at    timestamptz      NOT NULL,
    run_id         text             NOT NULL
);
CREATE INDEX facility_visits_arrival_date ON analytics.facility_visits (arrival_date);
CREATE INDEX facility_visits_departure ON analytics.facility_visits (departure_date, facility_id);

CREATE TABLE analytics.daily_delivery_performance (
    delivery_date             date             NOT NULL,
    service_level             text             NOT NULL,
    destination_postal_prefix text             NOT NULL,
    delivered_count           integer          NOT NULL,
    on_time_count             integer          NOT NULL,
    on_time_pct               double precision NOT NULL,
    delay_p50_min             double precision NOT NULL,
    delay_p95_min             double precision NOT NULL,
    computed_at               timestamptz      NOT NULL,
    run_id                    text             NOT NULL,
    PRIMARY KEY (delivery_date, service_level, destination_postal_prefix)
);

CREATE TABLE analytics.daily_facility_performance (
    activity_date   date             NOT NULL,  -- UTC
    facility_id     text             NOT NULL,
    arrivals        integer          NOT NULL,
    departures      integer          NOT NULL,
    dwell_p50_min   double precision,
    dwell_p95_min   double precision,
    exception_count integer          NOT NULL,
    computed_at     timestamptz      NOT NULL,
    run_id          text             NOT NULL,
    PRIMARY KEY (activity_date, facility_id)
);

-- C7: KPI-B4 exception rate.
CREATE TABLE analytics.daily_network_activity (
    activity_date           date             PRIMARY KEY,  -- UTC
    active_packages         integer          NOT NULL,
    packages_with_exception integer          NOT NULL,
    exception_rate          double precision NOT NULL,
    computed_at             timestamptz      NOT NULL,
    run_id                  text             NOT NULL
);

-- Staging for the one-transaction window replace (ops: not readable by ptp_reader).
CREATE TABLE ops.gold_stage_delivery_facts (LIKE analytics.delivery_facts INCLUDING DEFAULTS, gold_run_id text NOT NULL);
ALTER TABLE ops.gold_stage_delivery_facts DROP COLUMN computed_at, DROP COLUMN run_id;
CREATE TABLE ops.gold_stage_facility_visits (LIKE analytics.facility_visits INCLUDING DEFAULTS, gold_run_id text NOT NULL);
ALTER TABLE ops.gold_stage_facility_visits DROP COLUMN computed_at, DROP COLUMN run_id;
CREATE TABLE ops.gold_stage_facility_counts (
    activity_date date NOT NULL, facility_id text NOT NULL, arrivals integer NOT NULL, departures integer NOT NULL,
    exception_count integer NOT NULL, gold_run_id text NOT NULL
);
CREATE TABLE ops.gold_stage_network_activity (
    activity_date date NOT NULL, active_packages integer NOT NULL, packages_with_exception integer NOT NULL,
    gold_run_id text NOT NULL
);

-- Grain: one row per gold run (AC-6.8).
CREATE TABLE ops.gold_runs (
    run_id          text        PRIMARY KEY,
    as_of           date        NOT NULL,
    window_from     date        NOT NULL,
    window_to       date        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    delivery_facts  bigint,
    facility_visits bigint,
    error           text
);

-- ptp_batch_writer (spec 06): analytics + its own ops tables only. No access to tracking is needed.
GRANT USAGE ON SCHEMA analytics, ops TO ptp_batch_writer;
GRANT SELECT, INSERT, DELETE ON analytics.delivery_facts, analytics.facility_visits, analytics.daily_delivery_performance,
    analytics.daily_facility_performance, analytics.daily_network_activity TO ptp_batch_writer;
GRANT SELECT, INSERT, DELETE ON ops.gold_stage_delivery_facts, ops.gold_stage_facility_visits,
    ops.gold_stage_facility_counts, ops.gold_stage_network_activity TO ptp_batch_writer;
GRANT SELECT, INSERT, UPDATE ON ops.gold_runs TO ptp_batch_writer;
