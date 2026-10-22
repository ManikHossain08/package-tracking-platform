-- Least privilege (spec 06). The stream writer can only read/write what the status sink needs.
GRANT USAGE ON SCHEMA tracking, ops TO ptp_stream_writer;
GRANT SELECT, INSERT, UPDATE ON tracking.package_status TO ptp_stream_writer;
GRANT SELECT, INSERT ON tracking.processed_events TO ptp_stream_writer;
GRANT SELECT, INSERT, UPDATE ON ops.sink_batches TO ptp_stream_writer;
GRANT EXECUTE ON FUNCTION tracking.ensure_processed_events_partition(date) TO ptp_stream_writer;

GRANT USAGE ON SCHEMA tracking, analytics TO ptp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA tracking, analytics TO ptp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA tracking, analytics GRANT SELECT ON TABLES TO ptp_reader;
