-- Grafana's read-only access to pipeline run history (SC-0007 D). Run logs only: no staging, no bronze/DLQ content.
GRANT USAGE ON SCHEMA ops TO ptp_reader;
GRANT SELECT ON ops.dq_results, ops.gold_runs, ops.replay_runs, ops.sink_batches TO ptp_reader;
