#!/usr/bin/env bash
# Creates/updates application roles from .env passwords. Run by `make up` (idempotent).
set -euo pipefail
COMPOSE="${COMPOSE:?run via make}"
: "${PTP_MIGRATOR_PASSWORD:?missing in .env (run make env)}"
: "${PTP_STREAM_WRITER_PASSWORD:?missing in .env (run make env)}"
: "${PTP_READER_PASSWORD:?missing in .env (run make env)}"
: "${PTP_BATCH_WRITER_PASSWORD:?missing in .env (run make env)}"
: "${PTP_AIRFLOW_PASSWORD:?missing in .env (run make env)}"
$COMPOSE exec -T postgres sh -c 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v migrator_pw="$1" -v stream_writer_pw="$2" -v reader_pw="$3" -v batch_writer_pw="$4" -v airflow_pw="$5" -f /opt/ptp/bootstrap-roles.sql' \
  _ "$PTP_MIGRATOR_PASSWORD" "$PTP_STREAM_WRITER_PASSWORD" "$PTP_READER_PASSWORD" "$PTP_BATCH_WRITER_PASSWORD" "$PTP_AIRFLOW_PASSWORD"
echo '{"component":"pg-bootstrap","level":"info","message":"roles ptp_migrator, ptp_stream_writer, ptp_reader, ptp_batch_writer, ptp_airflow ready"}'
