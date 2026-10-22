-- Least-privilege roles (spec 06). Idempotent; passwords come from psql variables (never from files).
-- Run as the admin user: psql -v migrator_pw=... -v stream_writer_pw=... -v reader_pw=... -f bootstrap-roles.sql
\set ON_ERROR_STOP on

SELECT 'CREATE ROLE ptp_migrator LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ptp_migrator') \gexec
SELECT 'CREATE ROLE ptp_stream_writer LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ptp_stream_writer') \gexec
SELECT 'CREATE ROLE ptp_reader LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ptp_reader') \gexec

ALTER ROLE ptp_migrator      WITH LOGIN PASSWORD :'migrator_pw';
ALTER ROLE ptp_stream_writer WITH LOGIN PASSWORD :'stream_writer_pw';
ALTER ROLE ptp_reader        WITH LOGIN PASSWORD :'reader_pw';

-- The migrator owns the application schemas; the others get grants from the migrations.
SELECT format('GRANT CONNECT, CREATE ON DATABASE %I TO ptp_migrator', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO ptp_stream_writer, ptp_reader', current_database()) \gexec
