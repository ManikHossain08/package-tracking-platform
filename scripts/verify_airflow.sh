#!/usr/bin/env bash
# AC-7.7 … AC-7.10 against the running stack (TS-O-01, TS-O-02). Needs: make up, make image, data in bronze.
# Synchronisation: `up --wait` and synchronous `airflow dags test` runs; no sleeps.
set -uo pipefail
COMPOSE="${COMPOSE:?run via make verify-airflow}"
DC="$COMPOSE --profile core --profile orchestration"
set -a; . ./.env; set +a
failures=0
pass() { echo "PASS $1"; }
fail() { echo "FAIL $1"; failures=$((failures + 1)); }
sql() { $COMPOSE exec -T postgres sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -tAc \"$1\""; }
dagtest() { $DC exec -T airflow airflow dags test "$@" >/tmp/ptp-dagtest.log 2>&1; }
TODAY=$(date -u +%F)

# AC-7.7: healthy, bound to localhost only, password auth, separate metadata database.
$DC up -d --wait --wait-timeout 300 airflow >/dev/null 2>&1 && pass "AC-7.7 airflow healthy" || fail "AC-7.7 airflow not healthy"
[[ "$($DC port airflow 8080)" == 127.0.0.1:8080 ]] && pass "AC-7.7 UI bound to 127.0.0.1 only" || fail "AC-7.7 UI binding: $($DC port airflow 8080)"
login() { curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8080/auth/token -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$1\"}"; }
[[ "$(login "$AIRFLOW_ADMIN_PASSWORD")" =~ ^20[01]$ ]] && pass "AC-7.7 admin login with the .env password" || fail "AC-7.7 admin login"
[[ "$(login wrong-password)" =~ ^40[13]$ ]] && pass "AC-7.7 wrong password rejected" || fail "AC-7.7 wrong password accepted"
[[ "$(sql "SELECT has_database_privilege('ptp_airflow', 'ptp', 'CONNECT')")" == f ]] \
  && pass "AC-7.7 ptp_airflow cannot connect to the application database" || fail "AC-7.7 ptp_airflow can reach ptp"
[[ "$($COMPOSE exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d airflow -tAc "SELECT count(*) FROM alembic_version"')" == 1 ]] \
  && pass "AC-7.7 Airflow metadata lives in database airflow" || fail "AC-7.7 airflow metadata database"

# AC-7.8: DAG integrity.
$DC exec -T airflow python /opt/airflow/checks/check_dags.py 2>/dev/null | grep -q "check_dags: 0 failure(s)" \
  && pass "AC-7.8 DAG integrity (check_dags.py)" || fail "AC-7.8 DAG integrity"

# AC-7.9: lake_daily end to end, then with a planted critical fault (a duplicated silver file → DQ-02).
gold_before=$(sql "SELECT count(*) FROM ops.gold_runs WHERE status = 'succeeded'"); dq_before=$(sql "SELECT count(DISTINCT run_id) FROM ops.dq_results")
if dagtest lake_daily "$TODAY"; then
  [[ $(sql "SELECT count(*) FROM ops.gold_runs WHERE status = 'succeeded'") -gt $gold_before && \
     $(sql "SELECT count(DISTINCT run_id) FROM ops.dq_results") -gt $dq_before ]] \
    && pass "AC-7.9 lake_daily succeeded; new ops.gold_runs and ops.dq_results rows" || fail "AC-7.9 lake_daily ran but wrote no runs"
else fail "AC-7.9 lake_daily failed (see /tmp/ptp-dagtest.log in the airflow container)"; fi
part="$PTP_HOST_DATA_DIR/lake/silver/package_events/event_date=$TODAY"
file=$(ls "$part"/*.parquet | head -1); dup="$part/part-planted-duplicate.parquet"; cp "$file" "$dup"
if dagtest lake_daily "$TODAY"; then fail "AC-7.9 lake_daily succeeded despite a planted DQ-02 fault"
else
  [[ "$(sql "SELECT status FROM ops.dq_results WHERE check_id = 'DQ-02' ORDER BY checked_at DESC LIMIT 1")" == fail ]] \
    && pass "AC-7.9 planted critical fault fails the run at dq (DQ-02 = fail)" || fail "AC-7.9 run failed, but not at DQ-02"
fi
rm -f "$dup"

# AC-7.10: replay through Airflow; invalid params fail before Spark (no new ops.replay_runs row).
runs_before=$(sql "SELECT count(*) FROM ops.replay_runs")
dagtest replay_from_bronze "$TODAY" --conf "{\"ingest_date_from\":\"$TODAY\",\"ingest_date_to\":\"$TODAY\",\"mode\":\"as-ingested\",\"dry_run\":true}" \
  && [[ $(sql "SELECT count(*) FROM ops.replay_runs WHERE dry_run AND status = 'succeeded'") -ge 1 ]] \
  && pass "AC-7.10 replay_from_bronze (dry run) wrote an ops.replay_runs row" || fail "AC-7.10 valid replay"
runs_mid=$(sql "SELECT count(*) FROM ops.replay_runs")
if dagtest replay_from_bronze "$TODAY" --conf "{\"ingest_date_from\":\"$TODAY\",\"ingest_date_to\":\"2026-01-01\",\"mode\":\"as-ingested\",\"dry_run\":false}"; then
  fail "AC-7.10 invalid range accepted"
else
  [[ "$(sql "SELECT count(*) FROM ops.replay_runs")" == "$runs_mid" ]] \
    && pass "AC-7.10 invalid params rejected before any Spark job (no replay run recorded)" || fail "AC-7.10 Spark ran for invalid params"
fi

# AC-7.11 (wiring): maintenance runs through Airflow; the retention logic itself is TS-O-03 (RetentionIT).
dagtest maintenance "$TODAY" && pass "AC-7.11 maintenance DAG ran" || fail "AC-7.11 maintenance DAG"

echo "verify-airflow: $failures failure(s)"
exit $((failures > 0))
