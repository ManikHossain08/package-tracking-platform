#!/usr/bin/env bash
# AC-8.5 … AC-8.9 against the running stack (Prometheus, Pushgateway, Grafana, containerised processor).
# Needs: make up, make image, make obs-up. Bounded polling on the Prometheus/Grafana APIs; no sleeps as synchronisation.
set -uo pipefail
COMPOSE="${COMPOSE:?run via make verify-obs}"
set -a; . ./.env; set +a
DC="$COMPOSE --profile core --profile stream --profile obs"
failures=0
pass() { echo "PASS $1"; }
fail() { echo "FAIL $1"; failures=$((failures + 1)); }
prom() { curl -s "http://127.0.0.1:9090/api/v1/$1"; }
graf() { curl -s -u "admin:$GRAFANA_ADMIN_PASSWORD" "http://127.0.0.1:3000/api/$1"; }
# until <seconds> <command…>: re-evaluate a condition until it holds or the deadline passes.
until_true() { local deadline=$((SECONDS + $1)); shift; while ! "$@"; do (( SECONDS < deadline )) || return 1; python3 -c 'import time; time.sleep(2)'; done; }
alert_firing() { prom alerts | python3 -c "import sys,json; a=json.load(sys.stdin)['data']['alerts']; sys.exit(0 if any(x['labels']['alertname']=='$1' and x['state']=='firing' for x in a) else 1)"; }
target_up() { prom targets | python3 -c "import sys,json; t=json.load(sys.stdin)['data']['activeTargets']; sys.exit(0 if any(x['labels']['job']=='$1' and x['health']=='up' for x in t) else 1)"; }

# Data through the containerised processor, then the batch jobs on the host (pushing to the Pushgateway).
$DC up -d spark-processor >/dev/null 2>&1
OUT=data/generator/verify-obs
PYTHONPATH=generator .venv/bin/python -m ptp_generator --scenario bench/scenarios/smoke.yaml --seed "$RANDOM" --sink kafka --out-dir "$OUT" >/dev/null 2>&1
python3 scripts/reconcile.py "$OUT" --wait-seconds 240 | grep -q '"status_mismatches": 0' && pass "data processed (reconciled)" || fail "reconcile"
make lake >/dev/null 2>&1 && pass "make lake (silver, gold, dq) pushed batch metrics" || fail "make lake"

# AC-8.5: targets and rules.
until_true 120 target_up ptp-processor && pass "AC-8.5 ptp-processor target up" || fail "AC-8.5 ptp-processor target"
until_true 60 target_up pushgateway && pass "AC-8.5 pushgateway target up" || fail "AC-8.5 pushgateway target"
$DC exec -T prometheus promtool check config /etc/prometheus/prometheus.yml >/dev/null 2>&1 && pass "AC-8.5 promtool check config (rules included)" || fail "AC-8.5 config/rules"

# AC-8.8: Grafana provisioning, datasources and every referenced ptp_* metric.
[[ "$(graf health | python3 -c 'import sys,json; print(json.load(sys.stdin)["database"])')" == ok ]] && pass "AC-8.8 grafana healthy" || fail "AC-8.8 grafana health"
for uid in ptp-streaming ptp-lake-dq; do
  graf "dashboards/uid/$uid" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d['dashboard']['uid']=='$uid' else 1)" \
    && pass "AC-8.8 dashboard $uid provisioned" || fail "AC-8.8 dashboard $uid"
done
for ds in ptp-prometheus ptp-postgres; do
  graf "datasources/uid/$ds/health" | grep -qi '"status":"OK"' && pass "AC-8.8 datasource $ds healthy" || fail "AC-8.8 datasource $ds: $(graf "datasources/uid/$ds/health")"
done
missing_metrics() {
  local known; known=$(prom label/__name__/values | python3 -c "import sys,json; print('\n'.join(json.load(sys.stdin)['data']))")
  grep -ohE 'ptp_[a-z0-9_]+' infra/observability/grafana/dashboards/*.json infra/observability/rules/ptp-alerts.yml | sort -u \
    | while read -r m; do grep -qx "$m" <<<"$known" || echo "$m"; done
}
no_missing_metrics() { [[ -z "$(missing_metrics)" ]]; }
# Pushed batch metrics appear after the next Pushgateway scrape (10 s), so wait for the condition, bounded.
until_true 60 no_missing_metrics && pass "AC-8.8 every ptp_* metric in dashboards/rules exists in Prometheus" \
  || fail "AC-8.8 missing metrics: $(echo $(missing_metrics))"

python3 scripts/check_dashboards.py >/dev/null 2>&1 && pass "AC-8.8 every dashboard panel query executes without error" \
  || fail "AC-8.8 panel queries (run scripts/check_dashboards.py)"
until_true 60 sh -c "curl -s --get http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=ptp_heartbeat_timestamp_seconds' | grep -q spark-processor" \
  && pass "AC-8.3 driver heartbeat exported while idle" || fail "AC-8.3 heartbeat missing"

alert_quiet() { prom alerts | python3 -c "import sys,json; a=json.load(sys.stdin)['data']['alerts']; sys.exit(1 if any(x['labels']['alertname']=='$1' for x in a) else 0)"; }
alert_quiet PtpQueryStalled && pass "AC-8.7 an idle, healthy processor does not trigger PtpQueryStalled" || fail "AC-8.7 false stall alarm"

# AC-8.9: localhost-only ports, no anonymous Grafana, read-only Postgres datasource.
for s in prometheus:9090 pushgateway:9091 grafana:3000; do
  [[ "$($DC port "${s%%:*}" "${s##*:}")" == "127.0.0.1:${s##*:}" ]] && pass "AC-8.9 ${s%%:*} bound to 127.0.0.1" || fail "AC-8.9 ${s%%:*} binding"
done
[[ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:3000/api/search)" == 401 ]] && pass "AC-8.9 grafana rejects anonymous API access" || fail "AC-8.9 anonymous access"
[[ "$(graf datasources/uid/ptp-postgres | python3 -c 'import sys,json; print(json.load(sys.stdin)["user"])')" == ptp_reader ]] \
  && pass "AC-8.9 Postgres datasource uses ptp_reader" || fail "AC-8.9 datasource user"

# AC-8.7: live alerts. (1) a planted critical DQ failure; (2) the processor stopped.
part="$PTP_HOST_DATA_DIR/lake/silver/package_events/event_date=$(date -u +%F)"
dup="$part/part-planted-duplicate.parquet"; cp "$(ls "$part"/*.parquet | head -1)" "$dup"
make lake >/dev/null 2>&1 && fail "AC-8.7 make lake should exit 1 with a critical DQ failure" || true
until_true 90 alert_firing PtpDqCriticalFailure && pass "AC-8.7 PtpDqCriticalFailure fires" || fail "AC-8.7 PtpDqCriticalFailure"
rm -f "$dup"; make lake >/dev/null 2>&1
$DC stop spark-processor >/dev/null 2>&1
until_true 180 alert_firing PtpProcessorDown && pass "AC-8.7 PtpProcessorDown fires after the processor stops" || fail "AC-8.7 PtpProcessorDown"
$DC up -d spark-processor >/dev/null 2>&1

echo "verify-obs: $failures failure(s)"
exit $((failures > 0))
