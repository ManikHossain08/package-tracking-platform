#!/usr/bin/env bash
# AC-7.6 (TS-I-07): the containerised processor applies a generator run correctly and stops cleanly.
set -euo pipefail
COMPOSE="${COMPOSE:?run via make verify-processor}"
OUT=data/generator/verify-processor
$COMPOSE --profile core --profile stream up -d spark-processor >/dev/null
PYTHONPATH=generator .venv/bin/python -m ptp_generator --scenario bench/scenarios/smoke.yaml --seed "$RANDOM" \
  --sink kafka --out-dir "$OUT" >/dev/null 2>&1
python3 scripts/reconcile.py "$OUT" --wait-seconds 240 | tee /dev/stderr | grep -q '"status_mismatches": 0'
echo "PASS AC-7.6 containerised processor reconciles the run"
$COMPOSE --profile core --profile stream stop spark-processor >/dev/null
code=$(docker inspect -f '{{.State.ExitCode}}' "$($COMPOSE --profile core --profile stream ps -a -q spark-processor)")
logs=$($COMPOSE --profile core --profile stream logs --no-color spark-processor)
if [[ "$code" == "0" ]] && grep -q '"message":"queries stopped"' <<<"$logs" && grep -q '"graceful":true' <<<"$logs"; then
  echo "PASS AC-7.6 graceful stop (exit code 0)"
else
  echo "FAIL AC-7.6 stop: exit code $code"; grep -E 'stop requested|queries stopped|ERROR' <<<"$logs" | tail -5; exit 1
fi
