#!/usr/bin/env bash
# Entry point: run.sh <processor|replay|lake> [args...]. `exec` keeps the JVM as the signal target (with `init: true`).
set -euo pipefail
case "${1:-}" in
  processor) class=ptp.streaming.StatusProcessorApp ;;
  replay)    class=ptp.streaming.ReplayApp ;;
  lake)      class=ptp.lake.LakeApp ;;
  *) echo '{"component":"ptp-jobs","level":"error","error_code":"UNKNOWN_JOB","message":"usage: run.sh processor|replay|lake [args]"}' >&2; exit 2 ;;
esac
shift
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${DRIVER_MEMORY:-1536m}" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=/opt/ptp/log4j2.properties" \
  --conf spark.ui.enabled="${SPARK_UI_ENABLED:-false}" \
  --class "$class" /opt/ptp/ptp-jobs.jar "$@"
