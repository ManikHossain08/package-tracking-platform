#!/usr/bin/env bash
# Verifies AC-0.2..AC-0.4 against the running core stack. Uses explicit checks, no sleeps.
set -euo pipefail

# Note: avoid `| head` with pipefail; an early-closing reader makes the producer fail with SIGPIPE.
COMPOSE="${COMPOSE:?run via make verify}"
kexec() { $COMPOSE exec -T kafka "$@"; }
BS=kafka:9092
failures=0
pass() { echo "PASS $1"; }
fail() { echo "FAIL $1"; failures=$((failures + 1)); }

check_topic() { # name partitions retention_ms
  local desc parts ret
  desc=$(kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic "$1")
  parts=$(sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' <<<"$desc" | sed -n 1p)
  ret=$(kexec /opt/kafka/bin/kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics \
    --entity-name "$1" | grep -o 'retention.ms=[0-9]*' | sed -n 1p | cut -d= -f2)
  if [[ "$parts" == "$2" && "$ret" == "$3" ]]; then pass "AC-0.2 $1 partitions=$parts retention.ms=$ret"
  else fail "AC-0.2 $1 expected partitions=$2 retention.ms=$3, got partitions=$parts retention.ms=$ret"; fi
}

check_topic package-events.v1 6 604800000
check_topic package-events.dlq.v1 3 1209600000

# AC-0.3: producing to an unknown topic must fail fast and must not create it.
probe="ptp-autocreate-probe"
if echo "probe" | kexec /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --topic "$probe" \
     --producer-property max.block.ms=5000 --producer-property delivery.timeout.ms=6000 \
     --producer-property request.timeout.ms=5000 >/dev/null 2>&1 \
   && kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list | grep -x "$probe" >/dev/null; then
  fail "AC-0.3 topic $probe was auto-created"
elif kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list | grep -x "$probe" >/dev/null; then
  fail "AC-0.3 topic $probe exists"
else
  pass "AC-0.3 auto-creation disabled ($probe not created)"
fi

# AC-0.4: re-running the init succeeds and leaves topic settings unchanged.
before=$(kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic package-events.v1 | sed -n 1p)
if $COMPOSE --profile core run --rm kafka-init >/dev/null 2>&1; then
  after=$(kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic package-events.v1 | sed -n 1p)
  [[ "$before" == "$after" ]] && pass "AC-0.4 kafka-init idempotent" || fail "AC-0.4 topic changed on re-run"
else
  fail "AC-0.4 kafka-init re-run failed"
fi

# Postgres reachable with the configured credentials.
if $COMPOSE exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "select 1"' | grep -x 1 >/dev/null; then
  pass "AC-0.1 postgres accepts queries"
else
  fail "AC-0.1 postgres query failed"
fi

echo "verify: $failures failure(s)"
exit $((failures > 0))
