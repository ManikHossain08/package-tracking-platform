#!/usr/bin/env bash
# Idempotent topic setup (FR-ING-02, spec 02 §1). Safe to run any number of times:
# creates missing topics, re-applies configs, and fails loudly if partition counts drift.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:9092}"
BIN=/opt/kafka/bin

# name partitions retention_ms timestamp_type
# package-events.v1 uses broker LogAppendTime: the ingest-time clock for time-bound validation (SC-0004).
TOPICS=(
  "package-events.v1 6 604800000 LogAppendTime"
  "package-events.dlq.v1 3 1209600000 CreateTime"
)

log() { printf '{"component":"kafka-init","level":"%s","message":"%s"}\n' "$1" "$2"; }

for spec in "${TOPICS[@]}"; do
  read -r name partitions retention_ms timestamp_type <<<"$spec"

  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=$retention_ms" --config cleanup.policy=delete \
    --config "message.timestamp.type=$timestamp_type" >/dev/null

  # Re-apply configs so manual drift is corrected (no-op when already equal).
  "$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics \
    --entity-name "$name" --add-config "retention.ms=$retention_ms,cleanup.policy=delete,message.timestamp.type=$timestamp_type" >/dev/null

  actual=$("$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" \
    | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
  if [[ "$actual" != "$partitions" ]]; then
    log error "topic $name has $actual partitions, expected $partitions; partition count is part of the contract and cannot be changed in place (see spec 02 section 1)"
    exit 1
  fi
  log info "topic $name ready: partitions=$partitions retention.ms=$retention_ms timestamp=$timestamp_type"
done
