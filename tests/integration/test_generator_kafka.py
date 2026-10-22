"""TS-I-06: the generator publishes every record to Kafka, keyed by package_id (AC-2.8).

Needs the core stack (make up). Synchronisation is bounded polling on observable state.
"""

import json
import os
import time
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition
from ptp_generator.cli import main

pytestmark = [pytest.mark.integration, pytest.mark.req("FR-ING-01", "FR-GEN-01")]

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "package-events.v1"
SCENARIO = Path(__file__).resolve().parents[2] / "bench/scenarios/smoke.yaml"
READ_TIMEOUT_S = 60


def end_offsets(consumer: Consumer) -> dict[int, tuple[int, int]]:
    meta = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
    assert meta.error is None, f"topic {TOPIC} unavailable: {meta.error} (is the stack up? run make up)"
    return {p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10) for p in meta.partitions}


def test_every_record_is_published_once_and_keyed_by_package(tmp_path):
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": "ptp-it-generator", "enable.auto.commit": False}
    )
    try:
        before = end_offsets(consumer)
        assert (
            main(
                [
                    "--scenario",
                    str(SCENARIO),
                    "--sink",
                    "kafka",
                    "--out-dir",
                    str(tmp_path),
                    "--bootstrap-servers",
                    BOOTSTRAP,
                ]
            )
            == 0
        )

        summary = json.loads((tmp_path / "summary.json").read_text())
        deliveries = [json.loads(line) for line in (tmp_path / "delivery.jsonl").read_text().splitlines()]
        assert len(deliveries) == summary["records_total"]
        assert sorted(d["index"] for d in deliveries) == list(range(summary["records_total"]))

        after = end_offsets(consumer)
        produced = sum(after[p][1] - before[p][1] for p in after)
        assert produced == summary["records_total"]

        # Read back exactly the new range and check keys (bounded polling, no sleeps).
        consumer.assign([TopicPartition(TOPIC, p, before[p][1]) for p in after])
        remaining = produced
        keys_by_partition: dict[int, set[str]] = {}
        deadline = time.monotonic() + READ_TIMEOUT_S
        while remaining and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            if msg.offset() < after[msg.partition()][1]:
                keys_by_partition.setdefault(msg.partition(), set()).add(msg.key().decode())
                remaining -= 1
        assert remaining == 0, f"{remaining} records not read back within {READ_TIMEOUT_S}s"

        manifest = {
            json.loads(line)["package_id"] for line in (tmp_path / "manifest.jsonl").read_text().splitlines()
        }
        all_keys = set().union(*keys_by_partition.values())
        assert all_keys == manifest
        # Same key always lands on one partition (per-package ordering within Kafka).
        seen: dict[str, int] = {}
        for partition, keys in keys_by_partition.items():
            for key in keys:
                assert seen.setdefault(key, partition) == partition
    finally:
        consumer.close()
