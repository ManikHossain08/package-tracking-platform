"""TS-I-06: the generator publishes every record to Kafka, keyed by package_id (AC-2.8).

Needs the core stack (make up). Publishes to a throwaway topic with the production topic settings, so test
data never mixes with the live package-events.v1 (same seed = same package IDs). Synchronisation is
bounded polling on observable state.
"""

import json
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from ptp_generator.cli import main

pytestmark = [pytest.mark.integration, pytest.mark.req("FR-ING-01", "FR-GEN-01")]

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCENARIO = Path(__file__).resolve().parents[2] / "bench/scenarios/smoke.yaml"
READ_TIMEOUT_S = 60


@pytest.fixture
def topic() -> Iterator[str]:
    """A fresh topic shaped like package-events.v1 (6 partitions, LogAppendTime), deleted afterwards."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    name = f"it-generator-{uuid.uuid4().hex[:8]}"
    spec = NewTopic(
        name, num_partitions=6, replication_factor=1, config={"message.timestamp.type": "LogAppendTime"}
    )
    admin.create_topics([spec])[name].result(timeout=30)
    deadline = time.monotonic() + 30
    while True:  # bounded wait until every partition has an elected leader (a new topic briefly has none)
        meta = admin.list_topics(name, timeout=10).topics[name]
        if (
            meta.error is None
            and len(meta.partitions) == 6
            and all(pm.leader >= 0 for pm in meta.partitions.values())
        ):
            break
        assert time.monotonic() < deadline, f"topic {name} has no leaders after 30 s"
    yield name
    admin.delete_topics([name])[name].result(timeout=30)


def end_offsets(consumer: Consumer, topic: str) -> dict[int, tuple[int, int]]:
    """Low/high watermarks per partition. librdkafka can briefly report `_DESTROY_BROKER` while a fresh
    consumer reconnects, so transient errors are retried within a bounded deadline."""
    deadline = time.monotonic() + 30
    while True:
        try:
            meta = consumer.list_topics(topic, timeout=10).topics[topic]
            if (
                meta.error is None
                and meta.partitions
                and all(pm.leader >= 0 for pm in meta.partitions.values())
            ):
                return {
                    p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
                    for p in meta.partitions
                }
            problem = f"metadata not ready: {meta.error}"
        except KafkaException as e:
            problem = str(e)
        assert time.monotonic() < deadline, (
            f"topic {topic} offsets unavailable: {problem} (is the stack up? make up)"
        )


def test_every_record_is_published_once_and_keyed_by_package(tmp_path, topic):
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": "ptp-it-generator", "enable.auto.commit": False}
    )
    try:
        before = end_offsets(consumer, topic)
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
                    "--topic",
                    topic,
                ]
            )
            == 0
        )

        summary = json.loads((tmp_path / "summary.json").read_text())
        deliveries = [json.loads(line) for line in (tmp_path / "delivery.jsonl").read_text().splitlines()]
        assert len(deliveries) == summary["records_total"]
        assert sorted(d["index"] for d in deliveries) == list(range(summary["records_total"]))

        after = end_offsets(consumer, topic)
        produced = sum(after[p][1] - before[p][1] for p in after)
        assert produced == summary["records_total"]

        # Read back exactly the new range and check keys (bounded polling, no sleeps).
        consumer.assign([TopicPartition(topic, p, before[p][1]) for p in after])
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
