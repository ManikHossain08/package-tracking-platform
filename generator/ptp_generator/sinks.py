"""Output sinks: a JSONL file (tests, dry runs) and Kafka (FR-ING-01)."""

from __future__ import annotations

import functools
import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class FileSink:
    """Writes one JSON line per record: {"index", "key", "value"} with the value as text."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("w", encoding="utf-8")

    def send(self, index: int, key: bytes, value: bytes) -> None:
        row = {"index": index, "key": key.decode("utf-8"), "value": value.decode("utf-8")}
        self._file.write(json.dumps(row, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self._file.close()


class KafkaSink:
    """Idempotent, acks=all producer. Fails loudly if any record is not acknowledged."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        delivery_path: Path | None = None,
        flush_timeout_s: float = 60.0,
        producer: Any = None,
    ) -> None:
        if producer is None:
            from confluent_kafka import Producer  # imported lazily: unit tests need no librdkafka

            producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": "ptp-generator",
                    "enable.idempotence": True,  # implies acks=all; set explicitly for readability
                    "acks": "all",
                    "linger.ms": 10,
                    "compression.type": "lz4",
                }
            )
        self._producer = producer
        self._topic = topic
        self._delivery_path = delivery_path
        self._flush_timeout_s = flush_timeout_s
        self._deliveries: list[tuple[int, int, int]] = []
        self._errors: list[tuple[int, str]] = []

    def _on_delivery(self, index: int, err: Any, msg: Any) -> None:
        if err is not None:
            self._errors.append((index, str(err)))
        else:
            self._deliveries.append((index, msg.partition(), msg.offset()))

    def send(self, index: int, key: bytes, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    self._topic, key=key, value=value, on_delivery=functools.partial(self._on_delivery, index)
                )
                break
            except BufferError:
                self._producer.poll(0.1)  # local queue full: serve delivery reports, then retry
        self._producer.poll(0)

    def close(self) -> None:
        remaining = self._producer.flush(self._flush_timeout_s)
        if self._delivery_path is not None:
            with self._delivery_path.open("w") as f:
                for index, partition, offset in sorted(self._deliveries):
                    f.write(
                        json.dumps(
                            {"index": index, "topic": self._topic, "partition": partition, "offset": offset}
                        )
                        + "\n"
                    )
        if remaining or self._errors:
            sample = self._errors[:5]
            raise RuntimeError(
                f"Kafka delivery incomplete: topic={self._topic} acknowledged={len(self._deliveries)} "
                f"failed={len(self._errors)} unflushed={remaining} sample_errors={sample}. "
                "Check broker health (make ps) and re-run; the generator is deterministic per seed."
            )
        log.info(
            "kafka delivery complete", extra={"topic": self._topic, "acknowledged": len(self._deliveries)}
        )
