"""TS-U-12: pacing and outage schedule (AC-2.7), and Kafka sink failure handling."""

import pytest
from ptp_generator.config import OutageConfig
from ptp_generator.emit import emit, schedule
from ptp_generator.sinks import KafkaSink

from .conftest import NOW


@pytest.mark.req("FR-GEN-01")
def test_records_are_evenly_spaced_at_the_configured_rate():
    assert schedule(5, rate_eps=10) == pytest.approx([0.0, 0.1, 0.2, 0.3, 0.4])


@pytest.mark.req("FR-GEN-01")
def test_zero_rate_is_unthrottled():
    assert schedule(3, rate_eps=0) == [0.0, 0.0, 0.0]


@pytest.mark.req("FR-GEN-02")
def test_outage_holds_records_and_releases_them_together():
    offsets = schedule(10, rate_eps=1, outage=OutageConfig(at_fraction=0.3, duration_seconds=4))
    assert offsets == [0, 1, 2, 7, 7, 7, 7, 7, 8, 9]


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(round(s, 6))
        self.t += s


class ListSink:
    def __init__(self):
        self.sent, self.closed = [], False

    def send(self, index, key, value):
        self.sent.append(index)

    def close(self):
        self.closed = True


@pytest.mark.req("FR-GEN-01")
def test_emit_waits_according_to_schedule(dirty_plan):
    records = dirty_plan.records[:4]
    clock, sink = FakeClock(), ListSink()
    stats = emit(records, sink, rate_eps=2, clock=lambda: NOW, monotonic=clock.monotonic, sleep=clock.sleep)
    assert sink.sent == [r.index for r in records] and sink.closed
    assert clock.sleeps == [0.5, 0.5, 0.5]
    assert stats.records == 4 and stats.duration_seconds == pytest.approx(1.5)


class FakeMessage:
    def __init__(self, partition, offset):
        self._p, self._o = partition, offset

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeProducer:
    def __init__(self, fail_index=None):
        self.fail_index, self.pending, self.produced = fail_index, [], 0

    def produce(self, topic, key, value, on_delivery):
        self.pending.append(on_delivery)
        self.produced += 1

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for i, cb in enumerate(self.pending):
            cb("broker down" if i == self.fail_index else None, FakeMessage(0, i))
        return 0


@pytest.mark.req("FR-ING-01")
def test_kafka_sink_records_deliveries(tmp_path):
    sink = KafkaSink("unused", "package-events.v1", tmp_path / "delivery.jsonl", producer=FakeProducer())
    for i in range(3):
        sink.send(i, b"k", b"v")
    sink.close()
    assert len((tmp_path / "delivery.jsonl").read_text().splitlines()) == 3


@pytest.mark.req("FR-ING-01")
def test_kafka_sink_fails_loudly_on_unacknowledged_records(tmp_path):
    sink = KafkaSink(
        "unused", "package-events.v1", tmp_path / "delivery.jsonl", producer=FakeProducer(fail_index=1)
    )
    for i in range(3):
        sink.send(i, b"k", b"v")
    with pytest.raises(RuntimeError, match="acknowledged=2 failed=1"):
        sink.close()
