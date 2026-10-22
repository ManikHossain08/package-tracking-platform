"""Paced emission to a sink (rate control and outage simulation, FR-GEN-01/02)."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from .config import OutageConfig
from .plan import Record
from .render import render


class Sink(Protocol):
    def send(self, index: int, key: bytes, value: bytes) -> None: ...
    def close(self) -> None: ...


def schedule(n: int, rate_eps: float, outage: OutageConfig | None = None) -> list[float]:
    """Seconds after start at which record i may be sent.

    Records are evenly spaced at `rate_eps`. During an outage, everything scheduled inside the
    outage window is held and released together at its end (the burst after recovery).
    """
    if rate_eps == 0:
        return [0.0] * n
    offsets = [i / rate_eps for i in range(n)]
    if outage is not None:
        start = outage.at_fraction * n / rate_eps
        end = start + outage.duration_seconds
        offsets = [end if start <= t < end else t for t in offsets]
    return offsets


@dataclass(frozen=True)
class EmitStats:
    records: int
    duration_seconds: float

    @property
    def achieved_eps(self) -> float:
        return self.records / self.duration_seconds if self.duration_seconds > 0 else float("inf")


def emit(
    records: list[Record],
    sink: Sink,
    rate_eps: float,
    outage: OutageConfig | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> EmitStats:
    offsets = schedule(len(records), rate_eps, outage)
    start = monotonic()
    for record, offset in zip(records, offsets, strict=True):
        wait = start + offset - monotonic()
        if wait > 0:
            sleep(wait)  # rate pacing of a producer, not test synchronisation
        key, value = render(record, clock())
        sink.send(record.index, key, value)
    sink.close()
    return EmitStats(len(records), monotonic() - start)
