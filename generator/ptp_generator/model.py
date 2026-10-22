"""Domain constants and the generated event type, shared by the lifecycle model and the oracle."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SCHEMA_VERSION = "1.0"

# Lifecycle ranks and terminal states: spec 01 §Event lifecycle.
RANK: dict[str, int] = {
    "CREATED": 10,
    "PICKED_UP": 20,
    "ARRIVED_AT_FACILITY": 30,
    "DEPARTED_FACILITY": 40,
    "OUT_FOR_DELIVERY": 50,
    "DELIVERY_ATTEMPTED": 60,
    "EXCEPTION": 70,
    "DELIVERED": 90,
    "RETURNED_TO_SENDER": 90,
}
TERMINAL: frozenset[str] = frozenset({"DELIVERED", "RETURNED_TO_SENDER"})

_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


@dataclass
class Event:
    """One tracking event as it happened in the physical world (before fault injection)."""

    event_id: str
    package_id: str
    event_type: str
    event_time: datetime  # UTC, millisecond precision
    source_system: str
    device_id: str
    render_tz: str  # zone used to render event_time (scanners report local time)
    facility_id: str | None = None
    vehicle_id: str | None = None
    shipment: dict[str, Any] | None = None
    details: dict[str, Any] | None = None
    sequence: int = 0  # assigned per device after all lifecycles exist
    attributes: dict[str, str] = field(default_factory=dict)

    @property
    def ordering_key(self) -> tuple[datetime, int, str]:
        """Spec 01 / ADR-0004 ordering key. UUID text order equals Postgres uuid byte order."""
        return (self.event_time, RANK[self.event_type], self.event_id.lower())


def new_package_id(rng: random.Random) -> str:
    return "PKG-" + "".join(rng.choice(_ID_ALPHABET) for _ in range(12))


def new_event_id(rng: random.Random, at: datetime) -> str:
    """Deterministic UUIDv7: 48-bit Unix ms timestamp, version 7, RFC 4122 variant, seeded random bits."""
    ms = int(at.timestamp() * 1000) & ((1 << 48) - 1)
    rand_a = rng.getrandbits(12)
    rand_b = rng.getrandbits(62)
    value = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return str(uuid.UUID(int=value))
