"""Realistic package lifecycles (FR-GEN-01). Pure: all randomness comes from the seeded RNG."""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from .config import Scenario
from .model import Event, new_event_id, new_package_id

# (IATA-style code, IANA zone) for synthetic distribution centres.
_DC_LOCATIONS = [
    ("YUL", "America/Toronto"),
    ("YYZ", "America/Toronto"),
    ("YVR", "America/Vancouver"),
    ("YYC", "America/Edmonton"),
    ("YOW", "America/Toronto"),
    ("YHZ", "America/Halifax"),
    ("YWG", "America/Winnipeg"),
    ("YEG", "America/Edmonton"),
    ("YQB", "America/Toronto"),
    ("YXE", "America/Regina"),
]
_SERVICE_LEVELS = [("STANDARD", 0.60, 2), ("EXPRESS", 0.25, 1), ("ECONOMY", 0.15, 4)]
_EXCEPTION_CODES = ["DAMAGED_LABEL", "ADDRESS_UNCLEAR", "WEATHER_DELAY", "MISSORT", "CUSTOMS_HOLD"]
MAX_DELIVERY_ATTEMPTS = 3


@dataclass(frozen=True)
class Facility:
    facility_id: str
    code: str
    tz: str


@dataclass(frozen=True)
class Network:
    facilities: tuple[Facility, ...]
    vehicle_ids: tuple[str, ...]
    postal_prefixes: tuple[str, ...] = ()  # empty = any random prefix


def build_network(rng: random.Random, n_facilities: int, n_vehicles: int, n_prefixes: int = 0) -> Network:
    facilities = []
    for i in range(n_facilities):
        code, tz = _DC_LOCATIONS[i % len(_DC_LOCATIONS)]
        facilities.append(Facility(f"DC-{code}{i // len(_DC_LOCATIONS) + 1:02d}", code, tz))
    vehicles = tuple(f"VH-{n:06d}" for n in rng.sample(range(1, 1_000_000), n_vehicles))
    # Only consume randomness for a pool when one is configured, so existing seeds keep their streams.
    prefixes = tuple(sorted({_fsa(rng) for _ in range(n_prefixes * 4)}))[:n_prefixes] if n_prefixes else ()
    return Network(tuple(facilities), vehicles, prefixes)


def _ms(dt: datetime) -> datetime:
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def _hours(rng: random.Random, low: float, high: float) -> timedelta:
    return timedelta(seconds=rng.uniform(low * 3600, high * 3600))


def _fsa(rng: random.Random) -> str:
    letters = "ABCEGHJKLMNPRSTVXY"  # Canadian postal FSA style: letter-digit-letter
    return rng.choice(letters) + str(rng.randint(0, 9)) + rng.choice(letters)


def _service_level(rng: random.Random) -> tuple[str, int]:
    roll, acc = rng.random(), 0.0
    for name, weight, days in _SERVICE_LEVELS:
        acc += weight
        if roll < acc:
            return name, days
    return _SERVICE_LEVELS[-1][0], _SERVICE_LEVELS[-1][2]


class _PackageBuilder:
    def __init__(self, rng: random.Random, package_id: str) -> None:
        self.rng = rng
        self.package_id = package_id
        self.events: list[Event] = []

    def add(self, event_type: str, at: datetime, system: str, device: str, tz: str = "UTC", **kw) -> None:
        at = _ms(at)
        self.events.append(
            Event(
                event_id=new_event_id(self.rng, at),
                package_id=self.package_id,
                event_type=event_type,
                event_time=at,
                source_system=system,
                device_id=device,
                render_tz=tz,
                **kw,
            )
        )


def simulate_package(
    rng: random.Random, network: Network, created_at: datetime, scenario: Scenario
) -> list[Event]:
    """Full lifecycle from CREATED to a terminal event; events after window_end are dropped (in transit)."""
    b = _PackageBuilder(rng, new_package_id(rng))
    origin = rng.choice(network.facilities)
    destination = rng.choice(network.facilities)
    hubs = [f for f in network.facilities if f not in (origin, destination)]
    path = [origin, *rng.sample(hubs, k=min(len(hubs), rng.randint(0, 2)))]
    if destination != origin:
        path.append(destination)

    level, days = _service_level(rng)
    promised = (created_at.astimezone(ZoneInfo(destination.tz)).date() + timedelta(days=days)).isoformat()
    shipment = {
        "promised_delivery_date": promised,
        "service_level": level,
        "origin_facility_id": origin.facility_id,
        "destination_postal_prefix": rng.choice(network.postal_prefixes)
        if network.postal_prefixes
        else _fsa(rng),
        "destination_timezone": destination.tz,
    }
    t = created_at
    b.add("CREATED", t, "ORDER_SYSTEM", "order-api-1", shipment=shipment)

    pickup_vehicle = rng.choice(network.vehicle_ids)
    t += _hours(rng, 1, 8)
    b.add("PICKED_UP", t, "VEHICLE_APP", f"vh-app-{pickup_vehicle[3:]}", vehicle_id=pickup_vehicle)

    for i, facility in enumerate(path):
        scanner = f"scanner-{facility.code}-{rng.randint(1, 8):02d}"
        t += _hours(rng, 1, 10)
        b.add("ARRIVED_AT_FACILITY", t, "DC_SCANNER", scanner, facility.tz, facility_id=facility.facility_id)
        if rng.random() < scenario.p_exception:
            t += _hours(rng, 0.2, 2)
            b.add(
                "EXCEPTION",
                t,
                "DC_SCANNER",
                scanner,
                facility.tz,
                facility_id=facility.facility_id,
                details={"exception_code": rng.choice(_EXCEPTION_CODES)},
            )
        if i < len(path) - 1:  # at the destination the package leaves on a delivery van instead
            t += _hours(rng, 0.5, 12)
            b.add(
                "DEPARTED_FACILITY", t, "DC_SCANNER", scanner, facility.tz, facility_id=facility.facility_id
            )

    van = rng.choice(network.vehicle_ids)
    van_device = f"vh-app-{van[3:]}"
    for attempt in range(1, MAX_DELIVERY_ATTEMPTS + 1):
        t += _hours(rng, 1, 6)
        b.add("OUT_FOR_DELIVERY", t, "VEHICLE_APP", van_device, vehicle_id=van)
        t += _hours(rng, 0.5, 5)
        if rng.random() >= scenario.p_failed_attempt:
            b.add("DELIVERED", t, "VEHICLE_APP", van_device, vehicle_id=van)
            break
        b.add(
            "DELIVERY_ATTEMPTED",
            t,
            "VEHICLE_APP",
            van_device,
            vehicle_id=van,
            details={"attempt_number": attempt},
        )
        t += _hours(rng, 12, 20)  # next delivery day
    else:
        t += _hours(rng, 2, 8)
        b.add(
            "RETURNED_TO_SENDER",
            t,
            "DC_SCANNER",
            f"scanner-{destination.code}-01",
            destination.tz,
            facility_id=destination.facility_id,
        )

    return [e for e in b.events if e.event_time <= scenario.window_end]


def assign_sequences(events: list[Event]) -> None:
    """Per-device monotonic sequence numbers in event-time order (spec 02: diagnostics only)."""
    by_device: dict[str, list[Event]] = defaultdict(list)
    for e in events:
        by_device[e.device_id].append(e)
    for device_events in by_device.values():
        for seq, e in enumerate(sorted(device_events, key=lambda e: e.ordering_key), start=1):
            e.sequence = seq


def generate_lifecycles(scenario: Scenario, rng: random.Random) -> list[list[Event]]:
    network = build_network(rng, scenario.facilities, scenario.vehicles, scenario.postal_prefixes)
    span = (scenario.window_end - scenario.window_start).total_seconds()
    packages = []
    for _ in range(scenario.packages):
        created_at = scenario.window_start + timedelta(seconds=rng.uniform(0, span))
        packages.append(simulate_package(rng, network, created_at.astimezone(UTC), scenario))
    assign_sequences([e for p in packages for e in p])
    return packages
