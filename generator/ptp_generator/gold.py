"""Independent expected gold (SC-0005 part D): metric definitions C1–C7 in plain Python.

Deliberately separate from the Spark gold job, so integration tests compare two implementations.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .model import TERMINAL, Event
from .oracle import expected_status_event
from .render import format_time


def percentile_cont(values: Iterable[float], p: float) -> float | None:
    """Exact percentile with linear interpolation (Postgres percentile_cont, Spark percentile)."""
    v = sorted(values)
    if not v:
        return None
    pos = p * (len(v) - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def _utc_date(t: datetime) -> str:
    return t.astimezone(UTC).date().isoformat()


def delivery_fact(events: list[Event]) -> dict[str, Any] | None:
    """C1–C3: delivered only if DELIVERED defines the status, and CREATED exists."""
    winner = expected_status_event(events)
    created = [e for e in events if e.event_type == "CREATED"]
    if winner.event_type != "DELIVERED" or not created:
        return None
    shipment = min(created, key=lambda e: e.ordering_key).shipment
    zone = ZoneInfo(shipment["destination_timezone"])
    end_of_day = datetime.combine(
        date.fromisoformat(shipment["promised_delivery_date"]) + timedelta(days=1), time(0), zone
    )
    delivered_at = winner.event_time
    return {
        "package_id": winner.package_id,
        "delivery_date": delivered_at.astimezone(zone).date().isoformat(),
        "delivered_at": format_time(delivered_at),
        "promised_delivery_date": shipment["promised_delivery_date"],
        "service_level": shipment["service_level"],
        "destination_postal_prefix": shipment["destination_postal_prefix"],
        "destination_timezone": shipment["destination_timezone"],
        "delay_minutes": (delivered_at - end_of_day).total_seconds() / 60,
        "on_time": delivered_at < end_of_day,
    }


def facility_visits(events: list[Event]) -> list[dict[str, Any]]:
    """C5–C6: each arrival pairs with the departure whose ordering key is the smallest one above its own."""
    rows = []
    by_facility: dict[str, list[Event]] = defaultdict(list)
    for e in events:
        if e.facility_id and e.event_type in ("ARRIVED_AT_FACILITY", "DEPARTED_FACILITY"):
            by_facility[e.facility_id].append(e)
    for facility, fes in by_facility.items():
        departures = sorted(
            (e for e in fes if e.event_type == "DEPARTED_FACILITY"), key=lambda e: e.ordering_key
        )
        for a in (e for e in fes if e.event_type == "ARRIVED_AT_FACILITY"):
            d = next((d for d in departures if d.ordering_key > a.ordering_key), None)
            rows.append(
                {
                    "arrival_event_id": a.event_id,
                    "package_id": a.package_id,
                    "facility_id": facility,
                    "arrived_at": format_time(a.event_time),
                    "arrival_date": _utc_date(a.event_time),
                    "departed_at": format_time(d.event_time) if d else None,
                    "departure_date": _utc_date(d.event_time) if d else None,
                    "dwell_minutes": (d.event_time - a.event_time).total_seconds() / 60 if d else None,
                }
            )
    return rows


def expected_gold(events_by_package: dict[str, list[Event]]) -> dict[str, list[dict[str, Any]]]:
    all_events = [e for es in events_by_package.values() for e in es]
    facts = [f for es in events_by_package.values() if (f := delivery_fact(es))]
    visits = [v for es in events_by_package.values() for v in facility_visits(es)]

    # C7: network activity per UTC day.
    active: dict[str, set[str]] = defaultdict(set)
    with_exception: dict[str, set[str]] = defaultdict(set)
    for e in all_events:
        active[_utc_date(e.event_time)].add(e.package_id)
        if e.event_type == "EXCEPTION":
            with_exception[_utc_date(e.event_time)].add(e.package_id)
    network = [
        {
            "activity_date": d,
            "active_packages": len(p),
            "packages_with_exception": len(with_exception[d]),
            "exception_rate": len(with_exception[d]) / len(p),
        }
        for d, p in sorted(active.items())
    ]

    # Daily delivery performance from the facts.
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for f in facts:
        groups[(f["delivery_date"], f["service_level"], f["destination_postal_prefix"])].append(f)
    delivery_daily = [
        {
            "delivery_date": d,
            "service_level": s,
            "destination_postal_prefix": p,
            "delivered_count": len(fs),
            "on_time_count": sum(f["on_time"] for f in fs),
            "on_time_pct": 100.0 * sum(f["on_time"] for f in fs) / len(fs),
            "delay_p50_min": percentile_cont((f["delay_minutes"] for f in fs), 0.5),
            "delay_p95_min": percentile_cont((f["delay_minutes"] for f in fs), 0.95),
        }
        for (d, s, p), fs in sorted(groups.items())
    ]

    # C6: daily facility performance. Counts from events, dwell from visits on the departure date.
    counts: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"arrivals": 0, "departures": 0, "exception_count": 0}
    )
    field = {
        "ARRIVED_AT_FACILITY": "arrivals",
        "DEPARTED_FACILITY": "departures",
        "EXCEPTION": "exception_count",
    }
    for e in all_events:
        if e.facility_id and e.event_type in field:
            counts[(_utc_date(e.event_time), e.facility_id)][field[e.event_type]] += 1
    dwell: dict[tuple[str, str], list[float]] = defaultdict(list)
    for v in visits:
        if v["dwell_minutes"] is not None:
            dwell[(v["departure_date"], v["facility_id"])].append(v["dwell_minutes"])
    facility_daily = [
        {
            "activity_date": d,
            "facility_id": f,
            **counts.get((d, f), {"arrivals": 0, "departures": 0, "exception_count": 0}),
            "dwell_p50_min": percentile_cont(dwell.get((d, f), []), 0.5),
            "dwell_p95_min": percentile_cont(dwell.get((d, f), []), 0.95),
        }
        for d, f in sorted(set(counts) | set(dwell))
    ]
    # DQ-05 (SC-0006): events ordered after the package's status-defining terminal event.
    post_terminal = []
    for es in events_by_package.values():
        winner = expected_status_event(es)
        if winner.event_type in TERMINAL:
            post_terminal += [
                {"event_id": e.event_id, "package_id": e.package_id, "anomaly_code": "POST_TERMINAL_EVENT"}
                for e in es
                if e.ordering_key > winner.ordering_key
            ]
    return {
        "expected_anomalies": sorted(post_terminal, key=lambda a: a["event_id"]),
        "expected_delivery_facts": sorted(facts, key=lambda f: f["package_id"]),
        "expected_facility_visits": sorted(
            visits, key=lambda v: (v["package_id"], v["facility_id"], v["arrived_at"])
        ),
        "expected_network_activity": network,
        "expected_delivery_daily": delivery_daily,
        "expected_facility_daily": facility_daily,
    }
