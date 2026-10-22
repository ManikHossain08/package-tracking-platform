"""Serialize records to Kafka key/value bytes (contracts/package-event.v1.schema.json)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

from .model import SCHEMA_VERSION, Event
from .plan import Record


@lru_cache(maxsize=64)
def _zone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def format_time(dt: datetime, tz: str = "UTC") -> str:
    """RFC 3339 with millisecond precision and an explicit offset (`Z` for UTC)."""
    text = dt.astimezone(_zone(tz)).isoformat(timespec="milliseconds")
    return text[:-6] + "Z" if text.endswith("+00:00") else text


def event_to_dict(e: Event, emitted_at: datetime) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": e.event_id,
        "schema_version": SCHEMA_VERSION,
        "package_id": e.package_id,
        "event_type": e.event_type,
        "event_time": format_time(e.event_time, e.render_tz),
        "emitted_at": format_time(emitted_at),
        "source": {"system": e.source_system, "device_id": e.device_id, "sequence": e.sequence},
        "facility_id": e.facility_id,
        "vehicle_id": e.vehicle_id,
    }
    if e.shipment is not None:
        payload["shipment"] = dict(e.shipment)
    if e.details is not None:
        payload["details"] = dict(e.details)
    if e.attributes:
        payload["attributes"] = dict(e.attributes)
    return payload


def _dumps(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _malformed_value(r: Record, now: datetime) -> bytes:
    p = event_to_dict(r.event, now)
    p["event_id"] = r.malformed_event_id
    match r.malformed_type:
        case "invalid_json":
            text = _dumps(p)
            return text[: len(text) // 2]
        case "not_json_object":
            return _dumps([p["event_id"], p["event_type"]])
        case "unsupported_schema_version":
            p["schema_version"] = "2.0"
        case "missing_required_field":
            del p[r.dropped_field]
        case "unknown_event_type":
            p["event_type"] = "TELEPORTED"
        case "invalid_format":
            p["package_id"] = p["package_id"].lower()
        case "timestamp_missing_offset":
            p["event_time"] = r.event.event_time.replace(tzinfo=None).isoformat(timespec="milliseconds")
        case "missing_conditional_field":
            p["event_type"] = "ARRIVED_AT_FACILITY"
            p["facility_id"] = None
        case "event_time_in_future":
            p["event_time"] = format_time(now + timedelta(hours=1))
        case "event_time_too_old":
            p["event_time"] = format_time(now - timedelta(days=31))
        case other:
            raise ValueError(f"unknown malformed type {other!r}")
    return _dumps(p)


def render(r: Record, now: datetime) -> tuple[bytes, bytes]:
    """Kafka (key, value). `now` becomes emitted_at and anchors the time-relative malformed types."""
    now = now.astimezone(UTC)
    value = _dumps(event_to_dict(r.event, now)) if r.is_valid else _malformed_value(r, now)
    return r.key.encode("utf-8"), value
