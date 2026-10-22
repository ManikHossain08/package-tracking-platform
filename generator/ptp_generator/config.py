"""Scenario configuration (bench/scenarios/*.yaml, spec 09)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

# Mirrors the processor's runtime bounds (FR-VAL-02, ADR-0005). Valid events must stay inside them.
LIVE_MAX_EVENT_AGE = timedelta(days=30)

MALFORMED_TYPES: tuple[str, ...] = (
    "invalid_json",
    "not_json_object",
    "unsupported_schema_version",
    "missing_required_field",
    "unknown_event_type",
    "invalid_format",
    "timestamp_missing_offset",
    "missing_conditional_field",
    "event_time_in_future",
    "event_time_too_old",
)


@dataclass(frozen=True)
class LateConfig:
    pct: float = 0.0
    min_delay_minutes: int = 30
    max_delay_minutes: int = 180


@dataclass(frozen=True)
class MalformedConfig:
    pct: float = 0.0
    types: tuple[str, ...] = MALFORMED_TYPES


@dataclass(frozen=True)
class OutageConfig:
    """Hold everything scheduled in [at_fraction * run_length, + duration) and release it at once."""

    at_fraction: float
    duration_seconds: float


@dataclass(frozen=True)
class Scenario:
    name: str
    seed: int
    packages: int
    window_start: datetime
    window_end: datetime
    rate_eps: float = 0.0  # 0 = unthrottled
    facilities: int = 6
    vehicles: int = 40
    p_exception: float = 0.05
    p_failed_attempt: float = 0.08
    duplicate_pct: float = 0.0
    semantic_duplicate_pct: float = 0.0
    out_of_order_pct: float = 0.0
    late: LateConfig = field(default_factory=LateConfig)
    malformed: MalformedConfig = field(default_factory=MalformedConfig)
    outage: OutageConfig | None = None

    def validate(self, now: datetime) -> None:
        errors: list[str] = []
        if self.packages <= 0:
            errors.append("packages must be > 0")
        if not 1 <= self.facilities <= 99:
            errors.append("facilities must be in 1..99")
        if not 1 <= self.vehicles <= 999_999:
            errors.append("vehicles must be in 1..999999")
        if self.window_start >= self.window_end:
            errors.append("window_start must be before window_end")
        if self.window_end > now:
            errors.append(
                "window_end is in the future; valid events would be rejected as EVENT_TIME_IN_FUTURE"
            )
        if self.window_start < now - LIVE_MAX_EVENT_AGE:
            errors.append(
                "window_start is older than 30 days; valid events would be rejected as EVENT_TIME_TOO_OLD"
            )
        for name in ("duplicate_pct", "semantic_duplicate_pct", "out_of_order_pct"):
            if not 0 <= getattr(self, name) <= 100:
                errors.append(f"{name} must be in 0..100")
        if (
            not 0 <= self.late.pct <= 100
            or not 0 < self.late.min_delay_minutes <= self.late.max_delay_minutes
        ):
            errors.append("late: pct in 0..100 and 0 < min_delay_minutes <= max_delay_minutes")
        if not 0 <= self.malformed.pct <= 100:
            errors.append("malformed.pct must be in 0..100")
        unknown = set(self.malformed.types) - set(MALFORMED_TYPES)
        if unknown or not self.malformed.types:
            errors.append(f"malformed.types must be a non-empty subset of {MALFORMED_TYPES}")
        if self.rate_eps < 0:
            errors.append("rate_eps must be >= 0")
        if self.outage is not None:
            if self.rate_eps == 0:
                errors.append("outage requires rate_eps > 0 (an unthrottled run has no timeline)")
            if not 0 <= self.outage.at_fraction < 1 or self.outage.duration_seconds <= 0:
                errors.append("outage: 0 <= at_fraction < 1 and duration_seconds > 0")
        if errors:
            raise ValueError(f"invalid scenario {self.name!r}: " + "; ".join(errors))


def _parse_time(value: Any, now: datetime) -> datetime:
    if value == "now":
        return now
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        raise ValueError(f"timestamp {value!r} must include an offset")
    return dt.astimezone(UTC)


def scenario_from_dict(raw: dict[str, Any], now: datetime) -> Scenario:
    """Build and validate a scenario. `window_end` may be "now"; `window_hours` sets the start."""
    data = dict(raw)
    window_end = _parse_time(data.pop("window_end", "now"), now)
    if "window_start" in data:
        window_start = _parse_time(data.pop("window_start"), now)
    else:
        window_start = window_end - timedelta(hours=float(data.pop("window_hours", 72)))
    data.pop("window_hours", None)
    late = LateConfig(**data.pop("late", {}))
    malformed_raw = data.pop("malformed", {})
    if "types" in malformed_raw:
        malformed_raw = {**malformed_raw, "types": tuple(malformed_raw["types"])}
    malformed = MalformedConfig(**malformed_raw)
    outage_raw = data.pop("outage", None)
    outage = OutageConfig(**outage_raw) if outage_raw else None
    scenario = Scenario(
        window_start=window_start,
        window_end=window_end,
        late=late,
        malformed=malformed,
        outage=outage,
        **data,
    )
    scenario.validate(now)
    return scenario


def load_scenario(
    path: Path, overrides: dict[str, Any] | None = None, clock: Callable[[], datetime] | None = None
) -> Scenario:
    now = (clock or (lambda: datetime.now(UTC)))()
    raw = yaml.safe_load(path.read_text())
    raw.update({k: v for k, v in (overrides or {}).items() if v is not None})
    return scenario_from_dict(raw, now)
