"""Fault injection and emission order (FR-GEN-02).

Builds the full, deterministic list of records to emit: valid events in event-time order,
then out-of-order and late moves, duplicate and semantic-duplicate copies, and malformed
records. Positions are floats, sorted once at the end, so large runs stay O(n log n).
"""

from __future__ import annotations

import bisect
import dataclasses
import random
from dataclasses import dataclass, field
from datetime import timedelta

from .config import Scenario
from .lifecycle import generate_lifecycles
from .model import Event, new_event_id

# Expected primary error code per malformed type (spec 02 §2 precedence).
MALFORMED_ERROR_CODES: dict[str, str] = {
    "invalid_json": "INVALID_JSON",
    "not_json_object": "NOT_JSON_OBJECT",
    "unsupported_schema_version": "UNSUPPORTED_SCHEMA_VERSION",
    "missing_required_field": "MISSING_REQUIRED_FIELD",
    "unknown_event_type": "UNKNOWN_EVENT_TYPE",
    "invalid_format": "INVALID_FORMAT",
    "timestamp_missing_offset": "TIMESTAMP_MISSING_OFFSET",
    "missing_conditional_field": "MISSING_CONDITIONAL_FIELD",
    "event_time_in_future": "EVENT_TIME_IN_FUTURE",
    "event_time_too_old": "EVENT_TIME_TOO_OLD",
}
REQUIRED_FIELDS_TO_DROP = ("package_id", "event_type", "event_time", "source")
DUPLICATE_MAX_DISTANCE = 200  # records; copies land within this distance after the original


@dataclass
class Record:
    """One Kafka record to emit. Valid records reference their Event; malformed ones carry a mutation."""

    key: str
    event: Event
    fault: str | None = None  # duplicate | semantic_duplicate | out_of_order | late | malformed
    malformed_type: str | None = None
    malformed_event_id: str | None = None
    dropped_field: str | None = None
    late_delay: timedelta | None = None
    index: int = -1

    @property
    def is_valid(self) -> bool:
        return self.fault != "malformed"

    @property
    def expected_error_code(self) -> str | None:
        return MALFORMED_ERROR_CODES[self.malformed_type] if self.malformed_type else None


@dataclass
class Plan:
    scenario: Scenario
    packages: list[list[Event]]  # true lifecycles (before faults), per package
    semantic_duplicates: list[Event] = field(default_factory=list)
    records: list[Record] = field(default_factory=list)

    def events_by_package(self) -> dict[str, list[Event]]:
        """All unique valid events per package: lifecycle events plus semantic duplicates."""
        out: dict[str, list[Event]] = {p[0].package_id: list(p) for p in self.packages if p}
        for e in self.semantic_duplicates:
            out[e.package_id].append(e)
        return out


def _count(n: int, pct: float) -> int:
    return round(n * pct / 100)


def build_plan(scenario: Scenario) -> Plan:
    rng = random.Random(scenario.seed)
    packages = generate_lifecycles(scenario, rng)
    plan = Plan(scenario, packages)

    base = sorted(
        (e for p in packages for e in p), key=lambda e: (e.event_time, e.package_id, e.ordering_key)
    )
    n = len(base)
    pos: dict[int, float] = {i: float(i) for i in range(n)}  # base index -> emission position
    tag: dict[int, str] = {}

    # Next event of the same package in base order (for out-of-order moves).
    next_same: dict[int, int] = {}
    last_seen: dict[str, int] = {}
    for i in range(n - 1, -1, -1):
        pid = base[i].package_id
        if pid in last_seen:
            next_same[i] = last_seen[pid]
        last_seen[pid] = i

    # Out-of-order: emit right after the next (later-keyed) event of the same package.
    ooo = set(rng.sample(sorted(next_same), k=min(len(next_same), _count(n, scenario.out_of_order_pct))))
    anchors = {next_same[i] for i in ooo}
    for i in sorted(ooo, reverse=True):  # reverse, so a moved successor's final position is used
        pos[i] = pos[next_same[i]] + 0.01 + rng.random() * 0.5
        tag[i] = "out_of_order"

    # Late: emit once the max event_time already emitted (excluding late events) >= event_time + delay.
    late_pool = [i for i in range(n) if i not in ooo and i not in anchors]
    late_pick = rng.sample(late_pool, k=min(len(late_pool), _count(n, scenario.late.pct)))
    late_set = set(late_pick)
    stable = sorted((i for i in range(n) if i not in late_set), key=lambda i: pos[i])
    prefix_max, running = [], None
    for i in stable:
        running = base[i].event_time if running is None else max(running, base[i].event_time)
        prefix_max.append(running)
    late_delay: dict[int, timedelta] = {}
    for i in late_pick:
        delay = timedelta(
            minutes=rng.randint(scenario.late.min_delay_minutes, scenario.late.max_delay_minutes)
        )
        j = bisect.bisect_left(prefix_max, base[i].event_time + delay)
        if j >= len(prefix_max):
            continue  # the stream never gets that far ahead; this event cannot be late
        pos[i] = pos[stable[j]] + 0.01 + rng.random() * 0.5
        tag[i] = "late"
        late_delay[i] = delay

    records: list[tuple[float, Record]] = []
    for i, e in enumerate(base):
        records.append((pos[i], Record(e.package_id, e, fault=tag.get(i), late_delay=late_delay.get(i))))

    # Duplicates: the same event (same event_id) re-sent 1-2 more times, later in the stream.
    for i in rng.sample(range(n), k=min(n, _count(n, scenario.duplicate_pct))):
        for _ in range(rng.randint(1, 2)):
            records.append(
                (
                    pos[i] + rng.randint(1, DUPLICATE_MAX_DISTANCE) + rng.random(),
                    Record(base[i].package_id, base[i], fault="duplicate"),
                )
            )

    # Semantic duplicates: identical content with a new event_id (FR-DED-03, ADR-0003).
    for i in rng.sample(range(n), k=min(n, _count(n, scenario.semantic_duplicate_pct))):
        copy = dataclasses.replace(base[i], event_id=new_event_id(rng, base[i].event_time))
        plan.semantic_duplicates.append(copy)
        records.append(
            (
                pos[i] + rng.randint(1, DUPLICATE_MAX_DISTANCE) + rng.random(),
                Record(copy.package_id, copy, fault="semantic_duplicate"),
            )
        )

    # Malformed: extra records derived from real events, so they never change the expected state.
    valid_count = len(records)
    for _ in range(_count(valid_count, scenario.malformed.pct)):
        source = rng.choice(base)
        mtype = rng.choice(scenario.malformed.types)
        records.append(
            (
                rng.uniform(0, n),
                Record(
                    source.package_id,
                    source,
                    fault="malformed",
                    malformed_type=mtype,
                    malformed_event_id=new_event_id(rng, source.event_time),
                    dropped_field=rng.choice(REQUIRED_FIELDS_TO_DROP)
                    if mtype == "missing_required_field"
                    else None,
                ),
            )
        )

    # Stable sort: ties keep insertion order, so the result depends only on the seed.
    records.sort(key=lambda pr: pr[0])
    for index, (_, record) in enumerate(records):
        record.index = index
        plan.records.append(record)
    return plan
