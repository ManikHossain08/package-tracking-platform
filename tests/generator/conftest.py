from datetime import UTC, datetime
from typing import Any

import pytest
from ptp_generator.config import Scenario, scenario_from_dict
from ptp_generator.plan import Plan, build_plan

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)

DIRTY: dict[str, Any] = {
    "name": "test-dirty",
    "seed": 123,
    "packages": 300,
    "window_end": "now",
    "window_hours": 96,
    "duplicate_pct": 5,
    "semantic_duplicate_pct": 2,
    "out_of_order_pct": 10,
    "late": {"pct": 4, "min_delay_minutes": 30, "max_delay_minutes": 240},
    "malformed": {"pct": 3},
}


def make_scenario(**overrides: Any) -> Scenario:
    return scenario_from_dict({**DIRTY, **overrides}, NOW)


@pytest.fixture(scope="session")
def dirty_plan() -> Plan:
    return build_plan(make_scenario())
