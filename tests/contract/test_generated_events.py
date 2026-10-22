"""TS-C-02: generated events honour the contract; malformed ones break it as documented (AC-2.3, AC-2.4)."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from ptp_generator.config import scenario_from_dict
from ptp_generator.plan import build_plan
from ptp_generator.render import render

from tests.generator.conftest import NOW

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[2] / "contracts/package-event.v1.schema.json").read_text()
)
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
RUNTIME_CODES = {"EVENT_TIME_IN_FUTURE", "EVENT_TIME_TOO_OLD"}


@pytest.fixture(scope="module")
def big_plan():
    raw = {
        "name": "contract-10k",
        "seed": 2024,
        "packages": 1400,
        "window_end": "now",
        "window_hours": 120,
        "duplicate_pct": 2,
        "semantic_duplicate_pct": 1,
        "out_of_order_pct": 5,
        "late": {"pct": 2},
        "malformed": {"pct": 3},
    }
    plan = build_plan(scenario_from_dict(raw, NOW))
    assert sum(r.is_valid for r in plan.records) >= 10_000
    return plan


@pytest.mark.req("FR-ING-01")
def test_every_valid_generated_record_passes_the_schema(big_plan):
    failures = []
    for r in big_plan.records:
        if r.is_valid:
            errors = list(VALIDATOR.iter_errors(json.loads(render(r, NOW)[1])))
            if errors:
                failures.append((r.index, errors[0].message))
    assert failures == []


@pytest.mark.req("FR-GEN-02", "FR-VAL-01")
def test_malformed_records_break_the_contract_as_labelled(big_plan):
    malformed = [r for r in big_plan.records if not r.is_valid]
    assert {r.expected_error_code for r in malformed} >= {"INVALID_JSON", "MISSING_REQUIRED_FIELD"}
    for r in malformed:
        raw = render(r, NOW)[1]
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            assert r.expected_error_code == "INVALID_JSON"
            continue
        if r.expected_error_code in RUNTIME_CODES:
            assert VALIDATOR.is_valid(payload), "runtime-rule records must be structurally valid"
            event_time = payload["event_time"]
            assert event_time.endswith("Z")
            t = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            if r.expected_error_code == "EVENT_TIME_IN_FUTURE":
                assert t > NOW + timedelta(minutes=5)
            else:
                assert t < NOW - timedelta(days=30)
        else:
            assert not VALIDATOR.is_valid(payload), f"{r.malformed_type} passed the schema"
