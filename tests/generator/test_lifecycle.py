"""TS-U-09: lifecycles are realistic (AC-2.2)."""

from datetime import date, timedelta
from zoneinfo import ZoneInfo

import pytest
from ptp_generator.lifecycle import MAX_DELIVERY_ATTEMPTS
from ptp_generator.model import TERMINAL

from .conftest import make_scenario

DAYS = {"EXPRESS": 1, "STANDARD": 2, "ECONOMY": 4}

pytestmark = pytest.mark.req("FR-GEN-01")


def test_each_package_starts_with_created_and_times_strictly_increase(dirty_plan):
    for events in dirty_plan.packages:
        assert events[0].event_type == "CREATED"
        times = [e.event_time for e in events]
        assert times == sorted(times) and len(set(times)) == len(times)


def test_terminal_event_is_last_and_unique(dirty_plan):
    for events in dirty_plan.packages:
        terminals = [i for i, e in enumerate(events) if e.event_type in TERMINAL]
        assert terminals in ([], [len(events) - 1])


def test_facilities_visited_one_to_four_times_with_arrival_before_departure(dirty_plan):
    for events in dirty_plan.packages:
        arrivals = [e.facility_id for e in events if e.event_type == "ARRIVED_AT_FACILITY"]
        departures = [e.facility_id for e in events if e.event_type == "DEPARTED_FACILITY"]
        assert len(arrivals) <= 4
        assert departures == arrivals[: len(departures)]  # each departure follows its arrival
        assert len(arrivals) - len(departures) in (0, 1)


def test_delivery_attempts_are_bounded_then_returned(dirty_plan):
    for events in dirty_plan.packages:
        attempts = [e.details["attempt_number"] for e in events if e.event_type == "DELIVERY_ATTEMPTED"]
        assert attempts == list(range(1, len(attempts) + 1))
        assert len(attempts) <= MAX_DELIVERY_ATTEMPTS
        if events[-1].event_type == "RETURNED_TO_SENDER":
            assert len(attempts) == MAX_DELIVERY_ATTEMPTS


def test_promised_date_follows_service_level_in_destination_timezone(dirty_plan):
    for events in dirty_plan.packages:
        created = events[0]
        s = created.shipment
        local_day = created.event_time.astimezone(ZoneInfo(s["destination_timezone"])).date()
        assert date.fromisoformat(s["promised_delivery_date"]) == local_day + timedelta(
            days=DAYS[s["service_level"]]
        )


def test_no_event_after_window_end_and_some_packages_in_transit(dirty_plan):
    end = dirty_plan.scenario.window_end
    assert all(e.event_time <= end for p in dirty_plan.packages for e in p)
    assert any(p[-1].event_type not in TERMINAL for p in dirty_plan.packages)


def test_device_sequences_are_monotonic_per_device(dirty_plan):
    by_device = {}
    for e in sorted((e for p in dirty_plan.packages for e in p), key=lambda e: e.ordering_key):
        by_device.setdefault(e.device_id, []).append(e.sequence)
    for seqs in by_device.values():
        assert seqs == list(range(1, len(seqs) + 1))


def test_scenario_rejects_windows_the_processor_would_reject():
    with pytest.raises(ValueError, match="EVENT_TIME_TOO_OLD"):
        make_scenario(window_hours=24 * 31)
    with pytest.raises(ValueError, match="EVENT_TIME_IN_FUTURE"):
        make_scenario(window_end="2026-10-22T12:00:00+00:00")
    with pytest.raises(ValueError, match="outage requires rate_eps"):
        make_scenario(outage={"at_fraction": 0.5, "duration_seconds": 10})
