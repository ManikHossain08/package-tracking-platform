"""TS-U-11: expected status follows the SC-0002 rule and is order-independent (AC-2.6)."""

import random
from datetime import UTC, datetime, timedelta

import pytest
from ptp_generator.model import Event
from ptp_generator.oracle import expected_status_event

pytestmark = pytest.mark.req("FR-GEN-03", "FR-ORD-01")

T0 = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)


def ev(event_type: str, minutes: int, event_id: str = "00000000-0000-7000-8000-000000000000") -> Event:
    return Event(
        event_id, "PKG-AAAAAAAAAAAA", event_type, T0 + timedelta(minutes=minutes), "VEHICLE_APP", "d", "UTC"
    )


@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_terminal_beats_a_later_non_terminal_in_any_arrival_order(order):
    events = [ev("DELIVERED", 0, "0-a"), ev("OUT_FOR_DELIVERY", 60, "0-b")]
    assert expected_status_event([events[i] for i in order]).event_type == "DELIVERED"


def test_first_terminal_is_sticky():
    events = [ev("RETURNED_TO_SENDER", 120, "0-b"), ev("DELIVERED", 0, "0-a")]
    assert expected_status_event(events).event_type == "DELIVERED"


def test_latest_non_terminal_wins_without_terminal():
    events = [ev("ARRIVED_AT_FACILITY", 30, "0-b"), ev("CREATED", 0, "0-a"), ev("PICKED_UP", 10, "0-c")]
    assert expected_status_event(events).event_type == "ARRIVED_AT_FACILITY"


def test_same_time_ties_break_on_rank_then_event_id():
    assert (
        expected_status_event([ev("OUT_FOR_DELIVERY", 5, "0-z"), ev("EXCEPTION", 5, "0-a")]).event_type
        == "EXCEPTION"
    )
    a, b = ev("ARRIVED_AT_FACILITY", 5, "0-a"), ev("ARRIVED_AT_FACILITY", 5, "0-b")
    assert expected_status_event([b, a]).event_id == "0-b"


def test_permutation_and_duplication_invariance(dirty_plan):
    rng = random.Random(99)
    for events in list(dirty_plan.events_by_package().values())[:100]:
        expected = expected_status_event(events).event_id
        for _ in range(20):
            shuffled = events + rng.sample(events, k=rng.randint(0, len(events)))
            rng.shuffle(shuffled)
            assert expected_status_event(shuffled).event_id == expected
