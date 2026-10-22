"""SC-0005 D: the generator's expected-gold oracle implements C1–C7 (hand-computed cases)."""

from datetime import UTC, datetime, timedelta

import pytest
from ptp_generator.gold import delivery_fact, expected_gold, facility_visits, percentile_cont
from ptp_generator.model import RANK, Event

pytestmark = pytest.mark.req("FR-ANL-01", "FR-GEN-03")

PKG = "PKG-GOLDAAAAAAAA"
SHIP = {
    "promised_delivery_date": "2026-10-20",
    "service_level": "STANDARD",
    "origin_facility_id": "DC-YUL01",
    "destination_postal_prefix": "H2X",
    "destination_timezone": "America/Toronto",
}


def ev(tpe: str, at: datetime, eid: str, facility: str | None = None, **kw) -> Event:
    assert tpe in RANK
    return Event(eid, PKG, tpe, at, "DC_SCANNER", "d", "UTC", facility_id=facility, **kw)


# End of the promised day 2026-10-20 in Toronto (EDT, UTC-4) = 2026-10-21T04:00:00Z.
EOD = datetime(2026, 10, 21, 4, 0, tzinfo=UTC)


def created() -> Event:
    return ev("CREATED", EOD - timedelta(days=3), "0-a", shipment=SHIP)


def test_on_time_and_signed_delay_relative_to_local_end_of_day():
    early = delivery_fact([created(), ev("DELIVERED", EOD - timedelta(minutes=90), "0-b")])
    assert early["on_time"] and early["delay_minutes"] == -90 and early["delivery_date"] == "2026-10-20"
    late = delivery_fact([created(), ev("DELIVERED", EOD + timedelta(minutes=30), "0-b")])
    assert not late["on_time"] and late["delay_minutes"] == 30 and late["delivery_date"] == "2026-10-21"
    exactly = delivery_fact([created(), ev("DELIVERED", EOD, "0-b")])
    assert (
        not exactly["on_time"] and exactly["delay_minutes"] == 0
    )  # strict: on time iff delivered_at < end of day


def test_returned_first_or_missing_created_is_not_a_delivery():
    returned_first = [
        created(),
        ev("RETURNED_TO_SENDER", EOD - timedelta(hours=5), "0-r", "DC-YUL01"),
        ev("DELIVERED", EOD - timedelta(hours=1), "0-d"),
    ]
    assert delivery_fact(returned_first) is None  # C1: the first terminal defines the outcome
    assert delivery_fact([ev("DELIVERED", EOD, "0-d")]) is None  # C2


def test_visit_pairing_and_dwell():
    t0 = datetime(2026, 10, 19, 22, 0, tzinfo=UTC)
    events = [
        ev("ARRIVED_AT_FACILITY", t0, "0-1", "DC-YUL01"),
        ev("ARRIVED_AT_FACILITY", t0, "0-2", "DC-YUL01"),  # semantic duplicate arrival
        ev("DEPARTED_FACILITY", t0 + timedelta(hours=3), "0-3", "DC-YUL01"),
        ev("ARRIVED_AT_FACILITY", t0 + timedelta(hours=9), "0-4", "DC-YYZ01"),  # still inside
    ]
    visits = facility_visits(events)
    yul = [v for v in visits if v["facility_id"] == "DC-YUL01"]
    assert len(yul) == 2 and all(v["dwell_minutes"] == 180 for v in yul)
    assert all(v["arrival_date"] == "2026-10-19" and v["departure_date"] == "2026-10-20" for v in yul)
    yyz = [v for v in visits if v["facility_id"] == "DC-YYZ01"]
    assert yyz[0]["departed_at"] is None and yyz[0]["dwell_minutes"] is None


def test_percentile_cont_matches_postgres():
    assert percentile_cont([1, 2, 3, 4], 0.5) == 2.5
    assert percentile_cont([10.0], 0.95) == 10.0
    assert percentile_cont([], 0.5) is None
    assert percentile_cont([0, 10, 20, 30, 40], 0.95) == pytest.approx(38.0)


def test_daily_aggregates_and_exception_rate():
    t = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    other = "PKG-GOLDBBBBBBBB"
    gold = expected_gold(
        {
            PKG: [
                created(),
                ev("EXCEPTION", t, "0-e", "DC-YUL01", details={"exception_code": "MISSORT"}),
                ev("DELIVERED", EOD - timedelta(minutes=10), "0-b"),
            ],
            other: [Event("0-x", other, "PICKED_UP", t, "VEHICLE_APP", "d", "UTC", vehicle_id="VH-000001")],
        }
    )
    day = {r["activity_date"]: r for r in gold["expected_network_activity"]}["2026-10-20"]
    assert (
        day["active_packages"] == 2 and day["packages_with_exception"] == 1 and day["exception_rate"] == 0.5
    )
    daily = gold["expected_delivery_daily"]
    assert len(daily) == 1 and daily[0]["delivered_count"] == 1 and daily[0]["on_time_pct"] == 100.0
    fac = gold["expected_facility_daily"]
    assert fac == [
        {
            "activity_date": "2026-10-20",
            "facility_id": "DC-YUL01",
            "arrivals": 0,
            "departures": 0,
            "exception_count": 1,
            "dwell_p50_min": None,
            "dwell_p95_min": None,
        }
    ]
