"""TS-U-10: injected faults are real and exactly counted (AC-2.5)."""

import json
from collections import Counter

import pytest
from ptp_generator.manifest import package_manifest, summary
from ptp_generator.render import render

from .conftest import NOW

pytestmark = pytest.mark.req("FR-GEN-02")


def test_summary_counts_match_the_records(dirty_plan):
    s = summary(dirty_plan, package_manifest(dirty_plan))
    faults = Counter(r.fault for r in dirty_plan.records)
    assert s["records_total"] == len(dirty_plan.records)
    assert s["duplicate_copies"] == faults["duplicate"] > 0
    assert s["semantic_duplicates"] == faults["semantic_duplicate"] > 0
    assert s["out_of_order"] == faults["out_of_order"] > 0
    assert s["late"] == faults["late"] > 0
    assert s["malformed_total"] == faults["malformed"] > 0
    assert s["valid_records"] == s["unique_valid_events"] + s["duplicate_copies"]
    assert s["records_total"] == s["valid_records"] + s["malformed_total"]


def test_duplicates_repeat_an_earlier_event_id_with_identical_content(dirty_plan):
    first = {}
    for r in dirty_plan.records:
        if not r.is_valid:
            continue
        value = json.loads(render(r, NOW)[1])
        if r.fault == "duplicate":
            assert r.event.event_id in first, "duplicate must come after its original"
            assert value == first[r.event.event_id]
        else:
            first.setdefault(r.event.event_id, value)


def test_semantic_duplicates_have_new_ids_and_identical_content(dirty_plan):
    def content(v):
        return {k: v[k] for k in v if k not in ("event_id", "emitted_at")}

    by_key = {}
    for r in dirty_plan.records:
        if r.is_valid and r.fault != "semantic_duplicate":
            v = json.loads(render(r, NOW)[1])
            by_key.setdefault(json.dumps(content(v), sort_keys=True), v["event_id"])
    for r in (r for r in dirty_plan.records if r.fault == "semantic_duplicate"):
        v = json.loads(render(r, NOW)[1])
        original_id = by_key[json.dumps(content(v), sort_keys=True)]
        assert original_id != v["event_id"]


def test_out_of_order_events_follow_a_later_event_of_the_same_package(dirty_plan):
    seen_max = {}
    for r in dirty_plan.records:
        if not r.is_valid:
            continue
        if r.fault == "out_of_order":
            assert seen_max.get(r.key) is not None and seen_max[r.key] > r.event.ordering_key
        k = r.event.ordering_key
        seen_max[r.key] = max(seen_max.get(r.key, k), k)


def test_late_events_arrive_after_the_stream_passed_their_delay(dirty_plan):
    stream_max = None
    for r in dirty_plan.records:
        if not r.is_valid:
            continue
        if r.fault == "late":
            assert stream_max >= r.event.event_time + r.late_delay
        elif r.fault in (None, "out_of_order"):
            t = r.event.event_time
            stream_max = t if stream_max is None else max(stream_max, t)


def test_malformed_records_never_change_expected_state(dirty_plan):
    valid_ids = {e.event_id for events in dirty_plan.events_by_package().values() for e in events}
    malformed_ids = {r.malformed_event_id for r in dirty_plan.records if r.fault == "malformed"}
    assert malformed_ids.isdisjoint(valid_ids)
