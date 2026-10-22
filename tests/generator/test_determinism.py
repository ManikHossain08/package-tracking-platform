"""TS-U-06: same scenario and seed give the same stream (AC-2.1)."""

import pytest
from ptp_generator.plan import build_plan
from ptp_generator.render import render

from .conftest import NOW, make_scenario

pytestmark = pytest.mark.req("FR-GEN-01")


def stream(seed: int) -> list[tuple[bytes, bytes]]:
    return [render(r, NOW) for r in build_plan(make_scenario(seed=seed)).records]


def test_same_seed_gives_byte_identical_stream():
    assert stream(5) == stream(5)


def test_different_seed_gives_different_stream():
    assert stream(5) != stream(6)
