"""Independent ground truth for the expected package status (FR-GEN-03).

Implements the spec 01 status rule (ADR-0004 Amendment 1, SC-0002). This is deliberately
separate from the Scala processor, so tests compare two independent implementations.
"""

from __future__ import annotations

from collections.abc import Iterable

from .model import TERMINAL, Event


def preferred(a: Event, b: Event) -> Event:
    """Return the event that should define the status when both have been seen.

    Terminal beats non-terminal; among terminals the smallest ordering key wins (the
    first outcome is sticky); among non-terminals the largest ordering key wins.
    """
    a_term, b_term = a.event_type in TERMINAL, b.event_type in TERMINAL
    if a_term != b_term:
        return a if a_term else b
    if a_term:
        return a if a.ordering_key <= b.ordering_key else b
    return a if a.ordering_key >= b.ordering_key else b


def expected_status_event(events: Iterable[Event]) -> Event:
    winner: Event | None = None
    for e in events:
        winner = e if winner is None else preferred(winner, e)
    if winner is None:
        raise ValueError("expected_status_event needs at least one event")
    return winner
