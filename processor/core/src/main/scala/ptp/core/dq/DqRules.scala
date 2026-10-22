package ptp.core.dq

import java.time.{Duration, Instant}

import ptp.core.status.{ApplyEvent, StatusRule}

/** Pure parts of the data-quality checks (spec 05, SC-0006 A). */
object DqRules {

  /** DQ-05: events whose ordering key is greater than the package's status-defining terminal event. */
  def postTerminal(events: Seq[ApplyEvent]): Seq[ApplyEvent] = {
    val winner = StatusRule.winner(events)
    if (!winner.terminal) Nil
    else events.filter(e => StatusRule.keyOrdering.gt(StatusRule.key(e), StatusRule.key(winner)))
  }

  /** DQ-04: a package with no CREATED whose first event is older than `grace`; returns that first event. */
  def orphan(events: Seq[ApplyEvent], now: Instant, grace: Duration = Duration.ofHours(24)): Option[ApplyEvent] =
    if (events.exists(_.isCreated)) None
    else {
      val first = events.minBy(StatusRule.key)(StatusRule.keyOrdering)
      if (first.eventTime.isBefore(now.minus(grace))) Some(first) else None
    }

  /** DQ-10: `current` must be within ±tolerance of the median of `history` (same hour, previous days).
    * Returns (status, relative deviation). Needs a full history, otherwise "skipped".
    */
  def volume(current: Long, history: Seq[Long], requiredDays: Int = 7, tolerance: Double = 0.5): (String, Double) =
    if (history.size < requiredDays) ("skipped", Double.NaN)
    else {
      val sorted = history.sorted
      val median =
        if (sorted.size % 2 == 1) sorted(sorted.size / 2).toDouble
        else (sorted(sorted.size / 2 - 1) + sorted(sorted.size / 2)) / 2.0
      if (median == 0) (if (current == 0) "pass" else "warn", Double.PositiveInfinity)
      else {
        val deviation = (current - median) / median
        (if (math.abs(deviation) <= tolerance) "pass" else "warn", deviation)
      }
    }
}
