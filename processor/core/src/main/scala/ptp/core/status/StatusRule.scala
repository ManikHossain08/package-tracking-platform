package ptp.core.status

import java.time.Instant

/** The status preference rule (spec 01, ADR-0004 Amendment 1 / SC-0002).
  *
  * Status = the most preferred event seen: terminal beats non-terminal; among terminals the
  * smallest ordering key wins (first outcome is sticky); among non-terminals the largest key wins.
  * A maximum under a fixed total order, so it is independent of arrival order, duplicates and batching.
  * Mirrored in SQL by `PostgresStatusSink`; the generator's Python oracle is the independent reference.
  */
object StatusRule {

  /** Ordering key `(event_time, lifecycle_rank, event_id)`. Lowercase UUID text order = Postgres uuid order. */
  final case class Key(eventTime: Instant, rank: Int, eventId: String)

  implicit val keyOrdering: Ordering[Key] =
    Ordering.by((k: Key) => (k.eventTime, k.rank, k.eventId))

  def key(e: ApplyEvent): Key = Key(e.eventTime, e.rank, e.eventId)

  /** True when `candidate` should replace `current` as the status-defining event. */
  def prefers(candidate: ApplyEvent, current: ApplyEvent): Boolean =
    if (candidate.terminal != current.terminal) candidate.terminal
    else if (candidate.terminal) keyOrdering.lt(key(candidate), key(current))
    else keyOrdering.gt(key(candidate), key(current))

  def preferred(a: ApplyEvent, b: ApplyEvent): ApplyEvent = if (prefers(b, a)) b else a

  def winner(events: Iterable[ApplyEvent]): ApplyEvent = events.reduce(preferred)
}
