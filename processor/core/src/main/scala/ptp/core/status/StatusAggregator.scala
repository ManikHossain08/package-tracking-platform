package ptp.core.status

import java.time.Instant

/** What one batch of *new* (never-before-applied) events contributes to one package's status row. */
final case class PackageDelta(
    packageId: String,
    winner: ApplyEvent,
    newEventCount: Int,
    maxEventTime: Instant,
    created: Option[ApplyEvent],
    firstDeliveredTime: Option[Instant]
)

/** Folds new events into per-package deltas. Combining deltas with the stored row is done in SQL with the
  * same operators (preference, sum, max, min, first-non-null), so the result is order-independent.
  */
object StatusAggregator {

  def deltas(newEvents: Iterable[ApplyEvent]): Seq[PackageDelta] =
    newEvents
      .groupBy(_.packageId)
      .iterator
      .map { case (packageId, events) =>
        val created = events.filter(_.isCreated)
        PackageDelta(
          packageId = packageId,
          winner = StatusRule.winner(events),
          newEventCount = events.size,
          maxEventTime = events.map(_.eventTime).max,
          // If a package ever had two different CREATED events, the earliest key supplies the shipment.
          created = if (created.isEmpty) None else Some(created.minBy(StatusRule.key)(StatusRule.keyOrdering)),
          firstDeliveredTime = events.filter(_.isDelivered).map(_.eventTime).minOption
        )
      }
      .toSeq
      .sortBy(_.packageId)
}
