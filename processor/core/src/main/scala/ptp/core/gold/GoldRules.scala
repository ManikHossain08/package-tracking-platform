package ptp.core.gold

import java.time.{Duration, Instant, LocalDate, ZoneId, ZoneOffset}

import ptp.core.status.{ApplyEvent, StatusRule}

/** One row of analytics.delivery_facts. */
final case class DeliveryFact(
    packageId: String,
    deliveryDate: LocalDate,
    deliveredAt: Instant,
    promisedDeliveryDate: LocalDate,
    serviceLevel: String,
    destinationPostalPrefix: String,
    destinationTimezone: String,
    delayMinutes: Double,
    onTime: Boolean
)

/** One row of analytics.facility_visits (one per arrival event). */
final case class FacilityVisit(
    arrivalEventId: String,
    packageId: String,
    facilityId: String,
    arrivedAt: Instant,
    arrivalDate: LocalDate,
    departedAt: Option[Instant],
    departureDate: Option[LocalDate],
    dwellMinutes: Option[Double]
)

/** Gold metric definitions (SC-0005 C1–C6), per package. Pure; the Spark gold job applies them per package. */
object GoldRules {

  private def minutes(from: Instant, to: Instant): Double = Duration.between(from, to).toNanos / 60e9
  def utcDate(t: Instant): LocalDate = LocalDate.ofInstant(t, ZoneOffset.UTC)

  /** C1–C3. `events` are all of one package's unique valid events. */
  def deliveryFact(events: Seq[ApplyEvent]): Option[DeliveryFact] = {
    val winner  = StatusRule.winner(events) // C1: the same status rule as tracking.package_status
    val created = events.filter(_.isCreated)
    if (!winner.isDelivered || created.isEmpty) None // C1, C2
    else {
      val c = created.minBy(StatusRule.key)(StatusRule.keyOrdering)
      for {
        promised <- c.promisedDeliveryDate
        level    <- c.serviceLevel
        prefix   <- c.destinationPostalPrefix
        tz       <- c.destinationTimezone
      } yield {
        val zone     = ZoneId.of(tz)
        val endOfDay = promised.plusDays(1).atStartOfDay(zone).toInstant // C3
        DeliveryFact(winner.packageId, LocalDate.ofInstant(winner.eventTime, zone), winner.eventTime, promised, level,
          prefix, tz, minutes(endOfDay, winner.eventTime), winner.eventTime.isBefore(endOfDay))
      }
    }
  }

  /** C5–C6: each arrival pairs with the departure whose ordering key is the smallest one above its own. */
  def facilityVisits(events: Seq[ApplyEvent]): Seq[FacilityVisit] =
    events.filter(_.facilityId.isDefined).groupBy(_.facilityId.get).toSeq.flatMap { case (facility, es) =>
      val departures = es.filter(_.eventType == "DEPARTED_FACILITY").sortBy(StatusRule.key)(StatusRule.keyOrdering)
      es.filter(_.eventType == "ARRIVED_AT_FACILITY").map { a =>
        val d = departures.find(d => StatusRule.keyOrdering.gt(StatusRule.key(d), StatusRule.key(a)))
        FacilityVisit(a.eventId, a.packageId, facility, a.eventTime, utcDate(a.eventTime), d.map(_.eventTime),
          d.map(x => utcDate(x.eventTime)), d.map(x => minutes(a.eventTime, x.eventTime)))
      }
    }
}
