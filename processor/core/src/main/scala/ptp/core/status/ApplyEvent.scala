package ptp.core.status

import java.time.{Instant, LocalDate, ZoneOffset}

import ptp.core.model.PackageEvent

/** The flat projection of a valid event that the status sink needs.
  *
  * Plain types only (String, Instant, LocalDate, Option), so Spark can encode it directly.
  */
final case class ApplyEvent(
    eventId: String,
    packageId: String,
    eventType: String,
    rank: Int,
    terminal: Boolean,
    eventTime: Instant,
    facilityId: Option[String],
    vehicleId: Option[String],
    promisedDeliveryDate: Option[LocalDate],
    serviceLevel: Option[String],
    originFacilityId: Option[String],
    destinationPostalPrefix: Option[String],
    destinationTimezone: Option[String]
) {

  /** UTC date of event_time: the processed_events partition key (spec 02 §4). */
  def eventDate: LocalDate = LocalDate.ofInstant(eventTime, ZoneOffset.UTC)

  def isCreated: Boolean   = eventType == "CREATED"
  def isDelivered: Boolean = eventType == "DELIVERED"
}

object ApplyEvent {
  def from(e: PackageEvent): ApplyEvent = ApplyEvent(
    eventId = e.eventId,
    packageId = e.packageId,
    eventType = e.eventType.name,
    rank = e.eventType.rank,
    terminal = e.eventType.terminal,
    eventTime = e.eventTime,
    facilityId = e.facilityId,
    vehicleId = e.vehicleId,
    promisedDeliveryDate = e.shipment.map(_.promisedDeliveryDate),
    serviceLevel = e.shipment.map(_.serviceLevel.name),
    originFacilityId = e.shipment.map(_.originFacilityId),
    destinationPostalPrefix = e.shipment.map(_.destinationPostalPrefix),
    destinationTimezone = e.shipment.map(_.destinationTimezone.getId)
  )
}
