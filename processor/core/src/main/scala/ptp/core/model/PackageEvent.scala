package ptp.core.model

import java.time.{Instant, LocalDate, ZoneId}

/** Event types with lifecycle rank and terminal flag (spec 01 §Event lifecycle). */
sealed abstract class EventType(val name: String, val rank: Int, val terminal: Boolean)

object EventType {
  case object Created           extends EventType("CREATED", 10, terminal = false)
  case object PickedUp          extends EventType("PICKED_UP", 20, terminal = false)
  case object ArrivedAtFacility extends EventType("ARRIVED_AT_FACILITY", 30, terminal = false)
  case object DepartedFacility  extends EventType("DEPARTED_FACILITY", 40, terminal = false)
  case object OutForDelivery    extends EventType("OUT_FOR_DELIVERY", 50, terminal = false)
  case object DeliveryAttempted extends EventType("DELIVERY_ATTEMPTED", 60, terminal = false)
  case object Exception         extends EventType("EXCEPTION", 70, terminal = false)
  case object Delivered         extends EventType("DELIVERED", 90, terminal = true)
  case object ReturnedToSender  extends EventType("RETURNED_TO_SENDER", 90, terminal = true)

  val all: Seq[EventType] = Seq(
    Created, PickedUp, ArrivedAtFacility, DepartedFacility, OutForDelivery,
    DeliveryAttempted, Exception, Delivered, ReturnedToSender
  )
  private val byName: Map[String, EventType] = all.map(t => t.name -> t).toMap
  def fromName(name: String): Option[EventType] = byName.get(name)

  /** Types that require a facility_id, a vehicle_id, or either one (spec 02 §2). */
  val requiresFacility: Set[EventType] = Set(ArrivedAtFacility, DepartedFacility, ReturnedToSender)
  val requiresVehicle: Set[EventType]  = Set(OutForDelivery, DeliveryAttempted, Delivered)
}

sealed abstract class SourceSystem(val name: String)
object SourceSystem {
  case object DcScanner   extends SourceSystem("DC_SCANNER")
  case object VehicleApp  extends SourceSystem("VEHICLE_APP")
  case object OrderSystem extends SourceSystem("ORDER_SYSTEM")
  val all: Seq[SourceSystem] = Seq(DcScanner, VehicleApp, OrderSystem)
  def fromName(name: String): Option[SourceSystem] = all.find(_.name == name)
}

sealed abstract class ServiceLevel(val name: String)
object ServiceLevel {
  case object Economy  extends ServiceLevel("ECONOMY")
  case object Standard extends ServiceLevel("STANDARD")
  case object Express  extends ServiceLevel("EXPRESS")
  val all: Seq[ServiceLevel] = Seq(Economy, Standard, Express)
  def fromName(name: String): Option[ServiceLevel] = all.find(_.name == name)
}

final case class Source(system: SourceSystem, deviceId: String, sequence: Long)

final case class Shipment(
    promisedDeliveryDate: LocalDate,
    serviceLevel: ServiceLevel,
    originFacilityId: String,
    destinationPostalPrefix: String,
    destinationTimezone: ZoneId
)

final case class Details(exceptionCode: Option[String], attemptNumber: Option[Int])

/** A validated package-event v1 (contracts/package-event.v1.schema.json).
  *
  * `eventId` is the canonical lowercase UUID text: its string order equals Postgres uuid order
  * (spec 02 §4), which the status ordering key relies on.
  */
final case class PackageEvent(
    eventId: String,
    schemaVersion: String,
    packageId: String,
    eventType: EventType,
    eventTime: Instant,
    emittedAt: Instant,
    source: Source,
    facilityId: Option[String],
    vehicleId: Option[String],
    shipment: Option[Shipment],
    details: Option[Details],
    attributes: Map[String, String]
)
