package ptp.core.validation

import java.time.{Instant, LocalDate, ZoneId}

import org.scalatest.funsuite.AnyFunSuite
import ptp.core.model._
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

/** TS-U-13, AC-3.6: accepted events are typed faithfully. */
class TypingSpec extends AnyFunSuite {

  private def event(sampleName: String): PackageEvent =
    validate(read(samples.resolve(s"valid/$sampleName"))).fold(r => fail(r.errors.toString), identity)

  test("offsets are normalised to the same UTC instant", Req("FR-VAL-01")) {
    // sample: 2026-10-19T14:05:31.250-04:00
    assert(event("arrived_at_facility.json").eventTime == Instant.parse("2026-10-19T18:05:31.250Z"))
  }

  test("microsecond precision is kept", Req("FR-VAL-01")) {
    assert(event("delivered_v1_1_extra_field.json").eventTime == Instant.parse("2026-10-21T15:42:10.123456Z"))
  }

  test("CREATED is fully typed, including shipment", Req("FR-VAL-01")) {
    val e = event("created.json")
    assert(e.eventType == EventType.Created && e.eventType.rank == 10 && !e.eventType.terminal)
    assert(e.source == Source(SourceSystem.OrderSystem, "order-api-1", 1001L))
    assert(e.facilityId.isEmpty && e.vehicleId.isEmpty && e.details.isEmpty && e.attributes.isEmpty)
    assert(e.shipment.contains(Shipment(LocalDate.of(2026, 10, 21), ServiceLevel.Standard, "DC-YUL01", "H2X", ZoneId.of("America/Toronto"))))
  }

  test("optional and conditional fields map to Options; attributes are kept", Req("FR-VAL-01")) {
    val delivered = event("delivered_v1_1_extra_field.json")
    assert(delivered.eventType.terminal && delivered.vehicleId.contains("VH-000123"))
    assert(delivered.attributes == Map("door" -> "front"))
    assert(delivered.schemaVersion == "1.1")
    assert(event("exception.json").details.contains(Details(Some("DAMAGED_LABEL"), None)))
  }

  test("event_id is stored as canonical lowercase", Req("FR-DED-01")) {
    val node = base(); node.put("event_id", node.get("event_id").textValue.toUpperCase)
    assert(validate(node).map(_.eventId) == Right(node.get("event_id").textValue.toLowerCase))
  }
}
