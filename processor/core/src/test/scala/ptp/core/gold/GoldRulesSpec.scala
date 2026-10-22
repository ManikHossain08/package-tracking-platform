package ptp.core.gold

import java.time.{Duration, Instant, LocalDate}

import org.scalatest.funsuite.AnyFunSuite

import ptp.core.status.ApplyEvent
import ptp.testing.Req

/** SC-0005 C1–C6, the same hand-computed cases as the Python oracle's tests. */
class GoldRulesSpec extends AnyFunSuite {
  private val Ranks = Map("CREATED" -> 10, "ARRIVED_AT_FACILITY" -> 30, "DEPARTED_FACILITY" -> 40, "EXCEPTION" -> 70,
    "DELIVERED" -> 90, "RETURNED_TO_SENDER" -> 90)
  // End of promised day 2026-10-20 in Toronto (EDT, UTC-4).
  private val Eod = Instant.parse("2026-10-21T04:00:00Z")

  private def ev(tpe: String, at: Instant, id: String, facility: Option[String] = None) =
    ApplyEvent(id, "PKG-GOLDAAAAAAAA", tpe, Ranks(tpe), tpe == "DELIVERED" || tpe == "RETURNED_TO_SENDER", at, facility,
      None, None, None, None, None, None)
  private val created = ev("CREATED", Eod.minus(Duration.ofDays(3)), "0-a").copy(
    promisedDeliveryDate = Some(LocalDate.of(2026, 10, 20)), serviceLevel = Some("STANDARD"),
    destinationPostalPrefix = Some("H2X"), destinationTimezone = Some("America/Toronto"))

  test("C3: on time and signed delay relative to the destination's end of day", Req("FR-ANL-01")) {
    val early = GoldRules.deliveryFact(Seq(created, ev("DELIVERED", Eod.minusSeconds(90 * 60), "0-b"))).get
    assert(early.onTime && early.delayMinutes == -90.0 && early.deliveryDate == LocalDate.of(2026, 10, 20))
    val late = GoldRules.deliveryFact(Seq(created, ev("DELIVERED", Eod.plusSeconds(30 * 60), "0-b"))).get
    assert(!late.onTime && late.delayMinutes == 30.0 && late.deliveryDate == LocalDate.of(2026, 10, 21))
    val exactly = GoldRules.deliveryFact(Seq(created, ev("DELIVERED", Eod, "0-b"))).get
    assert(!exactly.onTime && exactly.delayMinutes == 0.0)
  }

  test("C1/C2: returned-first or missing CREATED is not a delivery", Req("FR-ANL-01")) {
    val returnedFirst = Seq(created, ev("RETURNED_TO_SENDER", Eod.minusSeconds(5 * 3600), "0-r", Some("DC-YUL01")),
      ev("DELIVERED", Eod.minusSeconds(3600), "0-d"))
    assert(GoldRules.deliveryFact(returnedFirst).isEmpty)
    assert(GoldRules.deliveryFact(Seq(ev("DELIVERED", Eod, "0-d"))).isEmpty)
  }

  test("C5/C6: visit pairing, shared departure for duplicate arrivals, open visits", Req("FR-ANL-01")) {
    val t0 = Instant.parse("2026-10-19T22:00:00Z")
    val visits = GoldRules.facilityVisits(Seq(
      ev("ARRIVED_AT_FACILITY", t0, "0-1", Some("DC-YUL01")),
      ev("ARRIVED_AT_FACILITY", t0, "0-2", Some("DC-YUL01")),
      ev("DEPARTED_FACILITY", t0.plusSeconds(3 * 3600), "0-3", Some("DC-YUL01")),
      ev("ARRIVED_AT_FACILITY", t0.plusSeconds(9 * 3600), "0-4", Some("DC-YYZ01"))
    ))
    val yul = visits.filter(_.facilityId == "DC-YUL01")
    assert(yul.size == 2 && yul.forall(_.dwellMinutes.contains(180.0)))
    assert(yul.forall(v => v.arrivalDate == LocalDate.of(2026, 10, 19) && v.departureDate.contains(LocalDate.of(2026, 10, 20))))
    val yyz = visits.filter(_.facilityId == "DC-YYZ01").head
    assert(yyz.departedAt.isEmpty && yyz.dwellMinutes.isEmpty)
  }
}
