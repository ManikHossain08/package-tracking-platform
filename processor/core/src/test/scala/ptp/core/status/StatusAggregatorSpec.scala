package ptp.core.status

import java.time.LocalDate

import org.scalatest.funsuite.AnyFunSuite
import ptp.core.status.StatusRuleSpec.ev
import ptp.testing.Req

class StatusAggregatorSpec extends AnyFunSuite {

  test("one delta per package with count, max time, CREATED attributes and first delivery", Req("FR-ORD-01")) {
    val created = ev("CREATED", 0, "0-a").copy(promisedDeliveryDate = Some(LocalDate.of(2026, 10, 22)), serviceLevel = Some("EXPRESS"))
    val ofd     = ev("OUT_FOR_DELIVERY", 50, "0-b")
    val del     = ev("DELIVERED", 60, "0-c")
    val other   = ev("PICKED_UP", 10, "0-d").copy(packageId = "PKG-BBBBBBBBBBBB")
    val deltas  = StatusAggregator.deltas(Seq(del, other, ofd, created))
    assert(deltas.map(_.packageId) == Seq("PKG-AAAAAAAAAAAA", "PKG-BBBBBBBBBBBB"))
    val a = deltas.head
    assert(a.winner == del && a.newEventCount == 3 && a.maxEventTime == ofd.eventTime.plusSeconds(600))
    assert(a.created.contains(created) && a.firstDeliveredTime.contains(del.eventTime))
    assert(deltas(1).created.isEmpty && deltas(1).firstDeliveredTime.isEmpty)
  }

  test("event_date is the UTC date of event_time", Req("FR-DED-01")) {
    val late = ev("CREATED", 0, "0-a").copy(eventTime = java.time.Instant.parse("2026-10-20T23:59:59.999999Z"))
    assert(late.eventDate == LocalDate.of(2026, 10, 20))
  }
}
