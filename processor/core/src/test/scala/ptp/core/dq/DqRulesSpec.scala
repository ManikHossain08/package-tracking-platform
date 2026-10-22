package ptp.core.dq

import java.time.{Duration, Instant}

import org.scalatest.funsuite.AnyFunSuite

import ptp.core.status.StatusRuleSpec.ev
import ptp.testing.Req

class DqRulesSpec extends AnyFunSuite {

  test("DQ-05: only events ordered after the status-defining terminal are post-terminal", Req("DQ-05")) {
    val created   = ev("CREATED", 0, "0-a")
    val delivered = ev("DELIVERED", 60, "0-c")
    val dupLater  = ev("DELIVERED", 60, "0-z") // semantic duplicate with a larger event_id
    val late      = ev("PICKED_UP", 90, "0-d")
    assert(DqRules.postTerminal(Seq(created, delivered, dupLater, late)).map(_.eventId).toSet == Set("0-z", "0-d"))
    assert(DqRules.postTerminal(Seq(created, ev("OUT_FOR_DELIVERY", 50, "0-b"))).isEmpty, "no terminal → none")
  }

  test("DQ-04: orphan only without CREATED and after the grace period", Req("DQ-04")) {
    val first = ev("PICKED_UP", 0, "0-a")
    val now   = first.eventTime.plus(Duration.ofHours(25))
    assert(DqRules.orphan(Seq(first, ev("ARRIVED_AT_FACILITY", 60, "0-b")), now).map(_.eventId).contains("0-a"))
    assert(DqRules.orphan(Seq(first), first.eventTime.plus(Duration.ofHours(23))).isEmpty, "inside the grace period")
    assert(DqRules.orphan(Seq(first, ev("CREATED", 120, "0-c")), now).isEmpty, "late CREATED counts")
  }

  test("DQ-10: ±50 % of the 7-day median, skipped without full history", Req("DQ-10")) {
    val week = Seq(100L, 90, 110, 100, 95, 105, 100)
    assert(DqRules.volume(140, week)._1 == "pass")
    assert(DqRules.volume(151, week)._1 == "warn")
    assert(DqRules.volume(49, week)._1 == "warn")
    assert(DqRules.volume(100, week.take(6))._1 == "skipped")
  }
}
