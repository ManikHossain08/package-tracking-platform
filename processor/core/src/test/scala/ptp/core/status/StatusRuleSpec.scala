package ptp.core.status

import java.time.Instant

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import ptp.testing.Req

import scala.util.Random

/** TS-U-04 / TS-U-05 (core part): the preference rule and its order-independence (FR-ORD-01, NFR-01). */
class StatusRuleSpec extends AnyFunSuite with ScalaCheckPropertyChecks {
  import StatusRuleSpec._

  test("terminal beats a later non-terminal in either arrival order (SC-0002 example)", Req("FR-ORD-01")) {
    val delivered = ev("DELIVERED", 0, "0-a")
    val ofd       = ev("OUT_FOR_DELIVERY", 60, "0-b")
    assert(StatusRule.winner(Seq(delivered, ofd)) == delivered)
    assert(StatusRule.winner(Seq(ofd, delivered)) == delivered)
  }

  test("the first terminal is sticky", Req("FR-ORD-01")) {
    val delivered = ev("DELIVERED", 0, "0-a")
    assert(StatusRule.winner(Seq(ev("RETURNED_TO_SENDER", 120, "0-b"), delivered)) == delivered)
  }

  test("latest non-terminal wins; ties break on rank, then event_id", Req("FR-ORD-01")) {
    assert(StatusRule.winner(Seq(ev("ARRIVED_AT_FACILITY", 30, "0-b"), ev("CREATED", 0, "0-a"))).eventType == "ARRIVED_AT_FACILITY")
    assert(StatusRule.winner(Seq(ev("OUT_FOR_DELIVERY", 5, "0-z"), ev("EXCEPTION", 5, "0-a"))).eventType == "EXCEPTION")
    assert(StatusRule.winner(Seq(ev("ARRIVED_AT_FACILITY", 5, "0-b"), ev("ARRIVED_AT_FACILITY", 5, "0-a"))).eventId == "0-b")
  }

  test("winner is invariant under permutation and duplication", Req("NFR-01"), Req("FR-ORD-01")) {
    forAll(events, Gen.choose(0L, Long.MaxValue)) { (es, seed) =>
      val rnd      = new Random(seed)
      val expected = StatusRule.winner(es)
      val mixed    = rnd.shuffle(es ++ es.filter(_ => rnd.nextBoolean()))
      assert(StatusRule.winner(mixed) == expected)
    }
  }

  test("aggregating split batches equals aggregating everything at once", Req("NFR-01")) {
    forAll(events, Gen.choose(1, 5)) { (es, parts) =>
      val whole   = StatusAggregator.deltas(es).head
      val batches = es.grouped(math.max(1, es.size / parts)).map(b => StatusAggregator.deltas(b).head).toSeq
      assert(StatusRule.winner(batches.map(_.winner)) == whole.winner)
      assert(batches.map(_.newEventCount).sum == whole.newEventCount)
      assert(batches.map(_.maxEventTime).max == whole.maxEventTime)
    }
  }
}

object StatusRuleSpec {
  private val T0    = Instant.parse("2026-10-20T10:00:00Z")
  private val ranks = Map("CREATED" -> 10, "PICKED_UP" -> 20, "ARRIVED_AT_FACILITY" -> 30, "DEPARTED_FACILITY" -> 40,
    "OUT_FOR_DELIVERY" -> 50, "DELIVERY_ATTEMPTED" -> 60, "EXCEPTION" -> 70, "DELIVERED" -> 90, "RETURNED_TO_SENDER" -> 90)

  def ev(tpe: String, minutes: Int, id: String): ApplyEvent =
    ApplyEvent(id, "PKG-AAAAAAAAAAAA", tpe, ranks(tpe), tpe == "DELIVERED" || tpe == "RETURNED_TO_SENDER",
      T0.plusSeconds(60L * minutes), None, None, None, None, None, None, None)

  /** 1-12 distinct events for one package, with deliberate time ties. */
  val events: Gen[Seq[ApplyEvent]] = for {
    n     <- Gen.choose(1, 12)
    types <- Gen.listOfN(n, Gen.oneOf(ranks.keys.toSeq))
    times <- Gen.listOfN(n, Gen.choose(0, 5))
  } yield types.zip(times).zipWithIndex.map { case ((t, m), i) => ev(t, m, f"0-$i%03d") }
}
