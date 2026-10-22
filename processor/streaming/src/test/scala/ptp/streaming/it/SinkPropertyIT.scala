package ptp.streaming.it

import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.util.Random

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import ptp.core.status.{ApplyEvent, StatusRule}
import ptp.streaming.PostgresStatusSink
import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** TS-U-05 / AC-1.6: the SQL apply logic is permutation-, duplication- and split-invariant, and agrees with the
  * Scala StatusRule (ADR-0004: "the Scala comparator and the Postgres comparison agree on random inputs").
  */
class SinkPropertyIT extends AnyFunSuite with ScalaCheckPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 25)

  private val types = Seq("CREATED" -> 10, "PICKED_UP" -> 20, "ARRIVED_AT_FACILITY" -> 30, "DEPARTED_FACILITY" -> 40,
    "OUT_FOR_DELIVERY" -> 50, "DELIVERY_ATTEMPTED" -> 60, "EXCEPTION" -> 70, "DELIVERED" -> 90, "RETURNED_TO_SENDER" -> 90)
  private val T0 = Instant.parse("2026-10-20T10:00:00Z")

  private val eventSets: Gen[Seq[ApplyEvent]] = for {
    packages <- Gen.choose(1, 5)
    n        <- Gen.choose(packages, 25)
    picks    <- Gen.listOfN(n, Gen.zip(Gen.choose(0, packages - 1), Gen.oneOf(types), Gen.choose(0, 3), Gen.choose(0, 999999)))
  } yield picks.map { case (p, (tpe, rank), hour, micros) =>
    // Few distinct hours → frequent time ties, so rank and event_id tiebreaks are exercised.
    ApplyEvent(UUID.randomUUID().toString, f"PKG-PROP$p%08d", tpe, rank, rank == 90,
      T0.plusSeconds(3600L * hour).plusNanos(1000L * (micros % 3)), None, None,
      if (tpe == "CREATED") Some(LocalDate.of(2026, 10, 23)) else None, None, None, None, None)
  }

  private def applyInBatches(events: Seq[ApplyEvent], rnd: Random): Map[String, (String, Int)] = {
    reset()
    val stream  = rnd.shuffle(events ++ events.filter(_ => rnd.nextInt(3) == 0)) // shuffled, with duplicates
    val batches = stream.grouped(math.max(1, stream.size / (1 + rnd.nextInt(4)))).toSeq
    PostgresStatusSink.ensurePartitions(writer, events.map(_.eventDate))
    batches.zipWithIndex.foreach { case (b, i) => PostgresStatusSink.writePartition(writer, b.iterator, i.toLong, "prop", Instant.now()) }
    statusRows().map { case (k, r) => k -> (r.eventId, r.eventCount) }
  }

  test("SQL apply = Scala rule, for any order, duplication and batch split", Integration, Req("NFR-01"), Req("FR-ORD-01")) {
    forAll(eventSets, Gen.choose(0L, Long.MaxValue)) { (events, seed) =>
      val expected = events.groupBy(_.packageId).map { case (pkg, es) => pkg -> (StatusRule.winner(es).eventId, es.size) }
      val rnd      = new Random(seed)
      (1 to 3).foreach(_ => assert(applyInBatches(events, rnd) == expected))
    }
  }
}
