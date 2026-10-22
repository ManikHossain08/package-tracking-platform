package ptp.streaming.it

import java.nio.file.Files
import java.sql.Date

import scala.io.Source
import scala.util.Using

import org.scalatest.funsuite.AnyFunSuite

import ptp.streaming.PostgresStatusSink
import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** TS-D-05 / AC-6.10: the SQL query pack returns hand-computed results on a fixed fixture, run as ptp_reader. */
class AnalyticsQueryPackIT extends AnyFunSuite {

  private val From = Date.valueOf("2026-10-20")
  private val To   = Date.valueOf("2026-10-21")

  private def loadFixture(): Unit = Using.resource(admin()) { c =>
    c.createStatement().execute("TRUNCATE analytics.delivery_facts, analytics.facility_visits, " +
      "analytics.daily_facility_performance, analytics.daily_network_activity, analytics.daily_delivery_performance")
    c.createStatement().execute(Using.resource(Source.fromResource("analytics-fixture.sql"))(_.mkString))
  }

  /** Runs a pack file as ptp_reader, binding :'from_date' / :'to_date'. Rows as "a|b|c" strings (NULL as "null"). */
  private def run(file: String): Seq[String] = {
    val text  = Files.readString(repoRoot.resolve(s"sql/analytics/$file"))
    val binds = """:'(from_date|to_date)'""".r.findAllMatchIn(text).map(_.group(1)).toSeq
    val sql   = text.replaceAll(""":'(from_date|to_date)'""", "?")
    Using.resource(PostgresStatusSink.connect(reader)) { c =>
      Using.resource(c.prepareStatement(sql)) { ps =>
        binds.zipWithIndex.foreach { case (b, i) => ps.setDate(i + 1, if (b == "from_date") From else To) }
        val rs = ps.executeQuery()
        val n  = rs.getMetaData.getColumnCount
        Iterator.continually(rs).takeWhile(_.next()).map(r => (1 to n).map(i => String.valueOf(r.getObject(i))).mkString("|")).toVector
      }
    }
  }

  test("query pack returns the hand-computed results on the fixture", Integration, Req("FR-ANL-02")) {
    reset()
    loadFixture()
    // STANDARD delays [-120, -60, 30]; EXPRESS [-10]; the 2026-10-22 row is outside the range.
    assert(run("01_on_time_rate.sql") == Seq("EXPRESS|1|1|100.00", "STANDARD|3|2|66.67", "null|4|3|75.00"))
    // p90 of [-120,-60,30]: pos 1.8 → -60 + 90×0.8 = 12.0; p95: pos 1.9 → -60 + 90×0.9 = 21.0.
    assert(run("02_delay_percentiles.sql") == Seq("EXPRESS|1|-10.0|-10.0|-10.0|-10.0", "STANDARD|3|-60.0|12.0|21.0|30.0"))
    // YUL dwell [60,120,180] → p50 120, p95 pos 1.9 → 120 + 60×0.9 = 174.0; YYZ [30]; open and out-of-range visits excluded.
    assert(run("03_facility_dwell.sql") == Seq("DC-YUL01|3|120.0|174.0", "DC-YYZ01|1|30.0|30.0"))
    // 2-day range: YUL arrivals 4+2 = 6 → 3.00/day; YYZ 3 → 1.50/day.
    assert(run("04_facility_throughput.sql") == Seq("DC-YUL01|6|3|1|3.00", "DC-YYZ01|3|1|2|1.50"))
    // 1/10 = 10%, 3/20 = 15%, range (1+3)/(10+20) = 13.33%.
    assert(run("05_exception_rate.sql") == Seq("2026-10-20|10|1|10.00", "2026-10-21|20|3|15.00", "null|30|4|13.33"))
  }

  test("ptp_reader cannot modify analytics or read ops staging", Integration, Req("NFR-06")) {
    Using.resource(PostgresStatusSink.connect(reader)) { c =>
      val denied = Seq("DELETE FROM analytics.delivery_facts", "SELECT count(*) FROM ops.gold_stage_delivery_facts")
        .filter(sql => scala.util.Try(c.createStatement().execute(sql)).isSuccess)
      assert(denied.isEmpty, s"reader was allowed: $denied")
    }
  }
}
