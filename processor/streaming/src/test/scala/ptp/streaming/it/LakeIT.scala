package ptp.streaming.it

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import ptp.core.gold.GoldRules
import ptp.lake.{GoldHooks, GoldLoad, LakeConfig, SilverBuild, SilverHooks}
import ptp.streaming.ProcessorConfig
import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** Step 6: silver build and gold load against the independent Python oracle (spec 07 AC-6.1 to AC-6.9). */
class LakeIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private val Today = LocalDate.now(ZoneOffset.UTC)

  private def lake(cfg: ProcessorConfig, target: Long = 128L * 1024 * 1024) = LakeConfig(cfg.lakeRoot, batchWriter, target, 7, 30)
  private def silverRows(lc: LakeConfig): Seq[Row] = spark.read.parquet(lc.silverPath).collect().toSeq
  private def comparable(rows: Seq[Row]): Set[String] =
    rows.map(r => r.schema.fieldNames.filterNot(_ == "silver_run_id").map(f => String.valueOf(r.getAs[AnyRef](f) match {
      case m: scala.collection.Map[_, _] => m.toSeq.map(_.toString).sorted.mkString(",")
      case other                          => other
    })).mkString("|")).toSet

  /** Every partition's data files with their modification times. */
  private def files(lc: LakeConfig): Map[String, Long] =
    Files.walk(Paths.get(lc.silverPath)).iterator().asScala
      .filter(p => p.toString.endsWith(".parquet"))
      .map(p => Paths.get(lc.silverPath).relativize(p).toString -> Files.getLastModifiedTime(p).toMillis).toMap

  private def pipeline(scenario: String, maxOffsets: Long = 50000): (Generated, ProcessorConfig, Topics) = {
    val t   = topics()
    val gen = generate(scenario, t.events)
    val cfg = config(t, maxOffsets)
    runBothToEnd(cfg)
    (gen, cfg, t)
  }

  private def lateEvent(pkg: String, tpe: String, at: Instant, id: String, extra: String,
                        system: String = "DC_SCANNER"): (String, Array[Byte]) =
    pkg -> s"""{"event_id":"$id","schema_version":"1.0","package_id":"$pkg","event_type":"$tpe","event_time":"$at",
              |"emitted_at":"${Instant.now()}","source":{"system":"$system","device_id":"device-it-01","sequence":1}$extra}"""
      .stripMargin.replace("\n", "").getBytes(UTF_8)

  test("AC-6.1/6.2/6.3 silver = the unique valid events, earliest copy kept, typed with lineage", Integration,
    Req("FR-STO-02"), Req("DQ-02")) {
    val (gen, cfg, _) = pipeline("bench/scenarios/smoke.yaml", maxOffsets = 150)
    val lc     = lake(cfg)
    val result = SilverBuild.run(spark, lc)
    val rows   = silverRows(lc)

    val expectedIds = gen.manifest.values.flatMap(_.get("unique_event_ids").elements().asScala.map(_.textValue)).toSet
    val ids         = rows.map(_.getAs[String]("event_id"))
    assert(ids.size == expectedIds.size && ids.toSet == expectedIds, "AC-6.1: exactly the unique valid events, each once")
    assert(rows.forall(r => r.getAs[java.sql.Date]("event_date").toLocalDate == GoldRules.utcDate(r.getAs[java.sql.Timestamp]("event_time").toInstant)))

    // AC-6.2: the kept copy is the earliest (kafka_timestamp, partition, offset) among all valid bronze copies.
    val earliest = spark.read.parquet(cfg.bronzePath).where("is_valid").collect().toSeq
      .groupBy(_.getAs[String]("event_id")).view.mapValues(_.map(r => (r.getAs[java.sql.Timestamp]("kafka_timestamp").toInstant,
        r.getAs[Int]("kafka_partition"), r.getAs[Long]("kafka_offset"))).min(Ordering.by((k: (Instant, Int, Long)) => (k._1.toEpochMilli * 1000000L + k._1.getNano % 1000000, k._2, k._3)))).toMap
    val kept = rows.map(r => r.getAs[String]("event_id") -> (r.getAs[java.sql.Timestamp]("kafka_timestamp").toInstant,
      r.getAs[Int]("kafka_partition"), r.getAs[Long]("kafka_offset"))).toMap
    assert(kept == earliest)
    assert(gen.manifest.values.exists(_.at("/injected/duplicates").intValue > 0), "the scenario must contain duplicates")

    // AC-6.3: typed columns, content hash, lineage.
    val created = rows.filter(_.getAs[String]("event_type") == "CREATED")
    assert(created.nonEmpty && created.forall(r => r.getAs[AnyRef]("promised_delivery_date") != null && r.getAs[String]("destination_timezone") != null))
    assert(rows.forall(r => r.getAs[String]("content_hash").matches("[0-9a-f]{64}") && r.getAs[String]("silver_run_id") == result.runId))
    val hashOf = rows.map(r => r.getAs[String]("event_id") -> r.getAs[String]("content_hash")).toMap
    val semantic = gen.manifest.values.flatMap(_.at("/injected/semantic_duplicate_event_ids").elements().asScala.map(_.textValue)).toSeq
    assert(semantic.nonEmpty && semantic.forall(id => hashOf.values.count(_ == hashOf(id)) >= 2), "semantic duplicates share a content hash")
  }

  test("AC-6.4 rebuilds are idempotent and targeted; a crash before the swap changes nothing", Integration, Req("FR-STO-02")) {
    val (_, cfg, t) = pipeline("bench/scenarios/clean.yaml")
    val lc    = lake(cfg)
    val first = SilverBuild.run(spark, lc)
    assert(first.affectedDates.size >= 2)
    val baseline = comparable(silverRows(lc))

    (1 to 2).foreach(_ => SilverBuild.run(spark, lc, ingestRange = Some((Today, Today))))
    assert(comparable(silverRows(lc)) == baseline, "full rebuilds give identical rows")

    // A late event for the oldest date touches only that partition.
    val oldest  = first.affectedDates.min
    val pkg     = statusRows().keys.min
    val lateAt  = oldest.atTime(12, 0).toInstant(ZoneOffset.UTC)
    produce(t.events, Seq(lateEvent(pkg, "EXCEPTION", lateAt, "0192a6b4-0000-7000-8000-0000000006a4",
      ""","facility_id":"DC-YUL01","details":{"exception_code":"WEATHER_DELAY"}""")))
    runBronzeToEnd(cfg)
    val before   = files(lc)
    val targeted = SilverBuild.run(spark, lc)
    assert(targeted.affectedDates == Seq(oldest))
    val after = files(lc)
    assert(before.filterNot(_._1.startsWith(s"event_date=$oldest")) == after.filterNot(_._1.startsWith(s"event_date=$oldest")),
      "other partitions' files must not be rewritten")
    assert(silverRows(lc).exists(_.getAs[String]("event_id") == "0192a6b4-0000-7000-8000-0000000006a4"))

    // Crash after staging, before the swap: live silver and the watermark are unchanged; the next build catches up.
    val secondOldest = first.affectedDates.sorted.apply(1)
    produce(t.events, Seq(lateEvent(pkg, "EXCEPTION", secondOldest.atTime(12, 0).toInstant(ZoneOffset.UTC),
      "0192a6b4-0000-7000-8000-0000000006a5", ""","facility_id":"DC-YUL01","details":{"exception_code":"MISSORT"}""")))
    runBronzeToEnd(cfg)
    val beforeCrash = (files(lc), comparable(silverRows(lc)))
    intercept[IllegalStateException](SilverBuild.run(spark, lc, hooks = SilverHooks(() => throw new IllegalStateException("crash"))))
    assert((files(lc), comparable(silverRows(lc))) == beforeCrash)
    assert(SilverBuild.run(spark, lc).affectedDates == Seq(secondOldest))
    assert(silverRows(lc).exists(_.getAs[String]("event_id") == "0192a6b4-0000-7000-8000-0000000006a5"))
  }

  test("AC-6.5 silver compacts bronze's small files to the target size", Integration, Req("FR-STO-02")) {
    val (_, cfg, _) = pipeline("bench/scenarios/it-restart.yaml", maxOffsets = 200)
    val bronzeFiles = Files.walk(Paths.get(cfg.bronzePath)).iterator().asScala.count(_.toString.endsWith(".parquet"))
    val lc = lake(cfg)
    val r  = SilverBuild.run(spark, lc)
    def perDate = files(lc).keys.groupBy(_.takeWhile(_ != '/')).view.mapValues(_.size).toMap
    assert(perDate.values.forall(_ == 1) && bronzeFiles > perDate.size, s"bronze $bronzeFiles files → silver $perDate")
    assert(r.filesPerDate.values.forall(_ == 1))

    val small = lake(cfg, target = 100L * 1024) // force several files per date
    val r2    = SilverBuild.run(spark, small, ingestRange = Some((Today, Today)))
    assert(perDate == r2.filesPerDate.map { case (d, n) => s"event_date=$d" -> n } && r2.filesPerDate.values.max > 1)
  }

  // ---- gold ----------------------------------------------------------------------------------------

  private def near(a: Double, b: Double) = math.abs(a - b) < 1e-6
  private def dbl(v: AnyRef): Double     = v.asInstanceOf[Number].doubleValue
  private def instant(v: AnyRef): Instant = v match { // JDBC returns timestamptz as java.sql.Timestamp
    case ts: java.sql.Timestamp          => ts.toInstant
    case odt: java.time.OffsetDateTime   => odt.toInstant
    case other                           => sys.error(s"not a timestamp: $other")
  }
  private def inWindow(d: String)        = !LocalDate.parse(d).isBefore(Today.minusDays(6)) && !LocalDate.parse(d).isAfter(Today)

  private def gold(scenario: String): (Generated, LakeConfig) = {
    val (gen, cfg, _) = pipeline(scenario)
    val lc = lake(cfg)
    SilverBuild.run(spark, lc)
    GoldLoad.run(spark, lc, Today)
    (gen, lc)
  }

  test("AC-6.6/6.7 gold equals the independent oracle and is consistent with package_status", Integration,
    Req("FR-ANL-01"), Req("FR-GEN-03"), Req("DQ-08")) {
    val (gen, _) = gold("bench/scenarios/it-gold.yaml")
    // Guard: the data must be able to tell percentiles apart (groups of several values), or this test proves little.
    assert(gen.lines("expected_delivery_daily.jsonl").exists(_.get("delivered_count").intValue >= 5))
    assert(gen.lines("expected_facility_daily.jsonl").exists(e => !e.get("dwell_p50_min").isNull &&
      e.get("dwell_p50_min").doubleValue != e.get("dwell_p95_min").doubleValue))

    val facts    = rowsOf("SELECT * FROM analytics.delivery_facts").map(r => r("package_id").toString -> r).toMap
    val expected = gen.lines("expected_delivery_facts.jsonl").filter(e => inWindow(e.get("delivery_date").textValue))
    assert(expected.nonEmpty && facts.keySet == expected.map(_.get("package_id").textValue).toSet)
    expected.foreach { e =>
      val f = facts(e.get("package_id").textValue)
      assert(f("delivery_date").toString == e.get("delivery_date").textValue)
      assert(instant(f("delivered_at")) == Instant.parse(e.get("delivered_at").textValue))
      assert(f("on_time") == e.get("on_time").booleanValue && near(dbl(f("delay_minutes")), e.get("delay_minutes").doubleValue))
      assert(f("service_level") == e.get("service_level").textValue && f("promised_delivery_date").toString == e.get("promised_delivery_date").textValue)
    }

    val visits = rowsOf("SELECT * FROM analytics.facility_visits").map(r => r("arrival_event_id").toString -> r).toMap
    val expVisits = gen.lines("expected_facility_visits.jsonl")
    assert(visits.size == expVisits.size)
    expVisits.foreach { e =>
      val v = visits(e.get("arrival_event_id").textValue)
      assert(v("package_id") == e.get("package_id").textValue && v("facility_id") == e.get("facility_id").textValue)
      assert(instant(v("arrived_at")) == Instant.parse(e.get("arrived_at").textValue))
      if (e.get("dwell_minutes").isNull) assert(v("dwell_minutes") == null && v("departed_at") == null)
      else assert(near(dbl(v("dwell_minutes")), e.get("dwell_minutes").doubleValue) && v("departure_date").toString == e.get("departure_date").textValue)
    }

    def compareDaily(table: String, keys: Seq[String], file: String, numeric: Seq[String]): Unit = {
      val actual = rowsOf(s"SELECT * FROM analytics.$table").map(r => keys.map(k => r(k).toString) -> r).toMap
      val exp    = gen.lines(file).filter(e => inWindow(e.get(keys.head).textValue))
      assert(actual.keySet == exp.map(e => keys.map(k => e.get(k).asText)).toSet, s"$table keys")
      exp.foreach { e =>
        val a = actual(keys.map(k => e.get(k).asText))
        numeric.foreach { n =>
          if (e.get(n).isNull) assert(a(n) == null, s"$table.$n")
          else assert(near(dbl(a(n)), e.get(n).doubleValue), s"$table.$n: ${a(n)} vs ${e.get(n)}")
        }
      }
    }
    compareDaily("daily_delivery_performance", Seq("delivery_date", "service_level", "destination_postal_prefix"),
      "expected_delivery_daily.jsonl", Seq("delivered_count", "on_time_count", "on_time_pct", "delay_p50_min", "delay_p95_min"))
    compareDaily("daily_facility_performance", Seq("activity_date", "facility_id"), "expected_facility_daily.jsonl",
      Seq("arrivals", "departures", "exception_count", "dwell_p50_min", "dwell_p95_min"))
    compareDaily("daily_network_activity", Seq("activity_date"), "expected_network_activity.jsonl",
      Seq("active_packages", "packages_with_exception", "exception_rate"))

    // AC-6.7 (DQ-08): gold agrees with the live status table.
    val delivered = scalar("SELECT count(*) FROM tracking.package_status WHERE current_status = 'DELIVERED' AND created_event_time IS NOT NULL")
    assert(scalar("SELECT sum(delivered_count) FROM analytics.daily_delivery_performance") == facts.size)
    assert(facts.size == delivered)
  }

  private def goldSnapshot(): Seq[Seq[String]] =
    Seq("delivery_facts ORDER BY package_id", "facility_visits ORDER BY arrival_event_id",
      "daily_delivery_performance ORDER BY 1, 2, 3", "daily_facility_performance ORDER BY 1, 2", "daily_network_activity ORDER BY 1")
      .map(q => rowsOf(s"SELECT * FROM analytics.$q").map(r => (r - "computed_at" - "run_id").toSeq.sortBy(_._1).mkString(",")))

  test("AC-6.8 the gold load is idempotent and atomic, and every run is logged", Integration, Req("FR-ANL-01"), Req("NFR-01")) {
    val (_, lc) = gold("bench/scenarios/smoke.yaml")
    val first   = goldSnapshot()
    GoldLoad.run(spark, lc, Today)
    assert(goldSnapshot() == first, "re-running the window gives identical rows")

    intercept[IllegalStateException](GoldLoad.run(spark, lc, Today, hooks = GoldHooks(inTransaction = () => throw new IllegalStateException("mid-load"))))
    assert(goldSnapshot() == first, "a failure inside the transaction leaves the previous rows intact")
    intercept[IllegalStateException](GoldLoad.run(spark, lc, Today, hooks = GoldHooks(afterStaging = () => throw new IllegalStateException("staged"))))
    assert(goldSnapshot() == first)
    assert(rowsOf("SELECT status, count(*) AS n FROM ops.gold_runs GROUP BY status").map(r => r("status") -> r("n").toString).toMap ==
      Map("succeeded" -> "2", "failed" -> "2"))
    assert(scalar("SELECT (SELECT count(*) FROM ops.gold_stage_delivery_facts) + (SELECT count(*) FROM ops.gold_stage_facility_visits)") == 0)
  }

  test("AC-6.9 late deliveries inside the window change gold; facts outside it are left alone", Integration,
    Req("FR-ANL-01"), Req("FR-LATE-01")) {
    val (gen, cfg, t) = pipeline("bench/scenarios/clean.yaml")
    val lc = lake(cfg)
    SilverBuild.run(spark, lc)
    GoldLoad.run(spark, lc, Today)
    // An old fact (outside the 7-day window) that this run must not touch.
    scala.util.Using.resource(admin())(_.createStatement().execute(
      s"INSERT INTO analytics.delivery_facts VALUES ('PKG-OUTSIDEWIND1', '${Today.minusDays(20)}', now() - interval '20 days', " +
        s"'${Today.minusDays(21)}', 'STANDARD', 'H2X', 'America/Toronto', 5, false, now(), 'planted')"))

    val inTransit = gen.manifest.collectFirst { case (p, m) if m.get("expected_final_status").textValue == "OUT_FOR_DELIVERY" => p }
      .getOrElse(fail("scenario has no package out for delivery"))
    val at = Instant.now().minus(Duration.ofMinutes(5)).truncatedTo(ChronoUnit.MILLIS)
    produce(t.events, Seq(lateEvent(inTransit, "DELIVERED", at, "0192a6b4-0000-7000-8000-0000000006a9",
      ""","vehicle_id":"VH-000123"""", system = "VEHICLE_APP")))
    runBothToEnd(cfg)
    SilverBuild.run(spark, lc)
    GoldLoad.run(spark, lc, Today)
    assert(rowsOf(s"SELECT * FROM analytics.delivery_facts WHERE package_id = '$inTransit'").size == 1)
    assert(rowsOf("SELECT run_id FROM analytics.delivery_facts WHERE package_id = 'PKG-OUTSIDEWIND1'").map(_("run_id")) == Seq("planted"))
  }
}
