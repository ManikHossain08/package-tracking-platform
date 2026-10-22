package ptp.streaming.it

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.UUID

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import ptp.lake.{DqConfig, DqJob, DqReport, GoldLoad, LakeConfig, SilverBuild}
import ptp.streaming.ProcessorConfig
import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** Step 7a: the data-quality job (spec 07 AC-7.1 to AC-7.5, SC-0006). */
class DqIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private val Today = LocalDate.now(ZoneOffset.UTC)
  /** Evaluate "tomorrow 01:00 UTC": today's ingest is closed (DQ-06) and today's packages are idle (DQ-07). */
  private val Later = Today.plusDays(1).atTime(1, 0).toInstant(ZoneOffset.UTC)
  private val Checks = Seq("DQ-02", "DQ-03", "DQ-04", "DQ-05", "DQ-06", "DQ-07", "DQ-08", "DQ-09", "DQ-10")

  private def lake(cfg: ProcessorConfig) =
    LakeConfig(cfg.lakeRoot, batchWriter, 128L * 1024 * 1024, 7, 30, kafka.getBootstrapServers, cfg.eventsTopic, cfg.dlqTopic)
  private def dq(lc: LakeConfig, now: Instant = Later): DqReport =
    DqJob.run(spark, lc, DqConfig(Today, now, lc.kafkaBootstrapServers, lc.eventsTopic, lc.dlqTopic))
  private def statuses(r: DqReport) = r.results.map(c => c.checkId -> c.status).toMap

  private def build(cfg: ProcessorConfig, lc: LakeConfig): Unit = {
    runBothToEnd(cfg)
    SilverBuild.run(spark, lc)
    GoldLoad.run(spark, lc, Today)
  }

  private def anomalies(): Set[(String, String)] =
    rowsOf("SELECT event_id, anomaly_code FROM tracking.status_anomalies").map(r => r("event_id").toString -> r("anomaly_code").toString).toSet

  test("AC-7.1/7.2/7.5 clean data passes, anomalies equal the oracle and are idempotent", Integration,
    Req("DQ-02"), Req("DQ-05"), Req("DQ-07"), Req("DQ-08"), Req("FR-DED-03")) {
    val t   = topics()
    val gen = generate("bench/scenarios/it-gold.yaml", t.events)
    val cfg = config(t)
    val lc  = lake(cfg)
    build(cfg, lc)

    val report = dq(lc)
    assert(report.results.map(_.checkId) == Checks, "AC-7.1: one result per check")
    val s = statuses(report)
    Seq("DQ-02", "DQ-03", "DQ-04", "DQ-06", "DQ-07", "DQ-08", "DQ-09").foreach(id => assert(s(id) == "pass", s"$id: ${report.result(id)}"))
    assert(Set("pass", "warn")(s("DQ-05")))
    assert(s("DQ-10") == "skipped" && report.result("DQ-10").detail.contains("history"))
    assert(report.result("DQ-07").detail.matches("compared=[1-9][0-9]*"), "DQ-07 must actually compare packages")
    assert(report.exitCode == 0)
    assert(scalar(s"SELECT count(*) FROM ops.dq_results WHERE run_id = '${report.runId}'") == Checks.size)

    // DQ-05 / AC-7.5: anomaly rows equal the generator's independent prediction, and re-running gives the same rows.
    val expected = gen.lines("expected_anomalies.jsonl").map(n => n.get("event_id").textValue -> n.get("anomaly_code").textValue).toSet
    assert(report.result("DQ-05").observed.contains(expected.size.toDouble))
    assert(anomalies() == expected)
    dq(lc)
    assert(anomalies() == expected)
    assert(scalar("SELECT count(DISTINCT run_id) FROM ops.dq_results") == 2)
  }

  test("AC-7.3/7.4 each check catches its planted fault; critical/high failures set the exit code", Integration,
    Req("DQ-02"), Req("DQ-03"), Req("DQ-04"), Req("DQ-05"), Req("DQ-06"), Req("DQ-07"), Req("DQ-08"), Req("DQ-09")) {
    val t   = topics()
    val gen = generate("bench/scenarios/it-gold.yaml", t.events)
    val cfg = config(t)
    val lc  = lake(cfg)
    build(cfg, lc)
    val baseline = statuses(dq(lc))

    /** The listed checks change as expected; every other check keeps its baseline status. */
    def expect(changes: Map[String, String], exit: Int, sampleOf: (String, String => Boolean)*): DqReport = {
      val r = dq(lc)
      val s = statuses(r)
      assert(s == baseline ++ changes, s"expected ${baseline ++ changes}, got $s")
      sampleOf.foreach { case (id, pred) => assert(r.result(id).sample.exists(pred), s"$id sample ${r.result(id).sample}") }
      assert(r.exitCode == exit)
      r
    }

    // DQ-02: a duplicated silver file. Recomputed counts also change, so DQ-07 fails too.
    val partition = Files.list(Paths.get(lc.silverPath)).iterator().asScala.filter(_.getFileName.toString == s"event_date=$Today").toSeq.head
    val file      = Files.list(partition).iterator().asScala.filter(_.toString.endsWith(".parquet")).toSeq.head
    val copy      = partition.resolve(s"part-dup-${UUID.randomUUID()}.parquet")
    Files.copy(file, copy, StandardCopyOption.REPLACE_EXISTING)
    val dupIds = spark.read.parquet(copy.toString).select("event_id").collect().map(_.getString(0)).toSet
    expect(Map("DQ-02" -> "fail", "DQ-07" -> "fail"), exit = 1, "DQ-02" -> dupIds)
    Files.delete(copy)

    // DQ-07: a tampered status row.
    val pkg = statusRows().collectFirst { case (p, r) if r.status == "DELIVERED" => p }.get
    Using.resource(admin())(_.createStatement().execute(s"UPDATE tracking.package_status SET current_status = 'EXCEPTION' WHERE package_id = '$pkg'"))
    expect(Map("DQ-07" -> "fail"), exit = 1, "DQ-07" -> (_ == pkg))
    Using.resource(admin())(_.createStatement().execute(s"UPDATE tracking.package_status SET current_status = 'DELIVERED' WHERE package_id = '$pkg'"))

    // DQ-08: a gold count off by one.
    Using.resource(admin())(_.createStatement().execute(
      "UPDATE analytics.daily_delivery_performance SET delivered_count = delivered_count + 1 WHERE ctid = (SELECT min(ctid) FROM analytics.daily_delivery_performance)"))
    expect(Map("DQ-08" -> "fail"), exit = 1)
    GoldLoad.run(spark, lc, Today) // restores the daily rows

    // DQ-09: an implausible dwell (warning only → exit code 0).
    Using.resource(admin())(_.createStatement().execute(
      s"INSERT INTO analytics.facility_visits VALUES ('visit-implausible', 'PKG-PLANTEDVISIT', 'DC-YUL01', now(), '$Today', now(), '$Today', 43200, now(), 'planted')"))
    expect(Map("DQ-09" -> "warn"), exit = 0, "DQ-09" -> (_ == "visit:visit-implausible"))
    Using.resource(admin())(_.createStatement().execute("DELETE FROM analytics.facility_visits WHERE arrival_event_id = 'visit-implausible'"))

    // DQ-04 + DQ-05: an orphan package (no CREATED, first event 30 h before evaluation) and post-terminal scans.
    val orphan    = "PKG-ORPHANDQTEST"
    val delivered = statusRows().collect { case (p, r) if r.status == "DELIVERED" => p -> r.eventTime }.toSeq.sortBy(_._1).take(60)
    val now       = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    produce(t.events, Seq(eventJson(orphan, "ARRIVED_AT_FACILITY", Later.minus(Duration.ofHours(30)), UUID.randomUUID().toString,
      ""","facility_id":"DC-YUL01"""")) ++ delivered.map { case (p, at) =>
      eventJson(p, "ARRIVED_AT_FACILITY", Seq(at.plusSeconds(60), now).min, UUID.randomUUID().toString, ""","facility_id":"DC-YUL01"""")
    })
    build(cfg, lc)
    val withAnomalies = expect(Map("DQ-04" -> "warn", "DQ-05" -> "warn"), exit = 0, "DQ-04" -> (_ == orphan))
    assert(withAnomalies.result("DQ-05").observed.exists(_ >= delivered.size))

    // DQ-06: a DLQ record that has no invalid bronze row behind it (critical).
    produce(t.dlq, Seq("x" -> s"""{"dlq_schema_version":"1.0","error_code":"INVALID_JSON","errors":[{"code":"INVALID_JSON","field":null,"message":"planted"}],
      |"raw_value":"{","raw_value_encoding":"utf8","source":{"topic":"${t.events}","partition":0,"offset":999999,
      |"kafka_timestamp":"${Instant.now()}","key":null},"processor":{"name":"planted","version":"0","run_id":"x","batch_id":0},
      |"rejected_at":"${Instant.now()}"}""".stripMargin.replace("\n", "").getBytes(UTF_8)))
    expect(Map("DQ-04" -> "warn", "DQ-05" -> "warn", "DQ-06" -> "fail"), exit = 1, "DQ-06" -> (_ == "dlq-only 0:999999"))

    // DQ-03: records waiting in Kafka, and no commit for longer than 15 minutes (high → exit 1).
    produce(t.events, (1 to 5).map(i => eventJson(s"PKG-LAGGINGDQ$i%03d", "PICKED_UP", now, UUID.randomUUID().toString,
      ""","vehicle_id":"VH-000123"""", system = "VEHICLE_APP")))
    expect(Map("DQ-04" -> "warn", "DQ-05" -> "warn", "DQ-06" -> "fail", "DQ-03" -> "fail"), exit = 1)
    assert(gen.summary.get("records_total").intValue > 0)
  }
}
