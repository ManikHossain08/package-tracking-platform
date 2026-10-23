package ptp.streaming.it

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Instant

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.GenericContainer

import ptp.lake.{BatchMetrics, CheckResult, DqReport}
import ptp.streaming.ProcessorMetrics
import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** Step 8: processor metrics (TS-M-01, AC-8.1–8.3) and batch-job pushes (TS-M-02, AC-8.4). */
class MetricsIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private val http = HttpClient.newHttpClient()
  private def get(url: String): String =
    http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString()).body()

  final case class Sample(name: String, labels: Map[String, String], value: Double)
  private val Line  = """([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? ([-+0-9.eE]+|NaN|\+Inf)""".r
  private val Label = """([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"""".r
  private def parse(text: String): Seq[Sample] = text.linesIterator.filterNot(_.startsWith("#")).collect {
    case Line(n, labels, v) => Sample(n, Option(labels).map(l => Label.findAllMatchIn(l).map(m => m.group(1) -> m.group(2)).toMap).getOrElse(Map.empty), v.toDouble)
  }.toSeq
  private def value(s: Seq[Sample], name: String, labels: (String, String)*): Double =
    s.filter(x => x.name == name && labels.forall { case (k, v) => x.labels.get(k).contains(v) }).map(_.value).sum

  test("AC-8.1/8.2/8.3 the /metrics endpoint is exact, bounded and current", Integration, Req("FR-OBS-01"), Req("KPI-P2")) {
    val server = ProcessorMetrics.serve(0, "127.0.0.1")
    spark.streams.addListener(ProcessorMetrics.listener)
    try {
      val url    = s"http://127.0.0.1:${server.getPort}/metrics"
      val before = parse(get(url))
      val t   = topics()
      val gen = generate("bench/scenarios/smoke.yaml", t.events)
      runBothToEnd(config(t, maxOffsetsPerTrigger = 150))
      val text  = get(url)
      val after = parse(text)
      def delta(name: String, labels: (String, String)*) = value(after, name, labels: _*) - value(before, name, labels: _*)
      val s = gen.summary

      // AC-8.2: exact counters (and equal to ops.sink_batches).
      assert(delta("ptp_input_rows_total", "query" -> "status-processor") == s.get("records_total").doubleValue)
      assert(delta("ptp_events_valid_total") == s.get("valid_records").doubleValue)
      assert(delta("ptp_events_applied_total") == s.get("unique_valid_events").doubleValue)
      assert(delta("ptp_events_duplicate_total", "stage" -> "sink") == s.get("duplicate_copies").doubleValue)
      val byCode = s.get("malformed_by_error_code")
      byCode.fieldNames().forEachRemaining(code => assert(delta("ptp_events_rejected_total", "error_code" -> code) == byCode.get(code).doubleValue, code))
      assert(delta("ptp_events_rejected_total") == s.get("malformed_total").doubleValue)
      assert(delta("ptp_events_applied_total") == scalar("SELECT sum(applied_rows) FROM ops.sink_batches").toDouble)

      // AC-8.3: progress metrics.
      assert(delta("ptp_batch_duration_seconds_count", "query" -> "status-processor") == scalar("SELECT count(*) FROM ops.sink_batches").toDouble)
      Seq("status-processor", "bronze-writer").foreach { q =>
        assert(value(after, "ptp_offsets_behind_latest", "query" -> q) == 0.0, s"$q caught up")
        assert(Instant.now().getEpochSecond - value(after, "ptp_last_progress_timestamp_seconds", "query" -> q) < 120, s"$q progress is recent")
      }

      // AC-8.1: families documented; labels bounded (never package or event ids).
      Seq("ptp_input_rows", "ptp_events_rejected", "ptp_events_applied", "ptp_batch_duration_seconds", "ptp_offsets_behind_latest")
        .foreach(f => assert(text.contains(s"# HELP $f") && text.contains(s"# TYPE $f"), f))
      val labelNames = after.filter(_.name.startsWith("ptp_")).flatMap(_.labels.keySet).toSet
      assert(labelNames.subsetOf(Set("query", "error_code", "stage", "sink", "le")), labelNames)
    } finally {
      spark.streams.removeListener(ProcessorMetrics.listener)
      server.close()
    }
  }

  test("AC-8.3 heartbeat: an idle query is 0 s active; a hung micro-batch shows a growing active trigger", Integration,
    Req("FR-OBS-01"), Req("FR-OBS-03")) {
    val t        = topics()
    val release  = new java.util.concurrent.CountDownLatch(1)
    val entered  = new java.util.concurrent.CountDownLatch(1)
    val hooks    = ptp.streaming.ProcessorHooks(afterSink = _ => { entered.countDown(); release.await() })
    val q        = new ptp.streaming.StatusProcessor(spark, config(t), hooks = hooks)
      .start(org.apache.spark.sql.streaming.Trigger.ProcessingTime("1 second"))
    val beat     = ProcessorMetrics.startHeartbeat(spark, java.time.Duration.ofMillis(200))
    def active() = ProcessorMetrics.triggerActive.labelValues("status-processor").get()
    def eventually(timeoutMs: Long)(cond: => Boolean): Boolean = {
      val deadline = System.currentTimeMillis() + timeoutMs
      while (!cond && System.currentTimeMillis() < deadline) Thread.sleep(50) // poll interval of a bounded wait
      cond
    }
    try {
      // Idle (empty topic): sampled as not active, and the heartbeat is fresh.
      assert(eventually(10000)(ProcessorMetrics.heartbeat.get() > Instant.now().getEpochSecond - 5))
      assert(eventually(10000)(active() == 0.0))
      // Hung batch: the hook blocks inside the trigger; the active time grows past 2 s.
      produce(t.events, Seq(eventJson("PKG-HEARTBEAT001", "PICKED_UP", Instant.now(), java.util.UUID.randomUUID().toString,
        ""","vehicle_id":"VH-000123"""", system = "VEHICLE_APP")))
      assert(entered.await(60, java.util.concurrent.TimeUnit.SECONDS), "batch never reached the hook")
      assert(eventually(15000)(active() > 2.0), s"active trigger seconds = ${active()}")
      release.countDown()
      assert(eventually(15000)(active() < 2.0), "returns to ~0 once the batch completes")
    } finally {
      release.countDown(); beat.close(); q.stop()
    }
  }

  test("AC-8.4 batch jobs push outcome, duration, last success and DQ check status; push failures never fail a job",
    Integration, Req("FR-OBS-01")) {
    val gw = new MetricsIT.Pushgateway(composeImage("prom/pushgateway"))
    gw.withExposedPorts(9091)
    gw.start()
    try {
      val address = s"${gw.getHost}:${gw.getMappedPort(9091)}"
      val metrics = new BatchMetrics(Some(address))
      metrics.timed("gold")(())
      intercept[IllegalStateException](metrics.timed("silver")(throw new IllegalStateException("boom")))
      metrics.pushDq(DqReport("run", java.time.LocalDate.now(), java.time.LocalDate.now(), Seq(
        CheckResult("DQ-07", "critical", "fail", Some(3), "", Nil, ""),
        CheckResult("DQ-09", "warn", "warn", Some(1), "", Nil, ""),
        CheckResult("DQ-04", "warn", "pass", Some(2), "", Nil, ""))), 1.5)
      val s = parse(get(s"http://$address/metrics"))
      assert(value(s, "ptp_batch_success", "task" -> "gold") == 1 && value(s, "ptp_batch_success", "task" -> "silver") == 0)
      assert(Instant.now().getEpochSecond - value(s, "ptp_batch_last_success_timestamp_seconds", "task" -> "gold") < 60)
      assert(!s.exists(x => x.name == "ptp_batch_last_success_timestamp_seconds" && x.labels.get("task").contains("silver")),
        "a failed run must not record a success time")
      assert(value(s, "ptp_dq_check_failed", "check_id" -> "DQ-07", "severity" -> "critical") == 1)
      assert(value(s, "ptp_dq_check_failed", "check_id" -> "DQ-09") == 0, "a warning is not a failure")
      assert(value(s, "ptp_status_anomalies", "code" -> "ORPHAN_EVENT") == 2)

      // Unreachable gateway: logged, not thrown.
      new BatchMetrics(Some("127.0.0.1:1")).timed("gold")(())
    } finally gw.stop()
  }
}

object MetricsIT {
  /** Named subclass: Scala can't infer Testcontainers' self-referencing `GenericContainer[SELF]`. */
  final class Pushgateway(image: org.testcontainers.utility.DockerImageName) extends GenericContainer[Pushgateway](image)
}
