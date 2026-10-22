package ptp.streaming.it

import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration, Instant, LocalDate}

import org.apache.spark.sql.functions.{max, min}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import ptp.streaming.it.ItInfra._
import ptp.streaming.{ProcessorConfig, Replay, ReplayMode, ReplayRequest}
import ptp.testing.{Integration, Req}

/** Step 5: replay from bronze through the shared apply path (FR-REC-02, spec 07 AC-5.5 to AC-5.7). */
class ReplayIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private def ingestRange(cfg: ProcessorConfig): (LocalDate, LocalDate) = {
    val r = spark.read.parquet(cfg.bronzePath).agg(min("ingest_date"), max("ingest_date")).head()
    (r.getAs[java.sql.Date](0).toLocalDate, r.getAs[java.sql.Date](1).toLocalDate)
  }

  private def statusMatchesManifest(gen: Generated): Seq[String] = {
    val rows = statusRows()
    gen.manifest.toSeq.flatMap { case (pkg, m) =>
      rows.get(pkg) match {
        case Some(r) if r.status == m.get("expected_final_status").textValue &&
          r.eventId == m.get("expected_status_event_id").textValue && r.eventCount == m.get("expected_event_count").intValue => None
        case other => Some(s"$pkg: $other")
      }
    } ++ (rows.keySet -- gen.manifest.keySet).map(p => s"unexpected package $p")
  }

  private def replayRun(runId: String): Map[String, String] = scala.util.Using.resource(admin()) { c =>
    val rs = c.createStatement().executeQuery(s"SELECT * FROM ops.replay_runs WHERE run_id = '$runId'")
    rs.next()
    (1 to rs.getMetaData.getColumnCount).map(i => rs.getMetaData.getColumnName(i) -> String.valueOf(rs.getObject(i))).toMap
  }

  test("AC-5.5 replaying an already-applied range (twice) changes nothing", Integration, Req("FR-REC-02"), Req("NFR-01")) {
    val t   = topics()
    val gen = generate("bench/scenarios/smoke.yaml", t.events)
    val cfg = config(t, maxOffsetsPerTrigger = 150)
    runBothToEnd(cfg)
    val before   = snapshot()
    val (lo, hi) = ingestRange(cfg)
    val runs     = (1 to 2).map(_ => Replay.run(spark, cfg.lakeRoot, writer, ReplayRequest(lo, hi)))
    assert(snapshot() == before)
    runs.foreach { r =>
      assert(r.counts.input == gen.summary.get("records_total").longValue)
      assert(r.counts.applied == 0 && r.counts.duplicates == r.counts.valid)
      assert(replayRun(r.runId)("status") == "succeeded")
    }
  }

  test("AC-5.6 after Postgres data loss, replay rebuilds the manifest state, also when run 31 days later",
    Integration, Req("FR-REC-02"), Req("NFR-03")) {
    val t   = topics()
    val gen = generate("bench/scenarios/smoke.yaml", t.events)
    val cfg = config(t)
    runBothToEnd(cfg)
    val (lo, hi) = ingestRange(cfg)
    val s        = gen.summary

    Seq(Duration.ZERO, Duration.ofDays(31)).foreach { delay =>
      truncateTracking() // simulated data loss
      val r = Replay.run(spark, cfg.lakeRoot, writer, ReplayRequest(lo, hi), clock = () => Instant.now().plus(delay))
      assert(statusMatchesManifest(gen).isEmpty, s"replayed after $delay: ${statusMatchesManifest(gen).take(3)}")
      assert(r.counts.applied == s.get("unique_valid_events").longValue)
      assert(r.counts.rejected == s.get("malformed_total").longValue)
      assert(scalar("SELECT count(*) FROM tracking.processed_events WHERE first_batch_id = -1") == r.counts.applied)
    }
  }

  test("AC-5.7 dry run writes nothing; runs are recorded; backfill mode is opt-in", Integration,
    Req("FR-REC-02"), Req("FR-VAL-02"), Req("FR-OBS-02")) {
    val t = topics()
    // One valid-looking event that was already 40 days old when it reached Kafka.
    val created = new String(java.nio.file.Files.readAllBytes(repoRoot.resolve("contracts/samples/valid/created.json")), UTF_8)
    val old     = Instant.now().minus(Duration.ofDays(40)).truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    produce(t.events, Seq("PKG-A1B2C3D4E5F6" -> created.replace("2026-10-19T08:00:00.000Z", old.toString).getBytes(UTF_8)))
    val cfg = config(t)
    runBothToEnd(cfg)
    assert(statusRows().isEmpty && readAll(t.dlq).map(_.get("error_code").textValue) == Seq("EVENT_TIME_TOO_OLD"))
    val (lo, hi) = ingestRange(cfg)

    val dry = Replay.run(spark, cfg.lakeRoot, writer, ReplayRequest(lo, hi, ReplayMode.Backfill, dryRun = true))
    assert(statusRows().isEmpty && dry.counts.valid == 1 && dry.counts.applied == 0)
    assert(replayRun(dry.runId)("dry_run") == "true" && replayRun(dry.runId)("mode") == "backfill")

    val asIngested = Replay.run(spark, cfg.lakeRoot, writer, ReplayRequest(lo, hi))
    assert(statusRows().isEmpty && asIngested.counts.rejected == 1, "as-ingested must reproduce the live rejection")

    val backfill = Replay.run(spark, cfg.lakeRoot, writer, ReplayRequest(lo, hi, ReplayMode.Backfill))
    assert(backfill.counts.applied == 1 && statusRows().keySet == Set("PKG-A1B2C3D4E5F6"))
    assert(readAll(t.dlq).size == 1, "replay never re-publishes to the DLQ")
    assert(scalar("SELECT count(*) FROM ops.replay_runs WHERE status = 'succeeded'") == 3)
  }
}
