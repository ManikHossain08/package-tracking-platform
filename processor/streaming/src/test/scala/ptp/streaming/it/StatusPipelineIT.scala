package ptp.streaming.it

import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration, Instant}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import ptp.streaming.it.ItInfra._
import ptp.streaming.{ProcessorHooks, StatusProcessor, StructuredLog}
import ptp.testing.{Integration, Req}

/** First vertical slice, end to end: generator → Kafka → status-processor → DLQ + Postgres (spec 07 AC-1.x). */
class StatusPipelineIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private def processor(t: Topics, maxOffsets: Long = 50000, hooks: ProcessorHooks = ProcessorHooks(),
                        log: StructuredLog = new StructuredLog("status-processor")) = {
    val cfg = config(t, maxOffsets) // one config = one checkpoint directory, reused on restart
    (new StatusProcessor(spark, cfg, hooks = hooks, log = log), cfg)
  }

  /** Every package's status, status event and event count equal the generator's independent oracle. */
  private def assertMatchesManifest(gen: Generated): Unit = {
    val rows = statusRows()
    assert(rows.keySet == gen.manifest.keySet, "packages differ")
    val mismatches = gen.manifest.toSeq.flatMap { case (pkg, m) =>
      val r = rows(pkg)
      val ok = r.status == m.get("expected_final_status").textValue &&
        r.eventId == m.get("expected_status_event_id").textValue &&
        r.eventTime == Instant.parse(m.get("expected_status_event_time").textValue) &&
        r.eventCount == m.get("expected_event_count").intValue
      if (ok) None else Some(s"$pkg: got $r, expected ${m.toString.take(300)}")
    }
    assert(mismatches.isEmpty, mismatches.take(5).mkString("\n"))
  }

  private def sumOf(column: String) = scalar(s"SELECT COALESCE(SUM($column), 0) FROM ops.sink_batches")

  test("AC-1.1 clean stream: every package status equals the generator manifest", Integration,
    Req("FR-ING-01"), Req("FR-STR-01"), Req("FR-ORD-01"), Req("FR-GEN-03")) {
    val t   = topics()
    val gen = generate("bench/scenarios/clean.yaml", t.events)
    runToEnd(processor(t)._1)
    assertMatchesManifest(gen)
    assert(readAll(t.dlq).isEmpty)
    assert(scalar("SELECT count(*) FROM tracking.processed_events") == gen.summary.get("unique_valid_events").longValue)
  }

  test("AC-1.2 duplicates within and across micro-batches are applied once", Integration, Req("FR-DED-01")) {
    val t   = topics()
    val gen = generate("bench/scenarios/it-duplicates.yaml", t.events)
    runToEnd(processor(t, maxOffsets = 100)._1) // small batches, so copies land in later batches too
    assert(scalar("SELECT count(*) FROM ops.sink_batches") > 3)
    assertMatchesManifest(gen)
    assert(scalar("SELECT count(*) FROM tracking.processed_events") == gen.summary.get("unique_valid_events").longValue)
    assert(sumOf("duplicate_rows") == gen.summary.get("duplicate_copies").longValue)
    assert(sumOf("applied_rows") == gen.summary.get("unique_valid_events").longValue)
  }

  private val T2 = Instant.now().minus(Duration.ofHours(2)).truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
  private val T1 = T2.minus(Duration.ofHours(1))

  private def event(id: String, pkg: String, tpe: String, at: Instant): (String, Array[Byte]) =
    pkg -> s"""{"event_id":"$id","schema_version":"1.0","package_id":"$pkg","event_type":"$tpe","event_time":"${iso(at)}",
              |"emitted_at":"${iso(Instant.now())}","source":{"system":"VEHICLE_APP","device_id":"vh-app-000123","sequence":1},
              |"vehicle_id":"VH-000123"}""".stripMargin.replace("\n", "").getBytes(UTF_8)

  test("AC-1.3 an older event in a later batch never overwrites a newer state (both arrival orders)", Integration,
    Req("FR-ORD-01")) {
    val t  = topics()
    val (p, cfg) = processor(t)
    val a  = "PKG-ORDERAAAAAAA"; val b = "PKG-ORDERBBBBBBB"
    val delivered  = event("0192a6b4-0000-7000-8000-00000000000a", a, "DELIVERED", T2)
    val ofdLate    = event("0192a6b4-0000-7000-8000-00000000000b", a, "OUT_FOR_DELIVERY", T1)
    val ofdFirst   = event("0192a6b4-0000-7000-8000-00000000000c", b, "OUT_FOR_DELIVERY", T2)
    val delivLater = event("0192a6b4-0000-7000-8000-00000000000d", b, "DELIVERED", T1)
    produce(t.events, Seq(delivered, ofdFirst))
    runToEnd(p)                                  // batch k
    produce(t.events, Seq(ofdLate, delivLater, delivered))
    runToEnd(new StatusProcessor(spark, cfg)) // batch k+1, same checkpoint
    val rows = statusRows()
    // A: DELIVERED@T2 then OUT_FOR_DELIVERY@T1 arrives later → stays DELIVERED.
    assert(rows(a).status == "DELIVERED" && rows(a).eventTime == T2 && rows(a).eventCount == 2)
    // B: OUT_FOR_DELIVERY@T2 first, then an earlier DELIVERED@T1 → DELIVERED (SC-0002: terminal is preferred).
    assert(rows(b).status == "DELIVERED" && rows(b).eventTime == T1 && rows(b).eventCount == 2)
  }

  private val dlqSchema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
    .getSchema(repoRoot.resolve("contracts/dlq-record.v1.schema.json").toUri)

  test("AC-1.4 invalid records go to the DLQ with the expected code and source offset, never to status", Integration,
    Req("FR-VAL-01"), Req("FR-VAL-02"), Req("FR-DLQ-01"), Req("FR-DLQ-02")) {
    val t        = topics()
    val samples  = repoRoot.resolve("contracts/samples/invalid")
    val expected = mapper.readTree(samples.resolve("expected.json").toFile).properties().asScala
      .collect { case e if !e.getKey.startsWith("_") => e.getKey -> e.getValue.textValue }.toMap
    val created  = new String(java.nio.file.Files.readAllBytes(repoRoot.resolve("contracts/samples/valid/created.json")), UTF_8)
    val runtime = Map(
      "future" -> ("EVENT_TIME_IN_FUTURE", Instant.now().plus(Duration.ofMinutes(5)).plusSeconds(60)),
      "too-old" -> ("EVENT_TIME_TOO_OLD", Instant.now().minus(Duration.ofDays(30)).minusSeconds(60))
    )
    val inputs = expected.keys.toSeq.sorted.map(f => f -> java.nio.file.Files.readAllBytes(samples.resolve(f))) ++
      runtime.toSeq.map { case (k, (_, at)) => k -> created.replace("2026-10-19T08:00:00.000Z", iso(at)).getBytes(UTF_8) }
    produce(t.events, inputs)
    runToEnd(processor(t)._1)

    val dlq = readAll(t.dlq)
    assert(dlq.size == inputs.size)
    val byKey = dlq.map(n => n.at("/source/key").textValue -> n).toMap
    (expected ++ runtime.map { case (k, (code, _)) => k -> code }).foreach { case (key, code) =>
      val rec = byKey(key)
      assert(rec.get("error_code").textValue == code, s"$key: ${rec.get("errors")}")
      assert(dlqSchema.validate(rec).isEmpty, s"$key DLQ record violates the contract")
      assert(rec.at("/source/topic").textValue == t.events && rec.at("/source/offset").isIntegralNumber)
    }
    assert(statusRows().isEmpty)
    assert(sumOf("rejected_rows") == inputs.size && sumOf("applied_rows") == 0)
  }

  test("AC-1.1..1.4 on a dirty stream: status, DLQ offsets and counts reconcile exactly with the generator", Integration,
    Req("FR-DED-01"), Req("FR-ORD-01"), Req("FR-LATE-01"), Req("FR-DLQ-01"), Req("FR-GEN-02")) {
    val t   = topics()
    val gen = generate("bench/scenarios/smoke.yaml", t.events)
    runToEnd(processor(t, maxOffsets = 150)._1)
    assertMatchesManifest(gen)

    // DLQ: exactly the malformed records, matched by Kafka coordinates (delivery.jsonl) to their expected codes.
    val offsets  = gen.lines("delivery.jsonl").map(n => n.get("index").intValue -> (n.get("partition").intValue, n.get("offset").longValue)).toMap
    val expected = gen.lines("malformed.jsonl").map(n => offsets(n.get("index").intValue) -> n.get("expected_error_code").textValue).toMap
    val actual   = readAll(t.dlq).map(n => (n.at("/source/partition").intValue, n.at("/source/offset").longValue) -> n.get("error_code").textValue).toMap
    assert(actual == expected)

    // KPI-P2: every input record is accounted for as applied, duplicate or rejected.
    val s = gen.summary
    assert(sumOf("input_rows") == s.get("records_total").longValue)
    assert(sumOf("rejected_rows") == s.get("malformed_total").longValue)
    assert(sumOf("applied_rows") == s.get("unique_valid_events").longValue)
    assert(sumOf("duplicate_rows") == s.get("duplicate_copies").longValue)
  }

  test("AC-1.5 / AC-1.8 crash after the sink commit, restart on the checkpoint: same state, no double counting", Integration,
    Req("FR-REC-01"), Req("NFR-02"), Req("FR-OBS-02")) {
    val t        = topics()
    val gen      = generate("bench/scenarios/it-restart.yaml", t.events)
    assert(gen.summary.get("records_total").longValue >= 5000)
    val captured = mutable.ArrayBuffer.empty[String]
    val log      = new StructuredLog("status-processor", (_, line) => captured.synchronized(captured += line))
    val crash    = ProcessorHooks(afterSink = id => if (id == 3) throw new IllegalStateException("simulated crash after sink"))
    val (first, cfg) = processor(t, maxOffsets = 500, hooks = crash, log = log)

    val failure = intercept[StreamingQueryException](runToEnd(first))
    assert(failure.getMessage.contains("simulated crash"))

    // AC-1.8: the failure is logged as one JSON line with the stable keys.
    val errorLine = captured.map(mapper.readTree).find(_.path("level").asText == "error").getOrElse(fail("no error log line"))
    Seq("ts", "component", "run_id", "query_name", "batch_id", "error_code", "offset_range", "message", "action")
      .foreach(k => assert(errorLine.has(k), s"missing key $k in $errorLine"))
    assert(errorLine.get("batch_id").longValue == 3)

    // Restart: a new processor (new run_id) on the same checkpoint; batch 3 is re-executed.
    runToEnd(new StatusProcessor(spark, cfg))
    assertMatchesManifest(gen)
    assert(scalar("SELECT count(*) FROM tracking.processed_events") == gen.summary.get("unique_valid_events").longValue)
    assert(scalar("SELECT attempts FROM ops.sink_batches WHERE batch_id = 3") == 2)
    assert(scalar("SELECT count(DISTINCT run_id) FROM ops.sink_batches") == 2)
    val s = gen.summary
    assert(sumOf("input_rows") == s.get("records_total").longValue)
    assert(sumOf("applied_rows") + sumOf("duplicate_rows") == s.get("valid_records").longValue)
  }
}
