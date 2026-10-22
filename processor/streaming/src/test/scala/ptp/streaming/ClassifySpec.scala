package ptp.streaming

import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Duration, Instant}

import org.scalatest.funsuite.AnyFunSuite

import ptp.core.dlq.ProcessorInfo
import ptp.core.validation.{ProcessingMode, ValidationConfig}
import ptp.core.validation.ValidationFixtures.samples
import ptp.testing.Req

/** SC-0004: time bounds use the record's ingest timestamp, so the decision never depends on when we process it. */
class ClassifySpec extends AnyFunSuite {

  private val created  = new String(Files.readAllBytes(samples.resolve("valid/created.json")), "UTF-8")
  private val ingested = Instant.parse("2026-10-19T08:00:05Z") // the sample's event_time + 5 s
  private val proc     = ProcessorInfo("status-processor", "test", "run", 1L)

  private def classify(eventTime: Instant, ingest: Instant, rejectedAt: Instant, mode: ProcessingMode = ProcessingMode.Live) = {
    val value = created.replace("2026-10-19T08:00:00.000Z", eventTime.toString).getBytes("UTF-8")
    val rec   = KafkaRecord("PKG-A1B2C3D4E5F6".getBytes, value, "package-events.v1", 0, 42L, Timestamp.from(ingest))
    Classify.partition(ValidationConfig(), mode, rejectedAt, proc)(Iterator(rec)).next()
  }

  test("a record valid at ingest stays valid when processed or replayed 31 days later", Req("FR-VAL-02"), Req("FR-REC-02")) {
    val later = ingested.plus(Duration.ofDays(31))
    val c     = classify(Instant.parse("2026-10-19T08:00:00Z"), ingested, rejectedAt = later)
    assert(c.valid, c.errorCode)
  }

  test("bounds are relative to ingest time, not to processing time", Req("FR-VAL-02")) {
    val future = classify(ingested.plus(Duration.ofMinutes(6)), ingested, rejectedAt = ingested.plus(Duration.ofHours(2)))
    assert(!future.valid && future.errorCode.contains("EVENT_TIME_IN_FUTURE"))
    val old = classify(ingested.minus(Duration.ofDays(31)), ingested, rejectedAt = ingested)
    assert(!old.valid && old.errorCode.contains("EVENT_TIME_TOO_OLD"))
    assert(classify(ingested.minus(Duration.ofDays(31)), ingested, ingested, ProcessingMode.Replay).valid, "backfill mode")
  }

  test("DLQ records are stamped with the processing time and carry the ingest timestamp", Req("FR-DLQ-01")) {
    val at = ingested.plus(Duration.ofHours(1))
    val c  = classify(ingested.plus(Duration.ofDays(1)), ingested, rejectedAt = at)
    assert(c.dlqValue.exists(v => v.contains(s""""rejected_at":"$at"""") && v.contains(s""""kafka_timestamp":"$ingested"""")))
  }
}
