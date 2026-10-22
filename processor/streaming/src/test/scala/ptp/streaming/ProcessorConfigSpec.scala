package ptp.streaming

import org.scalatest.funsuite.AnyFunSuite
import ptp.core.validation.ProcessingMode
import ptp.testing.Req

class ProcessorConfigSpec extends AnyFunSuite {
  private val base = Map("PTP_STREAM_WRITER_PASSWORD" -> "secret-value")

  test("defaults follow spec 08 and the password is required", Req("NFR-06"), Req("NFR-11")) {
    val c = ProcessorConfig.fromEnv(base).fold(fail(_), identity)
    assert(c.eventsTopic == "package-events.v1" && c.dlqTopic == "package-events.dlq.v1")
    assert(c.maxOffsetsPerTrigger == 50000 && c.triggerIntervalSeconds == 5 && c.mode == ProcessingMode.Live)
    assert(c.validation.futureSkew.toMinutes == 5 && c.validation.liveMaxAge.toDays == 30)
    assert(ProcessorConfig.fromEnv(Map.empty).left.exists(_.contains("PTP_STREAM_WRITER_PASSWORD")))
  }

  test("invalid values are rejected with an actionable message", Req("NFR-11")) {
    assert(ProcessorConfig.fromEnv(base + ("MAX_OFFSETS_PER_TRIGGER" -> "lots")).left.exists(_.contains("MAX_OFFSETS_PER_TRIGGER")))
    assert(ProcessorConfig.fromEnv(base + ("PROCESSING_MODE" -> "turbo")).left.exists(_.contains("PROCESSING_MODE")))
    assert(ProcessorConfig.fromEnv(base + ("PROCESSING_MODE" -> "replay")).map(_.mode) == Right(ProcessingMode.Replay))
  }

  test("the password never appears in toString", Req("NFR-06")) {
    val c = ProcessorConfig.fromEnv(base).fold(fail(_), identity)
    assert(!c.toString.contains("secret-value"))
  }
}
