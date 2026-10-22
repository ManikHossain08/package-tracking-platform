package ptp.streaming

import scala.collection.mutable

import com.fasterxml.jackson.databind.json.JsonMapper
import org.scalatest.funsuite.AnyFunSuite
import ptp.testing.Req

/** TS-U-08: structured log lines are single JSON objects with stable keys. */
class StructuredLogSpec extends AnyFunSuite {
  private val mapper = JsonMapper.builder().build()

  test("each line is one JSON object with ts, level, component, message and the given fields", Req("FR-OBS-02")) {
    val lines = mutable.ArrayBuffer.empty[(String, String)]
    val log   = new StructuredLog("status-processor", (lvl, line) => lines += lvl -> line)
    log.error("micro-batch failed", "run_id" -> "r1", "batch_id" -> 7L, "error_code" -> "SINK_POSTGRES_FAILED",
      "offset_range" -> """{"0":[1,5]}""", "action" -> "restart")
    assert(lines.size == 1)
    val (level, line) = lines.head
    assert(level == "error" && !line.contains("\n"))
    val n = mapper.readTree(line)
    Seq("ts", "level", "component", "message", "run_id", "batch_id", "error_code", "offset_range", "action").foreach(k => assert(n.has(k), k))
    assert(n.get("batch_id").isIntegralNumber && n.get("component").textValue == "status-processor")
  }
}
