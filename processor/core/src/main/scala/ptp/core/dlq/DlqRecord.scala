package ptp.core.dlq

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Base64

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import ptp.core.validation.Rejection

/** Where a rejected record came from (Kafka coordinates are the DLQ dedup key, spec 02 §3). */
final case class SourceCoordinates(topic: String, partition: Int, offset: Long, kafkaTimestamp: Instant, key: Option[String])

final case class ProcessorInfo(name: String, version: String, runId: String, batchId: Long)

/** Builds dlq-record v1 JSON (contracts/dlq-record.v1.schema.json). */
object DlqRecord {
  private val mapper = JsonMapper.builder().build()

  def toJson(
      rawValue: Array[Byte],
      rejection: Rejection,
      source: SourceCoordinates,
      processor: ProcessorInfo,
      rejectedAt: Instant
  ): String = {
    val root = mapper.createObjectNode()
    root.put("dlq_schema_version", "1.0")
    root.put("error_code", rejection.primary.name)
    val errors = root.putArray("errors")
    rejection.errors.foreach { e =>
      val n = errors.addObject()
      n.put("code", e.code.name)
      e.field match {
        case Some(f) => n.put("field", f)
        case None    => n.putNull("field")
      }
      n.put("message", e.message.take(1024))
    }
    if (rawValue == null) {
      root.putNull("raw_value")
      root.put("raw_value_encoding", "utf8")
    } else if (isUtf8(rawValue)) {
      root.put("raw_value", new String(rawValue, StandardCharsets.UTF_8))
      root.put("raw_value_encoding", "utf8")
    } else {
      root.put("raw_value", Base64.getEncoder.encodeToString(rawValue))
      root.put("raw_value_encoding", "base64")
    }
    val src: ObjectNode = root.putObject("source")
    src.put("topic", source.topic)
    src.put("partition", source.partition)
    src.put("offset", source.offset)
    src.put("kafka_timestamp", source.kafkaTimestamp.toString)
    source.key match {
      case Some(k) => src.put("key", k)
      case None    => src.putNull("key")
    }
    val proc = root.putObject("processor")
    proc.put("name", processor.name)
    proc.put("version", processor.version)
    proc.put("run_id", processor.runId)
    proc.put("batch_id", processor.batchId)
    root.put("rejected_at", rejectedAt.toString)
    mapper.writeValueAsString(root)
  }

  private def isUtf8(bytes: Array[Byte]): Boolean =
    try {
      StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes))
      true
    } catch { case _: java.nio.charset.CharacterCodingException => false }
}
