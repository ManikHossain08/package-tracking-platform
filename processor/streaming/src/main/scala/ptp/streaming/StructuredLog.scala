package ptp.streaming

import java.time.Instant

import com.fasterxml.jackson.databind.json.JsonMapper
import org.slf4j.LoggerFactory

/** One JSON object per line with stable keys (FR-OBS-02, spec 06 §Error message standard).
  * `emit` is pluggable so tests can capture lines; by default they go to the `ptp.events` logger.
  */
final class StructuredLog(component: String, emit: (String, String) => Unit = StructuredLog.toLogger)
    extends Serializable {

  def info(message: String, fields: (String, Any)*): Unit  = write("info", message, fields)
  def error(message: String, fields: (String, Any)*): Unit = write("error", message, fields)

  private def write(level: String, message: String, fields: Seq[(String, Any)]): Unit = {
    val node = StructuredLog.mapper.createObjectNode()
    node.put("ts", Instant.now().toString)
    node.put("level", level)
    node.put("component", component)
    node.put("message", message)
    fields.foreach {
      case (k, null)          => node.putNull(k)
      case (k, v: Int)        => node.put(k, v)
      case (k, v: Long)       => node.put(k, v)
      case (k, v: Boolean)    => node.put(k, v)
      case (k, v: Map[_, _])  => node.set[com.fasterxml.jackson.databind.JsonNode](k, StructuredLog.mapper.valueToTree(v.map { case (a, b) => a.toString -> b.toString }.toMap[String, String]))
      case (k, v)             => node.put(k, v.toString)
    }
    emit(level, StructuredLog.mapper.writeValueAsString(node))
  }
}

object StructuredLog {
  private val mapper = JsonMapper.builder().build()
  private lazy val logger = LoggerFactory.getLogger("ptp.events")

  val toLogger: (String, String) => Unit = (level, line) =>
    if (level == "error") logger.error(line) else logger.info(line)
}
