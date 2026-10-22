package ptp.core.bronze

import java.time.{Instant, OffsetDateTime}

import scala.util.Try

import com.fasterxml.jackson.databind.json.JsonMapper

/** Identifying fields pulled from a record that failed validation, for bronze lineage (spec 02 §5).
  * Never throws; anything missing or unparseable is None.
  */
final case class BestEffortFields(eventId: Option[String], packageId: Option[String], eventType: Option[String], eventTime: Option[Instant])

object BestEffortFields {
  private val mapper = JsonMapper.builder().build()
  val empty: BestEffortFields = BestEffortFields(None, None, None, None)

  def extract(value: Array[Byte]): BestEffortFields =
    Try(mapper.readTree(value)).toOption.filter(n => n != null && n.isObject).fold(empty) { n =>
      def text(name: String) = Option(n.get(name)).filter(_.isTextual).map(_.textValue)
      BestEffortFields(
        eventId = text("event_id").map(_.toLowerCase),
        packageId = text("package_id"),
        eventType = text("event_type"),
        eventTime = text("event_time").flatMap(t => Try(OffsetDateTime.parse(t).toInstant).toOption)
      )
    }
}
