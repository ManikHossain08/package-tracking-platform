package ptp.streaming

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant

import ptp.core.dlq.{DlqRecord, ProcessorInfo, SourceCoordinates}
import ptp.core.status.ApplyEvent
import ptp.core.validation.{EventValidator, ProcessingMode, ValidationConfig}

/** The Kafka source columns the processor reads. */
final case class KafkaRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, offset: Long, timestamp: Timestamp)

/** One consumed record after validation: either an event to apply or a DLQ record to publish. */
final case class Classified(
    topic: String,
    partition: Int,
    offset: Long,
    valid: Boolean,
    event: Option[ApplyEvent],
    dlqKey: Option[String],
    dlqValue: Option[String],
    errorCode: Option[String]
)

object Classify {

  /** Runs on executors. `now` is the batch's processing time, fixed on the driver, so every record in a
    * micro-batch (and its re-run) is judged against the same clock.
    */
  def partition(config: ValidationConfig, mode: ProcessingMode, now: Instant, processor: ProcessorInfo)(
      records: Iterator[KafkaRecord]
  ): Iterator[Classified] = {
    val validator = new EventValidator(config)
    records.map { r =>
      validator.validate(r.value, now, mode) match {
        case Right(event) =>
          Classified(r.topic, r.partition, r.offset, valid = true, Some(ApplyEvent.from(event)), None, None, None)
        case Left(rejection) =>
          val key    = Option(r.key).map(k => new String(k, StandardCharsets.UTF_8))
          val source = SourceCoordinates(r.topic, r.partition, r.offset, r.timestamp.toInstant, key)
          val json   = DlqRecord.toJson(r.value, rejection, source, processor, now)
          Classified(r.topic, r.partition, r.offset, valid = false, None, key, Some(json), Some(rejection.primary.name))
      }
    }
  }
}
