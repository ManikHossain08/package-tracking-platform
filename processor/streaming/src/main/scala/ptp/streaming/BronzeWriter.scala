package ptp.streaming

import java.time.{Instant, LocalDate, ZoneOffset}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import ptp.core.bronze.BestEffortFields
import ptp.core.validation.{EventValidator, ProcessingMode, ValidationConfig}

/** One bronze row per consumed Kafka record (spec 02 §5). Key and value are kept as binary: lossless,
  * including payloads that are not UTF-8. Validity is judged at ingest time (SC-0004), so it agrees with the DLQ.
  */
final case class BronzeRow(
    kafka_topic: String,
    kafka_partition: Int,
    kafka_offset: Long,
    kafka_timestamp: Instant,
    kafka_key: Array[Byte],
    raw_value: Array[Byte],
    is_valid: Boolean,
    error_codes: Seq[String],
    event_id: Option[String],
    package_id: Option[String],
    event_type: Option[String],
    event_time: Option[Instant],
    processor_version: String,
    ingest_date: LocalDate
)

object BronzeRow {
  def fromRecords(validation: ValidationConfig, mode: ProcessingMode, version: String)(records: Iterator[KafkaRecord]): Iterator[BronzeRow] = {
    val validator = new EventValidator(validation)
    records.map { r =>
      val ingest = r.timestamp.toInstant
      val (valid, codes, fields) = validator.validate(r.value, ingest, mode) match {
        case Right(e) => (true, Seq.empty[String], BestEffortFields(Some(e.eventId), Some(e.packageId), Some(e.eventType.name), Some(e.eventTime)))
        case Left(rej) =>
          // Primary code first, then the others in precedence order (matches the DLQ's error_code).
          (false, rej.errors.map(_.code).distinct.sortBy(_.precedence).map(_.name), BestEffortFields.extract(r.value))
      }
      BronzeRow(r.topic, r.partition, r.offset, ingest, r.key, r.value, valid, codes, fields.eventId, fields.packageId,
        fields.eventType, fields.eventTime, version, LocalDate.ofInstant(ingest, ZoneOffset.UTC))
    }
  }
}

/** Query B of ADR-0006: Kafka → bronze Parquet through the Spark file sink (exactly-once file commits for
  * readers that honour `_spark_metadata`). Own checkpoint, independent of the status processor.
  */
final class BronzeWriter(spark: SparkSession, config: ProcessorConfig, log: StructuredLog = new StructuredLog("bronze-writer")) {
  import spark.implicits._

  val queryName = "bronze-writer"

  def start(trigger: Trigger): StreamingQuery = {
    val (validation, mode, version) = (config.validation, config.mode, config.processorVersion)
    log.info("query starting", "query_name" -> queryName, "topic" -> config.eventsTopic, "path" -> config.bronzePath,
      "checkpoint" -> s"${config.checkpointRoot}/$queryName")
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.eventsTopic)
      .option("startingOffsets", config.startingOffsets)
      .option("maxOffsetsPerTrigger", config.maxOffsetsPerTrigger.toString)
      .option("failOnDataLoss", "true")
      .load()
      .select("key", "value", "topic", "partition", "offset", "timestamp")
      .as[KafkaRecord]
      .mapPartitions(BronzeRow.fromRecords(validation, mode, version))
      .writeStream
      .queryName(queryName)
      .format("parquet")
      .option("path", config.bronzePath)
      .option("checkpointLocation", s"${config.checkpointRoot}/$queryName")
      .partitionBy("ingest_date")
      .trigger(trigger)
      .start()
  }
}
