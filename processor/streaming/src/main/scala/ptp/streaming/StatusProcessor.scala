package ptp.streaming

import java.time.Instant
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.functions.{col, count, max, min, sum, when}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import ptp.core.dlq.ProcessorInfo
import ptp.core.status.ApplyEvent

/** Test seams. `afterSink` runs after Postgres and ops.sink_batches are written, before Spark commits the
  * batch's offsets: throwing there simulates a crash at the worst moment (TS-R-01).
  */
final case class ProcessorHooks(afterSink: Long => Unit = _ => ())

/** Query A of ADR-0006: Kafka → validate → DLQ (Kafka) + idempotent status apply (Postgres). */
final class StatusProcessor(
    spark: SparkSession,
    config: ProcessorConfig,
    clock: () => Instant = () => Instant.now(),
    hooks: ProcessorHooks = ProcessorHooks(),
    log: StructuredLog = new StructuredLog("status-processor")
) {
  import spark.implicits._

  val runId: String = UUID.randomUUID().toString

  def start(trigger: Trigger): StreamingQuery = {
    val source = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.eventsTopic)
      .option("startingOffsets", config.startingOffsets)
      .option("maxOffsetsPerTrigger", config.maxOffsetsPerTrigger.toString)
      .option("failOnDataLoss", "true") // offsets aged out of Kafka must stop the query (spec 06)
      .load()

    log.info("query starting", "run_id" -> runId, "query_name" -> config.queryName, "topic" -> config.eventsTopic,
      "checkpoint" -> config.checkpointLocation, "mode" -> config.mode.toString)

    val batchFn: (DataFrame, Long) => Unit = processBatch
    source.writeStream
      .queryName(config.queryName)
      .option("checkpointLocation", config.checkpointLocation)
      .trigger(trigger)
      .foreachBatch(batchFn)
      .start()
  }

  private[streaming] def processBatch(batch: DataFrame, batchId: Long): Unit = {
    val startedAt = clock()
    val processor = ProcessorInfo(config.queryName, config.processorVersion, runId, batchId)
    val (vcfg, mode, jdbc, queryName) = (config.validation, config.mode, config.jdbc, config.queryName)

    val classified = batch
      .select("key", "value", "topic", "partition", "offset", "timestamp")
      .as[KafkaRecord]
      .mapPartitions(Classify.partition(vcfg, mode, startedAt, processor))
      .persist(StorageLevel.MEMORY_AND_DISK)

    var stage = "classify"
    var offsetRanges = "{}"
    try {
      val perPartition = classified
        .groupBy("partition")
        .agg(min("offset"), max("offset"), count("*"), sum(when(col("valid"), 1).otherwise(0)))
        .as[(Int, Long, Long, Long, Long)]
        .collect()
      val input = perPartition.map(_._4).sum
      if (input == 0) return
      val validRows = perPartition.map(_._5).sum
      offsetRanges = perPartition.sortBy(_._1).map { case (p, lo, hi, _, _) => s""""$p":[$lo,$hi]""" }.mkString("{", ",", "}")

      stage = "dlq"
      if (input > validRows)
        classified
          .filter(!col("valid"))
          .select(col("dlqKey").as("key"), col("dlqValue").as("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
          .option("kafka.acks", "all")
          .option("topic", config.dlqTopic)
          .save()

      stage = "postgres"
      val valid = classified.filter(col("valid")).map(_.event.get)
      PostgresStatusSink.ensurePartitions(jdbc, valid.map(_.eventDate).distinct().collect().toSeq)
      val applied    = spark.sparkContext.longAccumulator(s"applied-$batchId")
      val duplicates = spark.sparkContext.longAccumulator(s"duplicates-$batchId")
      valid.repartition(col("packageId")).foreachPartition { (events: Iterator[ApplyEvent]) =>
        val r = PostgresStatusSink.writePartition(jdbc, events, batchId, queryName, startedAt)
        applied.add(r.applied)
        duplicates.add(r.duplicates)
      }

      stage = "record-batch"
      val summary = SinkBatch(queryName, batchId, runId, config.processorVersion, startedAt, clock(), input, validRows,
        input - validRows, applied.sum, duplicates.sum, offsetRanges)
      PostgresStatusSink.recordBatch(jdbc, summary)
      log.info("batch committed to sinks", "run_id" -> runId, "query_name" -> queryName, "batch_id" -> batchId,
        "input_rows" -> input, "valid_rows" -> validRows, "rejected_rows" -> (input - validRows),
        "applied_rows" -> applied.sum, "duplicate_rows" -> duplicates.sum, "offset_range" -> offsetRanges)

      stage = "after-sink"
      hooks.afterSink(batchId)
    } catch {
      case NonFatal(e) =>
        val (code, action) = stage match {
          case "dlq"          => ("SINK_DLQ_FAILED", "Check Kafka health and the DLQ topic; the batch is re-run on restart")
          case "postgres"     => ("SINK_POSTGRES_FAILED", "Check Postgres availability and grants; the batch is re-run on restart (idempotent)")
          case "record-batch" => ("SINK_BATCHES_FAILED", "Check ops.sink_batches; status rows are already committed and re-run is idempotent")
          case _              => ("BATCH_FAILED", "Inspect the error; offsets are not committed and the batch is re-run on restart")
        }
        log.error("micro-batch failed", "run_id" -> runId, "query_name" -> queryName, "batch_id" -> batchId,
          "error_code" -> code, "stage" -> stage, "offset_range" -> offsetRanges, "topic" -> config.eventsTopic,
          "error" -> s"${e.getClass.getName}: ${Option(e.getMessage).getOrElse("")}".take(500), "action" -> action)
        throw e
    } finally classified.unpersist()
  }
}
