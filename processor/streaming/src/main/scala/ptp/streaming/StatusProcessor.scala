package ptp.streaming

import java.time.Instant
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import ptp.core.dlq.ProcessorInfo

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
    val records   = batch.select("key", "value", "topic", "partition", "offset", "timestamp").as[KafkaRecord]
    var stage     = "classify"
    var counts    = ApplyCounts(0, 0, 0, 0, "{}")
    try {
      counts = StatusApply.run(spark, records, config.validation, config.mode, processor, config.jdbc, startedAt,
        dlq = Some(DlqTarget(config.kafkaBootstrapServers, config.dlqTopic)), write = true, onStage = s => stage = s)
      if (counts.input == 0) return

      stage = "record-batch"
      PostgresStatusSink.recordBatch(config.jdbc, SinkBatch(config.queryName, batchId, runId, config.processorVersion,
        startedAt, clock(), counts.input, counts.valid, counts.rejected, counts.applied, counts.duplicates, counts.offsetRanges))
      log.info("batch committed to sinks", "run_id" -> runId, "query_name" -> config.queryName, "batch_id" -> batchId,
        "input_rows" -> counts.input, "valid_rows" -> counts.valid, "rejected_rows" -> counts.rejected,
        "applied_rows" -> counts.applied, "duplicate_rows" -> counts.duplicates, "offset_range" -> counts.offsetRanges)

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
        log.error("micro-batch failed", "run_id" -> runId, "query_name" -> config.queryName, "batch_id" -> batchId,
          "error_code" -> code, "stage" -> stage, "offset_range" -> counts.offsetRanges, "topic" -> config.eventsTopic,
          "error" -> s"${e.getClass.getName}: ${Option(e.getMessage).getOrElse("")}".take(500), "action" -> action)
        throw e
    }
  }
}
