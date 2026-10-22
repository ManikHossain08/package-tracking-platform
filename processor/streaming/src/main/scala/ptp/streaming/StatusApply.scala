package ptp.streaming

import java.time.Instant

import org.apache.spark.sql.functions.{col, count, max, min, sum, when}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import ptp.core.dlq.ProcessorInfo
import ptp.core.status.ApplyEvent
import ptp.core.validation.{ProcessingMode, ValidationConfig}

/** Counts for one apply run (a micro-batch or a replay). */
final case class ApplyCounts(input: Long, valid: Long, applied: Long, duplicates: Long, offsetRanges: String) {
  def rejected: Long = input - valid
}

final case class DlqTarget(bootstrapServers: String, topic: String)

/** Classify → (optional) DLQ → (optional) idempotent Postgres apply.
  *
  * The single code path behind the streaming status processor and the bronze replay (FR-REC-02:
  * "replayed through the same validation and apply code").
  */
object StatusApply {

  def run(
      spark: SparkSession,
      records: Dataset[KafkaRecord],
      validation: ValidationConfig,
      mode: ProcessingMode,
      processor: ProcessorInfo,
      jdbc: JdbcTarget,
      now: Instant,
      dlq: Option[DlqTarget],
      write: Boolean,
      onStage: String => Unit = _ => ()
  ): ApplyCounts = {
    import spark.implicits._
    onStage("classify")
    val classified = records
      .mapPartitions(Classify.partition(validation, mode, now, processor))
      .persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val perPartition = classified
        .groupBy("partition")
        .agg(min("offset"), max("offset"), count("*"), sum(when(col("valid"), 1).otherwise(0)))
        .as[(Int, Long, Long, Long, Long)]
        .collect()
      val input = perPartition.map(_._4).sum
      val valid = perPartition.map(_._5).sum
      val ranges = perPartition.sortBy(_._1).map { case (p, lo, hi, _, _) => s""""$p":[$lo,$hi]""" }.mkString("{", ",", "}")
      if (input == 0 || !write) return ApplyCounts(input, valid, 0, 0, ranges)

      onStage("dlq")
      dlq.filter(_ => input > valid).foreach { target =>
        classified
          .filter(!col("valid"))
          .select(col("dlqKey").as("key"), col("dlqValue").as("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", target.bootstrapServers)
          .option("kafka.acks", "all")
          .option("topic", target.topic)
          .save()
      }

      onStage("postgres")
      val events = classified.filter(col("valid")).map(_.event.get)
      PostgresStatusSink.ensurePartitions(jdbc, events.map(_.eventDate).distinct().collect().toSeq)
      val applied    = spark.sparkContext.longAccumulator(s"applied-${processor.runId}-${processor.batchId}")
      val duplicates = spark.sparkContext.longAccumulator(s"duplicates-${processor.runId}-${processor.batchId}")
      val (batchId, queryName) = (processor.batchId, processor.name)
      events.repartition(col("packageId")).foreachPartition { (it: Iterator[ApplyEvent]) =>
        val r = PostgresStatusSink.writePartition(jdbc, it, batchId, queryName, now)
        applied.add(r.applied)
        duplicates.add(r.duplicates)
      }
      ApplyCounts(input, valid, applied.sum, duplicates.sum, ranges)
    } finally classified.unpersist()
  }
}
