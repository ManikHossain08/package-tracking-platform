package ptp.lake

import java.time.LocalDate

import scala.util.Using

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import ptp.streaming.{PostgresStatusSink, StructuredLog}

final case class RetentionConfig(bronzeDays: Int = 90, silverDays: Int = 400, processedEventsDays: Int = 30)

final case class RetentionResult(droppedProcessedPartitions: Seq[String], deletedBronze: Seq[String], deletedSilver: Seq[String])

/** NFR-08 retention (SC-0006 C), run by the maintenance DAG.
  *
  *  - processed_events: daily partitions older than the horizon are dropped through a SECURITY DEFINER function.
  *  - bronze: `ingest_date` directories older than the horizon are deleted. The file sink's `retention` option drops
  *    their entries from `_spark_metadata` at the next compaction; until then bronze readers (`BronzeRow.read`) skip the missing files.
  *  - silver: `event_date` directories older than the horizon are deleted (no metadata log).
  */
object Retention {

  def run(spark: SparkSession, lake: LakeConfig, today: LocalDate, cfg: RetentionConfig = RetentionConfig(),
          log: StructuredLog = new StructuredLog("maintenance")): RetentionResult = {
    val dropped = Using.resource(PostgresStatusSink.connect(lake.jdbc)) { c =>
      Using.resource(c.prepareStatement("SELECT * FROM tracking.drop_processed_events_partitions_before(?)")) { ps =>
        ps.setObject(1, today.minusDays(cfg.processedEventsDays.toLong))
        val rs = ps.executeQuery()
        Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toVector
      }
    }
    val fs = FileSystem.get(new java.net.URI(lake.lakeRoot), spark.sparkContext.hadoopConfiguration)
    def expire(root: String, column: String, days: Int): Seq[String] = {
      val dir = new Path(root)
      if (!fs.exists(dir)) Nil
      else fs.listStatus(dir).toSeq.map(_.getPath).filter(_.getName.startsWith(s"$column=")).flatMap { p =>
        scala.util.Try(LocalDate.parse(p.getName.stripPrefix(s"$column="))).toOption
          .filter(_.isBefore(today.minusDays(days.toLong)))
          .map { d => require(fs.delete(p, true), s"could not delete $p"); s"$column=$d" }
      }.sorted
    }
    val result = RetentionResult(dropped, expire(lake.bronzePath, "ingest_date", cfg.bronzeDays),
      expire(lake.silverPath, "event_date", cfg.silverDays))
    log.info("retention applied", "today" -> today.toString,
      "processed_events_partitions_dropped" -> result.droppedProcessedPartitions.mkString(","),
      "bronze_partitions_deleted" -> result.deletedBronze.mkString(","),
      "silver_partitions_deleted" -> result.deletedSilver.mkString(","))
    result
  }
}
