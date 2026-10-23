package ptp.streaming

import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import ptp.core.dlq.ProcessorInfo
import ptp.core.validation.{ProcessingMode, ValidationConfig}

/** `as-ingested` reproduces live decisions (SC-0004); `backfill` disables the lower age bound (FR-VAL-02). */
sealed abstract class ReplayMode(val name: String, val processing: ProcessingMode)
object ReplayMode {
  case object AsIngested extends ReplayMode("as-ingested", ProcessingMode.Live)
  case object Backfill   extends ReplayMode("backfill", ProcessingMode.Replay)
  def fromName(s: String): Option[ReplayMode] = Seq(AsIngested, Backfill).find(_.name == s)
}

final case class ReplayRequest(from: LocalDate, to: LocalDate, mode: ReplayMode = ReplayMode.AsIngested, dryRun: Boolean = false)

final case class ReplayResult(runId: String, counts: ApplyCounts, startedAt: Instant, finishedAt: Instant)

/** Replays an ingest-date range of bronze through `StatusApply` (FR-REC-02). Idempotent: already-applied events
  * are duplicates in `processed_events`. Invalid records are counted, not re-sent to the DLQ.
  */
object Replay {
  val QueryName = "replay"
  val BatchId   = -1L // marks processed_events rows first inserted by a replay

  def run(
      spark: SparkSession,
      lakeRoot: String,
      jdbc: JdbcTarget,
      request: ReplayRequest,
      validation: ValidationConfig = ValidationConfig(),
      clock: () => Instant = () => Instant.now(),
      log: StructuredLog = new StructuredLog("replay")
  ): ReplayResult = {
    import spark.implicits._
    require(!request.to.isBefore(request.from), s"--to ${request.to} is before --from ${request.from}")
    val path = s"$lakeRoot/bronze/package_events"
    require(Files.isDirectory(Paths.get(path)), s"no bronze data at $path (is the bronze writer running? LAKE_ROOT correct?)")

    val runId     = UUID.randomUUID().toString
    val startedAt = clock()
    PostgresStatusSink.startReplay(jdbc, runId, request, startedAt)
    log.info("replay starting", "run_id" -> runId, "from" -> request.from.toString, "to" -> request.to.toString,
      "mode" -> request.mode.name, "dry_run" -> request.dryRun, "path" -> path)
    try {
      val records = BronzeRow.read(spark, path) // committed files only (_spark_metadata); retention-tolerant
        .where(col("ingest_date").between(request.from.toString, request.to.toString))
        .select(col("kafka_key").as("key"), col("raw_value").as("value"), col("kafka_topic").as("topic"),
          col("kafka_partition").as("partition"), col("kafka_offset").as("offset"), col("kafka_timestamp").as("timestamp"))
        .as[KafkaRecord]
      val counts = StatusApply.run(spark, records, validation, request.mode.processing,
        ProcessorInfo(QueryName, ProcessorConfig.Version, runId, BatchId), jdbc, startedAt, dlq = None, write = !request.dryRun)
      val finishedAt = clock()
      PostgresStatusSink.finishReplay(jdbc, runId, Some(counts), finishedAt, error = None)
      log.info("replay finished", "run_id" -> runId, "input_rows" -> counts.input, "valid_rows" -> counts.valid,
        "rejected_rows" -> counts.rejected, "applied_rows" -> counts.applied, "duplicate_rows" -> counts.duplicates,
        "dry_run" -> request.dryRun)
      ReplayResult(runId, counts, startedAt, finishedAt)
    } catch {
      case NonFatal(e) =>
        val message = s"${e.getClass.getName}: ${Option(e.getMessage).getOrElse("")}".take(500)
        PostgresStatusSink.finishReplay(jdbc, runId, None, clock(), error = Some(message))
        log.error("replay failed", "run_id" -> runId, "error_code" -> "REPLAY_FAILED", "error" -> message,
          "action" -> "Fix the cause and re-run the same range; replay is idempotent")
        throw e
    }
  }
}

/** `make replay FROM=2026-10-22 TO=2026-10-22 [MODE=backfill] [DRY_RUN=true]` */
object ReplayApp {
  def main(args: Array[String]): Unit = {
    val log  = new StructuredLog("replay")
    val opts = args.sliding(2, 1).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    def fail(msg: String): Nothing = {
      log.error("invalid arguments", "error_code" -> "REPLAY_ARGS_INVALID", "error" -> msg,
        "action" -> "Usage: --from yyyy-MM-dd --to yyyy-MM-dd [--mode as-ingested|backfill] [--dry-run true|false]")
      sys.exit(2)
    }
    def date(k: String) = opts.get(k).flatMap(v => scala.util.Try(LocalDate.parse(v)).toOption).getOrElse(fail(s"--$k yyyy-MM-dd is required"))
    val request = ReplayRequest(
      from = date("from"),
      to = date("to"),
      mode = ReplayMode.fromName(opts.getOrElse("mode", "as-ingested")).getOrElse(fail("--mode must be as-ingested or backfill")),
      dryRun = opts.get("dry-run").exists(_.toLowerCase == "true")
    )
    val config = ProcessorConfig.fromEnv(sys.env).fold(fail, identity)
    val spark = SparkSession.builder().appName("ptp-replay").master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTITIONS", "6")).getOrCreate()
    try Replay.run(spark, config.lakeRoot, config.jdbc, request, config.validation, log = log)
    finally spark.stop()
  }
}
