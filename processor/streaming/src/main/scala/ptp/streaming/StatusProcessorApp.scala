package ptp.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/** Entry point: `make process` (host, local[*]). Configuration from the environment (spec 08). */
object StatusProcessorApp {
  def main(args: Array[String]): Unit = {
    val log = new StructuredLog("status-processor")
    val config = ProcessorConfig.fromEnv(sys.env) match {
      case Right(c) => c
      case Left(problem) =>
        log.error("invalid configuration", "error_code" -> "CONFIG_INVALID", "error" -> problem,
          "action" -> "Fix the variable in .env (see .env.example) and restart")
        sys.exit(2)
    }
    val spark = SparkSession
      .builder()
      .appName("ptp-status-processor")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTITIONS", "6"))
      .getOrCreate()
    val trigger = Trigger.ProcessingTime(s"${config.triggerIntervalSeconds} seconds")
    // ADR-0006: both queries in one driver locally; each has its own checkpoint and fails independently.
    val queries = Seq(new StatusProcessor(spark, config, log = log).start(trigger)) ++
      (if (config.bronzeEnabled) Seq(new BronzeWriter(spark, config).start(trigger)) else Nil)

    // Graceful stop (AC-7.6): SIGTERM (docker stop) or SIGINT (Ctrl-C) stops the queries between micro-batches and
    // exits 0. Without a handler the JVM exits 143/130 and in-flight batches are re-run on restart (still correct).
    val stopping = new java.util.concurrent.atomic.AtomicBoolean(false)
    Seq("TERM", "INT").foreach { name =>
      sun.misc.Signal.handle(new sun.misc.Signal(name), (_: sun.misc.Signal) => {
        if (stopping.compareAndSet(false, true)) {
          log.info("stop requested", "signal" -> name, "queries" -> queries.map(_.name).mkString(","))
          queries.filter(_.isActive).foreach(_.stop())
        }
      })
    }
    val failed =
      try { spark.streams.awaitAnyTermination(); !stopping.get }
      catch { case e: org.apache.spark.sql.streaming.StreamingQueryException =>
        log.error("query failed", "error_code" -> "QUERY_FAILED", "error" -> e.getMessage.take(500),
          "action" -> "The app exits non-zero; restart it to resume from the checkpoints")
        true }
    queries.filter(_.isActive).foreach(_.stop())
    log.info("queries stopped", "graceful" -> !failed)
    spark.stop()
    if (failed) sys.exit(1)
  }
}
