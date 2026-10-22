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
    val query = new StatusProcessor(spark, config, log = log).start(Trigger.ProcessingTime(s"${config.triggerIntervalSeconds} seconds"))
    sys.addShutdownHook { if (query.isActive) query.stop() }
    query.awaitTermination()
  }
}
