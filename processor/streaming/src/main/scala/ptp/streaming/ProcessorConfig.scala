package ptp.streaming

import java.time.Duration

import ptp.core.validation.{ProcessingMode, ValidationConfig}

/** JDBC target. `toString` never prints the password. */
final case class JdbcTarget(url: String, user: String, password: String) {
  override def toString: String = s"JdbcTarget($url, $user, ***)"
}

/** Status-processor configuration (spec 08). Every value comes from the environment. */
final case class ProcessorConfig(
    kafkaBootstrapServers: String,
    eventsTopic: String,
    dlqTopic: String,
    jdbc: JdbcTarget,
    checkpointRoot: String,
    lakeRoot: String,
    bronzeEnabled: Boolean,
    queryName: String,
    triggerIntervalSeconds: Long,
    maxOffsetsPerTrigger: Long,
    startingOffsets: String,
    mode: ProcessingMode,
    validation: ValidationConfig,
    processorVersion: String
) {
  def checkpointLocation: String = s"$checkpointRoot/$queryName"
  def bronzePath: String         = s"$lakeRoot/bronze/package_events"
}

object ProcessorConfig {
  val Version = "0.1.0"

  def fromEnv(env: Map[String, String]): Either[String, ProcessorConfig] = {
    def get(k: String, default: String) = env.getOrElse(k, default).trim
    def long(k: String, default: Long): Either[String, Long] =
      env.get(k).map(_.trim).filter(_.nonEmpty) match {
        case None    => Right(default)
        case Some(v) => v.toLongOption.filter(_ > 0).toRight(s"$k must be a positive integer, got '$v'")
      }
    for {
      password <- env.get("PTP_STREAM_WRITER_PASSWORD").filter(_.nonEmpty)
                    .toRight("PTP_STREAM_WRITER_PASSWORD is required (see .env.example; run make env)")
      mode <- get("PROCESSING_MODE", "live").toLowerCase match {
                case "live"   => Right(ProcessingMode.Live)
                case "replay" => Right(ProcessingMode.Replay)
                case other    => Left(s"PROCESSING_MODE must be live or replay, got '$other'")
              }
      trigger   <- long("TRIGGER_INTERVAL_SECONDS", 5)
      maxOffs   <- long("MAX_OFFSETS_PER_TRIGGER", 50000)
      skewSec   <- long("FUTURE_SKEW_TOLERANCE_SECONDS", 300)
      maxAgeDay <- long("LIVE_MAX_EVENT_AGE_DAYS", 30)
    } yield ProcessorConfig(
      kafkaBootstrapServers = get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
      eventsTopic = get("EVENTS_TOPIC", "package-events.v1"),
      dlqTopic = get("DLQ_TOPIC", "package-events.dlq.v1"),
      jdbc = JdbcTarget(get("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:55432/ptp"),
        get("POSTGRES_STREAM_WRITER_USER", "ptp_stream_writer"), password),
      checkpointRoot = get("CHECKPOINT_ROOT", "data/checkpoints"),
      lakeRoot = get("LAKE_ROOT", "data/lake"),
      bronzeEnabled = get("BRONZE_ENABLED", "true").toLowerCase != "false",
      queryName = get("QUERY_NAME", "status-processor"),
      triggerIntervalSeconds = trigger,
      maxOffsetsPerTrigger = maxOffs,
      startingOffsets = get("STARTING_OFFSETS", "earliest"),
      mode = mode,
      validation = ValidationConfig(futureSkew = Duration.ofSeconds(skewSec), liveMaxAge = Duration.ofDays(maxAgeDay)),
      processorVersion = Version
    )
  }
}
