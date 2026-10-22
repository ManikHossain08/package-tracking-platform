package ptp.lake

import ptp.streaming.JdbcTarget

/** Batch (silver/gold) configuration from the environment (spec 08). */
final case class LakeConfig(lakeRoot: String, jdbc: JdbcTarget, silverTargetFileBytes: Long, goldWindowDays: Int, goldLookbackDays: Int,
                            kafkaBootstrapServers: String = "localhost:29092", eventsTopic: String = "package-events.v1",
                            dlqTopic: String = "package-events.dlq.v1") {
  def bronzePath: String = s"$lakeRoot/bronze/package_events"
  def silverRoot: String = s"$lakeRoot/silver"
  def silverPath: String = s"$silverRoot/package_events"
}

object LakeConfig {
  def fromEnv(env: Map[String, String]): Either[String, LakeConfig] =
    for {
      password <- env.get("PTP_BATCH_WRITER_PASSWORD").filter(_.nonEmpty)
                    .toRight("PTP_BATCH_WRITER_PASSWORD is required (see .env.example; run make env)")
      target <- env.get("SILVER_TARGET_FILE_BYTES").map(_.trim).filter(_.nonEmpty) match {
                  case None    => Right(128L * 1024 * 1024)
                  case Some(v) => v.toLongOption.filter(_ > 0).toRight(s"SILVER_TARGET_FILE_BYTES must be a positive integer, got '$v'")
                }
    } yield LakeConfig(
      lakeRoot = env.getOrElse("LAKE_ROOT", "data/lake"),
      jdbc = JdbcTarget(env.getOrElse("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:55432/ptp"),
        env.getOrElse("POSTGRES_BATCH_WRITER_USER", "ptp_batch_writer"), password),
      silverTargetFileBytes = target,
      goldWindowDays = 7,
      goldLookbackDays = 30,
      kafkaBootstrapServers = env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
      eventsTopic = env.getOrElse("EVENTS_TOPIC", "package-events.v1"),
      dlqTopic = env.getOrElse("DLQ_TOPIC", "package-events.dlq.v1")
    )
}
