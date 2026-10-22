package ptp.lake

import java.sql.Connection
import java.time.{Duration, Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.{Properties, UUID}

import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import ptp.core.dq.DqRules
import ptp.core.gold.GoldRules
import ptp.core.status.{ApplyEvent, StatusRule}
import ptp.streaming.{BronzeRow, PostgresStatusSink, StructuredLog}

/** Parameters of one DQ run. `now` is injectable so tests can evaluate "later". */
final case class DqConfig(
    asOf: LocalDate,
    now: Instant,
    kafkaBootstrapServers: String,
    eventsTopic: String,
    dlqTopic: String,
    freshness: Duration = Duration.ofMinutes(15),
    watermark: Duration = Duration.ofHours(2),
    orphanGrace: Duration = Duration.ofHours(24),
    postTerminalMaxRate: Double = 0.01
)

final case class CheckResult(checkId: String, severity: String, status: String, observed: Option[Double], threshold: String,
                             sample: Seq[String], detail: String)

final case class DqReport(runId: String, windowFrom: LocalDate, windowTo: LocalDate, results: Seq[CheckResult]) {
  def result(id: String): CheckResult = results.find(_.checkId == id).getOrElse(sys.error(s"no result for $id"))

  /** SC-0006: non-zero when any critical or high check fails; warnings never fail the run. */
  def exitCode: Int = if (results.exists(r => r.status == "fail" && Set("critical", "high")(r.severity))) 1 else 0
}

/** Batch data-quality checks DQ-02 … DQ-10 (spec 05, SC-0006). Results go to `ops.dq_results`; DQ-04/05 anomalies to
  * `tracking.status_anomalies`, replaced for the packages evaluated, so re-runs are idempotent.
  */
object DqJob {
  private val mapper = JsonMapper.builder().build()

  def run(spark: SparkSession, lake: LakeConfig, dq: DqConfig, log: StructuredLog = new StructuredLog("dq")): DqReport = {
    import spark.implicits._
    val runId      = UUID.randomUUID().toString
    val windowFrom = dq.asOf.minusDays(lake.goldWindowDays - 1L)
    val readFrom   = windowFrom.minusDays(lake.goldLookbackDays.toLong)
    log.info("dq run starting", "run_id" -> runId, "window_from" -> windowFrom.toString, "window_to" -> dq.asOf.toString)

    val silver = spark.read.parquet(lake.silverPath)
      .where(col("event_date").between(readFrom.toString, dq.asOf.plusDays(1).toString)).cache()
    val events: Dataset[ApplyEvent] = silver.select(
      col("event_id").as("eventId"), col("package_id").as("packageId"), col("event_type").as("eventType"),
      col("event_rank").as("rank"), col("is_terminal").as("terminal"), col("event_time").as("eventTime"),
      col("facility_id").as("facilityId"), col("vehicle_id").as("vehicleId"),
      col("promised_delivery_date").as("promisedDeliveryDate"), col("service_level").as("serviceLevel"),
      col("origin_facility_id").as("originFacilityId"), col("destination_postal_prefix").as("destinationPostalPrefix"),
      col("destination_timezone").as("destinationTimezone")).as[ApplyEvent].cache()
    val inWindow = (d: LocalDate) => !d.isBefore(windowFrom) && !d.isAfter(dq.asOf)
    val windowPackages = events.filter(e => inWindow(GoldRules.utcDate(e.eventTime))).map(_.packageId).distinct().collect().toSet

    def guarded(id: String, severity: String)(check: => CheckResult): CheckResult =
      try check
      catch {
        case NonFatal(e) =>
          CheckResult(id, severity, "fail", None, "check must run", Nil, s"check error: ${e.getClass.getSimpleName}: ${e.getMessage}".take(500))
      }

    // ---- anomalies (DQ-04, DQ-05) -----------------------------------------------------------------
    val (now, grace) = (dq.now, dq.orphanGrace)
    val perPackage = events.filter(e => windowPackages(e.packageId)).groupByKey(_.packageId)
    val anomalies = perPackage.flatMapGroups { (_, it) =>
      val es = it.toSeq
      DqRules.orphan(es, now, grace).map(e => (e.eventId, "ORPHAN_EVENT", e.packageId)).toSeq ++
        DqRules.postTerminal(es).map(e => (e.eventId, "POST_TERMINAL_EVENT", e.packageId))
    }.distinct().collect().toSeq // distinct: duplicated input (the very fault DQ-02 detects) must not crash the run
    writeAnomalies(lake, windowPackages.toSeq, anomalies, runId, dq.now)
    val windowEvents = events.filter(e => inWindow(GoldRules.utcDate(e.eventTime))).count()

    val results = Seq(
      guarded("DQ-02", "critical") {
        val dups = silver.where(col("event_date").between(windowFrom.toString, dq.asOf.toString))
          .groupBy("event_id").count().where(col("count") > 1).select("event_id").as[String].collect().toSeq
        CheckResult("DQ-02", "critical", if (dups.isEmpty) "pass" else "fail", Some(dups.size.toDouble), "0 duplicate event_ids",
          dups.take(10), "silver event_id uniqueness; processed_events is unique by its primary key")
      },
      guarded("DQ-03", "high")(freshness(lake, dq)),
      guarded("DQ-04", "warn") {
        val orphans = anomalies.filter(_._2 == "ORPHAN_EVENT").map(_._3)
        CheckResult("DQ-04", "warn", if (orphans.isEmpty) "pass" else "warn", Some(orphans.size.toDouble),
          s"0 packages without CREATED after ${dq.orphanGrace.toHours} h", orphans.take(10), "ORPHAN_EVENT rows written")
      },
      guarded("DQ-05", "warn") {
        val post = anomalies.filter(_._2 == "POST_TERMINAL_EVENT")
        val rate = if (windowEvents == 0) 0.0 else post.size.toDouble / windowEvents
        CheckResult("DQ-05", "warn", if (rate <= dq.postTerminalMaxRate) "pass" else "warn", Some(post.size.toDouble),
          s"rate <= ${dq.postTerminalMaxRate} of $windowEvents window events", post.map(_._1).take(10),
          f"rate=$rate%.4f; POST_TERMINAL_EVENT rows written")
      },
      guarded("DQ-06", "critical")(counts(spark, lake, dq, windowFrom)),
      guarded("DQ-07", "critical")(state(spark, lake, dq, events, windowFrom, readFrom)),
      guarded("DQ-08", "critical") {
        val silverDelivered = events.groupByKey(_.packageId).flatMapGroups((_, es) => GoldRules.deliveryFact(es.toSeq))
          .filter(f => inWindow(f.deliveryDate)).count()
        val (daily, facts) = Using.resource(PostgresStatusSink.connect(lake.jdbc)) { c =>
          (scalar(c, "SELECT COALESCE(sum(delivered_count), 0) FROM analytics.daily_delivery_performance WHERE delivery_date BETWEEN ? AND ?", windowFrom, dq.asOf),
            scalar(c, "SELECT count(*) FROM analytics.delivery_facts WHERE delivery_date BETWEEN ? AND ?", windowFrom, dq.asOf))
        }
        val ok = daily == facts && facts == silverDelivered
        CheckResult("DQ-08", "critical", if (ok) "pass" else "fail", Some((daily - silverDelivered).abs.max((facts - silverDelivered).abs).toDouble),
          "daily sum = facts = silver deliveries", Nil, s"daily_sum=$daily delivery_facts=$facts silver=$silverDelivered")
      },
      guarded("DQ-09", "warn") {
        val bad = Using.resource(PostgresStatusSink.connect(lake.jdbc)) { c =>
          strings(c, """SELECT 'visit:' || arrival_event_id FROM analytics.facility_visits
                       |WHERE arrival_date BETWEEN ? AND ? AND (dwell_minutes < 0 OR dwell_minutes > 14 * 1440)
                       |UNION ALL
                       |SELECT 'delivery:' || package_id FROM analytics.delivery_facts
                       |WHERE delivery_date BETWEEN ? AND ? AND (delay_minutes < -30 * 1440 OR delay_minutes > 60 * 1440)""".stripMargin,
            windowFrom, dq.asOf, windowFrom, dq.asOf)
        }
        CheckResult("DQ-09", "warn", if (bad.isEmpty) "pass" else "warn", Some(bad.size.toDouble),
          "dwell in [0, 14 d], delay in [-30 d, +60 d]", bad.take(10), "")
      },
      guarded("DQ-10", "warn")(volume(spark, lake, dq))
    )
    silver.unpersist(); events.unpersist()

    writeResults(lake, runId, windowFrom, dq.asOf, results, dq.now)
    val report = DqReport(runId, windowFrom, dq.asOf, results)
    results.foreach(r => log.info("dq check", "run_id" -> runId, "check_id" -> r.checkId, "status" -> r.status,
      "severity" -> r.severity, "observed" -> r.observed.map(_.toString).orNull, "detail" -> r.detail))
    log.info("dq run finished", "run_id" -> runId, "exit_code" -> report.exitCode,
      "failed" -> results.filter(_.status == "fail").map(_.checkId).mkString(","))
    report
  }

  /** DQ-03, lag-aware: data waiting in Kafka and no commit for longer than the threshold. */
  private def freshness(lake: LakeConfig, dq: DqConfig): CheckResult = {
    val props = new Properties()
    props.put("bootstrap.servers", dq.kafkaBootstrapServers)
    val ends = Using.resource(new KafkaConsumer[Array[Byte], Array[Byte]](props, new ByteArrayDeserializer, new ByteArrayDeserializer)) { c =>
      val parts = c.partitionsFor(dq.eventsTopic).asScala.map(p => new TopicPartition(dq.eventsTopic, p.partition())).asJava
      c.endOffsets(parts).asScala.map { case (tp, end) => tp.partition() -> end.longValue }.toMap
    }
    val (committed, lastCommit) = Using.resource(PostgresStatusSink.connect(lake.jdbc)) { c =>
      val rs = c.createStatement().executeQuery(
        """SELECT key::int, max((value->>1)::bigint) FROM ops.sink_batches, jsonb_each(offset_ranges)
          |WHERE query_name = 'status-processor' GROUP BY key""".stripMargin)
      val m = Iterator.continually(rs).takeWhile(_.next()).map(r => r.getInt(1) -> r.getLong(2)).toMap
      val r2 = c.createStatement().executeQuery("SELECT max(committed_at) FROM ops.sink_batches WHERE query_name = 'status-processor'")
      r2.next()
      (m, Option(r2.getObject(1, classOf[OffsetDateTime])).map(_.toInstant))
    }
    val lagByPartition = ends.map { case (p, end) => p -> math.max(0L, end - (committed.getOrElse(p, -1L) + 1)) }
    val lag   = lagByPartition.values.sum
    val stale = lastCommit.forall(_.isBefore(dq.now.minus(dq.freshness)))
    CheckResult("DQ-03", "high", if (lag > 0 && stale) "fail" else "pass", Some(lag.toDouble),
      s"no waiting records older than ${dq.freshness.toMinutes} min without a commit",
      lagByPartition.filter(_._2 > 0).map { case (p, l) => s"partition $p lag $l" }.toSeq.sorted.take(10),
      s"lag=$lag last_commit=${lastCommit.map(_.toString).getOrElse("never")}")
  }

  /** DQ-06: closed ingest dates (before today per `now`) reconcile across bronze, silver and the DLQ. */
  private def counts(spark: SparkSession, lake: LakeConfig, dq: DqConfig, windowFrom: LocalDate): CheckResult = {
    import spark.implicits._
    val closedTo = Seq(dq.asOf, LocalDate.ofInstant(dq.now, ZoneOffset.UTC).minusDays(1)).min
    if (closedTo.isBefore(windowFrom))
      return CheckResult("DQ-06", "critical", "skipped", None, "closed ingest dates exist", Nil, "no closed ingest date in the window yet")
    val bronze = spark.read.parquet(lake.bronzePath).as[BronzeRow]
      .filter(r => !r.ingest_date.isBefore(windowFrom) && !r.ingest_date.isAfter(closedTo) && r.kafka_topic == dq.eventsTopic).cache()
    val dupOffsets = bronze.groupBy("kafka_partition", "kafka_offset").count().where(col("count") > 1).count()
    val invalid    = bronze.filter(!_.is_valid).map(r => (r.kafka_partition, r.kafka_offset)).collect().toSet
    val validIds   = bronze.filter(_.is_valid).flatMap(_.event_id).distinct()
    val dates      = bronze.filter(_.is_valid).flatMap(_.event_time.map(GoldRules.utcDate)).distinct().collect().map(_.toString)
    val silverIds  = if (dates.isEmpty) spark.emptyDataset[String]
                     else spark.read.parquet(lake.silverPath).where(col("event_date").isin(dates.toIndexedSeq: _*)).select("event_id").as[String]
    val missing    = validIds.except(silverIds).collect().toSeq
    val dlq = spark.read.format("kafka").option("kafka.bootstrap.servers", dq.kafkaBootstrapServers)
      .option("subscribe", dq.dlqTopic).option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
      .select(col("value").cast("string")).as[String].collect().toSeq
      .map(mapper.readTree)
      .filter(n => n.at("/source/topic").asText == dq.eventsTopic)
      .filter { n =>
        val d = LocalDate.ofInstant(Instant.parse(n.at("/source/kafka_timestamp").asText), ZoneOffset.UTC)
        !d.isBefore(windowFrom) && !d.isAfter(closedTo)
      }
      .map(n => (n.at("/source/partition").asInt, n.at("/source/offset").asLong)).toSet
    bronze.unpersist()
    val dlqOnly    = (dlq -- invalid).toSeq.map { case (p, o) => s"dlq-only $p:$o" }
    val bronzeOnly = (invalid -- dlq).toSeq.map { case (p, o) => s"bronze-only $p:$o" }
    val problems   = dupOffsets + missing.size + dlqOnly.size + bronzeOnly.size
    CheckResult("DQ-06", "critical", if (problems == 0) "pass" else "fail", Some(problems.toDouble),
      "bronze offsets unique; valid ids in silver; DLQ offsets = bronze invalid",
      (missing.map(id => s"missing-in-silver $id") ++ dlqOnly ++ bronzeOnly).take(10),
      s"closed ingest dates $windowFrom..$closedTo; duplicate offsets=$dupOffsets missing=${missing.size} dlq_only=${dlqOnly.size} bronze_only=${bronzeOnly.size}")
  }

  /** DQ-07: idle packages' status equals StatusRule over their silver events. */
  private def state(spark: SparkSession, lake: LakeConfig, dq: DqConfig, events: Dataset[ApplyEvent],
                    windowFrom: LocalDate, readFrom: LocalDate): CheckResult = {
    import spark.implicits._
    val cutoff = dq.now.minus(dq.watermark)
    val props  = new Properties()
    props.put("user", lake.jdbc.user); props.put("password", lake.jdbc.password); props.put("driver", "org.postgresql.Driver")
    val status = spark.read.jdbc(lake.jdbc.url, "tracking.package_status", props)
      .where(col("last_event_time") < java.sql.Timestamp.from(cutoff) && col("updated_at") < java.sql.Timestamp.from(cutoff) &&
        col("last_event_time") >= java.sql.Timestamp.from(windowFrom.atStartOfDay(ZoneOffset.UTC).toInstant))
      .select(col("package_id"), col("current_status"), col("status_event_id").cast("string").as("status_event_id"), col("event_count"))
      .as[(String, String, String, Int)].collect().map(r => r._1 -> r).toMap
    val coveredFrom = readFrom.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant
    val recomputed = events.filter(e => status.contains(e.packageId)).groupByKey(_.packageId).mapGroups { (pkg, it) =>
      val es = it.toSeq
      val w  = StatusRule.winner(es)
      (pkg, w.eventType, w.eventId, es.size, es.map(_.eventTime).min.isBefore(coveredFrom))
    }.collect().map(r => r._1 -> r).toMap
    val checked    = status.keys.filterNot(p => recomputed.get(p).exists(_._5)) // skip histories older than the read range
    val mismatches = checked.filterNot { p =>
      recomputed.get(p).exists { case (_, tpe, id, n, _) => status(p)._2 == tpe && status(p)._3 == id && status(p)._4 == n }
    }.toSeq.sorted
    CheckResult("DQ-07", "critical", if (mismatches.isEmpty) "pass" else "fail", Some(mismatches.size.toDouble),
      s"0 mismatches among packages idle > ${dq.watermark.toHours} h", mismatches.take(10), s"compared=${checked.size}")
  }

  /** DQ-10: last complete hour vs the same hour on the previous 7 days; skipped without full history. */
  private def volume(spark: SparkSession, lake: LakeConfig, dq: DqConfig): CheckResult = {
    import spark.implicits._
    val hour  = dq.now.truncatedTo(ChronoUnit.HOURS).minus(Duration.ofHours(1))
    val hours = (0 to 7).map(d => hour.minus(Duration.ofDays(d.toLong)))
    val days  = hours.map(h => LocalDate.ofInstant(h, ZoneOffset.UTC).toString).distinct
    val present = spark.read.parquet(lake.silverPath).select("event_date").distinct().as[java.sql.Date].collect().map(_.toLocalDate.toString).toSet
    if (!days.tail.forall(present)) {
      CheckResult("DQ-10", "warn", "skipped", None, "7 full days of history", Nil, s"history days present: ${days.tail.count(present)}/7")
    } else {
      val counts = spark.read.parquet(lake.silverPath).where(col("event_date").isin(days: _*)).select("event_time").as[java.sql.Timestamp]
        .map(t => t.toInstant.truncatedTo(ChronoUnit.HOURS).toEpochMilli).groupByKey(identity).count().collect().toMap
      def at(h: Instant) = counts.getOrElse(h.toEpochMilli, 0L)
      val (status, deviation) = DqRules.volume(at(hours.head), hours.tail.map(at))
      CheckResult("DQ-10", "warn", status, Some(at(hours.head).toDouble), "within ±50 % of the 7-day same-hour median", Nil,
        f"hour=$hour deviation=$deviation%.3f")
    }
  }

  // ---- persistence ------------------------------------------------------------------------------------

  private def writeAnomalies(lake: LakeConfig, packages: Seq[String], rows: Seq[(String, String, String)], runId: String, now: Instant): Unit =
    Using.resource(PostgresStatusSink.connect(lake.jdbc)) { c =>
      c.setAutoCommit(false)
      try {
        Using.resource(c.prepareStatement(
          "DELETE FROM tracking.status_anomalies WHERE anomaly_code IN ('ORPHAN_EVENT', 'POST_TERMINAL_EVENT') AND package_id = ANY(?)")) { ps =>
          ps.setArray(1, c.createArrayOf("text", packages.toArray[AnyRef])); ps.executeUpdate()
        }
        Using.resource(c.prepareStatement(
          "INSERT INTO tracking.status_anomalies (event_id, anomaly_code, package_id, detected_at, run_id) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING")) { ps =>
          rows.foreach { case (id, code, pkg) =>
            ps.setString(1, id); ps.setString(2, code); ps.setString(3, pkg); ps.setObject(4, OffsetDateTime.ofInstant(now, ZoneOffset.UTC))
            ps.setString(5, runId); ps.addBatch()
          }
          ps.executeBatch()
        }
        c.commit()
      } catch { case e: Throwable => c.rollback(); throw e }
    }

  private def writeResults(lake: LakeConfig, runId: String, from: LocalDate, to: LocalDate, results: Seq[CheckResult], now: Instant): Unit =
    Using.resource(PostgresStatusSink.connect(lake.jdbc)) { c =>
      Using.resource(c.prepareStatement(
        """INSERT INTO ops.dq_results (run_id, check_id, window_from, window_to, status, severity, observed, threshold, sample, detail, checked_at)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin)) { ps =>
        results.foreach { r =>
          ps.setString(1, runId); ps.setString(2, r.checkId); ps.setObject(3, from); ps.setObject(4, to); ps.setString(5, r.status)
          ps.setString(6, r.severity)
          r.observed match { case Some(v) => ps.setDouble(7, v); case None => ps.setNull(7, java.sql.Types.DOUBLE) }
          ps.setString(8, r.threshold); ps.setArray(9, c.createArrayOf("text", r.sample.toArray[AnyRef])); ps.setString(10, r.detail)
          ps.setObject(11, OffsetDateTime.ofInstant(now, ZoneOffset.UTC)); ps.addBatch()
        }
        ps.executeBatch()
      }
    }

  private def scalar(c: Connection, sql: String, params: AnyRef*): Long =
    Using.resource(c.prepareStatement(sql)) { ps =>
      params.zipWithIndex.foreach { case (p, i) => ps.setObject(i + 1, p) }
      val rs = ps.executeQuery(); rs.next(); rs.getLong(1)
    }

  private def strings(c: Connection, sql: String, params: AnyRef*): Seq[String] =
    Using.resource(c.prepareStatement(sql)) { ps =>
      params.zipWithIndex.foreach { case (p, i) => ps.setObject(i + 1, p) }
      val rs = ps.executeQuery()
      Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toVector
    }

}
