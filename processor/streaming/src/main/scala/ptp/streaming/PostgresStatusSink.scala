package ptp.streaming

import java.sql.{Connection, DriverManager, PreparedStatement, Types}
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.util.UUID

import scala.collection.mutable
import scala.util.Using

import ptp.core.status.{ApplyEvent, PackageDelta, StatusAggregator}

/** Counts from one partition write. */
final case class WriteResult(input: Long, applied: Long, duplicates: Long)

/** One row of ops.sink_batches. */
final case class SinkBatch(
    queryName: String,
    batchId: Long,
    runId: String,
    processorVersion: String,
    startedAt: Instant,
    committedAt: Instant,
    inputRows: Long,
    validRows: Long,
    rejectedRows: Long,
    appliedRows: Long,
    duplicateRows: Long,
    offsetRangesJson: String
)

/** Idempotent status sink (spec 03 §Status apply algorithm, ADR-0002 option C).
  *
  * Per Spark partition, in one transaction:
  *   1. insert event IDs into tracking.processed_events (ON CONFLICT DO NOTHING) → the events new to the platform;
  *   2. fold the new events into per-package deltas (`StatusAggregator`);
  *   3. upsert tracking.package_status, replacing status columns only when the delta's winner is preferred
  *      over the stored event (spec 01 rule, mirrored from `StatusRule`).
  * Re-running any input any number of times converges to the same rows.
  */
object PostgresStatusSink {

  private val ChunkSize = 5000

  def connect(target: JdbcTarget): Connection = DriverManager.getConnection(target.url, target.user, target.password)

  /** Creates the daily processed_events partitions for these dates (driver side, once per batch). */
  def ensurePartitions(target: JdbcTarget, dates: Seq[LocalDate]): Unit =
    if (dates.nonEmpty) Using.resource(connect(target)) { conn =>
      Using.resource(conn.prepareStatement("SELECT tracking.ensure_processed_events_partition(?)")) { ps =>
        dates.distinct.sorted.foreach { d => ps.setObject(1, d); ps.execute() }
      }
    }

  def writePartition(target: JdbcTarget, events: Iterator[ApplyEvent], batchId: Long, queryName: String, now: Instant): WriteResult = {
    val all = events.toVector
    if (all.isEmpty) return WriteResult(0, 0, 0)
    val distinct = all.groupBy(_.eventId).valuesIterator.map(_.head).toVector

    Using.resource(connect(target)) { conn =>
      conn.setAutoCommit(false)
      try {
        val newIds    = insertProcessed(conn, distinct, batchId, now)
        val newEvents = distinct.filter(e => newIds.contains(e.eventId))
        upsertStatus(conn, StatusAggregator.deltas(newEvents), batchId, queryName, now)
        conn.commit()
        WriteResult(all.size.toLong, newEvents.size.toLong, (all.size - newEvents.size).toLong)
      } catch {
        case e: Throwable => conn.rollback(); throw e
      }
    }
  }

  private def insertProcessed(conn: Connection, events: Vector[ApplyEvent], batchId: Long, now: Instant): Set[String] = {
    val sql =
      """INSERT INTO tracking.processed_events (event_id, event_date, package_id, first_batch_id, first_processed_at)
        |SELECT e.id, e.d, e.p, ?, ? FROM unnest(?::uuid[], ?::date[], ?::text[]) AS e(id, d, p)
        |ON CONFLICT DO NOTHING
        |RETURNING event_id""".stripMargin
    val inserted = mutable.Set.empty[String]
    Using.resource(conn.prepareStatement(sql)) { ps =>
      events.grouped(ChunkSize).foreach { chunk =>
        ps.setLong(1, batchId)
        ps.setObject(2, utc(now))
        ps.setArray(3, conn.createArrayOf("uuid", chunk.map(e => UUID.fromString(e.eventId): AnyRef).toArray))
        ps.setArray(4, conn.createArrayOf("date", chunk.map(e => java.sql.Date.valueOf(e.eventDate): AnyRef).toArray))
        ps.setArray(5, conn.createArrayOf("text", chunk.map(e => e.packageId: AnyRef).toArray))
        Using.resource(ps.executeQuery()) { rs =>
          while (rs.next()) inserted += rs.getObject(1).toString.toLowerCase
        }
      }
    }
    inserted.toSet
  }

  /** `excluded` (the delta's winner) is preferred over the stored status event: spec 01 / StatusRule.prefers. */
  private val Preferred =
    """CASE WHEN excluded.status_terminal <> s.status_terminal THEN excluded.status_terminal
      |     WHEN excluded.status_terminal
      |       THEN (excluded.status_event_time, excluded.status_rank, excluded.status_event_id)
      |          < (s.status_event_time, s.status_rank, s.status_event_id)
      |     ELSE (excluded.status_event_time, excluded.status_rank, excluded.status_event_id)
      |        > (s.status_event_time, s.status_rank, s.status_event_id)
      |END""".stripMargin

  private val StatusColumns =
    Seq("current_status", "status_event_id", "status_event_time", "status_rank", "status_terminal", "facility_id", "vehicle_id")
  private val ShipmentColumns =
    Seq("promised_delivery_date", "service_level", "origin_facility_id", "destination_postal_prefix", "destination_timezone")

  private[streaming] val UpsertSql: String = {
    val columns = StatusColumns ++ ShipmentColumns ++ Seq("created_event_time", "delivered_event_time", "last_event_time",
      "event_count", "first_seen_at", "updated_at", "updated_by_query", "updated_batch_id")
    val values = columns.map {
      case "status_event_id" => "?::uuid"
      case _                 => "?"
    }
    val sets =
      StatusColumns.map(c => s"$c = CASE WHEN $Preferred THEN excluded.$c ELSE s.$c END") ++
        ShipmentColumns.map(c => s"$c = COALESCE(s.$c, excluded.$c)") ++
        Seq(
          "created_event_time = LEAST(s.created_event_time, excluded.created_event_time)",
          "delivered_event_time = LEAST(s.delivered_event_time, excluded.delivered_event_time)",
          "last_event_time = GREATEST(s.last_event_time, excluded.last_event_time)",
          "event_count = s.event_count + excluded.event_count",
          "first_seen_at = LEAST(s.first_seen_at, excluded.first_seen_at)",
          "updated_at = excluded.updated_at",
          "updated_by_query = excluded.updated_by_query",
          "updated_batch_id = excluded.updated_batch_id"
        )
    s"""INSERT INTO tracking.package_status AS s (${("package_id" +: columns).mkString(", ")})
       |VALUES (${("?" +: values).mkString(", ")})
       |ON CONFLICT (package_id) DO UPDATE SET
       |  ${sets.mkString(",\n  ")}""".stripMargin
  }

  private def upsertStatus(conn: Connection, deltas: Seq[PackageDelta], batchId: Long, queryName: String, now: Instant): Unit =
    if (deltas.nonEmpty) Using.resource(conn.prepareStatement(UpsertSql)) { ps =>
      deltas.foreach { d =>
        val w = d.winner
        val c = d.created
        var i = 0
        def next(): Int = { i += 1; i }
        ps.setString(next(), d.packageId)
        ps.setString(next(), w.eventType)
        ps.setString(next(), w.eventId)
        ps.setObject(next(), utc(w.eventTime))
        ps.setShort(next(), w.rank.toShort)
        ps.setBoolean(next(), w.terminal)
        setText(ps, next(), w.facilityId)
        setText(ps, next(), w.vehicleId)
        c.flatMap(_.promisedDeliveryDate) match {
          case Some(date) => ps.setObject(next(), date)
          case None       => ps.setNull(next(), Types.DATE)
        }
        setText(ps, next(), c.flatMap(_.serviceLevel))
        setText(ps, next(), c.flatMap(_.originFacilityId))
        setText(ps, next(), c.flatMap(_.destinationPostalPrefix))
        setText(ps, next(), c.flatMap(_.destinationTimezone))
        setTime(ps, next(), c.map(_.eventTime))
        setTime(ps, next(), d.firstDeliveredTime)
        ps.setObject(next(), utc(d.maxEventTime))
        ps.setInt(next(), d.newEventCount)
        ps.setObject(next(), utc(now))
        ps.setObject(next(), utc(now))
        ps.setString(next(), queryName)
        ps.setLong(next(), batchId)
        ps.addBatch()
      }
      ps.executeBatch()
    }

  /** Driver side, after all partitions committed. A re-run of the same batch increments `attempts`. */
  def recordBatch(target: JdbcTarget, b: SinkBatch): Unit =
    Using.resource(connect(target)) { conn =>
      val sql =
        """INSERT INTO ops.sink_batches (query_name, batch_id, run_id, processor_version, started_at, committed_at,
          |  input_rows, valid_rows, rejected_rows, applied_rows, duplicate_rows, offset_ranges)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
          |ON CONFLICT (query_name, batch_id) DO UPDATE SET
          |  run_id = excluded.run_id, processor_version = excluded.processor_version,
          |  started_at = excluded.started_at, committed_at = excluded.committed_at,
          |  input_rows = excluded.input_rows, valid_rows = excluded.valid_rows, rejected_rows = excluded.rejected_rows,
          |  applied_rows = excluded.applied_rows, duplicate_rows = excluded.duplicate_rows,
          |  offset_ranges = excluded.offset_ranges, attempts = ops.sink_batches.attempts + 1""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, b.queryName); ps.setLong(2, b.batchId); ps.setString(3, b.runId); ps.setString(4, b.processorVersion)
        ps.setObject(5, utc(b.startedAt)); ps.setObject(6, utc(b.committedAt))
        ps.setLong(7, b.inputRows); ps.setLong(8, b.validRows); ps.setLong(9, b.rejectedRows)
        ps.setLong(10, b.appliedRows); ps.setLong(11, b.duplicateRows); ps.setString(12, b.offsetRangesJson)
        ps.executeUpdate()
      }
    }

  def startReplay(target: JdbcTarget, runId: String, r: ReplayRequest, startedAt: Instant): Unit =
    Using.resource(connect(target)) { conn =>
      Using.resource(conn.prepareStatement(
        """INSERT INTO ops.replay_runs (run_id, ingest_date_from, ingest_date_to, mode, dry_run, status, started_at)
          |VALUES (?, ?, ?, ?, ?, 'running', ?)""".stripMargin)) { ps =>
        ps.setString(1, runId); ps.setObject(2, r.from); ps.setObject(3, r.to); ps.setString(4, r.mode.name)
        ps.setBoolean(5, r.dryRun); ps.setObject(6, utc(startedAt)); ps.executeUpdate()
      }
    }

  def finishReplay(target: JdbcTarget, runId: String, counts: Option[ApplyCounts], finishedAt: Instant, error: Option[String]): Unit =
    Using.resource(connect(target)) { conn =>
      Using.resource(conn.prepareStatement(
        """UPDATE ops.replay_runs SET status = ?, finished_at = ?, input_rows = ?, valid_rows = ?, rejected_rows = ?,
          |  applied_rows = ?, duplicate_rows = ?, error = ? WHERE run_id = ?""".stripMargin)) { ps =>
        ps.setString(1, if (error.isEmpty) "succeeded" else "failed"); ps.setObject(2, utc(finishedAt))
        Seq(counts.map(_.input), counts.map(_.valid), counts.map(_.rejected), counts.map(_.applied), counts.map(_.duplicates))
          .zipWithIndex.foreach {
            case (Some(v), i) => ps.setLong(3 + i, v)
            case (None, i)    => ps.setNull(3 + i, Types.BIGINT)
          }
        setText(ps, 8, error); ps.setString(9, runId); ps.executeUpdate()
      }
    }

  private def utc(i: Instant): OffsetDateTime = OffsetDateTime.ofInstant(i, ZoneOffset.UTC)

  private def setText(ps: PreparedStatement, i: Int, v: Option[String]): Unit =
    v match { case Some(s) => ps.setString(i, s); case None => ps.setNull(i, Types.VARCHAR) }

  private def setTime(ps: PreparedStatement, i: Int, v: Option[Instant]): Unit =
    v match { case Some(t) => ps.setObject(i, utc(t)); case None => ps.setNull(i, Types.TIMESTAMP_WITH_TIMEZONE) }
}
