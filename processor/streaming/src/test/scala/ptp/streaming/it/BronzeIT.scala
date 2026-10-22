package ptp.streaming.it

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, ZoneOffset}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** Step 5: bronze Parquet via the Spark file sink (FR-STO-01, spec 07 AC-5.1 to AC-5.4). */
class BronzeIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private def bronze(path: String) = spark.read.parquet(path) // root of the file sink: honours _spark_metadata

  test("AC-5.1/5.2/5.3 bronze is complete, byte-faithful, agrees with the DLQ and is partitioned by ingest date",
    Integration, Req("FR-STO-01"), Req("DQ-06")) {
    val t   = topics()
    val gen = generate("bench/scenarios/smoke.yaml", t.events)
    val cfg = config(t, maxOffsetsPerTrigger = 150)
    runBothToEnd(cfg)
    val rows = bronze(cfg.bronzePath).collect().toSeq

    // AC-5.1 completeness: exactly one row per produced record.
    val produced = gen.lines("delivery.jsonl").map(n => (n.get("partition").intValue, n.get("offset").longValue)).toSet
    val keys     = rows.map(r => (r.getAs[Int]("kafka_partition"), r.getAs[Long]("kafka_offset")))
    assert(keys.size == gen.summary.get("records_total").intValue && keys.toSet == produced)

    // AC-5.2 fidelity: key and value bytes equal what is in Kafka.
    val kafkaRaw = readRaw(t.events)
    val corrupted = rows.filterNot { r =>
      val (k, v) = kafkaRaw((r.getAs[Int]("kafka_partition"), r.getAs[Long]("kafka_offset")))
      java.util.Arrays.equals(k, r.getAs[Array[Byte]]("kafka_key")) && java.util.Arrays.equals(v, r.getAs[Array[Byte]]("raw_value"))
    }
    assert(corrupted.isEmpty)

    // AC-5.2 agreement (DQ-06): invalid bronze rows are exactly the DLQ's source offsets, with the same primary code.
    val bronzeInvalid = rows.filterNot(_.getAs[Boolean]("is_valid"))
      .map(r => (r.getAs[Int]("kafka_partition"), r.getAs[Long]("kafka_offset")) -> r.getSeq[String](r.fieldIndex("error_codes")).head).toMap
    val dlq = readAll(t.dlq).map(n => (n.at("/source/partition").intValue, n.at("/source/offset").longValue) -> n.get("error_code").textValue).toMap
    assert(bronzeInvalid == dlq && dlq.size == gen.summary.get("malformed_total").intValue)

    // AC-5.3 layout: ingest_date = UTC date of the Kafka timestamp; directories by date; filters prune.
    // Row values come back as java.sql types.
    def ingestDate(r: org.apache.spark.sql.Row) = r.getAs[java.sql.Date]("ingest_date").toLocalDate
    assert(rows.forall(r => ingestDate(r) == LocalDate.ofInstant(r.getAs[java.sql.Timestamp]("kafka_timestamp").toInstant, ZoneOffset.UTC)))
    val dirs = Files.list(Paths.get(cfg.bronzePath)).iterator().asScala.map(_.getFileName.toString).filter(_.startsWith("ingest_date=")).toSet
    assert(dirs == rows.map(r => s"ingest_date=${ingestDate(r)}").toSet)
    val plan = bronze(cfg.bronzePath).where(col("ingest_date") === ingestDate(rows.head).toString)
      .queryExecution.executedPlan.toString
    assert(plan.contains("PartitionFilters: [") && plan.contains("ingest_date"), plan)
  }

  test("AC-5.4 a batch re-executed after a lost checkpoint commit writes no duplicate files", Integration,
    Req("FR-STO-01"), Req("NFR-02")) {
    val t   = topics()
    val gen = generate("bench/scenarios/clean.yaml", t.events)
    val cfg = config(t, maxOffsetsPerTrigger = 200)
    runBronzeToEnd(cfg)
    val before = bronze(cfg.bronzePath).count()
    assert(before == gen.summary.get("records_total").longValue)

    // Simulate a crash after the sink committed its files but before Spark committed the batch.
    val commits = Paths.get(cfg.checkpointRoot, "bronze-writer", "commits")
    val last    = Files.list(commits).iterator().asScala.filter(_.getFileName.toString.forall(_.isDigit)).maxBy(_.getFileName.toString.toInt)
    Files.delete(last)
    Files.deleteIfExists(last.resolveSibling(s".${last.getFileName}.crc")) // a crash leaves neither the file nor its checksum
    runBronzeToEnd(cfg)

    assert(Files.exists(last), "the batch was not re-executed")
    val after = bronze(cfg.bronzePath).select("kafka_partition", "kafka_offset")
    assert(after.count() == before && after.distinct().count() == before)
  }
}
