package ptp.streaming.it

import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import ptp.lake.{LakeConfig, Retention, RetentionConfig, SilverBuild}
import ptp.streaming.PostgresStatusSink
import ptp.streaming.it.ItInfra._
import ptp.testing.{Integration, Req}

/** TS-O-03 / AC-7.11: retention removes only expired data; bronze stays readable and writable afterwards. */
class RetentionIT extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = reset()

  private val Today = LocalDate.now(ZoneOffset.UTC)

  private def copyDir(from: Path, to: Path): Unit =
    Files.walk(from).iterator().asScala.toSeq.foreach { p =>
      val target = to.resolve(from.relativize(p).toString)
      if (Files.isDirectory(p)) Files.createDirectories(target) else Files.copy(p, target)
    }

  private def bronzeCount(lc: LakeConfig) = ptp.streaming.BronzeRow.read(spark, lc.bronzePath).count()

  test("AC-7.11 expired partitions are removed; data inside retention is untouched", Integration, Req("NFR-08")) {
    val t   = topics()
    val gen = generate("bench/scenarios/clean.yaml", t.events)
    val cfg = config(t)
    runBothToEnd(cfg)
    val lc = LakeConfig(cfg.lakeRoot, batchWriter, 128L * 1024 * 1024, 7, 30, kafka.getBootstrapServers, t.events, t.dlq)
    SilverBuild.run(spark, lc)

    // Plant expired data in all three stores.
    val oldProcessed = Today.minusDays(40)
    PostgresStatusSink.ensurePartitions(writer, Seq(oldProcessed))
    Using.resource(admin())(_.createStatement().execute(
      s"INSERT INTO tracking.processed_events VALUES ('${UUID.randomUUID()}', '$oldProcessed', 'PKG-OLD', 0, now())"))
    copyDir(Paths.get(cfg.bronzePath, s"ingest_date=$Today"), Paths.get(cfg.bronzePath, s"ingest_date=${Today.minusDays(100)}"))
    val someSilver = Files.list(Paths.get(lc.silverPath)).iterator().asScala.toSeq.head
    copyDir(someSilver, Paths.get(lc.silverPath, s"event_date=${Today.minusDays(500)}"))

    val processedBefore = scalar(s"SELECT count(*) FROM tracking.processed_events WHERE event_date > '$oldProcessed'")
    val bronzeBefore    = bronzeCount(lc)
    val silverBefore    = spark.read.parquet(lc.silverPath).count() - spark.read.parquet(someSilver.toString).count()

    val result = Retention.run(spark, lc, Today)
    assert(result.droppedProcessedPartitions == Seq(s"processed_events_${oldProcessed.toString.replace("-", "")}"))
    assert(result.deletedBronze == Seq(s"ingest_date=${Today.minusDays(100)}"))
    assert(result.deletedSilver == Seq(s"event_date=${Today.minusDays(500)}"))
    assert(scalar(s"SELECT count(*) FROM tracking.processed_events WHERE event_date > '$oldProcessed'") == processedBefore)
    assert(scalar("SELECT count(*) FROM tracking.processed_events WHERE package_id = 'PKG-OLD'") == 0)
    assert(bronzeCount(lc) == bronzeBefore && bronzeBefore == gen.summary.get("records_total").longValue)
    assert(spark.read.parquet(lc.silverPath).count() == silverBefore)
    assert(Retention.run(spark, lc, Today).deletedBronze.isEmpty, "a second run finds nothing to expire")
  }

  test("AC-7.11 deleting a bronze partition the sink log still references keeps bronze readable and writable", Integration,
    Req("NFR-08"), Req("FR-STO-01")) {
    val t   = topics()
    generate("bench/scenarios/clean.yaml", t.events)
    val cfg = config(t)
    runBronzeToEnd(cfg)
    val lc = LakeConfig(cfg.lakeRoot, batchWriter, 128L * 1024 * 1024, 7, 30, kafka.getBootstrapServers, t.events, t.dlq)
    assert(bronzeCount(lc) > 0)

    // Evaluate as if 91 days had passed: today's ingest partition (referenced by _spark_metadata) expires.
    val result = Retention.run(spark, lc, Today.plusDays(91), RetentionConfig(bronzeDays = 90, silverDays = 100000, processedEventsDays = 100000))
    assert(result.deletedBronze == Seq(s"ingest_date=$Today"))
    intercept[Exception](spark.read.parquet(lc.bronzePath).count()) // without the option the missing files are an error
    assert(bronzeCount(lc) == 0, "BronzeRow.read sees an empty bronze, not an error")

    // The bronze writer keeps going; new data is readable.
    produce(t.events, Seq(eventJson("PKG-AFTERRETENT1", "PICKED_UP", Instant.now(), UUID.randomUUID().toString,
      ""","vehicle_id":"VH-000123"""", system = "VEHICLE_APP")))
    runBronzeToEnd(cfg)
    assert(bronzeCount(lc) == 1)
    SilverBuild.run(spark, lc) // silver tolerates the missing files too
  }
}
