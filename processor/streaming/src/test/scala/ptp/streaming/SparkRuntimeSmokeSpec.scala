package ptp.streaming

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import ptp.testing.Req

/** Step 1 (AC-0.5): proves the pinned Spark runtime works locally on Java 17 before any feature code. */
class SparkRuntimeSmokeSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("ptp-smoke")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("runs the pinned Spark version", Req("FR-DEV-01")) {
    assert(spark.version == "4.1.3")
  }

  test("Kafka source connector is on the classpath", Req("FR-DEV-01")) {
    assert(Class.forName("org.apache.spark.sql.kafka010.KafkaSourceProvider") != null)
  }

  test("a streaming query processes all available input deterministically", Req("FR-DEV-01")) {
    implicit val sqlContext: org.apache.spark.sql.SQLContext = spark.sqlContext
    val input = MemoryStream[Int](Encoders.scalaInt, sqlContext)
    val query = input.toDS().writeStream
      .format("memory")
      .queryName("smoke_out")
      .outputMode("append")
      .start()
    try {
      input.addData(1, 2, 3)
      query.processAllAvailable() // explicit synchronisation, no sleeps (spec 07)
      val rows = spark.table("smoke_out").as(Encoders.scalaInt).collect().sorted.toSeq
      assert(rows == Seq(1, 2, 3))
    } finally query.stop()
  }
}
