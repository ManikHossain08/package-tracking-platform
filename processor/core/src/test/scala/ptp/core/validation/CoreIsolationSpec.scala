package ptp.core.validation

import org.scalatest.funsuite.AnyFunSuite
import ptp.testing.Req

/** TS-U-14, AC-3.7: processor-core stays free of Spark/Kafka/JDBC, and matches Spark's Jackson. */
class CoreIsolationSpec extends AnyFunSuite {

  private def onClasspath(name: String): Boolean =
    try { Class.forName(name); true }
    catch { case _: ClassNotFoundException => false }

  test("no Spark, Kafka or JDBC driver on the core classpath", Req("FR-DEV-01")) {
    assert(!onClasspath("org.apache.spark.SparkContext"))
    assert(!onClasspath("org.apache.kafka.clients.producer.KafkaProducer"))
    assert(!onClasspath("org.postgresql.Driver"))
  }

  test("Jackson version equals Spark 4.1.3's", Req("NFR-07")) {
    assert(com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION.toString == "2.21.2")
  }
}
