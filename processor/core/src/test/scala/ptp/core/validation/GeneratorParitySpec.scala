package ptp.core.validation

import java.time.Instant

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

/** TS-C-04, AC-3.2: Scala validation agrees with the Python generator's labels on a committed snapshot
  * (regenerate with `make fixtures`).
  */
class GeneratorParitySpec extends AnyFunSuite {

  private val dir = repoRoot.resolve("processor/core/src/test/resources/fixtures/generator")

  private case class Rec(index: Int, value: Array[Byte])

  private val records: Seq[Rec] = files(dir).find(_.getFileName.toString == "records.jsonl").toSeq.flatMap { p =>
    java.nio.file.Files.readAllLines(p).asScala.map { line =>
      val node = mapper.readTree(line)
      Rec(node.get("index").intValue, node.get("value").textValue.getBytes("UTF-8"))
    }
  }

  private val expectedCodes: Map[Int, String] =
    java.nio.file.Files.readAllLines(dir.resolve("malformed.jsonl")).asScala.map { line =>
      val node = mapper.readTree(line)
      node.get("index").intValue -> node.get("expected_error_code").textValue
    }.toMap

  /** The generator stamps emitted_at with its clock; time-relative malformed records are relative to it. */
  private val generatedAt: Instant = records.iterator
    .flatMap(r => scala.util.Try(mapper.readTree(r.value).get("emitted_at").textValue).toOption)
    .map(Instant.parse)
    .next()

  test("snapshot contains every malformed type", Req("FR-GEN-02")) {
    assert(records.nonEmpty)
    assert(expectedCodes.values.toSet.size == 10, expectedCodes.values.toSet)
  }

  test("every generated record is classified exactly as the generator labelled it", Req("FR-VAL-01"), Req("FR-VAL-02")) {
    val mismatches = records.flatMap { r =>
      (validate(r.value, generatedAt), expectedCodes.get(r.index)) match {
        case (Right(_), None)                                   => None
        case (Left(rej), Some(code)) if rej.primary.name == code => None
        case (result, expected) =>
          Some(s"#${r.index}: expected ${expected.getOrElse("valid")}, got ${result.left.map(_.errors)}")
      }
    }
    assert(mismatches.isEmpty, mismatches.take(10).mkString("\n"))
  }
}
