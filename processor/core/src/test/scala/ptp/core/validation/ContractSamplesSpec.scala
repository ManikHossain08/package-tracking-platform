package ptp.core.validation

import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.funsuite.AnyFunSuite
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

import scala.jdk.CollectionConverters._

/** TS-C-01 (Scala side), AC-3.1: the processor agrees with contracts/samples. */
class ContractSamplesSpec extends AnyFunSuite {

  private val expected: Map[String, String] = {
    val node = mapper.readTree(samples.resolve("invalid/expected.json").toFile).asInstanceOf[ObjectNode]
    node.properties().asScala.collect { case e if !e.getKey.startsWith("_") => e.getKey -> e.getValue.textValue }.toMap
  }

  for (path <- files(samples.resolve("valid"))) {
    test(s"accepts valid sample ${path.getFileName}", Req("FR-VAL-01")) {
      assert(validate(read(path)).isRight, validate(read(path)).left.map(_.errors))
    }
  }

  private val invalid = files(samples.resolve("invalid")).filterNot(_.getFileName.toString == "expected.json")

  test("every invalid sample has an expected code", Req("FR-VAL-01")) {
    assert(invalid.map(_.getFileName.toString).toSet == expected.keySet)
  }

  for (path <- invalid) {
    val name = path.getFileName.toString
    test(s"rejects invalid sample $name with ${expected.getOrElse(name, "?")}", Req("FR-VAL-01")) {
      validate(read(path)) match {
        case Left(rejection) => assert(rejection.primary.name == expected(name), rejection.errors)
        case Right(event)    => fail(s"$name was accepted: $event")
      }
    }
  }

  test("every code in the DLQ contract exists in the validator and vice versa", Req("FR-DLQ-01")) {
    val schema = mapper.readTree(repoRoot.resolve("contracts/dlq-record.v1.schema.json").toFile)
    val contractCodes = schema.at("/$defs/errorCode/enum").elements().asScala.map(_.textValue).toSet
    assert(contractCodes == ErrorCode.all.map(_.name).toSet)
  }
}
