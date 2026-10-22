package ptp.core.validation

import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

import scala.jdk.CollectionConverters._

/** TS-U-02, AC-3.5: validate is total and its rejections are well-formed, for arbitrary input. */
class TotalitySpec extends AnyFunSuite with ScalaCheckPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 4000)

  private def wellFormed(value: Array[Byte]): Unit =
    validate(value) match {
      case Right(_) => ()
      case Left(rej) =>
        assert(rej.errors.nonEmpty)
        assert(rej.primary == rej.errors.map(_.code).minBy(_.precedence))
        assert(rej.errors.forall(_.message.nonEmpty))
    }

  // Trailing whitespace stripped, so any shorter prefix cuts into the JSON object itself.
  private val validBytes: Seq[Array[Byte]] =
    files(samples.resolve("valid")).map(p => new String(read(p), "UTF-8").trim.getBytes("UTF-8"))

  test("random bytes never throw", Req("FR-VAL-01")) {
    forAll(Gen.containerOf[Array, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) { bytes => wellFormed(bytes) }
  }

  test("truncated valid events never throw and are rejected", Req("FR-VAL-01")) {
    forAll(Gen.oneOf(validBytes), Gen.choose(0.0, 0.999)) { (bytes, fraction) =>
      val cut = bytes.take((bytes.length * fraction).toInt)
      wellFormed(cut)
      assert(validate(cut).isLeft)
    }
  }

  private val values: Gen[com.fasterxml.jackson.databind.JsonNode] = Gen.oneOf(
    Gen.const(mapper.nullNode()),
    Gen.choose(-5L, 5L).map(mapper.getNodeFactory.numberNode(_)),
    Gen.const(mapper.getNodeFactory.numberNode(1.5)),
    Gen.const(mapper.getNodeFactory.booleanNode(true)),
    Gen.alphaNumStr.map(mapper.getNodeFactory.textNode),
    Gen.const(mapper.createObjectNode()),
    Gen.const(mapper.createArrayNode())
  )

  test("field-level mutations of valid events never throw", Req("FR-VAL-01"), Req("FR-VAL-03")) {
    val samplesAsNodes = files(samples.resolve("valid")).map(p => mapper.readTree(p.toFile).asInstanceOf[ObjectNode])
    forAll(Gen.oneOf(samplesAsNodes), Gen.choose(0, 100), values, Gen.oneOf(true, false)) { (sample, pick, value, remove) =>
      val node   = sample.deepCopy()
      val fields = node.fieldNames().asScala.toSeq
      val field  = fields(pick % fields.size)
      if (remove) node.remove(field) else node.set[ObjectNode](field, value)
      wellFormed(bytes(node))
    }
  }
}
