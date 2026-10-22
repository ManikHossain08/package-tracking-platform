package ptp.core.dlq

import java.time.Instant

import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import org.scalatest.funsuite.AnyFunSuite
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

import scala.jdk.CollectionConverters._

/** FR-DLQ-01: DLQ records built for every invalid sample validate against the DLQ contract. */
class DlqRecordSpec extends AnyFunSuite {

  private val schema = JsonSchemaFactory
    .getInstance(SpecVersion.VersionFlag.V202012)
    .getSchema(repoRoot.resolve("contracts/dlq-record.v1.schema.json").toUri)

  private val source    = SourceCoordinates("package-events.v1", 4, 182734L, Instant.parse("2026-10-21T09:00:01.050Z"), Some("PKG-A1B2C3D4E5F6"))
  private val processor = ProcessorInfo("status-processor", "0.1.0", "run-1", 42L)

  private def dlq(raw: Array[Byte]) = {
    val rejection = validate(raw).swap.getOrElse(fail("expected a rejection"))
    DlqRecord.toJson(raw, rejection, source, processor, Instant.parse("2026-10-21T09:00:03Z"))
  }

  private def schemaErrors(json: String) = schema.validate(mapper.readTree(json)).asScala.map(_.getMessage).toSeq

  for (path <- files(samples.resolve("invalid")).filterNot(_.getFileName.toString == "expected.json")) {
    test(s"DLQ record for ${path.getFileName} is schema-valid and carries the source coordinates", Req("FR-DLQ-01")) {
      val json = dlq(read(path))
      assert(schemaErrors(json).isEmpty)
      val node = mapper.readTree(json)
      assert(node.at("/source/offset").longValue == 182734L && node.at("/source/partition").intValue == 4)
      assert(node.at("/raw_value").textValue == new String(read(path), "UTF-8"))
      assert(node.at("/errors").size >= 1)
    }
  }

  test("non-UTF-8 payloads are base64-encoded", Req("FR-DLQ-01")) {
    val raw  = Array[Byte](0xc3.toByte, 0x28, 0x7b)
    val node = mapper.readTree(dlq(raw))
    assert(schemaErrors(node.toString).isEmpty)
    assert(node.at("/raw_value_encoding").textValue == "base64")
    assert(java.util.Base64.getDecoder.decode(node.at("/raw_value").textValue).sameElements(raw))
    assert(node.at("/error_code").textValue == "INVALID_ENCODING")
  }
}
