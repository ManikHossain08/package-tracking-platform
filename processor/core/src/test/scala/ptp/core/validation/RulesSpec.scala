package ptp.core.validation

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.scalatest.funsuite.AnyFunSuite
import ptp.core.validation.ErrorCode._
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

/** TS-U-01, AC-3.3: each rule in spec 02 §2 rejects what it should and accepts what it should. */
class RulesSpec extends AnyFunSuite {

  private val json = JsonNodeFactory.instance

  /** (description, base sample, mutation, expected primary code) */
  private val rejections: Seq[(String, String, ObjectNode => Unit, ErrorCode)] = Seq(
    ("missing event_id", "created.json", _.remove("event_id"), MissingRequiredField),
    ("missing source", "created.json", _.remove("source"), MissingRequiredField),
    ("missing source.sequence", "created.json", _.get("source").asInstanceOf[ObjectNode].remove("sequence"), MissingRequiredField),
    ("null package_id", "created.json", _.putNull("package_id"), InvalidFieldType),
    ("numeric event_type", "created.json", _.put("event_type", 7), InvalidFieldType),
    ("source not an object", "created.json", _.put("source", "scanner"), InvalidFieldType),
    ("fractional sequence", "created.json", _.get("source").asInstanceOf[ObjectNode].put("sequence", 1.5), InvalidFieldType),
    ("details not an object", "exception.json", _.put("details", "x"), InvalidFieldType),
    ("attribute value not a string", "created.json", _.set[ObjectNode]("attributes", json.objectNode().put("n", 1)), InvalidFieldType),
    ("unknown event_type", "created.json", _.put("event_type", "LOST_IN_SPACE"), UnknownEventType),
    ("lowercase event_type", "created.json", _.put("event_type", "created"), UnknownEventType),
    ("event_id not a UUID", "created.json", _.put("event_id", "1-2-3-4-5"), InvalidFormat),
    ("package_id pattern", "created.json", _.put("package_id", "PKG-short"), InvalidFormat),
    ("facility_id pattern", "arrived_at_facility.json", _.put("facility_id", "DC-yul01"), InvalidFormat),
    ("vehicle_id pattern", "out_for_delivery.json", _.put("vehicle_id", "VH-12"), InvalidFormat),
    ("unknown source.system", "created.json", _.get("source").asInstanceOf[ObjectNode].put("system", "FAX"), InvalidFormat),
    ("empty device_id", "created.json", _.get("source").asInstanceOf[ObjectNode].put("device_id", ""), InvalidFormat),
    ("65-char device_id", "created.json", _.get("source").asInstanceOf[ObjectNode].put("device_id", "d" * 65), InvalidFormat),
    ("negative sequence", "created.json", _.get("source").asInstanceOf[ObjectNode].put("sequence", -1), InvalidFormat),
    ("schema_version not numeric", "created.json", _.put("schema_version", "one"), InvalidFormat),
    ("impossible promised date", "created.json", shipment(_).put("promised_delivery_date", "2026-02-30"), InvalidFormat),
    ("unknown service level", "created.json", shipment(_).put("service_level", "OVERNIGHT"), InvalidFormat),
    ("non-IANA time zone", "created.json", shipment(_).put("destination_timezone", "Mars/Olympus"), InvalidFormat),
    ("postal prefix pattern", "created.json", shipment(_).put("destination_postal_prefix", "h2x"), InvalidFormat),
    ("attempt_number out of range", "exception.json", details(_).put("attempt_number", 6), InvalidFormat),
    ("exception_code pattern", "exception.json", details(_).put("exception_code", "bad code"), InvalidFormat),
    ("21 attributes", "created.json", n => n.set[ObjectNode]("attributes", (1 to 21).foldLeft(json.objectNode())((o, i) => o.put(s"k$i", "v"))), InvalidFormat),
    ("257-char attribute", "created.json", _.set[ObjectNode]("attributes", json.objectNode().put("k", "v" * 257)), InvalidFormat),
    ("timestamp without offset", "created.json", _.put("event_time", "2026-10-19T08:00:00.000"), TimestampMissingOffset),
    ("emitted_at without offset", "created.json", _.put("emitted_at", "2026-10-19T08:00:00"), TimestampMissingOffset),
    ("date-only event_time", "created.json", _.put("event_time", "2026-10-19"), InvalidTimestamp),
    ("month 13", "created.json", _.put("event_time", "2026-13-01T00:00:00Z"), InvalidTimestamp),
    ("7 fractional digits", "created.json", _.put("event_time", "2026-10-19T08:00:00.1234567Z"), InvalidTimestamp),
    ("space separator", "created.json", _.put("event_time", "2026-10-19 08:00:00Z"), InvalidTimestamp),
    ("CREATED without shipment", "created.json", _.remove("shipment"), MissingConditionalField),
    ("shipment missing a field", "created.json", shipment(_).remove("service_level"), MissingConditionalField),
    ("ARRIVED with null facility", "arrived_at_facility.json", _.putNull("facility_id"), MissingConditionalField),
    ("DEPARTED without facility", "arrived_at_facility.json", n => { n.put("event_type", "DEPARTED_FACILITY"); n.remove("facility_id") }, MissingConditionalField),
    ("RETURNED without facility", "out_for_delivery.json", _.put("event_type", "RETURNED_TO_SENDER"), MissingConditionalField),
    ("DELIVERED without vehicle", "arrived_at_facility.json", _.put("event_type", "DELIVERED"), MissingConditionalField),
    ("PICKED_UP with neither", "created.json", _.put("event_type", "PICKED_UP"), MissingConditionalField),
    ("EXCEPTION without details", "exception.json", _.remove("details"), MissingConditionalField),
    ("ATTEMPTED without attempt_number", "out_for_delivery.json", _.put("event_type", "DELIVERY_ATTEMPTED"), MissingConditionalField)
  )

  private def shipment(n: ObjectNode): ObjectNode = n.get("shipment").asInstanceOf[ObjectNode]
  private def details(n: ObjectNode): ObjectNode  = n.get("details").asInstanceOf[ObjectNode]

  for ((name, sampleName, mutate, code) <- rejections) {
    test(s"rejects $name as ${code.name}", Req("FR-VAL-01")) {
      val node = base(sampleName)
      mutate(node)
      validate(node) match {
        case Left(rej) => assert(rej.primary == code, rej.errors)
        case Right(_)  => fail(s"accepted: $node")
      }
    }
  }

  private val acceptances: Seq[(String, String, ObjectNode => Unit)] = Seq(
    ("schema_version 1.7 (minor bump)", "created.json", _.put("schema_version", "1.7")),
    ("unknown extra fields", "created.json", _.put("future_field", "x")),
    ("PICKED_UP with facility only", "arrived_at_facility.json", _.put("event_type", "PICKED_UP")),
    ("PICKED_UP with vehicle only", "out_for_delivery.json", _.put("event_type", "PICKED_UP")),
    ("ATTEMPTED with attempt_number", "out_for_delivery.json", n => { n.put("event_type", "DELIVERY_ATTEMPTED"); n.set[ObjectNode]("details", json.objectNode().put("attempt_number", 2)) }),
    ("explicit nulls on optional fields", "out_for_delivery.json", n => { n.putNull("facility_id"); n.putNull("attributes"); n.putNull("details") }),
    ("uppercase UUID", "created.json", n => n.put("event_id", n.get("event_id").textValue.toUpperCase)),
    ("20 attributes of 256 chars", "created.json", n => n.set[ObjectNode]("attributes", (1 to 20).foldLeft(json.objectNode())((o, i) => o.put(s"k$i", "v" * 256)))),
    ("negative offset", "created.json", _.put("event_time", "2026-10-19T04:00:00-04:00")),
    ("no fractional seconds", "created.json", _.put("event_time", "2026-10-19T08:00:00Z")),
    ("64-char device_id", "created.json", _.get("source").asInstanceOf[ObjectNode].put("device_id", "d" * 64))
  )

  for ((name, sampleName, mutate) <- acceptances) {
    test(s"accepts $name", Req("FR-VAL-01")) {
      val node = base(sampleName)
      mutate(node)
      assert(validate(node).isRight, validate(node).left.map(_.errors))
    }
  }

  test("whole-record failures", Req("FR-VAL-01")) {
    def primary(value: Array[Byte]) = validate(value).left.map(_.primary)
    assert(primary(null) == Left(InvalidJson))
    assert(primary(Array[Byte](0xc3.toByte, 0x28)) == Left(InvalidEncoding))
    assert(primary(bytes("")) == Left(InvalidJson))
    assert(primary(bytes("""{"a":1}{"b":2}""")) == Left(InvalidJson), "trailing content")
    assert(primary(bytes("""{"a":1,"a":2}""")) == Left(InvalidJson), "duplicate keys")
    assert(primary(bytes("null")) == Left(NotJsonObject))
    assert(primary(bytes("[1,2]")) == Left(NotJsonObject))
    assert(primary(bytes("\"text\"")) == Left(NotJsonObject))
    val big = base(); big.put("pad", "x" * (64 * 1024))
    assert(primary(ValidationFixtures.bytes(big)) == Left(PayloadTooLarge))
  }

  test("another major schema version reports only UNSUPPORTED_SCHEMA_VERSION", Req("FR-VAL-01")) {
    val node = base(); node.put("schema_version", "2.0"); node.remove("event_id"); node.put("event_type", "NEW_V2_TYPE")
    val rej = validate(node).swap.getOrElse(fail("accepted"))
    assert(rej.errors.map(_.code) == Seq(UnsupportedSchemaVersion))
  }

  test("collects every violation and picks the primary by precedence (FR-VAL-03)", Req("FR-VAL-03")) {
    val node = base()
    node.put("event_type", "TELEPORTED")         // UNKNOWN_EVENT_TYPE (8)
    node.put("package_id", "pkg-1")              // INVALID_FORMAT (9)
    node.put("event_time", "2026-10-19T08:00:00") // TIMESTAMP_MISSING_OFFSET (11)
    val rej = validate(node).swap.getOrElse(fail("accepted"))
    assert(rej.errors.map(e => e.code -> e.field) .toSet == Set(
      UnknownEventType -> Some("/event_type"),
      InvalidFormat -> Some("/package_id"),
      TimestampMissingOffset -> Some("/event_time")
    ))
    assert(rej.primary == UnknownEventType)
    assert(rej.errors.forall(_.message.nonEmpty))
  }
}
