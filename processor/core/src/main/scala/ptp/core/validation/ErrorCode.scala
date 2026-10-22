package ptp.core.validation

/** Validation error codes. Lower precedence value = more fundamental = chosen as the primary code
  * (spec 02 §2). Names match contracts/dlq-record.v1.schema.json.
  */
sealed abstract class ErrorCode(val name: String, val precedence: Int)

object ErrorCode {
  case object InvalidEncoding          extends ErrorCode("INVALID_ENCODING", 1)
  case object PayloadTooLarge          extends ErrorCode("PAYLOAD_TOO_LARGE", 2)
  case object InvalidJson              extends ErrorCode("INVALID_JSON", 3)
  case object NotJsonObject            extends ErrorCode("NOT_JSON_OBJECT", 4)
  case object UnsupportedSchemaVersion extends ErrorCode("UNSUPPORTED_SCHEMA_VERSION", 5)
  case object MissingRequiredField     extends ErrorCode("MISSING_REQUIRED_FIELD", 6)
  case object InvalidFieldType         extends ErrorCode("INVALID_FIELD_TYPE", 7)
  case object UnknownEventType         extends ErrorCode("UNKNOWN_EVENT_TYPE", 8)
  case object InvalidFormat            extends ErrorCode("INVALID_FORMAT", 9)
  case object InvalidTimestamp         extends ErrorCode("INVALID_TIMESTAMP", 10)
  case object TimestampMissingOffset   extends ErrorCode("TIMESTAMP_MISSING_OFFSET", 11)
  case object MissingConditionalField  extends ErrorCode("MISSING_CONDITIONAL_FIELD", 12)
  case object EventTimeInFuture        extends ErrorCode("EVENT_TIME_IN_FUTURE", 13)
  case object EventTimeTooOld          extends ErrorCode("EVENT_TIME_TOO_OLD", 14)

  val all: Seq[ErrorCode] = Seq(
    InvalidEncoding, PayloadTooLarge, InvalidJson, NotJsonObject, UnsupportedSchemaVersion,
    MissingRequiredField, InvalidFieldType, UnknownEventType, InvalidFormat, InvalidTimestamp,
    TimestampMissingOffset, MissingConditionalField, EventTimeInFuture, EventTimeTooOld
  ).sortBy(_.precedence)

  def fromName(name: String): Option[ErrorCode] = all.find(_.name == name)
}

/** One violation. `field` is a JSON pointer (e.g. `/source/device_id`), or None for whole-record errors. */
final case class FieldError(code: ErrorCode, field: Option[String], message: String)

/** All violations found in one record (FR-VAL-03). */
final case class Rejection(errors: Seq[FieldError]) {
  require(errors.nonEmpty, "a rejection needs at least one error")
  def primary: ErrorCode = errors.map(_.code).minBy(_.precedence)
}
