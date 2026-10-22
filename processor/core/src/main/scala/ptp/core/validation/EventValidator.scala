package ptp.core.validation

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{Duration, Instant, LocalDate, OffsetDateTime, ZoneId}
import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}

import ptp.core.model._
import ptp.core.validation.ErrorCode._

sealed trait ProcessingMode
object ProcessingMode {

  /** Normal streaming: both time bounds apply. */
  case object Live extends ProcessingMode

  /** Replay from bronze: historical events are expected, so the lower age bound is off (FR-VAL-02). */
  case object Replay extends ProcessingMode
}

/** Runtime bounds (FR-VAL-02, ADR-0005). Defaults match spec 08. */
final case class ValidationConfig(
    futureSkew: Duration = Duration.ofMinutes(5),
    liveMaxAge: Duration = Duration.ofDays(30),
    maxPayloadBytes: Int = 64 * 1024
)

/** Validates one Kafka record value against package-event v1 (spec 02 §2).
  *
  * Total: never throws for any input. Every violation found is reported (FR-VAL-03); checks that
  * cannot run because an earlier stage failed (encoding, JSON parsing, unsupported major) are skipped.
  * Thread-safe; create once and share.
  */
final class EventValidator(config: ValidationConfig = ValidationConfig()) {

  private val mapper: JsonMapper = JsonMapper
    .builder()
    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .build()

  def validate(value: Array[Byte], processingTime: Instant, mode: ProcessingMode): Either[Rejection, PackageEvent] =
    if (value == null) reject(InvalidJson, "record value is null (Kafka tombstone)")
    else
      decodeUtf8(value) match {
        case None => reject(InvalidEncoding, "record value is not valid UTF-8")
        case Some(_) if value.length > config.maxPayloadBytes =>
          reject(PayloadTooLarge, s"record value is ${value.length} bytes; the limit is ${config.maxPayloadBytes}")
        case Some(text) => parse(text).flatMap(obj => new RecordCheck(obj, processingTime, mode).run())
      }

  private def reject(code: ErrorCode, message: String): Either[Rejection, Nothing] =
    Left(Rejection(Seq(FieldError(code, None, message))))

  private def decodeUtf8(bytes: Array[Byte]): Option[String] =
    try
      Some(
        StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes))
          .toString
      )
    catch { case _: CharacterCodingException => None }

  private def parse(text: String): Either[Rejection, ObjectNode] = {
    val tree =
      try Right(mapper.readTree(text))
      catch { case NonFatal(e) => Left(firstLine(e.getMessage)) }
    tree match {
      case Left(reason)                    => reject(InvalidJson, s"not valid JSON: $reason")
      case Right(node) if node == null || node.isMissingNode => reject(InvalidJson, "empty value")
      case Right(obj: ObjectNode)          => Right(obj)
      case Right(node)                     => reject(NotJsonObject, s"expected a JSON object, got ${kind(node)}")
    }
  }

  /** Field-level checks for one parsed object. Not thread-safe; one instance per record. */
  private final class RecordCheck(root: ObjectNode, now: Instant, mode: ProcessingMode) {
    private val errors = ListBuffer.empty[FieldError]

    private def err(code: ErrorCode, ptr: String, msg: String): Unit = errors += FieldError(code, Some(ptr), msg)

    def run(): Either[Rejection, PackageEvent] = {
      val schemaVersion = text(root, "schema_version", "/schema_version")
      schemaVersion match {
        case Some(v) if OtherMajorVersion.matches(v) && !SupportedVersion.matches(v) =>
          // Fields of another major are not checked against v1 rules (spec 02 §2, validation details).
          Left(Rejection(Seq(FieldError(UnsupportedSchemaVersion, Some("/schema_version"),
            s"schema_version '${clip(v)}' is not supported by this v1 consumer"))))
        case Some(v) if !SupportedVersion.matches(v) =>
          err(InvalidFormat, "/schema_version", s"schema_version '${clip(v)}' must look like 1.<minor>")
          checkV1(schemaVersion)
        case _ => checkV1(schemaVersion)
      }
    }

    private def checkV1(schemaVersion: Option[String]): Either[Rejection, PackageEvent] = {
      val eventId = text(root, "event_id", "/event_id")
        .flatMap(matching(_, Uuid, "/event_id", "a UUID"))
        .map(_.toLowerCase(Locale.ROOT))
      val packageId = text(root, "package_id", "/package_id").flatMap(matching(_, PackageId, "/package_id", "PKG- + 12 [A-Z0-9]"))
      val eventType = text(root, "event_type", "/event_type").flatMap { t =>
        val parsed = EventType.fromName(t)
        if (parsed.isEmpty) err(UnknownEventType, "/event_type", s"event_type '${clip(t)}' is not one of ${EventType.all.map(_.name).mkString(", ")}")
        parsed
      }
      val eventTime  = timestamp("event_time")
      val emittedAt  = timestamp("emitted_at")
      val source     = sourceField()
      val facilityId = optionalText(root, "facility_id", "/facility_id").flatMap(matching(_, FacilityId, "/facility_id", "DC-AAA00"))
      val vehicleId  = optionalText(root, "vehicle_id", "/vehicle_id").flatMap(matching(_, VehicleId, "/vehicle_id", "VH-000000"))
      val shipment   = shipmentField()
      val details    = detailsField()
      val attributes = attributesField()

      eventType.foreach(conditionalFields)
      eventTime.foreach(timeBounds)

      if (errors.nonEmpty) Left(Rejection(errors.toList))
      else
        (eventId, schemaVersion, packageId, eventType, eventTime, emittedAt, source) match {
          case (Some(id), Some(v), Some(pkg), Some(tpe), Some(et), Some(ea), Some(src)) =>
            Right(PackageEvent(id, v, pkg, tpe, et, ea, src, facilityId, vehicleId, shipment, details, attributes))
          case _ =>
            // Unreachable: every None above records an error. Reported, not thrown, to keep validate total.
            Left(Rejection(Seq(FieldError(InvalidFormat, None, "internal: incomplete event without errors"))))
        }
    }

    // ---- field helpers -------------------------------------------------------------------------

    /** A required string field. Absent → `missing`; null or non-string → INVALID_FIELD_TYPE. */
    private def text(obj: ObjectNode, name: String, ptr: String, missing: ErrorCode = MissingRequiredField): Option[String] =
      obj.get(name) match {
        case null                                   => err(missing, ptr, s"required field '$name' is missing"); None
        case n if n.isTextual                       => Some(n.textValue)
        case n if n.isNull && missing == MissingConditionalField => err(missing, ptr, s"'$name' is required but null"); None
        case n                                      => err(InvalidFieldType, ptr, s"'$name' must be a string, got ${kind(n)}"); None
      }

    /** An optional string field: absent or null → None. */
    private def optionalText(obj: ObjectNode, name: String, ptr: String): Option[String] =
      obj.get(name) match {
        case null                      => None
        case n if n.isNull             => None
        case n if n.isTextual          => Some(n.textValue)
        case n                         => err(InvalidFieldType, ptr, s"'$name' must be a string or null, got ${kind(n)}"); None
      }

    private def optionalObject(obj: ObjectNode, name: String, ptr: String): Option[ObjectNode] =
      obj.get(name) match {
        case null              => None
        case n if n.isNull     => None
        case o: ObjectNode     => Some(o)
        case n                 => err(InvalidFieldType, ptr, s"'$name' must be an object, got ${kind(n)}"); None
      }

    private def matching(value: String, pattern: Regex, ptr: String, expected: String): Option[String] =
      if (pattern.matches(value)) Some(value)
      else { err(InvalidFormat, ptr, s"'${clip(value)}' does not match $expected"); None }

    private def absentOrNull(obj: ObjectNode, name: String): Boolean = {
      val n = obj.get(name)
      n == null || n.isNull
    }

    private def integral(obj: ObjectNode, name: String, ptr: String, min: Long, max: Long, required: Boolean): Option[Long] =
      obj.get(name) match {
        case null if required => err(MissingRequiredField, ptr, s"required field '$name' is missing"); None
        case null             => None
        case n if n.isNull && !required => None
        case n if n.isIntegralNumber && n.canConvertToLong =>
          val v = n.longValue
          if (v >= min && v <= max) Some(v)
          else { err(InvalidFormat, ptr, s"'$name' = $v is outside [$min, $max]"); None }
        case n => err(InvalidFieldType, ptr, s"'$name' must be an integer, got ${kind(n)}"); None
      }

    // ---- fields --------------------------------------------------------------------------------

    private def timestamp(name: String): Option[Instant] =
      text(root, name, s"/$name").flatMap {
        case s if WithOffset.matches(s) =>
          try Some(OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant)
          catch { case _: DateTimeParseException => err(InvalidTimestamp, s"/$name", s"'${clip(s)}' is not a valid date-time"); None }
        case s if WithoutOffset.matches(s) =>
          err(TimestampMissingOffset, s"/$name", s"'${clip(s)}' has no UTC offset; use Z or ±hh:mm"); None
        case s =>
          err(InvalidTimestamp, s"/$name", s"'${clip(s)}' is not RFC 3339 (yyyy-MM-ddTHH:mm:ss[.ffffff]±hh:mm)"); None
      }

    private def sourceField(): Option[Source] =
      root.get("source") match {
        case null => err(MissingRequiredField, "/source", "required field 'source' is missing"); None
        case o: ObjectNode =>
          val system = text(o, "system", "/source/system").flatMap { s =>
            val parsed = SourceSystem.fromName(s)
            if (parsed.isEmpty) err(InvalidFormat, "/source/system", s"source.system '${clip(s)}' is not one of ${SourceSystem.all.map(_.name).mkString(", ")}")
            parsed
          }
          val device = text(o, "device_id", "/source/device_id").filter { d =>
            val ok = d.nonEmpty && d.length <= 64
            if (!ok) err(InvalidFormat, "/source/device_id", s"device_id length ${d.length} is outside 1..64")
            ok
          }
          val sequence = integral(o, "sequence", "/source/sequence", 0L, Long.MaxValue, required = true)
          for (s <- system; d <- device; q <- sequence) yield Source(s, d, q)
        case n => err(InvalidFieldType, "/source", s"'source' must be an object, got ${kind(n)}"); None
      }

    private def shipmentField(): Option[Shipment] =
      optionalObject(root, "shipment", "/shipment").flatMap { o =>
        def req(name: String) = text(o, name, s"/shipment/$name", missing = MissingConditionalField)
        val date = req("promised_delivery_date").flatMap { d =>
          val parsed =
            if (IsoDate.matches(d)) try Some(LocalDate.parse(d)) catch { case _: DateTimeParseException => None }
            else None
          if (parsed.isEmpty) err(InvalidFormat, "/shipment/promised_delivery_date", s"'${clip(d)}' is not a valid yyyy-MM-dd date")
          parsed
        }
        val level = req("service_level").flatMap { l =>
          val parsed = ServiceLevel.fromName(l)
          if (parsed.isEmpty) err(InvalidFormat, "/shipment/service_level", s"service_level '${clip(l)}' is not one of ${ServiceLevel.all.map(_.name).mkString(", ")}")
          parsed
        }
        val origin = req("origin_facility_id").flatMap(matching(_, FacilityId, "/shipment/origin_facility_id", "DC-AAA00"))
        val postal = req("destination_postal_prefix").flatMap(matching(_, PostalPrefix, "/shipment/destination_postal_prefix", "3 chars [A-Z0-9]"))
        val zone = req("destination_timezone").flatMap { z =>
          val parsed = if (z.length <= 64 && IanaZones.contains(z)) Some(ZoneId.of(z)) else None
          if (parsed.isEmpty) err(InvalidFormat, "/shipment/destination_timezone", s"'${clip(z)}' is not an IANA time zone")
          parsed
        }
        for (d <- date; l <- level; og <- origin; p <- postal; z <- zone) yield Shipment(d, l, og, p, z)
      }

    private def detailsField(): Option[Details] =
      optionalObject(root, "details", "/details").map { o =>
        val code = optionalText(o, "exception_code", "/details/exception_code")
          .flatMap(matching(_, ExceptionCode, "/details/exception_code", "3-40 chars [A-Z_]"))
        val attempt = integral(o, "attempt_number", "/details/attempt_number", 1L, 5L, required = false).map(_.toInt)
        Details(code, attempt)
      }

    private def attributesField(): Map[String, String] =
      optionalObject(root, "attributes", "/attributes") match {
        case None => Map.empty
        case Some(o) =>
          if (o.size > 20) err(InvalidFormat, "/attributes", s"attributes has ${o.size} entries; the limit is 20")
          o.properties().asScala.flatMap { entry =>
            val (k, v) = (entry.getKey, entry.getValue)
            val ptr = s"/attributes/${escapePointer(k)}"
            if (!v.isTextual) { err(InvalidFieldType, ptr, s"attribute values must be strings, got ${kind(v)}"); None }
            else if (v.textValue.length > 256) { err(InvalidFormat, ptr, s"attribute value length ${v.textValue.length} exceeds 256"); None }
            else Some(k -> v.textValue)
          }.toMap
      }

    /** Fields required only because of event_type (spec 02 §2). */
    private def conditionalFields(tpe: EventType): Unit = {
      def need(name: String): Unit =
        if (absentOrNull(root, name)) err(MissingConditionalField, s"/$name", s"'$name' is required for ${tpe.name}")
      if (EventType.requiresFacility(tpe)) need("facility_id")
      if (EventType.requiresVehicle(tpe)) need("vehicle_id")
      if (tpe == EventType.PickedUp && absentOrNull(root, "facility_id") && absentOrNull(root, "vehicle_id"))
        err(MissingConditionalField, "/vehicle_id", "PICKED_UP requires vehicle_id or facility_id")
      if (tpe == EventType.Created) need("shipment")
      def needDetail(name: String): Unit = {
        def missing(): Unit = err(MissingConditionalField, s"/details/$name", s"details.$name is required for ${tpe.name}")
        root.get("details") match {
          case o: ObjectNode            => if (absentOrNull(o, name)) missing()
          case n if n == null || n.isNull => missing()
          case _                        => () // a non-object `details` is already INVALID_FIELD_TYPE
        }
      }
      if (tpe == EventType.Exception) needDetail("exception_code")
      if (tpe == EventType.DeliveryAttempted) needDetail("attempt_number")
    }

    /** Runtime bounds; inclusive at the limits (spec 02 §2, FR-VAL-02). */
    private def timeBounds(eventTime: Instant): Unit = {
      val latest = now.plus(config.futureSkew)
      if (eventTime.isAfter(latest))
        err(EventTimeInFuture, "/event_time", s"event_time $eventTime is more than ${config.futureSkew} after processing time $now")
      if (mode == ProcessingMode.Live) {
        val earliest = now.minus(config.liveMaxAge)
        if (eventTime.isBefore(earliest))
          err(EventTimeTooOld, "/event_time", s"event_time $eventTime is more than ${config.liveMaxAge} before processing time $now (live mode; use replay mode for history)")
      }
    }
  }

  // ---- patterns (spec 02 §2, contracts/package-event.v1.schema.json) -----------------------------

  private val SupportedVersion  = """1\.\d+""".r
  private val OtherMajorVersion = """\d+\.\d+""".r
  private val Uuid              = """[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""".r
  private val PackageId         = """PKG-[A-Z0-9]{12}""".r
  private val FacilityId        = """DC-[A-Z]{3}[0-9]{2}""".r
  private val VehicleId         = """VH-[0-9]{6}""".r
  private val PostalPrefix      = """[A-Z0-9]{3}""".r
  private val ExceptionCode     = """[A-Z_]{3,40}""".r
  private val IsoDate           = """\d{4}-\d{2}-\d{2}""".r
  private val WithOffset        = """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})""".r
  private val WithoutOffset     = """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?""".r
  private val IanaZones: Set[String] = ZoneId.getAvailableZoneIds.asScala.toSet

  private def kind(n: JsonNode): String = n.getNodeType.toString.toLowerCase(Locale.ROOT)
  private def clip(s: String): String  = if (s.length <= 40) s else s.take(40) + "…"
  private def firstLine(s: String): String = Option(s).map(_.linesIterator.nextOption().getOrElse("")).getOrElse("").take(160)
  private def escapePointer(key: String): String = key.replace("~", "~0").replace("/", "~1")
}
