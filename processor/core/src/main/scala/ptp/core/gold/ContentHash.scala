package ptp.core.gold

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

/** ADR-0003 content hash: SHA-256 of (package_id, event_type, event_time, facility_id, vehicle_id), hex. */
object ContentHash {
  def of(packageId: String, eventType: String, eventTime: Instant, facilityId: Option[String], vehicleId: Option[String]): String = {
    val text   = Seq(packageId, eventType, eventTime.toString, facilityId.getOrElse(""), vehicleId.getOrElse("")).mkString("\u0000")
    val digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))
    digest.map(b => f"${b & 0xff}%02x").mkString
  }
}
