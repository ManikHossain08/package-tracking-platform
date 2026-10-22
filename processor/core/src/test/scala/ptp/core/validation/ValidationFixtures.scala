package ptp.core.validation

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.Instant

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import ptp.core.model.PackageEvent

/** Shared test inputs: repository paths, a fixed clock and JSON mutation helpers. */
object ValidationFixtures {
  val repoRoot: Path = Paths.get(sys.props.getOrElse("ptp.repoRoot", sys.error("ptp.repoRoot not set (run via sbt)")))
  val samples: Path  = repoRoot.resolve("contracts/samples")

  /** Fixed processing time, after every sample's event_time. */
  val Now: Instant = Instant.parse("2026-10-22T00:00:00Z")

  val validator = new EventValidator()
  val mapper: JsonMapper = JsonMapper.builder().build()

  def files(dir: Path): Seq[Path] =
    Files.list(dir).iterator().asScala.filter(Files.isRegularFile(_)).toSeq.sortBy(_.getFileName.toString)

  def read(path: Path): Array[Byte] = Files.readAllBytes(path)

  def sample(name: String): ObjectNode =
    mapper.readTree(samples.resolve(s"valid/$name").toFile).asInstanceOf[ObjectNode]

  /** A valid event to mutate: the CREATED sample (has shipment) unless another is named. */
  def base(name: String = "created.json"): ObjectNode = sample(name).deepCopy()

  def bytes(node: ObjectNode): Array[Byte] = mapper.writeValueAsBytes(node)
  def bytes(text: String): Array[Byte]     = text.getBytes(UTF_8)

  def validate(value: Array[Byte], now: Instant = Now, mode: ProcessingMode = ProcessingMode.Live): Either[Rejection, PackageEvent] =
    validator.validate(value, now, mode)

  def validate(node: ObjectNode): Either[Rejection, PackageEvent] = validate(bytes(node))
}
