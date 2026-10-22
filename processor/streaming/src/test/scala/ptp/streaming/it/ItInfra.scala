package ptp.streaming.it

import java.nio.file.{Files, Path, Paths}
import java.sql.{Connection, DriverManager}
import java.time.{Duration, Instant}
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.util.Using

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import ptp.core.validation.{ProcessingMode, ValidationConfig}
import ptp.streaming._

/** Shared, lazily started test infrastructure (one Kafka + one Postgres per test JVM). Images and
  * migrations are the same ones the Compose stack uses, read from the repository.
  */
object ItInfra {
  val repoRoot: Path = Paths.get(sys.props("ptp.repoRoot"))
  val mapper: JsonMapper = JsonMapper.builder().build()

  /** `repo:tag@sha256:…` from infra/docker-compose.yml, as a digest-pinned image reference. */
  private def composeImage(repo: String): DockerImageName = {
    val compose = Files.readString(repoRoot.resolve("infra/docker-compose.yml"))
    val ref = s"""image: ($repo):[\\w.-]+@(sha256:[0-9a-f]{64})""".r
      .findFirstMatchIn(compose).getOrElse(sys.error(s"image $repo not found in docker-compose.yml"))
    DockerImageName.parse(s"${ref.group(1)}@${ref.group(2)}").asCompatibleSubstituteFor(repo)
  }

  lazy val kafka: KafkaContainer = {
    val k = new KafkaContainer(composeImage("apache/kafka"))
    k.start()
    k
  }

  val Passwords: Map[String, String] =
    Map("ptp_migrator" -> "it-migrator", "ptp_stream_writer" -> "it-writer", "ptp_reader" -> "it-reader")

  lazy val postgres: PostgreSQLContainer = {
    val p = new PostgreSQLContainer(composeImage("postgres"))
      .withDatabaseName("ptp").withUsername("it_admin").withPassword("it-admin")
    p.start()
    bootstrap(p)
    p
  }

  private def bootstrap(p: PostgreSQLContainer): Unit = {
    // Same roles and grants as infra/postgres/bootstrap-roles.sql.
    Using.resource(DriverManager.getConnection(p.getJdbcUrl, p.getUsername, p.getPassword)) { c =>
      Passwords.foreach { case (role, pw) => c.createStatement().execute(s"CREATE ROLE $role LOGIN PASSWORD '$pw'") }
      c.createStatement().execute("GRANT CONNECT, CREATE ON DATABASE ptp TO ptp_migrator")
      c.createStatement().execute("GRANT CONNECT ON DATABASE ptp TO ptp_stream_writer, ptp_reader")
    }
    // Same migrations Flyway applies, in version order, as the migrator role.
    val migrations = Files.list(repoRoot.resolve("sql/postgres/migrations")).iterator().asScala.toSeq
      .sortBy(f => f.getFileName.toString.drop(1).takeWhile(_.isDigit).toInt)
    Using.resource(DriverManager.getConnection(p.getJdbcUrl, "ptp_migrator", Passwords("ptp_migrator"))) { c =>
      migrations.foreach(f => c.createStatement().execute(Files.readString(f)))
    }
  }

  def admin(): Connection = DriverManager.getConnection(postgres.getJdbcUrl, postgres.getUsername, postgres.getPassword)
  def writer: JdbcTarget  = JdbcTarget(postgres.getJdbcUrl, "ptp_stream_writer", Passwords("ptp_stream_writer"))

  def reset(): Unit = Using.resource(admin()) { c =>
    c.createStatement().execute("TRUNCATE tracking.package_status, tracking.processed_events, ops.sink_batches")
  }

  // ---- Kafka ----------------------------------------------------------------------------------

  final case class Topics(events: String, dlq: String)
  private val topicSeq = new AtomicInteger()

  /** Fresh topics per test, with the production partition counts (spec 02 §1). */
  def topics(): Topics = {
    val n = topicSeq.incrementAndGet()
    val t = Topics(s"it-events-$n", s"it-dlq-$n")
    Using.resource(AdminClient.create(Map[String, AnyRef]("bootstrap.servers" -> kafka.getBootstrapServers).asJava)) { admin =>
      admin.createTopics(Seq(new NewTopic(t.events, 6, 1.toShort), new NewTopic(t.dlq, 3, 1.toShort)).asJava).all().get(60, TimeUnit.SECONDS)
      // Bounded wait until every partition has a leader, so the first produce does not hit stale metadata.
      val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
      def ready = admin.describeTopics(Seq(t.events, t.dlq).asJava).allTopicNames().get(10, TimeUnit.SECONDS)
        .values().asScala.forall(_.partitions().asScala.forall(_.leader() != null))
      while (!ready) require(System.nanoTime() < deadline, s"topics $t have no leaders after 30 s")
    }
    t
  }

  def produce(topic: String, records: Seq[(String, Array[Byte])]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafka.getBootstrapServers)
    props.put("acks", "all")
    props.put("enable.idempotence", "true")
    Using.resource(new KafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer, new ByteArraySerializer)) { p =>
      records.foreach { case (k, v) => p.send(new ProducerRecord(topic, k.getBytes("UTF-8"), v)) }
      p.flush()
    }
  }

  /** Reads a whole topic: polls until everything up to the end offsets has been consumed (bounded). */
  def readAll(topic: String, timeout: Duration = Duration.ofSeconds(60)): Seq[JsonNode] = {
    val props = new Properties()
    props.put("bootstrap.servers", kafka.getBootstrapServers)
    props.put("enable.auto.commit", "false")
    Using.resource(new KafkaConsumer[Array[Byte], Array[Byte]](props, new ByteArrayDeserializer, new ByteArrayDeserializer)) { c =>
      val parts = c.partitionsFor(topic).asScala.map(p => new TopicPartition(topic, p.partition())).asJava
      c.assign(parts)
      c.seekToBeginning(parts)
      val total    = c.endOffsets(parts).values().asScala.map(_.longValue).sum
      val out      = Vector.newBuilder[JsonNode]
      var read     = 0L
      val deadline = System.nanoTime() + timeout.toNanos
      while (read < total && System.nanoTime() < deadline) {
        c.poll(java.time.Duration.ofMillis(500)).asScala.foreach { r => out += mapper.readTree(r.value()); read += 1 }
      }
      require(read == total, s"read $read of $total records from $topic within $timeout")
      out.result()
    }
  }

  // ---- generator -------------------------------------------------------------------------------

  final case class Generated(dir: Path, summary: JsonNode, manifest: Map[String, JsonNode]) {
    def lines(file: String): Seq[JsonNode] =
      Files.readAllLines(dir.resolve(file)).asScala.toSeq.map(l => mapper.readTree(l))
  }

  /** Runs the real Python generator against the test Kafka (same code path as `make generate`). */
  def generate(scenario: String, topic: String): Generated = {
    val python = repoRoot.resolve(".venv/bin/python")
    require(Files.isExecutable(python), s"$python not found: run `make venv` first")
    val out = Files.createTempDirectory("ptp-it-gen-")
    val pb = new ProcessBuilder(python.toString, "-m", "ptp_generator", "--scenario", repoRoot.resolve(scenario).toString,
      "--sink", "kafka", "--bootstrap-servers", kafka.getBootstrapServers, "--topic", topic, "--out-dir", out.toString)
      .directory(repoRoot.toFile)
      .redirectErrorStream(true)
      .redirectOutput(out.resolve("generator.log").toFile)
    pb.environment().put("PYTHONPATH", repoRoot.resolve("generator").toString)
    val proc = pb.start()
    require(proc.waitFor(180, TimeUnit.SECONDS), "generator did not finish within 180 s")
    require(proc.exitValue() == 0, s"generator failed: ${Files.readString(out.resolve("generator.log"))}")
    val manifest = Files.readAllLines(out.resolve("manifest.jsonl")).asScala
      .map(l => mapper.readTree(l)).map(n => n.get("package_id").textValue -> n).toMap
    Generated(out, mapper.readTree(out.resolve("summary.json").toFile), manifest)
  }

  // ---- Spark -----------------------------------------------------------------------------------

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("ptp-it")
    .master("local[4]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  def config(t: Topics, maxOffsetsPerTrigger: Long = 50000): ProcessorConfig = ProcessorConfig(
    kafkaBootstrapServers = kafka.getBootstrapServers,
    eventsTopic = t.events,
    dlqTopic = t.dlq,
    jdbc = writer,
    checkpointRoot = Files.createTempDirectory("ptp-it-ckpt-").toString,
    queryName = "status-processor",
    triggerIntervalSeconds = 1,
    maxOffsetsPerTrigger = maxOffsetsPerTrigger,
    startingOffsets = "earliest",
    mode = ProcessingMode.Live,
    validation = ValidationConfig(),
    processorVersion = ProcessorConfig.Version
  )

  /** Processes everything currently in the topic, then stops (Trigger.AvailableNow; no sleeps). */
  def runToEnd(processor: StatusProcessor, timeout: Duration = Duration.ofMinutes(3)): Unit = {
    val q = processor.start(Trigger.AvailableNow())
    try require(q.awaitTermination(timeout.toMillis), s"query did not finish within $timeout")
    finally if (q.isActive) q.stop()
  }

  // ---- Postgres readback ----------------------------------------------------------------------

  final case class StatusRow(status: String, eventId: String, eventTime: Instant, eventCount: Int, promised: Option[String])

  def statusRows(): Map[String, StatusRow] = Using.resource(admin()) { c =>
    val rs = c.createStatement().executeQuery(
      "SELECT package_id, current_status, status_event_id, status_event_time, event_count, promised_delivery_date FROM tracking.package_status")
    Iterator.continually(rs).takeWhile(_.next()).map { r =>
      r.getString(1) -> StatusRow(r.getString(2), r.getString(3), r.getObject(4, classOf[java.time.OffsetDateTime]).toInstant,
        r.getInt(5), Option(r.getString(6)))
    }.toMap
  }

  def scalar(sql: String): Long = Using.resource(admin()) { c =>
    val rs = c.createStatement().executeQuery(sql); rs.next(); rs.getLong(1)
  }

  def newTempDir(prefix: String): Path = Files.createTempDirectory(prefix + UUID.randomUUID().toString.take(8))
}
