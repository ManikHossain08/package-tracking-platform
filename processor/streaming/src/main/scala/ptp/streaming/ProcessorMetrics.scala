package ptp.streaming

import java.time.Instant

import io.prometheus.metrics.core.metrics.{Counter, Gauge, Histogram}
import io.prometheus.metrics.exporter.httpserver.HTTPServer
import org.apache.spark.sql.streaming.StreamingQueryListener

/** Streaming metrics (spec 04 §Metrics, SC-0007 A). Driver-side, default registry. Labels are bounded:
  * `query`, `error_code` (14 values), `stage`, `sink`. Never a package or event id.
  */
object ProcessorMetrics {
  val inputRows: Counter = Counter.builder().name("ptp_input_rows")
    .help("Records consumed from Kafka, per query").labelNames("query").register()
  val validEvents: Counter = Counter.builder().name("ptp_events_valid")
    .help("Records that passed validation").register()
  val rejectedEvents: Counter = Counter.builder().name("ptp_events_rejected")
    .help("Records sent to the DLQ, by primary error code").labelNames("error_code").register()
  val appliedEvents: Counter = Counter.builder().name("ptp_events_applied")
    .help("Valid events new to the platform (applied to package_status)").register()
  val duplicateEvents: Counter = Counter.builder().name("ptp_events_duplicate")
    .help("Valid events already applied before (idempotent no-ops)").labelNames("stage").register()
  val sinkFailures: Counter = Counter.builder().name("ptp_sink_failures")
    .help("Failed micro-batches, by failing sink/stage").labelNames("sink").register()
  val batchDuration: Histogram = Histogram.builder().name("ptp_batch_duration_seconds")
    .help("Wall time of a micro-batch, from start to committed sinks").labelNames("query")
    .classicOnly().classicUpperBounds(0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300).register()
  val offsetsBehind: Gauge = Gauge.builder().name("ptp_offsets_behind_latest")
    .help("Kafka offsets not yet consumed (max over partitions), from Spark query progress").labelNames("query").register()
  val lastProgress: Gauge = Gauge.builder().name("ptp_last_progress_timestamp_seconds")
    .help("Unix time of the last micro-batch progress event (Spark posts none while a query is idle)").labelNames("query").register()
  val triggerActive: Gauge = Gauge.builder().name("ptp_trigger_active_seconds")
    .help("How long the current trigger has been running (0 when idle); a large value means a hung micro-batch")
    .labelNames("query").register()
  val heartbeat: Gauge = Gauge.builder().name("ptp_heartbeat_timestamp_seconds")
    .help("Unix time of the driver's last status sample; stale while the endpoint answers = unresponsive driver").register()

  // Pre-initialise known label values at 0, so the series exist (and rate() works) before the first increment.
  Seq("classify", "dlq", "postgres", "record-batch", "after-sink").foreach(stage => sinkFailures.labelValues(stage))
  duplicateEvents.labelValues("sink")

  /** Serves /metrics. `bindAddress` 0.0.0.0 in containers; the port is never published on the host. */
  def serve(port: Int, bindAddress: String = "0.0.0.0"): HTTPServer =
    HTTPServer.builder().hostname(bindAddress).port(port).buildAndStart()

  /** Updates progress gauges from Spark's own progress events, for every query in the session. */
  val listener: StreamingQueryListener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()
    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      val p     = event.progress
      val query = Option(p.name).getOrElse("unnamed")
      lastProgress.labelValues(query).set(Instant.parse(p.timestamp).toEpochMilli / 1000.0)
      p.sources.headOption.flatMap(s => Option(s.metrics).flatMap(m => Option(m.get("maxOffsetsBehindLatest"))))
        .flatMap(v => scala.util.Try(v.toDouble).toOption)
        .foreach(v => offsetsBehind.labelValues(query).set(v))
    }
  }

  /** Samples every active query's status. Spark posts progress only when a micro-batch runs, so "time since last
    * progress" can't tell an idle query from a hung one; "how long has the current trigger been active" can.
    */
  def startHeartbeat(spark: org.apache.spark.sql.SparkSession, every: java.time.Duration): AutoCloseable = {
    val activeSince = new java.util.concurrent.ConcurrentHashMap[String, java.lang.Long]()
    val executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
      val t = new Thread(r, "ptp-metrics-heartbeat"); t.setDaemon(true); t
    }
    val sample: Runnable = () => {
      val now = System.currentTimeMillis()
      spark.streams.active.foreach { q =>
        val name = Option(q.name).getOrElse("unnamed")
        if (q.status.isTriggerActive) {
          val since = activeSince.computeIfAbsent(name, _ => now)
          triggerActive.labelValues(name).set((now - since) / 1000.0)
        } else {
          activeSince.remove(name)
          triggerActive.labelValues(name).set(0)
        }
      }
      heartbeat.set(now / 1000.0)
    }
    executor.scheduleAtFixedRate(sample, 0, every.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    () => executor.shutdownNow()
  }

  /** Records one committed micro-batch of the status processor. */
  def recordBatch(query: String, c: ApplyCounts, seconds: Double): Unit = {
    inputRows.labelValues(query).inc(c.input.toDouble)
    validEvents.inc(c.valid.toDouble)
    c.rejectedByCode.foreach { case (code, n) => rejectedEvents.labelValues(code).inc(n.toDouble) }
    appliedEvents.inc(c.applied.toDouble)
    duplicateEvents.labelValues("sink").inc(c.duplicates.toDouble)
    batchDuration.labelValues(query).observe(seconds)
  }
}
