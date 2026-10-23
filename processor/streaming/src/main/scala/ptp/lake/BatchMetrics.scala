package ptp.lake

import scala.util.control.NonFatal

import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.exporter.pushgateway.PushGateway
import io.prometheus.metrics.model.registry.PrometheusRegistry

import ptp.streaming.StructuredLog

/** Batch-job metrics pushed to the Pushgateway (SC-0007 B), grouped by job="ptp" and task.
  *
  * `pushAdd` replaces only the pushed metric names, so a failure push (success = 0) keeps the last-success timestamp.
  * Disabled when `PUSHGATEWAY_URL` is unset. A push failure is logged and never fails the job.
  */
final class BatchMetrics(address: Option[String], log: StructuredLog = new StructuredLog("metrics")) {

  def timed[A](task: String)(body: => A): A = {
    val started = System.nanoTime()
    try {
      val result = body
      push(task, success = true, (System.nanoTime() - started) / 1e9)
      result
    } catch {
      case NonFatal(e) =>
        push(task, success = false, (System.nanoTime() - started) / 1e9)
        throw e
    }
  }

  /** Like `timed`, but pushes only on failure (the caller pushes the success with extra series, e.g. DQ). */
  def timedFailure[A](task: String)(body: => A): A = {
    val started = System.nanoTime()
    try body
    catch {
      case NonFatal(e) =>
        push(task, success = false, (System.nanoTime() - started) / 1e9)
        throw e
    }
  }

  def push(task: String, success: Boolean, seconds: Double, extra: PrometheusRegistry => Unit = _ => ()): Unit =
    address.foreach { addr =>
      try {
        val registry = new PrometheusRegistry()
        def gauge(name: String, help: String) = Gauge.builder().name(name).help(help).register(registry)
        gauge("ptp_batch_success", "1 if the last run of this task completed, 0 if it failed").set(if (success) 1 else 0)
        gauge("ptp_batch_duration_seconds", "Duration of the last run of this task").set(seconds)
        if (success) gauge("ptp_batch_last_success_timestamp_seconds", "Unix time of the last successful run")
          .set(System.currentTimeMillis() / 1000.0)
        extra(registry)
        PushGateway.builder().address(addr).job("ptp").groupingKey("task", task).registry(registry).build().pushAdd()
      } catch {
        case NonFatal(e) =>
          log.error("metrics push failed", "error_code" -> "METRICS_PUSH_FAILED", "task" -> task, "address" -> addr,
            "error" -> s"${e.getClass.getSimpleName}: ${e.getMessage}".take(300), "action" -> "Job result is unaffected; check the Pushgateway")
      }
    }

  /** DQ outcome: one series per check (1 = fail) and the anomaly counts. */
  def pushDq(report: DqReport, seconds: Double): Unit =
    push("dq", success = true, seconds, registry => {
      val failed = Gauge.builder().name("ptp_dq_check_failed").help("1 if the DQ check failed in the last run")
        .labelNames("check_id", "severity").register(registry)
      report.results.foreach(r => failed.labelValues(r.checkId, r.severity).set(if (r.status == "fail") 1 else 0))
      val anomalies = Gauge.builder().name("ptp_status_anomalies").help("Anomaly rows found by the last DQ run")
        .labelNames("code").register(registry)
      report.results.find(_.checkId == "DQ-04").flatMap(_.observed).foreach(v => anomalies.labelValues("ORPHAN_EVENT").set(v))
      report.results.find(_.checkId == "DQ-05").flatMap(_.observed).foreach(v => anomalies.labelValues("POST_TERMINAL_EVENT").set(v))
    })
}

object BatchMetrics {
  def fromEnv(): BatchMetrics = new BatchMetrics(sys.env.get("PUSHGATEWAY_URL").map(_.stripPrefix("http://")).filter(_.nonEmpty))
}
