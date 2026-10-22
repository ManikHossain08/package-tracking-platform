package ptp.core.validation

import java.time.Duration

import org.scalatest.funsuite.AnyFunSuite
import ptp.core.validation.ErrorCode._
import ptp.core.validation.ProcessingMode._
import ptp.core.validation.ValidationFixtures._
import ptp.testing.Req

/** TS-U-03, AC-3.4: runtime time bounds, inclusive at the limit, ±1 s around it. */
class TimeBoundsSpec extends AnyFunSuite {

  private def at(offset: Duration, mode: ProcessingMode = Live) = {
    val node = base()
    node.put("event_time", Now.plus(offset).toString)
    validate(bytes(node), Now, mode).left.map(_.primary)
  }

  private val oneSecond = Duration.ofSeconds(1)
  private val fiveMin   = Duration.ofMinutes(5)
  private val thirtyD   = Duration.ofDays(30)

  test("future skew: +5 min accepted, +5 min +1 s rejected", Req("FR-VAL-02")) {
    assert(at(fiveMin).isRight)
    assert(at(fiveMin.minus(oneSecond)).isRight)
    assert(at(fiveMin.plus(oneSecond)) == Left(EventTimeInFuture))
  }

  test("live max age: -30 d accepted, -30 d -1 s rejected", Req("FR-VAL-02")) {
    assert(at(thirtyD.negated()).isRight)
    assert(at(thirtyD.negated().plus(oneSecond)).isRight)
    assert(at(thirtyD.negated().minus(oneSecond)) == Left(EventTimeTooOld))
  }

  test("replay mode accepts history older than 30 days but still rejects the future", Req("FR-VAL-02")) {
    assert(at(Duration.ofDays(31).negated(), Replay).isRight)
    assert(at(Duration.ofDays(400).negated(), Replay).isRight)
    assert(at(fiveMin.plus(oneSecond), Replay) == Left(EventTimeInFuture))
  }

  test("bounds are configurable", Req("FR-VAL-02"), Req("NFR-11")) {
    val strict = new EventValidator(ValidationConfig(futureSkew = Duration.ZERO, liveMaxAge = Duration.ofDays(1)))
    val node = base(); node.put("event_time", Now.plusMillis(1).toString)
    assert(strict.validate(bytes(node), Now, Live).left.map(_.primary) == Left(EventTimeInFuture))
    node.put("event_time", Now.minus(Duration.ofDays(2)).toString)
    assert(strict.validate(bytes(node), Now, Live).left.map(_.primary) == Left(EventTimeTooOld))
  }
}
