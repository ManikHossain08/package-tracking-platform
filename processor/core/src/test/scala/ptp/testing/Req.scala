package ptp.testing

import org.scalatest.Tag

/** Tags a test with the requirement ID it verifies (CLAUDE.md, FR-DEV-03 traceability). */
object Req {
  def apply(id: String): Tag = Tag(id)
}

/** Marks tests that need Docker (Testcontainers). Excluded from `make test`, run by `make test-it`. */
object Integration extends Tag("Integration")
