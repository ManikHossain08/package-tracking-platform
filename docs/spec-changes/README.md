# Spec change proposals

An approved spec, contract or ADR is never edited silently to make an implementation pass.
Instead, add `NNNN-<slug>.md` here with:

1. **Change:** the exact requirement, contract field or ADR section, before → after.
2. **Reason:** evidence (failing test, measurement, new business input).
3. **Impact:** affected requirements, tests, data already stored, consumers, backfill needed?
4. **Compatibility:** per the schema evolution rules in spec 02 §2.
5. **Decision:** approved / rejected, by whom and when.

The spec edit lands in the same PR as the approved proposal.
