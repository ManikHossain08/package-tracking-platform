# ADR-0010: Branching, merging and delivery

- **Status:** Accepted (2026-10-23)
- **Requirements:** FR-DEV-02, NFR-07

## Context

Until step 8 every change was committed to `main` locally, and CI had never run. The owner asked for GitHub with CI/CD
and **a merge-based workflow instead of direct commits to main**.

## Decision

| Topic | Decision | Alternatives considered |
|---|---|---|
| Hosting | Public GitHub repository | Private: on a free plan GitHub can't enforce branch protection there |
| Flow | Short-lived branches → PR → merge to `main`; `main` protected (PR required, all CI checks required and up to date, admins included, no force pushes or deletion) | Direct commits (rejected); GitFlow (too heavy for one deployable) |
| Merge method | **Merge commits** only; branches auto-deleted | Squash (loses step history); rebase (rewrites the PR's commits) |
| Reviews | PR required, 0 approvals (single maintainer can't approve their own PR) | 1 approval (would block a solo project) |
| CD | Publish `ptp-jobs` to GHCR after CI succeeds on `main` (`sha-*`, `main`); tags `vX.Y.Z` add semver tags and a GitHub Release | Deploy to a cloud runtime (excluded by the brief) |

## Consequences

- Every change carries its PR (template: requirement IDs, ACs, spec changes, evidence), which links process to history.
- CD publishes only commits that passed CI (`workflow_run` on CI success).
- The initial history (steps 1–8) was pushed to `main` once to create the repository; from then on `main` only changes by merge.
