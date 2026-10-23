# Contributing

## Branches and pull requests (ADR-0010)

`main` is protected: **no direct commits or pushes**, including by admins. Every change goes through a pull request.

1. Branch from an up-to-date `main`: `git switch main && git pull && git switch -c <type>/<short-name>`
   with `<type>` = `feat`, `fix`, `chore`, `docs`, `test` or `refactor` (e.g. `feat/postgres-retry`).
2. Follow the spec-driven workflow in [CLAUDE.md](CLAUDE.md): acceptance criteria first, then code and tests.
3. Open a PR (the template asks for requirement IDs, ACs, spec changes and test evidence).
4. All required CI checks must pass, and the branch must be up to date with `main`.
5. Merge with a **merge commit** (squash and rebase merges are disabled). The branch is deleted automatically.

## Required checks (workflow `ci`)

| Check | What it runs |
|---|---|
| `pins, secrets, python` | `check-pins`, gitleaks, ruff, pytest, `promtool` alert tests |
| `scala + spark smoke` | unit tests (no Docker) |
| `integration (Testcontainers)` | `make test-it` |
| `compose stack` | `make up verify`, jobs image, containerised processor, observability |
| `airflow end to end (AC-7.7..7.11)` | Airflow DAGs end to end |

## Delivery (workflow `cd`)

After CI passes on `main`, the `ptp-jobs` image is published to `ghcr.io/<owner>/ptp-jobs` as `sha-<commit>` and `main`.
A version tag (`git tag v0.9.0 && git push origin v0.9.0`, from `main`) also publishes `:0.9.0` / `:latest` and creates a
GitHub Release.
