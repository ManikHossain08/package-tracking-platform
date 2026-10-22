# Developer entry points (spec 08). Run `make help` for the list.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose --project-directory infra -f infra/docker-compose.yml --env-file .env
VENV    := .venv
PY      := $(VENV)/bin/python
GITLEAKS_IMAGE := zricethezav/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f
# Pins Python deps to what was published before this date (reproducible re-locks).
LOCK_EXCLUDE_NEWER := 2026-10-07

.PHONY: help env up down reset ps logs verify venv lock lint test test-py test-scala test-it test-it-py generate fixtures process replay silver gold lake query status reconcile check-pins secrets-scan ci

help: ## Show targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

env: ## Create .env, or add keys it is missing; *_PASSWORD values are generated (never overwrites)
	@scripts/ensure_env.sh

up: env ## Start the core stack, create topics, roles and schema (AC-0.1)
	$(COMPOSE) --profile core up -d --wait --wait-timeout 120 kafka postgres
	$(COMPOSE) --profile core run --rm kafka-init
	set -a; . ./.env; set +a; COMPOSE="$(COMPOSE)" infra/postgres/bootstrap-roles.sh
	$(COMPOSE) --profile core run --rm pg-migrate

down: ## Stop the stack, keep volumes
	$(COMPOSE) --profile core down

reset: ## Stop the stack and DELETE its volumes (asks first)
	@read -p "Delete Kafka and Postgres volumes? [y/N] " ans; [ "$$ans" = y ] && $(COMPOSE) --profile core down -v || echo "aborted"

ps: ## Show stack status
	$(COMPOSE) --profile core ps

logs: ## Follow stack logs
	$(COMPOSE) --profile core logs -f

verify: ## Check topics, configs, auto-create and init idempotency against the running stack (AC-0.2..0.4)
	COMPOSE="$(COMPOSE)" scripts/verify_stack.sh

venv: $(VENV)/.installed ## Create the Python venv from the hashed lockfile
$(VENV)/.installed: requirements-dev.txt
	python3.12 -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet --require-hashes -r requirements-dev.txt
	touch $@

lock: ## Re-lock Python dev dependencies (needs uv)
	uv pip compile requirements-dev.in --generate-hashes --python-version 3.12 \
	  --exclude-newer $(LOCK_EXCLUDE_NEWER) -o requirements-dev.txt

lint: venv ## Lint and format-check Python code
	$(PY) -m ruff check scripts tests generator
	$(PY) -m ruff format --check scripts tests generator

test-py: venv ## Python unit and contract tests
	$(PY) -m pytest

test-it-py: venv ## Python integration tests against the running stack (needs make up)
	$(PY) -m pytest -m integration

SCENARIO ?= bench/scenarios/smoke.yaml
SINK ?= kafka
OUT ?= data/generator/$(basename $(notdir $(SCENARIO)))
generate: venv ## Run the generator: make generate [SCENARIO=bench/scenarios/x.yaml] [SINK=kafka|file]
	PYTHONPATH=generator $(PY) -m ptp_generator --scenario $(SCENARIO) --sink $(SINK) --out-dir $(OUT)

FIXTURE_DIR := processor/core/src/test/resources/fixtures/generator
fixtures: venv ## Regenerate the committed generator snapshot used by Scala parity tests (TS-C-04)
	rm -rf $(FIXTURE_DIR)
	PYTHONPATH=generator $(PY) -m ptp_generator --scenario bench/scenarios/fixture-validation.yaml \
	  --sink file --out-dir $(FIXTURE_DIR)
	rm -f $(FIXTURE_DIR)/run.json $(FIXTURE_DIR)/expected_*.jsonl  # not deterministic / not needed by the parity test

# Testcontainers on macOS: Docker Desktop's socket, unless DOCKER_HOST is already set.
DOCKER_HOST ?= $(if $(wildcard $(HOME)/.docker/run/docker.sock),unix://$(HOME)/.docker/run/docker.sock,)
export DOCKER_HOST

test-scala: ## Scala unit tests (no Docker), including the Spark smoke test
	cd processor && sbt -batch "testOnly -- -l Integration"

test-it: venv ## Scala integration tests: Testcontainers Kafka + Postgres + the real generator (AC-1.x)
	cd processor && sbt -batch "streaming/testOnly -- -n Integration"

process: env ## Run the status processor + bronze writer on the host against the core stack (Ctrl-C to stop)
	set -a; . ./.env; set +a; export CHECKPOINT_ROOT="$(CURDIR)/$${CHECKPOINT_ROOT:-data/checkpoints}" \
	  LAKE_ROOT="$(CURDIR)/$${LAKE_ROOT:-data/lake}"; cd processor && sbt -batch "streaming/run"

FROM ?= $(shell date -u +%F)
TO ?= $(FROM)
MODE ?= as-ingested
DRY_RUN ?= false
replay: env ## Replay bronze into Postgres: make replay FROM=yyyy-mm-dd [TO=…] [MODE=as-ingested|backfill] [DRY_RUN=true]
	set -a; . ./.env; set +a; export LAKE_ROOT="$(CURDIR)/$${LAKE_ROOT:-data/lake}"; cd processor && \
	  sbt -batch "streaming/runMain ptp.streaming.ReplayApp --from $(FROM) --to $(TO) --mode $(MODE) --dry-run $(DRY_RUN)"

LAKE_RUN = set -a; . ./.env; set +a; export LAKE_ROOT="$(CURDIR)/$${LAKE_ROOT:-data/lake}"; cd processor && \
  sbt -batch "streaming/runMain ptp.lake.LakeApp
AS_OF ?= $(shell date -u +%F)
silver: env ## Build silver incrementally from bronze, or a range: make silver [INGEST_FROM=yyyy-mm-dd INGEST_TO=…]
	$(LAKE_RUN) silver $(if $(INGEST_FROM),--from $(INGEST_FROM) --to $(or $(INGEST_TO),$(INGEST_FROM)),)"

gold: env ## Load gold for the 7 days ending AS_OF (default today, UTC): make gold [AS_OF=yyyy-mm-dd]
	$(LAKE_RUN) gold --as-of $(AS_OF)"

lake: env ## Silver (incremental) then gold, as the lake_daily DAG will: make lake [AS_OF=…]
	$(LAKE_RUN) all --as-of $(AS_OF)"

Q ?= 01_on_time_rate
query: ## Run a SQL query-pack file as ptp_reader: make query Q=01_on_time_rate [FROM=… TO=…]
	@set -a; . ./.env; set +a; $(COMPOSE) exec -T -e PGPASSWORD="$$PTP_READER_PASSWORD" postgres \
	  psql -h localhost -U ptp_reader -d "$${POSTGRES_DB:-ptp}" -P pager=off -v from_date=$(FROM) -v to_date=$(TO) -f - \
	  < sql/analytics/$(Q).sql

RUN ?= data/generator/smoke
reconcile: ## Wait for the processor, then compare Postgres with a generator run: make reconcile [RUN=data/generator/x]
	python3 scripts/reconcile.py $(RUN)

status: ## Summarise package status, DLQ and batch counts from the running stack
	@$(COMPOSE) exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -P pager=off -c "\
	  SELECT current_status, count(*) AS packages FROM tracking.package_status GROUP BY 1 ORDER BY 2 DESC;" -c "\
	  SELECT count(*) AS batches, sum(input_rows) AS input, sum(applied_rows) AS applied, sum(duplicate_rows) AS duplicates, \
	         sum(rejected_rows) AS rejected, max(committed_at) AS last_commit FROM ops.sink_batches;"'

test: test-py test-scala ## All tests that need no Docker (AC-0.5)

check-pins: ## Fail on any unpinned image or dependency (AC-0.6)
	python3 scripts/check_pins.py

secrets-scan: ## Scan the working tree and git history for secrets (AC-0.7)
	docker run --rm -v "$(CURDIR):/repo:ro" $(GITLEAKS_IMAGE) git /repo --redact --no-banner

ci: check-pins lint test ## What CI runs without the Compose stack
