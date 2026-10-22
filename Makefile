# Developer entry points (spec 08). Run `make help` for the list.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose --project-directory infra -f infra/docker-compose.yml --env-file .env
VENV    := .venv
PY      := $(VENV)/bin/python
GITLEAKS_IMAGE := zricethezav/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f
# Pins Python deps to what was published before this date (reproducible re-locks).
LOCK_EXCLUDE_NEWER := 2026-10-07

.PHONY: help env up down reset ps logs verify venv lock lint test test-py test-scala test-it-py generate fixtures check-pins secrets-scan ci

help: ## Show targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

env: ## Create .env from .env.example with a generated Postgres password (never overwrites)
	@if [ -f .env ]; then echo ".env already exists; leaving it unchanged"; else \
	  sed "s/^POSTGRES_ADMIN_PASSWORD=.*/POSTGRES_ADMIN_PASSWORD=$$(openssl rand -hex 24)/" .env.example > .env; \
	  chmod 600 .env; echo "created .env"; fi

up: env ## Start the core stack and wait until healthy (AC-0.1)
	$(COMPOSE) --profile core up -d --wait --wait-timeout 120 kafka postgres
	$(COMPOSE) --profile core run --rm kafka-init

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
	rm -f $(FIXTURE_DIR)/run.json  # run.json is not deterministic

test-scala: ## Scala unit tests, including the Spark smoke test
	cd processor && sbt -batch test

test: test-py test-scala ## All tests that need no Docker (AC-0.5)

check-pins: ## Fail on any unpinned image or dependency (AC-0.6)
	python3 scripts/check_pins.py

secrets-scan: ## Scan the working tree and git history for secrets (AC-0.7)
	docker run --rm -v "$(CURDIR):/repo:ro" $(GITLEAKS_IMAGE) git /repo --redact --no-banner

ci: check-pins lint test ## What CI runs without the Compose stack
