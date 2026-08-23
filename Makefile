.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Sync the uv workspace (runtime deps only)
	uv sync --no-dev

install-dev: ## Sync the workspace with dev tooling
	uv sync

lock: ## Refresh uv.lock
	uv lock

test: ## Run the test suite
	uv run pytest -q

lint: ## Ruff check
	uv run ruff check .

format: ## Ruff format + import sort
	uv run ruff format .
	uv run ruff check --fix .

check: lint test ## Lint and test — what CI runs

infra: ## Start redis, postgres and nats only
	docker compose up -d redis-cache postgres-audit nats

up: ## Start the whole stack
	docker compose up -d --build

down: ## Stop the stack (volumes survive)
	docker compose down

logs: ## Tail every service
	docker compose logs -f --tail=100

nuke: ## Stop the stack and DELETE its volumes (audit trail included)
	docker compose down -v

download: ## Fetch Tiingo history for QTE_ENGINE__SYMBOLS into parquet
	uv run qte-backtest download

history: ## List the parquet history on disk
	uv run qte-backtest list

backtest: ## Replay one strategy: make backtest STRATEGY=... SYMBOL=XAUUSD [TF=M15]
	uv run qte-backtest run --strategy $(STRATEGY) --symbol $(SYMBOL) \
		--timeframe $(or $(TF),M15) --report

reports: ## List the backtest reports written so far
	@ls -lht data/reports 2>/dev/null | head -20 || echo "No reports yet — run make backtest"

ingestion: ## Run the ingestion service locally
	uv run qte-ingestion

runner: ## Run the strategy runner locally
	uv run qte-strategy-runner

api: ## Run the control-plane API locally
	uv run qte-api

shadow-on: ## Pause delivery to the broker on every running runner
	curl -fsS -X POST localhost:8000/admin/shadow-mode -H 'Content-Type: application/json' \
		$(if $(API_KEY),-H 'X-API-KEY: $(API_KEY)',) -d '{"enabled": true}' | jq .

shadow-off: ## Resume delivery to the broker (GOES LIVE)
	curl -fsS -X POST localhost:8000/admin/shadow-mode -H 'Content-Type: application/json' \
		$(if $(API_KEY),-H 'X-API-KEY: $(API_KEY)',) -d '{"enabled": false}' | jq .

.PHONY: help install install-dev lock test lint format check infra up down logs nuke \
	download history backtest reports ingestion runner api shadow-on shadow-off
