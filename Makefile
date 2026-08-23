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

db-upgrade: ## Apply every pending migration
	uv run alembic upgrade head

db-downgrade: ## Roll back one migration
	uv run alembic downgrade -1

db-revision: ## Autogenerate a migration from model changes: make db-revision M="add x"
	uv run alembic revision --autogenerate -m "$(M)"

db-current: ## Which revision the database is on
	uv run alembic current --verbose

db-history: ## The migration history
	uv run alembic history --indicate-current

db-check: ## Fail if the models have drifted from the migrations
	uv run alembic check

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

shadow-status: ## Show whether signals are reaching the broker
	uv run qte-control shadow status

shadow-on: ## Pause delivery to the broker on every running runner
	uv run qte-control shadow on

shadow-off: ## Resume delivery to the broker (GOES LIVE — prompts to confirm)
	uv run qte-control shadow off

ping: ## Ask the running runners to identify themselves
	uv run qte-control ping

.PHONY: help install install-dev lock test lint format check infra up down logs nuke \
	db-upgrade db-downgrade db-revision db-current db-history db-check \
	download history backtest reports ingestion runner \
	shadow-status shadow-on shadow-off ping
