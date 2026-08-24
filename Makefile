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

# ── Strategy plugins ────────────────────────────────────────────────────
#
# __strategies__/ is a separate repository with its own lockfile. Its code is a
# mounted volume and needs no installing, but its *dependencies* do: the runner
# imports the strategies into its own process. STRATEGY_REPO points at the
# checkout; override it if yours is cloned somewhere else.

STRATEGY_REPO ?= __strategies__/quant-trading-strategies

strategy-deps: ## Install the mounted strategy repo's deps into this venv
	uv export --project $(STRATEGY_REPO) --no-dev --no-emit-project \
		--format requirements-txt | uv pip install -r -

strategy-requirements: ## Freeze those deps into deploy/ for the image build
	@if [ -f $(STRATEGY_REPO)/pyproject.toml ]; then \
		uv export --project $(STRATEGY_REPO) --no-dev --no-emit-project \
			--format requirements-txt -o deploy/strategy-requirements.txt; \
	else \
		echo "No strategy repo at $(STRATEGY_REPO) - nothing to freeze"; \
	fi

strategy-test: ## Run the strategy repo's own suite (separate from ours)
	$(MAKE) -C $(STRATEGY_REPO) check

audit: ## Validate __strategies__/ against the QTE signal contract + routing table
	uv run qte-strategy-audit

audit-strict: ## Same, but warnings fail too — what CI should run
	uv run qte-strategy-audit --strict

routing: ## Copy the strategies-mapping template into place (never overwrites)
	@if [ -f config/strategies_mapping.toml ]; then \
		echo "config/strategies_mapping.toml exists - leaving it alone"; \
	else \
		cp config/strategies_mapping.example.toml config/strategies_mapping.toml; \
		echo "Wrote config/strategies_mapping.toml (git-ignored) - pair your symbols with strategies in it"; \
	fi

strategies: ## List what the mounted strategy repo publishes
	uv run python -c "from qte_shared.config import settings; \
		from qte_shared.plugin_loader import load_strategies; \
		[print(e.name, 'from', e.source) for e in load_strategies(settings.engine.strategies_dir)]"

infra: ## Start redis, postgres and nats only
	docker compose up -d redis-cache postgres-audit nats

up: strategy-requirements ## Start the whole stack
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

csv-import: ## Convert an MT5 CSV export to parquet: make csv-import CSV=data/csv/x.csv [TZ=EET] [ARGS=--overwrite]
	uv run python scripts/mt5_csv_to_parquet.py $(CSV) --tz $(or $(TZ),UTC) $(ARGS)

download: ## Fetch provider history for QTE_ENGINE__SYMBOLS into parquet
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

# ── Dev market data simulator ───────────────────────────────────────────
#
# A WebSocket feed you drive by hand, so the whole pipeline can be rehearsed
# without a market being open. Refuses to run unless QTE_ENV=dev. The full
# walkthrough is docs/simulator.md.

SIM_SYMBOL ?= XAUUSD
SIM_TF     ?= M15
SIM_BARS   ?= 300

sim: ## Run the dev websocket market data simulator (QTE_ENV=dev only)
	uv run qte-simulator serve

sim-up: ## Same, in compose, on the dev profile
	docker compose --profile dev up -d --build market-simulator

sim-status: ## What the simulator is doing, and who is attached to it
	uv run qte-simulator status

sim-replay: ## Warm the engine: make sim-replay [SIM_SYMBOL=XAUUSD] [SIM_BARS=300]
	uv run qte-simulator replay --symbol $(SIM_SYMBOL) --timeframe $(SIM_TF) \
		--generate $(SIM_BARS) --seed 7

sim-bar: ## Send one bar and check the candle comes back: make sim-bar O=.. H=.. L=.. C=..
	uv run qte-simulator bar --symbol $(SIM_SYMBOL) --timeframe $(SIM_TF) \
		--open $(O) --high $(H) --low $(L) --close $(C) --verify

sim-walk: ## Stream a live-ish random walk until stopped
	uv run qte-simulator walk --symbol $(SIM_SYMBOL) --rate 5

sim-stop: ## Stop every background generator
	uv run qte-simulator stop

sim-watch: ## Tail closed candles and emitted signals on NATS
	uv run qte-simulator watch --symbol $(SIM_SYMBOL) --timeframe $(SIM_TF)

shadow-status: ## Show whether signals are reaching the broker
	uv run qte-control shadow status

shadow-on: ## Pause delivery to the broker on every running runner
	uv run qte-control shadow on

shadow-off: ## Resume delivery to the broker (GOES LIVE — prompts to confirm)
	uv run qte-control shadow off

ping: ## Ask the running runners to identify themselves
	uv run qte-control ping

.PHONY: help install install-dev lock test lint format check infra up down logs nuke \
	strategy-deps strategy-requirements strategy-test strategies audit audit-strict routing \
	db-upgrade db-downgrade db-revision db-current db-history db-check \
	download history backtest reports ingestion runner csv-import \
	sim sim-up sim-status sim-replay sim-bar sim-walk sim-stop sim-watch \
	shadow-status shadow-on shadow-off ping
