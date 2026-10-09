.DEFAULT_GOAL := help

# Every recipe here is POSIX shell. Inside Git Bash, WSL or Linux, /bin/bash
# resolves and there is nothing to do. A native Windows make started from
# PowerShell or cmd resolves no POSIX path at all, so point it at the bash Git
# for Windows ships. It has to be the 8.3 form: a space in SHELL defeats make's
# "is this a Unix shell" check and it falls back to running recipes through cmd.
SHELL := /bin/bash
WINDOWS_BASH ?= C:/PROGRA~1/Git/bin/bash.exe
ifeq ($(OS),Windows_NT)
ifndef MSYSTEM
ifeq ($(wildcard $(WINDOWS_BASH)),)
$(error No shell at $(WINDOWS_BASH). Run make from Git Bash, or pass WINDOWS_BASH=<path to bash.exe, no spaces>)
endif
SHELL := $(WINDOWS_BASH)
# Simple recipe lines are launched directly, without the shell, so the coreutils
# Git ships (awk, sed, cp) have to be findable on PATH as well.
export PATH := $(dir $(WINDOWS_BASH))../usr/bin;$(PATH)
endif
endif

# QTE_MARKET_DATA__PROVIDER may list several providers (`mt5,binance`), and a
# comma is not valid in a compose project or volume name. Compose names them
# with this key instead: the providers lowercased, sorted and joined by `-`,
# exactly as `MarketDataSettings.provider_key` computes it, which checks the two
# agree. A shell export of the provider wins over .env, as it does for compose.
MARKET_DATA_PROVIDERS := $(or $(QTE_MARKET_DATA__PROVIDER),$(shell sed -n 's/^QTE_MARKET_DATA__PROVIDER=//p' .env 2>/dev/null | tail -n1 | tr -d "\"'\r "))
QTE_STATE__PROVIDER_KEY := $(shell printf '%s' '$(MARKET_DATA_PROVIDERS)' | tr ',' '\n' | tr -d ' ' | tr 'A-Z' 'a-z' | sed '/^$$/d' | sort -u | paste -sd- -)
export QTE_STATE__PROVIDER_KEY

# Tailscale is opt-in: .env sets TAILSCALE_ENABLED=true (default false), the
# same switch algo-trading-broker reads. It decides every half of the setup, so
# .env never has to spell them out:
#   - the `tailscale` and `broker-nats-relay` containers (docker-compose.yml,
#     profile `tailscale`) are started: the node joins the tailnet, forwards
#     NATS/Postgres onto it, and carries the runner's signals to the broker;
#   - the Redis, Postgres and NATS host ports are published on 127.0.0.1 only,
#     so another machine — the MT5 ingester — can reach them through the
#     tailnet and nothing else;
#   - the runner's container dials the broker through that relay.
# With it off, neither container is pulled, those ports bind every interface
# (0.0.0.0) and the runner dials the broker as before.
TAILSCALE_ENABLED := $(shell sed -n 's/^TAILSCALE_ENABLED=//p' .env 2>/dev/null | tail -n1 | sed 's/[[:space:]]*\#.*//' | tr -d '\r"' | tr -d "' " | tr '[:upper:]' '[:lower:]')
TAILSCALE_ACTIVE := $(if $(filter true 1 yes on,$(TAILSCALE_ENABLED)),1,)

# The Telegram bot is opt-in the same way: .env sets QTE_BOT__ENABLED=true
# (default false) and the `bot` profile follows. It is its own switch rather
# than "a token is set" so that an operator can keep the token in .env and
# still choose whether the container runs.
TELEGRAM_BOT_ENABLED := $(shell sed -n 's/^QTE_BOT__ENABLED=//p' .env 2>/dev/null | tail -n1 | sed 's/[[:space:]]*\#.*//' | tr -d '\r"' | tr -d "' " | tr '[:upper:]' '[:lower:]')
TELEGRAM_BOT_ACTIVE := $(if $(filter true 1 yes on,$(TELEGRAM_BOT_ENABLED)),1,)

COMPOSE_PROFILE := $(if $(TAILSCALE_ACTIVE),--profile tailscale,)$(if $(TELEGRAM_BOT_ACTIVE), --profile bot,)
# Named explicitly where a target lists its services (`start-prod`): naming a
# service is what enables its profile there.
TAILSCALE_SERVICES := $(if $(TAILSCALE_ACTIVE),tailscale broker-nats-relay,)
TELEGRAM_BOT_SERVICES := $(if $(TELEGRAM_BOT_ACTIVE),telegram-bot,)

# Passed explicitly so the switch is authoritative over whatever the shell
# happens to export.
PRIVATE_BIND_ADDRESS := $(if $(TAILSCALE_ACTIVE),127.0.0.1,0.0.0.0)
export PRIVATE_BIND_ADDRESS
TAILNET_BROKER_NATS_URL := $(if $(TAILSCALE_ACTIVE),nats://broker-nats-relay:4222,)
export TAILNET_BROKER_NATS_URL

help: ## Show this help, grouped by section
	@awk 'BEGIN {FS = ":.*?## "} \
		/^##@ / {printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next} \
		/^[a-zA-Z0-9_-]+([ ]+[a-zA-Z0-9_-]+)*:.*?## / \
		{split($$1, names, " "); printf "  \033[36m%-20s\033[0m %s\n", names[1], $$2}' \
		$(MAKEFILE_LIST)
	@echo

##@ Project

# --all-extras locally: one venv has to run every entry point, including
# `make backtest` (pyarrow) and `make simulator` (websockets). The images are
# the place where extras get selected, not the developer's machine.
install: ## Sync the environment (runtime deps only)
	uv sync --no-dev --all-extras

install-dev: ## Sync the environment with dev tooling
	uv sync --all-extras

lock: ## Refresh uv.lock
	uv lock

##@ Quality

test: ## Run the test suite
	uv run pytest -q

lint: ## Ruff check
	uv run ruff check .

format: ## Ruff format + import sort
	uv run ruff format .
	uv run ruff check --fix .

check: lint test ## Lint and test — what CI runs

##@ Strategies

# ── Strategy plugins ────────────────────────────────────────────────────
#
# __strategies__/ can hold more than one strategy repository, each its own
# checkout with its own lockfile. Their code is a mounted volume and needs no
# installing, but their *dependencies* do: the runner imports the strategies
# into its own process. STRATEGY names the subfolder to act on
# (__strategies__/<name>); leave it unset to sweep every checkout under
# __strategies__/ that has a pyproject.toml. strategy-mount installs each
# repo's deps, then audits it and records one verdict per strategy it
# publishes — not one for the repo — in STRATEGIES_MANIFEST, an auto-generated
# file that must exist before `make up`/`make start` will run, even if it lists
# nothing. `qte-strategy-mount --show` prints what it holds.
#
# strategy-requirements freezes the repos that still have a passing strategy,
# and the engine's loader skips the strategies that failed: one whose deps
# never reached the image must not be imported and traded inside it.
#
# Audit runs *after* install, not before: the commonest audit failure is a
# missing third-party import, which is exactly what installing fixes, so
# auditing a repo before its deps exist would mark it false for a reason the
# next line was about to correct.

STRATEGIES_DIR := __strategies__
STRATEGIES_MANIFEST := $(STRATEGIES_DIR)/strategies.toml
STRATEGY ?=

strategy-audit: ## Audit one mounted strategy repo; prints true or false to stdout (STRATEGY=<name> required)
	@if [ -z "$(STRATEGY)" ]; then \
		echo "STRATEGY is required, e.g. make strategy-audit STRATEGY=my-strategies" >&2; \
		exit 1; \
	fi; \
	repo="$(STRATEGIES_DIR)/$(STRATEGY)"; \
	if [ ! -d "$$repo" ]; then \
		echo "No such strategy folder: $$repo" >&2; \
		echo false; \
		exit 0; \
	fi; \
	if uv run qte-strategy-audit --dir "$$repo" --no-mapping 1>&2; then \
		echo true; \
	else \
		echo false; \
	fi

strategy-mount: ## Install one (STRATEGY=<name>) or every mounted strategy repo's deps into this venv
	@set -e; \
	if [ -n "$(STRATEGY)" ]; then \
		names="$(STRATEGY)"; \
	else \
		names=$$(cd $(STRATEGIES_DIR) && for dir in */; do name=$${dir%/}; if [ -f "$$name/pyproject.toml" ]; then echo "$$name"; fi; done; true); \
	fi; \
	if [ -z "$$names" ]; then \
		echo "No strategy repos with a pyproject.toml under $(STRATEGIES_DIR)/"; \
	fi; \
	for name in $$names; do \
		repo="$(STRATEGIES_DIR)/$$name"; \
		[ -f "$$repo/pyproject.toml" ] || { echo "No pyproject.toml at $$repo"; exit 1; }; \
		echo "Mounting $$name"; \
		uv export --project "$$repo" --no-dev --no-emit-project \
			--format requirements-txt | uv pip install -r -; \
		uv run qte-strategy-mount --dir $(STRATEGIES_DIR) --record "$$name"; \
	done; \
	uv run qte-strategy-mount --dir $(STRATEGIES_DIR) --ensure >/dev/null; \
	uv run qte-strategy-mount --dir $(STRATEGIES_DIR) --show

strategy-requirements: ## Freeze deps for one (STRATEGY=<name>) or every mounted repo (from STRATEGIES_MANIFEST) into deploy/
	@set -e; \
	mkdir -p deploy; \
	if [ -n "$(STRATEGY)" ]; then \
		names="$(STRATEGY)"; \
	else \
		if [ ! -f $(STRATEGIES_MANIFEST) ]; then \
			echo "$(STRATEGIES_MANIFEST) is missing - run 'make strategy-mount' first (even with no repos to mount, it must exist)"; \
			exit 1; \
		fi; \
		names=$$(uv run qte-strategy-mount --dir $(STRATEGIES_DIR) --passing); \
	fi; \
	if [ -z "$$names" ]; then \
		echo "No strategy in $(STRATEGIES_MANIFEST) passed its audit - nothing to freeze"; \
		: > deploy/strategy-requirements.txt; \
		exit 0; \
	fi; \
	tmp=$$(mktemp); \
	for name in $$names; do \
		repo="$(STRATEGIES_DIR)/$$name"; \
		if [ -f "$$repo/pyproject.toml" ]; then \
			uv export --project "$$repo" --no-dev --no-emit-project \
				--format requirements-txt >> "$$tmp"; \
		else \
			echo "No strategy repo at $$repo - skipping"; \
		fi; \
	done; \
	mv "$$tmp" deploy/strategy-requirements.txt; \
	echo "Wrote deploy/strategy-requirements.txt from: $$names"

strategy-test: ## Run one mounted strategy repo's own suite (STRATEGY=<name> required)
	@if [ -z "$(STRATEGY)" ]; then \
		echo "STRATEGY is required, e.g. make strategy-test STRATEGY=my-strategies"; \
		exit 1; \
	fi
	$(MAKE) -C $(STRATEGIES_DIR)/$(STRATEGY) check

audit: ## Validate __strategies__/ against the QTE signal contract + mapping table
	uv run qte-strategy-audit

audit-strict: ## Same, but warnings fail too — what CI should run
	uv run qte-strategy-audit --strict

strategy-mapping: ## Copy the strategies-mapping template into place (never overwrites)
	@if [ -f config/strategies_mapping.toml ]; then \
		echo "config/strategies_mapping.toml exists - leaving it alone"; \
	else \
		sed -e '/^[ \t]*#/d' config/strategies_mapping.example.toml | cat -s | sed -e '/./,$$!d' > config/strategies_mapping.toml; \
		echo "Wrote config/strategies_mapping.toml (git-ignored, comments stripped) - pair your symbols with strategies in it"; \
	fi

strategies: ## List what the mounted strategy repo publishes
	uv run python -c "from qte_shared.config import settings; \
		from qte_shared.strategies.plugin_loader import load_strategies; \
		[print(e.name, 'from', e.source) for e in load_strategies(settings.engine.strategies_dir)]"

##@ Market data

# What a provider is asked to feed — symbols, their markets and timeframes, and
# the vendor's own knobs — lives in config/data_providers/<provider>.toml, not in .env: it is a
# per-symbol matrix and the flat form could not express one. The real file is
# git-ignored like the mapping table; the template beside it is tracked.
# The key is the exception and stays in .env as QTE_DATA_PROVIDER_API_KEY
# (for mt5, the NATS token as QTE_MT5__NATS_TOKEN, blank = QTE_NATS__TOKEN).

tiingo: ## Copy the Tiingo market-data plan into place (never overwrites)
	@if [ -f config/data_providers/tiingo.toml ]; then \
		echo "config/data_providers/tiingo.toml exists - leaving it alone"; \
	else \
		sed -e '/^[ \t]*#/d' config/data_providers/tiingo.example.toml | cat -s | sed -e '/./,$$!d' > config/data_providers/tiingo.toml; \
		echo "Wrote config/data_providers/tiingo.toml (git-ignored, comments stripped) - list the symbols you want fed in it"; \
	fi

mt5: ## Copy the MT5 (algo-trading-ingester) market-data plan into place (never overwrites)
	@if [ -f config/data_providers/mt5.toml ]; then \
		echo "config/data_providers/mt5.toml exists - leaving it alone"; \
	else \
		sed -e '/^[ \t]*#/d' config/data_providers/mt5.example.toml | cat -s | sed -e '/./,$$!d' > config/data_providers/mt5.toml; \
		echo "Wrote config/data_providers/mt5.toml (git-ignored, comments stripped) - list the symbols the ingester publishes"; \
	fi

binance: ## Copy the Binance market-data plan into place - placeholder provider, not implemented yet
	@if [ -f config/data_providers/binance.toml ]; then \
		echo "config/data_providers/binance.toml exists - leaving it alone"; \
	else \
		sed -e '/^[ \t]*#/d' config/data_providers/binance.example.toml | cat -s | sed -e '/./,$$!d' > config/data_providers/binance.toml; \
		echo "Wrote config/data_providers/binance.toml (git-ignored, comments stripped) - the binance provider is not implemented yet"; \
	fi

simulator: ## Copy the simulator market-data plan into place (never overwrites)
	@if [ -f config/data_providers/simulator.toml ]; then \
		echo "config/data_providers/simulator.toml exists - leaving it alone"; \
	else \
		sed -e '/^[ \t]*#/d' config/data_providers/simulator.example.toml | cat -s | sed -e '/./,$$!d' > config/data_providers/simulator.toml; \
		echo "Wrote config/data_providers/simulator.toml (git-ignored, comments stripped) - list the symbols you want fed in it"; \
	fi

# Guards `up` and `start`. A missing plan does not fail at runtime: the engine
# falls back to the QTE_ENGINE__* defaults and quietly subscribes to something
# nobody asked for, which on a live vendor is the wrong kind of surprise. The
# simulator answers the same "what to feed" question and is held to it too — it
# has no key, but it still needs a plan (`make simulator`).
market-plan: ## Fail unless every configured provider has its plan file
	@providers="$(MARKET_DATA_PROVIDERS)"; \
	override=$$(sed -n 's/^QTE_MARKET_DATA__CONFIG_FILE=//p' .env 2>/dev/null | tail -n1 | tr -d '"' | tr -d "\r' "); \
	[ -n "$$providers" ] || providers=tiingo; \
	for provider in $$(printf '%s' "$$providers" | tr ',' ' ' | tr 'A-Z' 'a-z'); do \
		plan="$$override"; \
		[ -n "$$plan" ] || plan="config/data_providers/$$provider.toml"; \
		if [ ! -f "$$plan" ]; then \
			echo "QTE_MARKET_DATA__PROVIDER=$$providers, but $$plan does not exist." >&2; \
			echo "Without it the engine falls back to the QTE_ENGINE__* defaults and" >&2; \
			echo "subscribes to symbols nobody chose." >&2; \
			if [ -f "config/data_providers/$$provider.example.toml" ]; then \
				echo "Write one:  make $$provider" >&2; \
			else \
				echo "There is no config/data_providers/$$provider.example.toml to copy - write $$plan by hand." >&2; \
			fi; \
			exit 1; \
		fi; \
		echo "Market-data plan ($$provider): $$plan"; \
	done

##@ Stack

# `up`, `start`, `restart` and `dev` build before they boot; these two only
# build. `--profile "*"` is what makes `build` mean every image: a bare
# `docker compose build` enables no profile, so it silently skipped the
# telegram bot, the dev simulator and the audit tool — a changed bot would
# then still run from a stale image. `build-prod` names its services because
# compose has no "all but one" flag — the list is every buildable service a
# production stack runs, which is everything except the dev-only simulator and
# the one-shot audit tool.
build: strategy-requirements ## Rebuild every service image, in every profile
	docker compose --profile "*" build

build-prod: strategy-requirements ## Rebuild every production service image (no dev simulator)
	docker compose build db-migrate data-ingestion strategy-runner telegram-bot

up: market-plan strategy-requirements ## Start the application stack without the dev simulator
	docker compose $(COMPOSE_PROFILE) up -d --build

start: market-plan strategy-requirements ## Start app images; migration completes before apps boot
	docker compose $(COMPOSE_PROFILE) up -d --build
	@echo "Stack is up. Data-ingestion / strategy-runner block until db-migrate exits 0."
	@echo "For the simulator and source mounts, use make dev."

start-prod: market-plan strategy-requirements ## Build and start production services with QTE_ENV=prod
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build db-migrate data-ingestion strategy-runner $(TAILSCALE_SERVICES) $(TELEGRAM_BOT_SERVICES)

# `--profile tailscale` whatever the switch says: a stack started with it on
# and stopped after it was turned off still has its tailnet containers.
stop: ## Stop the stack (volumes survive) — alias of `down`
	docker compose --profile tailscale down

down: ## Stop the stack (volumes survive)
	docker compose --profile tailscale down

restart: market-plan strategy-requirements ## Recreate ingestion and runner, preserving the simulator clock
	docker compose up -d --build --no-deps --force-recreate data-ingestion strategy-runner

# ── Live-editing dev loop ──────────────────────────────────────────────
#
# `make dev` is `start` with the workspace source bind-mounted over the copy in
# the image (docker-compose.dev.yml). Build it once; after that a code edit is
# live in the container, so the loop is: edit -> `make dev-restart` (seconds,
# no rebuild) -> `make logs`. A dependency change still needs `make dev` again.

DEV_COMPOSE := -f docker-compose.yml -f docker-compose.dev.yml --profile dev $(COMPOSE_PROFILE)

dev: market-plan strategy-requirements ## `start` with src/ bind-mounted for live editing
	docker compose $(DEV_COMPOSE) up -d --build

dev-restart: ## Reload ingestion and runner code, preserving the simulator clock
	docker compose $(DEV_COMPOSE) restart data-ingestion strategy-runner

# Streams until interrupted. SERVICE narrows it to one container — the way to
# watch a single process now that the services only run in compose:
#   make logs SERVICE=data-ingestion   (or strategy-runner, market-simulator)
logs: ## Tail every service, or one: make logs SERVICE=strategy-runner
	docker compose logs -f --tail=100 $(SERVICE)

logging: ## Stream the whole stack's logs live, newest lines only (SERVICE= narrows it)
	docker compose logs -f --tail=0 --timestamps $(SERVICE)

nuke: ## Stop the stack and DELETE its volumes (audit trail included)
	docker compose --profile tailscale down -v

# The image's containerboot puts tailscaled's socket at /tmp/tailscaled.sock,
# not where the CLI looks by default. A node that has not logged in yet fails
# `status`; its container log then carries the login URL. MSYS_NO_PATHCONV stops
# Git Bash on Windows from rewriting that path into C:/Users/.../Temp/...; it is
# ignored everywhere else.
TS_CLI = MSYS_NO_PATHCONV=1 docker compose $(COMPOSE_PROFILE) exec tailscale tailscale --socket=/tmp/tailscaled.sock

tailscale-status: ## Show the tailscale container's tailnet status, Serve forwards and login URL
ifeq ($(TAILSCALE_ACTIVE),1)
	@$(TS_CLI) status || docker compose $(COMPOSE_PROFILE) logs --tail=30 tailscale
	@$(TS_CLI) serve status
else
	@echo "Tailscale is disabled (TAILSCALE_ENABLED is not true in .env) — nothing to show."
endif

##@ Database

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

##@ History and backtests

csv-import: ## Convert an MT5 CSV export to parquet: make csv-import CSV=data/csv/x.csv [TZ=EET] [ARGS=--overwrite]
	uv run python scripts/mt5_csv_to_parquet.py $(CSV) --tz $(or $(TZ),UTC) $(ARGS)

# Writes data/parquet/<provider>/<SYMBOL>_<TF>.parquet - one file per source,
# so `make backtest` has to name the one it replays.
download: ## Fetch provider history for the market-data plan [ARGS="--symbol X --timeframe M15 --market fx"]
	uv run qte-backtest download $(ARGS)

SPREAD ?=
SLIPPAGE ?=
COMMISSION ?=

backtest: ## Replay one strategy: make backtest STRATEGY=... SYMBOL=XAUUSD TF=M15 FILE=... [SPREAD=] [SLIPPAGE=] [COMMISSION=]
	uv run qte-backtest run --strategy $(STRATEGY) --symbol $(SYMBOL) \
		--timeframe $(TF) --file $(FILE) --report \
		$(if $(SPREAD),--spread $(SPREAD)) \
		$(if $(SLIPPAGE),--slippage $(SLIPPAGE)) \
		$(if $(COMMISSION),--commission $(COMMISSION))

chart: ## Draw a report as an interactive HTML dashboard: make chart REPORT=data/reports/x.json
	uv run qte-backtest chart $(REPORT)

reports: ## List the backtest reports written so far
	@ls -lht data/reports 2>/dev/null | head -20 || echo "No reports yet — run make backtest"

##@ Simulator

# ── Dev market data simulator ───────────────────────────────────────────
#
# A WebSocket feed you drive by hand, so the whole pipeline can be rehearsed
# without a market being open. Refuses to run unless QTE_ENV=dev. The full
# walkthrough is docs/simulator.md.

# These targets carry no parameters of their own. The symbol, the timeframe,
# the bar counts and the cached history file all come from .env — the same file
# ingestion and the runner read — so a rehearsal cannot aim at a symbol the
# engine is not watching or a window the runner will not keep:
#
#   QTE_ENGINE__SYMBOLS            the symbol, first entry
#   QTE_ENGINE__SIGNAL_TIMEFRAME   the timeframe
#   QTE_SIMULATOR__GENERATE_BARS   synthetic bars for `make warmup`
#   QTE_SIMULATOR_PARQUET_FILE     the vendor parquet `make warmup-cache` plays
#   QTE_SIMULATOR__CACHE_BARS      how many of its trailing bars
#
# The one exception: a synthetic run needs a starting price, and the simulator
# only knows one once a tick, a bar or an earlier replay has set it. On a cold
# simulator pass it — `make warmup START=2400`, `make sim-walk PRICE=2400` — or
# run `make bar ...` first. It is never guessed from the symbol name.
#
# For a one-off, pass the flag instead of editing .env:
#   uv run qte-simulator replay --generate 500 --seed 7 --start-price 2400

sim: ## Run the dev websocket market data simulator (QTE_ENV=dev only)
	uv run qte-simulator serve

sim-status: ## What the simulator is doing, and who is attached to it
	uv run qte-simulator status

# File prices continue the forward series too, so the wall-clock flush cannot
# split historical bars. Verify the full warm-up, including runs sent in batches.
warmup-cache: ## Warm the engine from the cached vendor parquet (QTE_SIMULATOR_PARQUET_FILE)
	uv run qte-simulator replay --verify --timeout 120

warmup sim-replay: ## Warm the engine with synthetic bars (needs START=<price> on a cold simulator)
	uv run qte-simulator replay --generate --seed 7 $(if $(START),--start-price $(START),) --verify

# SYMBOL and TF are overrides, not defaults: left unset the CLI takes the first
# of QTE_ENGINE__SYMBOLS and the engine signal timeframe, which is what
# ingestion subscribed to. Naming anything else sends a bar the engine ignores.
bar sim-bar: ## One bar, round-tripped: make bar O=2400 H=2412.5 L=2396.25 C=2408.75 [V=150] [SYMBOL=] [TF=]
	uv run qte-simulator bar $(if $(SYMBOL),--symbol $(SYMBOL),) $(if $(TF),--timeframe $(TF),) \
		--open $(O) --high $(H) --low $(L) --close $(C) \
		$(if $(V),--volume $(V),) --verify

signal: ## Warmup + drift replay expected to fire a signal (needs START=<price> on a cold simulator)
	uv run qte-simulator replay --generate --seed 7 $(if $(START),--start-price $(START),) --verify
	uv run qte-simulator replay --generate 60 --seed 3 --drift 0.004 \
		--volatility 0.0015 --verify --expect-signal

sim-walk: ## Stream a live-ish random walk until stopped (needs PRICE=<price> on a cold simulator)
	uv run qte-simulator walk --rate 5 $(if $(PRICE),--price $(PRICE),)

sim-stop: ## Stop every background generator
	uv run qte-simulator stop

sim-reset: ## Clear Redis + reset the simulator cursor + restart ingestion (fixes 'no candle arrived')
	-uv run qte-simulator stop
	-uv run qte-simulator reset
	docker compose exec -T redis-cache redis-cli FLUSHDB
	docker compose restart data-ingestion strategy-runner

sim-watch: ## Tail closed candles and emitted signals on NATS
	uv run qte-simulator watch

##@ Live control

shadow-status: ## Show whether signals are reaching the broker
	uv run qte-control shadow status

shadow-on: ## Pause delivery to the broker on every running runner
	uv run qte-control shadow on

shadow-off: ## Resume delivery to the broker (GOES LIVE — prompts to confirm)
	uv run qte-control shadow off

ping: ## Ask the running runners to identify themselves
	uv run qte-control ping

owner-status: ## Show which runner holds the ownership claim for this namespace
	uv run qte-control owner status

owner-clear: ## Remove a stale runner claim after an unclean exit (refuses while a runner answers)
	uv run qte-control owner clear

.PHONY: help install install-dev lock test lint format check tiingo mt5 binance simulator market-plan \
	build build-prod up start stop down restart dev dev-restart logs logging logging-broker nuke tailscale-status \
	strategy-mount strategy-audit strategy-requirements strategy-test strategies audit audit-strict strategy-mapping \
	db-upgrade db-downgrade db-revision db-current db-history db-check \
	download backtest chart reports csv-import \
	sim sim-status sim-replay warmup warmup-cache sim-bar bar signal sim-walk sim-stop sim-reset sim-watch \
	shadow-status shadow-on shadow-off ping owner-status owner-clear
