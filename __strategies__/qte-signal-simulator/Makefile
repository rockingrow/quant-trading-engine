.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Sync the venv (runtime deps only)
	uv sync --no-dev

install-dev: ## Sync the venv with pytest and ruff
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

list: ## Print every strategy alias this repo publishes
	@uv run python -c "import manifest; print('\n'.join(manifest.aliases()))"

describe: ## Metadata for one strategy: make describe ALIAS=SIGNAL_SIMULATOR_LIFECYCLE_V1
	@uv run python -c "import json, manifest; print(json.dumps(manifest.load('$(ALIAS)')().describe(), indent=2, default=str))"

.PHONY: help install install-dev lock test lint format check list describe
