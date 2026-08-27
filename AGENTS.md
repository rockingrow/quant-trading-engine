# Repository Instructions

Shared instructions for every coding agent working in this repository. Codex
reads this file directly; Claude Code reads it through the `@AGENTS.md` import
at the top of `CLAUDE.md`. **Keep shared content here** — anything specific to
one tool goes in that tool's own file, so the two never drift apart.

Follow a more specific `AGENTS.md` in a subdirectory when one exists.

## The project in five lines

Event-driven quant trading engine. Ingestion → Redis/NATS → strategy runner →
`algo-trading-broker`; the backtest engine replays the same strategy interface
offline. The engine is public, the alpha is not: strategies load by path from
the git-ignored `__strategies__/`. Python 3.13, uv workspace over `engines/*`.

## Rules — [AUDIT.md](AUDIT.md) is the authority

Digest, binding on every change. AUDIT.md holds the rationale, the exception
table and the check commands; read it before you commit or open a PR.

1. **Commits**: only when the user asks. English, imperative, 72-char subject
   at most. **No AI attribution of any kind** — no `Generated with Claude
   Code`, no `Co-Authored-By: Claude`, no ChatGPT/Codex footer, no session link.
2. **English** for comments, docstrings, identifiers, log and exception strings,
   and every committed Markdown file.
3. **Names**: explicit and **at least 6 characters** (`candles` not `df`,
   `bar_count` not `n`). Exceptions only for `self`/`cls`/`_`, names fixed by an
   external contract (`tp1`, `sl`, OHLC columns) and whole-word market terms
   (`bid`, `atr`). Do not rename legacy variables the task does not touch.
4. **Pull requests always target `dev`** — `gh pr create --base dev`. Never
   `master`. Opening the PR is the user's call.
5. **Artefacts**: backtest output to `data/reports/`, audits to `data/audits/`
   (`YYYY-MM-DD-<topic>.md`) — both git-ignored and local-only, never pushed to
   GitHub. Never into `docs/` or the repository root.

## Working approach

- Read the relevant source, tests, configuration and documentation before
  editing.
- Inspect `git status` first. Preserve every unrelated change and untracked
  file in the working tree.
- Make the smallest coherent change that solves the problem and matches the
  existing architecture.
- Do not add or upgrade production dependencies unless the task requires it;
  say why when you do.
- Never expose, commit or copy secrets from `.env`, credentials, tokens, or
  private strategy data.

## Commands

```bash
make check          # Ruff + pytest — what CI runs; the gate before any commit
make test           # uv run pytest -q   (one file: uv run pytest tests/test_replay.py -q)
make lint / format  # ruff check / ruff format + --fix
make audit          # validate __strategies__/ against the signal contract
make db-check       # fail if the models drifted from the migrations
make db-upgrade     # Alembic; there is no init script
make infra          # redis + postgres + nats only
make backtest STRATEGY=QTE_EXAMPLE_EMA_ATR SYMBOL=XAUUSD [TF=M15]
make chart REPORT=data/reports/<file>.json
make help           # every target, one line each
```

## Repository navigation

Route the task with the table before searching. Start inside the owning
package; never scan from the repository root.

1. Table below, to find the owning package.
2. `sed -n '1,25p' <file>` — **every module opens with a docstring** stating its
   job and its trade-offs. That usually answers "does this file do X".
3. `rg -n "<symbol>" <owning-package>/src tests` — scope the search.
4. `tests/test_<topic>.py` — the suite is organised by topic and reads as the
   executable spec for that module.

| Task or concept | Primary location |
| --- | --- |
| Wire models, enums, candles, ticks, `SignalIntent` | `engines/shared/src/qte_shared/models.py` |
| Strategy contract and its seven methods | `engines/shared/src/qte_shared/strategy_base.py` |
| Strategy discovery and manifests | `engines/shared/src/qte_shared/plugin_loader.py` |
| Intent to broker payload | `engines/shared/src/qte_shared/signal_factory.py` |
| Position sizing and account risk | `engines/shared/src/qte_shared/sizing.py` |
| Indicators (pure, arrays in and out) | `engines/shared/src/qte_shared/indicators.py` |
| Timeframes, candle buckets, symbol markets | `engines/shared/src/qte_shared/{timeframes,symbols}.py` |
| Symbol to strategy routing | `engines/shared/src/qte_shared/routing.py`, `config/strategies_mapping.example.toml` |
| Settings and `QTE_*` environment variables | `engines/shared/src/qte_shared/config.py`, each service's `settings.py`, `.env.example` |
| NATS subjects and publishing | `engines/shared/src/qte_shared/bus/{subjects,nats_bus}.py` |
| Redis state and the candle outbox | `engines/shared/src/qte_shared/cache/redis_state.py` |
| Postgres models and repositories | `engines/shared/src/qte_shared/db/`, each engine's `db/`, `migrations/versions/` |
| Market data interface and vendors | `engines/shared/src/qte_shared/interfaces/market_data.py`, `providers/` (`registry.py`, `tiingo/`, `simulator/`) |
| Live feed, resampling, Redis and NATS | `engines/data_ingestion/src/qte_ingestion/{service,resampler}.py` |
| Live loop, broker delivery, control CLI | `engines/strategy_engine/src/qte_strategy_engine/{runner,broker_sink,preflight,control}.py` |
| Backtest replay, fills, metrics, reports | `engines/backtest_engine/src/qte_backtest/{replay,execution,metrics,report,diagnostics}.py` |
| Parquet history: download, read, list | `engines/backtest_engine/src/qte_backtest/{downloader,data_store}.py` |
| Backtest HTML dashboard | `engines/backtest_engine/src/qte_backtest/visualize/` |
| Strategy deploy audit | `engines/strategy_audit/src/qte_strategy_audit/{auditor,contract}.py` |
| Dev-only WebSocket simulator | `engines/market_simulator/src/qte_simulator/` (refuses to run unless `QTE_ENV=dev`) |
| **Why** something is built this way | `docs/architecture.md` — 20 "Why X" sections; run `rg -n '^## ' docs/architecture.md`, then read only the one you need |
| Broker payload contract | `docs/broker-contract.md` |
| Backtest report schema | `docs/backtest-report.md` |
| Simulator walkthrough | `docs/simulator.md` |

CLI entry points: `qte-backtest`, `qte-ingestion`, `qte-strategy-runner`,
`qte-control`, `qte-strategy-audit`, `qte-simulator`.

`README.md` is ~46KB — never read it whole. Run `rg -n '^#{1,3} ' README.md` for
the section index, then read only that range.

Do not scan `.venv/`, `uv.lock`, caches, or generated data under `data/csv/`,
`data/parquet/`, `data/reports/`. Treat `__strategies__/` as private and out of
scope unless the task targets it explicitly; read
`__strategies__/_boilerplate/` or `examples/__strategies__/` when you only need
the plugin shape.

## Architecture invariants

- `qte_shared` must not import another engine. Every other engine depends on
  `qte_shared` and on nothing else in the repository.
- Signal behaviour must stay identical between `qte_backtest.replay` and
  `qte_strategy_engine.runner`, and both must keep going through
  `signal_factory` — otherwise a backtest stops predicting live behaviour.
- The plugin contract is **structural, not nominal**: a strategy repo restates
  the interface on its own side. Changing `strategy_base.py` or `models.py` is a
  contract change — check
  `engines/strategy_audit/src/qte_strategy_audit/contract.py` and run
  `make audit`.
- Schema changes go through the single Alembic chain under `migrations/`
  (`make db-revision M="add x"`); `env.py` there imports every engine's models.
  No ad-hoc schemas.
- Bars close on the clock, not on the next tick. Preserve that in ingestion and
  in the simulator.

## Code style

- Python 3.13, `uv` workspace. Run Python tooling through `uv run`.
- Ruff rules `E,F,I,UP,B`, line length 100. Run `make format` before committing.
- Async throughout the services; pytest runs with `asyncio_mode = "auto"`.
- Prefer explicit types and domain terminology over clever, compressed code.
- Cover behaviour changes with focused tests, including failure paths and
  boundary cases.

## Verification

- Run the narrowest relevant tests while iterating.
- Before handing back a code change: `make check` (or `uv run ruff check .` and
  `uv run pytest -q`).
- If models or migrations changed, also run `make db-check`.
- If the signal contract or anything under `__strategies__/` changed, run
  `make audit`.
- Report every command you ran and every failure or skipped check. Never claim
  a check passed without running it.

## Trading and destructive operations

- Do not enable live trading, disable shadow mode, send live orders, or change
  live credentials without an explicit request and confirmation of the target
  environment.
- `make shadow-off`, `make nuke`, database downgrades, volume deletion and
  history rewrites are destructive: verify the exact target and get explicit
  approval immediately before running them.
- Prefer deterministic backtests, and record the data range, configuration,
  strategy, symbol, timeframe, seed and material assumptions needed to
  reproduce a result.
