# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Backtest dashboard** (`qte-backtest chart`) — A report rendered as one
  self-contained HTML page, laid out like a strategy tester: cumulative P&L
  against buy-and-hold with the per-trade excursion and underwater band as
  overlays, the price window with every trade marked on it, P&L by
  day/week/month/year, benchmarking with weekly correlation, alternating
  run-ups and drawdowns, the returns distribution and streaks, MAE/MFE against
  realised R, the diagnostics, and the full sortable trade list. The layout is
  borrowed deliberately — it is the one every discretionary trader already
  reads, so the numbers land without a legend.

  It takes the report JSON and nothing else, so a run from three months ago
  still draws on a machine with no parquet history, no strategy installed and
  no engine; and it fetches nothing when it opens, because a report is
  something you email or open offline. Statistics the replay never had the data
  for — intrabar equity, margin, liquidation — are absent rather than
  approximated: a drawdown that ignores open positions is a smaller number and
  would read as an improvement. `--report-format json,md,html` (or `--chart`)
  writes it straight out of a run.

### Changed

- **Report schema 1.1** — adds a `market` block: a downsampled OHLC window of
  the replayed history (each row aggregating `bucket_bars` bars — first open,
  highest high, lowest low, last close) plus the buy-and-hold basis, anchored
  at the first bar *after* warm-up so the benchmark is not credited with a
  stretch the strategy was never allowed to trade. It is the one thing the
  trade list cannot re-derive, and it is for drawing only — every metric in the
  report still comes from the full series. `run.default_quantity` is recorded
  alongside it so the benchmark is sized the way the strategy was. Additive:
  a consumer of 1.0 reads a 1.1 report unchanged.

## [0.1.0] - 2026-08-24

First release of **Quant Trading Engine** — an event-driven framework for
developing, backtesting and running quantitative trading strategies, publishing
signals to [`algo-trading-broker`](https://github.com/rockingrow/algo-trading-broker).

The engine is public and carries no edge: strategies live in `__strategies__/`,
a git-ignored mount cloned from a private repository at deploy time. Pre-1.0 —
the broker payload contract is pinned by tests, but the plugin and provider
interfaces may still move.

### Added

- **Data ingestion** (`qte-ingestion`) — Provider live feed → resampler → Redis
  (hot state) + NATS (`QTE.candle.closed.<symbol>.<timeframe>`). Redis is
  written before NATS on purpose: the runner rebuilds its warm-up window from
  Redis, so a candle that reached the bus but not the cache would be a bar the
  engine acts on now and cannot see after a restart. Ticks are published on
  `QTE.tick.<symbol>` only when asked for
  (`QTE_INGESTION__PUBLISH_TICKS`) — a busy FX feed is a lot of traffic to move
  for subscribers that discard it.

- **Resampler** — Ticks into candles across several timeframes at once. A bar
  closes when the **clock** passes its bucket's end, not when the next tick
  arrives: in a thin session the next print can be two minutes late, and every
  worker downstream expected the M15 bar on the quarter hour. A bucket with no
  ticks produces no candle — forward-filling a flat synthetic bar would feed
  strategies a body that never traded and corrupt any indicator with a range in
  it. The in-progress bar is persisted to Redis so a restart mid-bar resumes
  rather than losing it, and an out-of-order tick from a reconnect replay is
  dropped rather than repainting a candle strategies have already acted on.

- **Strategy runner** (`qte-strategy-runner`) — The live loop: NATS candle
  closes in, broker signals out. One instance per `(strategy, symbol)` pair, so
  a strategy carrying state between bars never has gold's last bar deciding
  what happens on bitcoin's next one. Warms its indicator window from Redis on
  boot instead of waiting hours for live candles, restores the open trade-cycle
  id so a restart mid-trade still closes the position it opened, and publishes
  before it audits — a slow Postgres delays a log line, never a trade. A
  strategy that raises is logged and skipped; the other strategies are still
  trading.

- **Backtest engine** (`qte-backtest`) — History downloader, parquet store,
  replay loop, fill simulator, metrics and reports. The simulator is
  deliberately pessimistic: entries and exits cross the spread and pay
  slippage, a bar whose range covers both the stop and the target takes the
  **stop**, a gap through a level fills at the bar's open, a second entry while
  a position is open is rejected exactly as the broker's worker rejects it, and
  a position still open on the last bar is marked out so unrealised P&L cannot
  flatter the result.

- **Backtest report** — A JSON artefact for an agent plus a Markdown companion,
  same object twice. Beyond the headline metrics it carries R-multiples,
  MAE/MFE per trade, each partial exit leg, the exact broker payloads the run
  would have published, and a `reading_guide` block spelling out the
  conventions. The part worth having is `diagnostics`: a rule set that reads
  the finished run and says what is wrong with it, each finding stating the
  threshold it tripped and proposing one concrete change.
  `report.is_trustworthy` goes false when anything critical fired, so an agent
  knows to stop reading the metrics as meaningful. Documented in
  [`docs/backtest-report.md`](docs/backtest-report.md).

- **Plugin strategies** — The engine loads strategies out of `__strategies__/`
  by file path — a mounted volume, not an installed package — so the public
  engine and private algorithms are versioned and released independently. Two
  ways in: a `strategies.py`/`manifest.py` exposing `load_all()`, or a
  recursive directory scan for a single loose file. The contract is
  **structural**: a plugin repo may restate `SignalStrategy` and `SignalIntent`
  on its own side and never import `qte_shared` at all, so it can build, lint
  and test with this repo nowhere in sight.

- **The signal interface** — A strategy presents one method per broker action:
  `long`, `short`, `tp1`, `tp2`, `sl` required, `r_sl` and `flat` optional.
  `SignalStrategy` dispatches them in a fixed order — holding asks
  `sl → r_sl → tp1 → tp2 → flat`, flat asks `long → short` — with the stop
  before any target, because if one bar both stopped out and reached a target,
  the stop is what happened. A method returning somebody else's action raises
  rather than reaching the broker as a valid-looking payload.

- **`history_window()`** — The live runner and the backtest hand a strategy the
  same number of bars (`max(warmup * 2, 400)` by default). Before it existed
  the runner kept a bounded deque while the replay passed the whole file, so a
  strategy using a running sum or a session VWAP computed one thing on the
  chart and another in production. The runner warns when Redis retains fewer
  bars than a strategy asks for, since that is the same divergence by another
  route.

- **Strategy audit** (`qte-strategy-audit`) — The deploy gate. The loader is
  forgiving by design — what it cannot drive it logs and skips — which is right
  for a running process and useless as a deploy check, because "skipped" and
  "there were none" read identically in a log until the P&L does not arrive.
  The auditor is the strict pass over the same directory: missing signal
  methods, bad arity, undrivable classes, duplicate names, ambiguous manifests,
  bad timeframes or warm-ups, and routing that names a strategy nobody
  publishes. Human, JSON and Markdown output; every finding carries a `fix`;
  the exit code is the product. Also runs in the runner's own process on every
  start (`QTE_RUNNER__AUDIT_ON_START=off|warn|error|strict`), because
  `__strategies__/` is a bind mount that can be changed after CI looked at it.

- **Symbol → strategy routing** (`config/strategies_mapping.toml`) — Which
  strategies trade which symbols, with per-pair parameter overrides, in a
  git-ignored TOML file beside a tracked template. What you trade and at what
  risk is position information and this repo is public, so the schema stays
  reviewable in history while the book does not. TOML rather than environment
  variables because this is a matrix — symbol × strategy × parameters — and
  flattening a matrix into `QTE_ROUTING__XAUUSD_0` is how it stops being
  reviewable. With no file at all each strategy keeps the symbols it declares;
  a file that exists but routes nothing means *trade nothing*, which is a
  different thing and is treated as one.

- **Market data provider seam** — Engines call `create_provider()` and program
  against `HistorySource` and `LiveFeed`; nothing above that line knows a URL,
  an auth header or a wire format. Adding a vendor is a file under
  `qte_shared/providers/` and a `register_provider` call; swapping one is
  `QTE_MARKET_DATA__PROVIDER`. A vendor's own configuration lives with the
  vendor, so root `Settings` has no vendor-shaped hole in it, and its client
  libraries are an extra on `qte-shared` rather than a hard dependency.

- **Tiingo provider** — FX and crypto, history over REST and ticks over
  WebSocket, one socket per market. Reconnect is unbounded with capped
  exponential backoff and jitter: a feed that gives up at 3am leaves the engine
  silently blind, which is worse than a socket that keeps rattling the door and
  logging that it cannot get in.

- **Dev-only market data simulator** (`qte-simulator`) — A WebSocket server
  that speaks a market feed, so the whole pipeline — feed → ingestion → NATS →
  runner → signal — can be rehearsed with no market open and no vendor key.
  It is reached as an ordinary provider (`QTE_MARKET_DATA__PROVIDER=simulator`),
  which means there is no branch in any service and no second code path to rot:
  the code being rehearsed is the code that trades. Drive it with
  `qte-simulator tick | bar | replay | walk | watch | status`. Documented
  step by step in [`docs/simulator.md`](docs/simulator.md).

  - **Bars are sent as ticks, never as candles.** A bar is expanded into the
    four prints it is made of and ingestion's own resampler rebuilds it, so the
    component with the most arithmetic and the least visibility is always in
    the path. Publishing a ready-made candle would only prove the simulator can
    publish a candle.
  - **`--verify` checks the far end.** It subscribes to
    `QTE.candle.closed.<symbol>.<timeframe>` *before* sending, then compares
    what came back against what went in — open, high, low, close, volume, tick
    count — and exits non-zero on a mismatch. `--expect-signal` additionally
    waits for the runner to emit one on `QTE.signal.emitted`.
  - **One forward series, so the commands compose.** A resampler also closes
    bars on the wall clock, so replaying historical timestamps lets that timer
    fire between two ticks of the same bar and publish half of it. Every
    command therefore continues one forward series: `--anchor next` (the
    default) is the first bucket nothing has been sent into yet, no bucket's
    end has passed, and each bar is closed by the arrival of the next with the
    last sealed by an explicit tick. An unstamped tick and a `walk` start from
    the later of the wall clock and the series, since anything behind the bar
    the resampler holds open is dropped as late. `--anchor past` is kept for
    exercising the flush path itself.
  - `--generate N --seed S` synthesises a gapless run for warming a strategy up
    (300 M15 bars in about a second, every one verified); `--file` replays
    parquet, CSV or JSONL; `walk` streams a random walk at `--speed 1` (a live
    feed) or `--speed 120`, where an M15 bar closes every seven seconds.
  - Behind the `dev` compose profile, because starting it alongside a real feed
    gives one symbol two sources and the resampler drops whichever tick arrives
    second.

- **Broker delivery** — QTE emits exactly the payload `algo-trading-broker`
  validates. Two transports: `nats` publishes the envelope to JetStream
  `SIGNALS.<strategy>` — the same buffer the broker's own webhook endpoint
  writes to, inheriting its persistence, retry and de-duplication with no HTTP
  hop in the trade path — and `http` POSTs `/secret/webhook`, which is the path
  that verifies the token. Each publish carries a fresh `Nats-Msg-Id`, so a
  retry inside the stream's duplicate window opens one position rather than two.

- **Trade cycles** — `signal_uxid` is minted by an entry and reused by every
  TP/SL/FLAT that follows it, which is how the broker groups a whole trade into
  one broadcast. Kept in Redis, so a runner restarted mid-trade still closes the
  position it opened. Malformed ids are rejected in our own process rather than
  discovered as a 422 after the trade decision has been made.

- **Shadow mode** — Signals are built, logged and audited but not delivered
  (`QTE_BROKER__SHADOW_MODE`, on by default). The switch travels on NATS so it
  reaches a running runner without a restart mid-position, and is written to
  Redis first so a runner that starts after the broadcast still comes up in the
  mode you last chose.

- **Operator CLI** (`qte-control`) — `shadow status | on | off` and `ping`.
  Going live prompts unless `--yes`. When NATS is unreachable the command says
  so explicitly rather than reporting success: "stored, but the process running
  right now did not hear it" is a different outcome from "applied".

- **First-party indicators** — Trend, volatility, momentum and volume:
  `ema`/`rma`/`wma`/`hma`, `atr`/`bollinger`/`keltner`, `rsi`/`macd`/
  `stochastic`/`adx`, `vwap`, `crossover`/`crossunder`, and a WaveTrend — the
  one the broker's payload schema names and `pandas_ta` does not have. `ema` and
  `rma` use TradingView-compatible SMA seeding so a strategy ported off a Pine
  chart crosses at the same bars. No `pandas_ta` dependency to break;
  `indicators.pandas_ta_frame(df)` is the escape hatch. A strategy repository is
  free to decide otherwise — it owns its own dependencies.

- **Audit trail** — Every signal rowed into PostgreSQL (JSONB) whether it was
  delivered, shadowed or failed, plus `engine_events` for service lifecycle.
  Each engine owns the tables it writes, with its models and repository in its
  own `db/` package, sharing one `DeclarativeBase` so Alembic autogenerate can
  see them all. Alembic owns the schema; there is no init script, because that
  hook only ever runs on an empty data directory — exactly the case a migration
  tool exists to outgrow.

- **One image per service** — A uv workspace with `QTE_PACKAGE` selecting the
  member a build installs, so the ingestion container does not carry pyarrow
  (152 MB, backtest only) and the runner installs no HTTP or WebSocket stack
  for a socket it never opens. A full-workspace venv is 352 MB; each service's
  is ~142 MB. `tests/test_packaging.py` asserts the dependency graph stays a
  star, since a direct edge between two leaf engines is how a service boundary
  quietly stops being one.

- **Docs** — [`docs/architecture.md`](docs/architecture.md) (the decisions that
  are easy to reverse by accident, and why they are the way they are),
  [`docs/broker-contract.md`](docs/broker-contract.md),
  [`docs/backtest-report.md`](docs/backtest-report.md) and
  [`docs/simulator.md`](docs/simulator.md).

- **Tooling** — `make check` (ruff + pytest) with a suite that needs no Redis,
  Postgres or NATS; `make audit-strict` as the deploy gate; Makefile helpers for
  the database, history, backtests, the strategy repo and the simulator.

### Security

- **The simulator refuses to run outside `QTE_ENV=dev`**, and so does the
  provider that reads it — `require_dev_env()` is called before the server binds
  a port and before the provider is constructed, with no override flag. A
  simulator looks exactly like a feed, so an engine wired to one in production
  would trade fabricated prices and report nothing unusual doing it; there is no
  log line that reads "these bars were invented". An escape hatch would be found
  and used by the first person in a hurry.
- **Shadow mode defaults to on.** A fresh deployment builds and audits signals
  and sends nothing until someone deliberately turns delivery on.
- **`__strategies__/` is mounted read-only** into both service containers and is
  never baked into an image, so the private repo can be updated without
  rebuilding the public engine and the engine can never write to it.
- **`config/strategies_mapping.toml` is git-ignored** and mounted read-only.
  Point `QTE_ENGINE__ROUTING_FILE` elsewhere to supply it as a secret in
  production.
- **NATS access is the authentication on the `nats` transport**; the in-payload
  token is only enforced on the HTTP path, which is the one to use across any
  boundary you do not control.

[Unreleased]: https://github.com/rockingrow/quant-trading-engine/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rockingrow/quant-trading-engine/releases/tag/v0.1.0
