# Taking MT5 bars from algo-trading-ingester

[`algo-trading-ingester`](https://github.com/rockingrow/algo-trading-ingester)
runs beside the MT5 terminal on Windows, detects every closed bar and publishes
it to NATS on `INGESTER.bar.closed.mt5.<SYMBOL>.<TF>`. The `mt5` provider is
QTE's side of that hand-off:

```bash
make mt5                              # config/data_providers/mt5.toml — symbols, timeframes, stream
QTE_MARKET_DATA__PROVIDER=mt5         # in .env
```

- **Bars, not ticks.** The ingester already closed them, so ingestion skips its
  resampler: each bar goes through the same Redis outbox and
  `QTE.candle.closed.<symbol>.<tf>` subject as a resampled one.
- **The ingester is never kept waiting.** Every message is acknowledged the
  moment it is held — a JetStream ack, or `{"code": 200}` to a core-NATS
  request — and decoded and staged afterwards by a separate worker. When its
  queue is full QTE stops pulling rather than refusing what it already took.
- **QTE asks for its indicator window; the ingester never pushes one.** At
  start-up ingestion sends one request per planned series on
  `INGESTER_RPC.history.mt5.<SYMBOL>.<TF>` and gets the newest `history_bars`
  closed bars back in a single reply, merged into Redis behind the staging
  watermark. QTE decides which series and how many bars — the ingester holds no
  warm-up setting. A symbol the ingester is not configured for is refused once
  and not asked for again.
- **QTE starts without the ingester and does not poll for it.** A boot that
  finds nobody to ask carries on with what Redis holds. The ingester announces
  itself on `INGESTER_RPC.online.mt5` once it is connected and answering — and
  again after every reconnect — and only then does ingestion check each planned
  window against Redis and request the ones that are short or stale; a full,
  current window costs one Redis read and no request. Nothing is asked on a
  timer in between (the one announcement that could go unheard, made while
  QTE's own NATS connection was down, is covered by checking once when that
  connection returns). An ingester that is connected but not ready — its
  terminal still logging in — is asked again after
  `QTE_INGESTION__HISTORY_RETRY_INTERVAL`, doubling to `..._MAX_INTERVAL`.
- **The request never touches the stream.** It is core NATS request/reply on a
  prefix of its own (`rpc_prefix`, default `<subject_prefix>_RPC`). Under
  `subject_prefix` it would be stored as market data and answered by the
  stream's own acknowledgement, so both sides refuse that configuration.
- **A hole is filled before the bar after it is staged.** The ingester
  publishes nothing it found already closed when it started, so a live bar
  that does not follow the newest stored one has the bars in between fetched
  first; the runner never decides on a window with a gap in it.
- **The stream is the fallback, not the history.** A new durable consumer still
  replays what `INGESTER` holds (`deliver_policy`), but inside a filled window
  those bars are duplicates and are dropped; bars older than
  `QTE_RUNNER__CATCH_UP_MAX_AGE` are kept as history, never decided on. A
  restart resumes from the last bar QTE acknowledged. "The newest bars" is not
  an archive: for a backtest, export from MT5 and `make csv-import`.
- **Keep the two `.env` files in step.** `subject_prefix`, `jetstream` and
  `rpc_prefix` in `config/data_providers/mt5.toml` mirror the ingester's
  `NATS_SUBJECT_PREFIX`, `NATS_JETSTREAM_ENABLED` and
  `NATS_RPC_SUBJECT_PREFIX`; `stream` mirrors nothing it can set, because the
  ingester names the stream after that prefix's first token. Its
  `NATS_HOST`/`NATS_PORT` normally name QTE's own NATS, so
  `QTE_MT5__NATS_URL`/`QTE_MT5__NATS_TOKEN` stay blank and fall back to
  `QTE_NATS__*`. Symbols are the bare names in its `MT5_SYMBOLS`, timeframes a
  subset of its `MT5_TIMEFRAMES`.
- **The wire contract is a list, not a constant.** `schema_versions` in
  `config/data_providers/mt5.toml` is which of the ingester's `schema_version`
  values QTE decodes. Each is a full `major.minor.patch` and matches that
  version alone, so every version to accept is named: the default `["1.0.0"]`
  is the ingester's own `SCHEMA_VERSION` default (a shortened version on the
  wire reads as semver, so `1.0` is `1.0.0`), and `["1.0.0", "2.0.0"]` keeps
  bars flowing while a fleet of ingesters is upgraded host by host. A bar on any
  other version is logged with its version and dropped, never decoded on a
  guess — so **any** ingester bump, a patch one included, needs this list edited
  with it, and until it is the feed goes quiet with that warning in the log.
- **Beside another vendor.** `QTE_MARKET_DATA__PROVIDER=mt5,binance` runs both,
  each feeding the symbols of its own `config/data_providers/<provider>.toml`; a symbol may be
  planned by only one. The list is part of the state identity — see
  [state isolation](state-isolation.md). `binance` is a placeholder
  today: listed, it stops ingestion at start-up as not implemented.
