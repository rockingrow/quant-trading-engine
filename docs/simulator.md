# Testing the pipeline locally with the market data simulator

This walkthrough runs **simulator → ingestion → Redis/NATS → strategy runner
→ shadow signal → Postgres audit** on one machine. Sections 0–4 need no vendor
account. Section 5 optionally runs a separate offline backtest over local data.

The simulator sends WebSocket ticks through the real ingestion tick handler,
resampler, Redis candle outbox and NATS subjects. It does not publish candles
directly. Its protocol is simulator-specific, not Tiingo's wire format; the
provider boundary is what makes both feeds drive the same pipeline.

Keep `QTE_ENV=dev` and `QTE_BROKER__SHADOW_MODE=true` throughout. The server and
provider refuse to run outside dev. Shadow signals are built, risk-sized,
audited and mirrored on `QTE.signal.emitted`, but never delivered to the broker.

```text
market-simulator --WebSocket ticks--> data-ingestion
                                       |-- Redis: ticks, open bars, history
                                       |-- NATS: QTE.candle.closed.XAUUSD.M15
                                                       |
                                                strategy-runner
                                                       |
                                             QTE.signal.emitted
                                             Postgres signals audit
                                             broker delivery: shadow
```

## 0. Prerequisites and an isolated workspace

- Docker with Compose v2, Python 3.13, uv, and GNU Make.
- On Windows, use Git Bash for the Bash snippets. The Makefile also works
  from PowerShell/cmd using Git for Windows' Bash. Override `WINDOWS_BASH` with
  the installed `bash.exe` path without spaces if Git is elsewhere.
- Free host ports `6379`, `5432`, `4222`, `8222`, `8901`.

Use a separate clone if your checkout contains private strategies, a live
configuration or retained trading state. Do not overwrite its `.env` or mapping.

```bash
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine
make install-dev
```

Standalone CLIs below use `uv run`: installation alone does not add the
virtualenv's executables to PATH. Redis and Postgres CLIs run inside Docker.

## 1. Write the local `.env` and market-data plan

Create `.env` in this rehearsal checkout:

```dotenv
# ── .env — local Docker dev ───────────────────────────────────────────
QTE_ENV=dev
QTE_LOG_LEVEL=INFO

# ── Host ports (the container side never changes) ─────────────────────
QTE_REDIS_PORT=6379
QTE_POSTGRES_PORT=5432
QTE_NATS_PORT=4222
QTE_NATS_MONITOR_PORT=8222
QTE_SIMULATOR_PORT=8901

# ── QTE's own event bus ───────────────────────────────────────────────
QTE_NATS__URL=nats://127.0.0.1:4222
QTE_NATS__TOKEN=
QTE_NATS__SUBJECT_PREFIX=QTE

# ── Broker delivery: built and audited, never sent ────────────────────
QTE_BROKER__TRANSPORT=nats
QTE_BROKER__NATS_URL=
QTE_BROKER__NATS_TOKEN=
QTE_BROKER__HTTP_URL=http://127.0.0.1:8080
QTE_BROKER__TOKEN=
QTE_BROKER__SHADOW_MODE=true

# ── Hot state ─────────────────────────────────────────────────────────
QTE_REDIS__URL=redis://127.0.0.1:6379/0
QTE_REDIS__CANDLE_HISTORY=6000

# ── Database Audit trail ───────────────────────────────────────────────────────
POSTGRES_USER=qte
POSTGRES_PASSWORD=qte
POSTGRES_DB=qte_audit
QTE_POSTGRES__DSN=postgresql+asyncpg://qte:qte@127.0.0.1:5432/qte_audit
QTE_POSTGRES__ENABLED=true

# ── Strategy runner ───────────────────────────────────────────────────
QTE_RUNNER__AUDIT_ON_START=warn
QTE_RUNNER__DEFAULT_QUANTITY=0.01

# ── Market data: the dev simulator, not a vendor ──────────────────────
# The simulator needs no key and no plan file. Point this at `tiingo` and the
# symbols, timeframes and vendor knobs come from config/tiingo.toml
# (`make tiingo`) — `make start` checks it is there.
QTE_MARKET_DATA__PROVIDER=simulator
QTE_DATA_PROVIDER_API_KEY=

# ── The trading account ───────────────────────────────────────────────
QTE_ACCOUNT__CAPITAL=1000.0
QTE_ACCOUNT__RISK_PERCENT=1.0
QTE_ACCOUNT__COMMISSION_PER_UNIT=0.0
QTE_ACCOUNT__CONTRACT_SIZE=1.0
QTE_ACCOUNT__MAX_QUANTITY=0.0
QTE_ACCOUNT__QUANTITY_PRECISION=4

# ── Simulator (refuses to run unless QTE_ENV=dev) ─────────────────────
# Host-side URL; compose swaps it for ws://market-simulator:8901/stream
# inside the network.
QTE_SIMULATOR__URL=ws://127.0.0.1:8901/stream
QTE_SIMULATOR__HOST=0.0.0.0
QTE_SIMULATOR__PORT=8901
QTE_SIMULATOR__CONTROL_URL=ws://127.0.0.1:8901/control
QTE_SIMULATOR__LOG_TICKS=false
QTE_SIMULATOR_PARQUET_FILE=data/parquet/tiingo/XAUUSD_M15.parquet

# Cached vendor history for `make warmup-cache`. A bare `qte-simulator replay`
# plays the last QTE_SIMULATOR__CACHE_BARS of this file into the feed, so a dev
# stack rehearses on prices that really printed. The file's own timestamps are
# ignored: the run is re-anchored onto the buckets ending at the current one.
# Trailing bars taken from it. Unset it follows QTE_REDIS__CANDLE_HISTORY, so
# one run fills exactly the window the runner reads back.
#QTE_SIMULATOR__CACHE_BARS=6000
# Bars a bare `--generate` synthesises. Unset it follows
# QTE_ENGINE__WARMUP_CANDLES, which is what the strategies are waiting for.
#QTE_SIMULATOR__GENERATE_BARS=300
```

Compose overrides these host URLs with service names inside containers:
`redis-cache`, `postgres-audit`, `nats`, `market-simulator`. Keep `127.0.0.1`
in `.env` so host CLIs can connect.

If a port is occupied, change its `QTE_*_PORT` **and every host URL containing
that port**, including both simulator URLs. Keep the internal simulator port
`QTE_SIMULATOR__PORT=8901` unchanged. A new `COMPOSE_PROJECT_NAME` creates
separate volumes; stop the previous project first if it uses the same ports.

The simulator needs no key, but it does need a market-data plan — the same
`config/<provider>.toml` mechanism Tiingo uses to say *what* to feed. Write it:

```bash
make simulator
```

That copies `config/simulator.example.toml` to the git-ignored
`config/simulator.toml` (comments stripped), pinning one symbol: `XAUUSD` at
`M15`. `make start` refuses to come up without it. To test another pair, edit
`config/simulator.toml`, the strategy and the mapping together.

## 2. Install and map the example strategy

```bash
mkdir -p __strategies__
cp examples/__strategies__/ema_atr_breakout.py __strategies__/
make strategy-mount
```

The example demonstrates EMA crossover entries with ATR brackets, not a
strategy to trade. `strategy-mount` writes `__strategies__/strategies.toml`,
required before building even with no private repo. A fresh checkout also
contains the inert `_boilerplate` repo; mounting it is harmless.

Create `config/strategies_mapping.toml` with this **complete** content:

```toml
[symbols.XAUUSD]
strategies = ["QTE_EXAMPLE_EMA_ATR"]
```

```bash
make strategies
make audit
```

Expect `QTE_EXAMPLE_EMA_ATR` to load and **zero audit errors**. A warning that
`QTE_BOILERPLATE_M15` is mapped to no symbol is expected.

Do not use `make strategy-mapping` unchanged here: its general template names
unrelated placeholders on several symbols. Editing only XAUUSD still leaves
invalid mappings elsewhere.

For your own repo, clone under `__strategies__/<name>`, run
`make strategy-mount STRATEGY=<name>`, and add it to the mapping table. The
mount records an audit verdict per published strategy in
`__strategies__/strategies.toml`, which is what decides whether the runner
loads each of them — a `false` entry is skipped, so re-run
`make strategy-mount STRATEGY=<name>` once you have fixed what the audit
found. `make strategy-requirements` freezes audited mounts'
dependencies into `deploy/` for the images, creating that directory when
needed. `make start` runs this step for you.

## 3. Bring up and check the stack

```bash
make start
docker compose ps -a
docker compose logs --tail=50 db-migrate data-ingestion strategy-runner
make sim-status
```

`db-migrate` runs `alembic upgrade head`; ingestion and runner wait for its
successful exit. No separate `make db-upgrade` is needed.

| Service | Expected state |
| --- | --- |
| redis-cache, postgres-audit | Up, healthy |
| nats, market-simulator | Up |
| db-migrate | Exited (0); `ps -a` includes stopped containers |
| data-ingestion, strategy-runner | Up, then ready application logs |

Wait for ingestion's `Simulator feed open` and the runner's
`Runner started slots=1 shadow_mode=True`. `sim-status` must list an attached
XAUUSD feed; `ping` must get a runner response. Container startup alone is not
application readiness.

Once those startup messages appear, confirm the runner and shadow state:

```bash
make ping
make shadow-status
```

With fresh Redis, `shadow-status` reports no stored flag and a true environment
fallback. A stored runtime flag overrides that fallback and must also be true.
Setting `QTE_POSTGRES__ENABLED=false` disables application audit writes;
Compose still starts Postgres and runs migrations.

For streaming logs, use another terminal and stop with Ctrl-C:

```bash
make logs
make logs SERVICE=data-ingestion
make logs SERVICE=strategy-runner
```

### Editing code while the stack runs

```bash
make dev
# Edit ingestion or runner code, then:
make dev-restart
```

`make dev` bind-mounts `src/`; Python processes pick up edits after restart.
`dev-restart` restarts ingestion and runner, preserving the simulator clock.
`make restart` rebuilds/recreates those same two services. Dependency or
migration changes need `make dev` or `make start` again. Changes to simulator
code require restarting that service itself; read section 6 before continuing
an existing series.

## 4. Drive the feed, warm up and verify

Run 4.1–4.3 in order on a fresh rehearsal. Stop background walks before
deterministic bar commands: concurrent producers can change a candle's OHLCV.

### 4.1 One bar and one tick

#### 4.1.1 One bar

```bash
make bar O=2400 H=2412.5 L=2396.25 C=2408.75 V=150
```

Expect `1 feed client(s)`, `Verify 1/1`, exact supplied OHLCV and `ticks=4`.
With this walkthrough's default symbol and timeframe, the wrapper resolves to:

```bash
uv run qte-simulator bar --symbol XAUUSD --timeframe M15 \
  --open 2400 --high 2412.5 --low 2396.25 --close 2408.75 --volume 150 --verify
```

It reaches `XAUUSD M15` without naming either: `--symbol` falls back to the
first of `QTE_ENGINE__SYMBOLS` and `--timeframe` to the engine signal
timeframe, and the symbol list is read from `config/simulator.toml` — the same
plan ingestion subscribed to. `make bar SYMBOL=EURUSD TF=M5` overrides both,
but a symbol outside the plan is accepted by the simulator and then ignored
downstream, because ingestion subscribed to that list and nothing else.

Four ticks form the bar; an extra tick in the following bucket seals it.
`--verify` subscribes to NATS before sending and compares ingestion's actual
output. The sealing tick can also leave a separate one-tick candle behind:
verification checks requested bars, not the absence of additional candles.

`bar` states its own `--open`, so it needs no earlier price and runs first on a
cold simulator — unlike `--generate` and `walk`, which continue from a last
price unless given one. It leaves that last price at the close, `2408.75`,
which 4.2 continues from.

#### 4.1.2 One tick

```bash
uv run qte-simulator tick --symbol XAUUSD --bid 2408.55 --ask 2408.95
docker compose exec -T redis-cache redis-cli get qte:tick:XAUUSD
```

A tick is a quote, not one price: the engine reads the midpoint, here 2408.75
(`last` takes precedence when supplied). Redis must show that tick under
`qte:tick:XAUUSD`. The bar above drives the same tick handler but only reports
the candle it produced; this sends a tick you choose and reads back the state it
left.

Quote it at the bar's close so the series continues undisturbed. A tick at any
other price becomes the last price 4.2 continues from, which changes every
figure quoted downstream.

### 4.2 Synthetic warm-up

```bash
make warmup
docker compose exec -T redis-cache redis-cli LLEN qte:candles:XAUUSD:M15
```

This runs `uv run qte-simulator replay --generate --seed 7 --verify`.
Expect `Verify 300/300`. The example needs 220 candles before its decision
hook runs. Redis restore progress is logged at startup; ongoing warm-up
progress is at DEBUG.

Commands continue from the last price the simulator saw — here the `2408.75`
close from 4.1. A synthetic run on a simulator that has seen nothing yet has
no price to continue, so pass one: `make warmup START=2408.75`, or
`uv run qte-simulator replay --generate --seed 7 --start-price 2408.75`. Seed,
start price and generator parameters determine the price path; use the same
`--start-price` to reproduce a run independently. Warm-up over the feed is
ordinary candle traffic: after the minimum window is reached, a strategy can
emit signals during the remaining replay.

### 4.3 Trigger a signal and check the audit

```bash
uv run qte-simulator replay --symbol XAUUSD --generate 60 --seed 3 --drift 0.004 --volatility 0.0015 --verify --expect-signal
docker compose exec -T postgres-audit psql -U qte -d qte_audit \
  -c "SELECT strategy, symbol, action, price, quantity, delivery_status, shadow FROM signals ORDER BY created_at DESC LIMIT 5;"
```

This step builds on 4.2: `--generate` continues from the price the warm-up
left (as 4.2 continued from 4.1), and the strategy has cleared its 220-candle
warm-up only because 4.2 ran first. Run 4.1–4.3 in order. Jumping straight to
this command on a cold simulator fails asking for `--start-price`; the fix is
to run 4.2, not to pass a start price here.

Expect `Verify 60/60` and an example LONG marked `[shadow]`. With the preceding
price path, the entry is approximately 2528.181922 and stop 2513.63404.
Quantity is risk-sized from account capital and stop distance, not fixed at
the strategy's 0.01 proposal. Postgres must show `delivery_status=shadow`
and `shadow=true`.

`--expect-signal` fails if no signal on this symbol arrives. It does not check
a specific strategy/action; inspect the printed name and use the one-strategy
mapping above.

`make signal` combines warm-up and drift. Use it **instead of 4.2–4.3**, once
on a fresh series (after 4.1 has set a price, or with `make signal
START=2408.75`). Repeating it may emit nothing because the runner retains
open cycles. The example delegates exits to broker brackets; shadow mode
does not simulate fills or broker close feedback. This proves signal
production, not a complete entry/fill/exit cycle.

### 4.4 Warm-up from a cached file

A fresh clone has no history file. Import an MT5 export as in section 5 or
set `QTE_SIMULATOR_PARQUET_FILE` to an existing parquet before running:

```bash
make warmup-cache
# Or choose a source explicitly:
uv run qte-simulator replay --file data/parquet/tiingo/XAUUSD_M15.parquet --limit 400 --verify
uv run qte-simulator replay --file scenario.jsonl --verify
```

`warmup-cache` runs `replay --verify --timeout 120`, taking at most
`QTE_SIMULATOR__CACHE_BARS` trailing rows. A shorter file sends its available
rows. CSV/JSONL need `open,high,low,close`; `volume` is optional. Supply rows
oldest first. Convert raw MT5 exports before replaying them.

Every source defaults to `--anchor next` (`auto` resolves to `next`), so file
replay continues the same series after synthetic warm-up without late ticks.
Runs over 5,000 bars use consecutive batches and one final sealing tick.
The wrapper fails if ingestion's output differs or does not arrive in time.

Every history file sits under the source that wrote it: `data/parquet/tiingo/`
for a provider download, `data/parquet/mt5/` for a CSV import. Nothing has a
default path any more — the simulator reads `QTE_SIMULATOR_PARQUET_FILE` or
`--file`, and a backtest reads its own `--file`, so the two never share a file
by accident.

### 4.5 Watch and stream

In one terminal:

```bash
uv run qte-simulator watch --symbol XAUUSD --timeframe M15 --seconds 30
# Or stream until Ctrl-C:
make sim-watch
```

In another:

```bash
uv run qte-simulator walk --symbol XAUUSD --rate 20 --speed 120 --ticks 200 --seed 7
make sim-status
# Let the bounded walk finish before stopping if you want a complete M15 bar.
make sim-stop
```

At 120x speed, an M15 bucket advances in about 7.5 seconds. Without `--ticks`,
a walk continues until stopped. `make sim-walk` starts one at 5 ticks/s and
normal speed; on a simulator that has seen no price for the symbol yet, pass
`make sim-walk PRICE=2400` or `walk --price 2400`. On a fresh series at speed
1, ingestion's flush timer closes quiet bars on the clock. After accelerated
replay, the walk continues the future series; it does not immediately return
to wall-clock timestamps.

### 4.6 Verify warm-up after runner restart

```bash
make sim-stop
docker compose restart strategy-runner
docker compose logs --tail=30 strategy-runner
```

Wait for `Warm-up QTE_EXAMPLE_EMA_ATR/XAUUSD M15: <count>/220 candles from Redis`
and `Runner started`. The example's history window is 440 candles even if
Redis retains 6,000. A restored count at least 220 is ready; an existing open
cycle is restored too. Then confirm the feed continues:

```bash
make ping
make bar O=2400 H=2412.5 L=2396.25 C=2408.75 V=150
```

## 5. Optional offline backtest and chart

Backtests read stored history on the host, independently of the Docker stack.
They do not pass through WebSocket/ingestion/NATS.

```bash
make csv-import CSV=data/csv/XAUUSD_M15.csv TZ=EET ARGS="--symbol XAUUSD --timeframe M15"
make backtest STRATEGY=QTE_EXAMPLE_EMA_ATR SYMBOL=XAUUSD TF=M15 FILE=data/parquet/mt5/XAUUSD_M15.parquet
make chart REPORT=data/reports/<actual-report-name>.json
```

Use the MT5 server's timezone; `EET` is only an example. Short filenames need
explicit symbol/timeframe arguments; standard MT5 filenames containing date
ranges can be inferred. The importer writes `data/parquet/mt5/` by default, and
`make backtest` needs `FILE=` naming the parquet to replay.
Use the JSON path printed by the backtest for `make chart`, then open the
generated HTML in a browser. Reports stay local under `data/reports/`.
Synthetic fixtures can smoke-test the tooling but do not measure performance.

Downloading history is optional and needs a vendor key. `make download` uses
the selected provider and cannot download from the simulator. With a configured
Tiingo key, use this Bash override for the host job only:

```bash
make tiingo
QTE_MARKET_DATA__PROVIDER=tiingo make download
# One symbol and timeframe only:
QTE_MARKET_DATA__PROVIDER=tiingo make download \
    ARGS="--symbol XAUUSD --timeframe M15 --market fx"
```

It writes `data/parquet/tiingo/XAUUSD_M15.parquet` — the same file
`QTE_SIMULATOR_PARQUET_FILE` points at, and the one to pass to
`make backtest FILE=`.

This does not switch the running ingestion container.

## 6. Clocks, restarts and recovery

Ingestion closes a bar when a later-bucket tick arrives or the wall clock
passes its end. Empty buckets produce no candle. Consecutive buckets may have
price gaps: the next open need not equal the previous close.

| Placement | Behaviour |
| --- | --- |
| `next` (default for all sources) | First untouched bucket at or after the current bucket, then forward. A final sealing tick closes the last bar by default. |
| `past` (explicit diagnostic only) | Ends on the last completed bucket. The flush timer can split a bar between its ticks. A previous forward replay can make the whole run late. |
| ISO timestamp | Explicit first bucket, then contiguous bars. The same flush/late-tick rules apply. |

Forward replay avoids the historical flush race but stamps candles ahead of
wall time. Each sealed command leaves a one-tick bucket before the next
command. `--no-seal` leaves the final bar open, so verification may time out
unless another command or the wall clock closes it.

File timestamps, session gaps and original tick order are not preserved.
Four invented ticks reproduce each OHLCV row. This tests ingestion, warm-up
and signal wiring; use offline backtests for original dates/session logic
and fill simulation.

```bash
make db-current
make restart       # ingestion + runner; simulator clock stays alive
make down          # stop containers/network, retain volumes
```

The simulator clock is in memory. Ingestion's open bar and runner history
survive in Redis; open cycles also survive in Postgres. Restarting the
simulator, or `down` followed by `start`, can leave it behind the restored
future bar. Restarting both processes does not remove persisted state.

For independent scenarios, stop the old project and select a new
`COMPOSE_PROJECT_NAME` in this rehearsal's `.env` before `make start`.
That retains previous results and gives the new scenario empty volumes.

`make sim-reset` is destructive: it clears the selected Redis database using
`FLUSHDB`, resets the simulator and restarts ingestion/runner. It is **not**
a complete account reset: Postgres open positions can be restored immediately.
Use it only after verifying the exact development target and accepting cache
deletion. `make nuke` deletes the project's volumes and audit history;
neither command is required for this walkthrough.

## When a check fails

| Symptom | Check |
| --- | --- |
| Simulator refuses to start | Both server and provider require `QTE_ENV=dev`. |
| No attached XAUUSD feed | Wait for startup; check provider and container simulator URL. |
| `db-migrate` failed | Read its logs; ingestion/runner wait for exit 0. |
| Unknown strategy names in audit | Use the complete example mapping, not the placeholder template. |
| Candle arrives, no signal | Warm-up, enabled strategies, mapping, entry rule, or already-open cycle. |
| File replay loses all candles | Explicit past/old ISO anchor, or simulator restart behind persisted state. |
| Wrong OHLCV or tick count | Stop concurrent walks, avoid past timestamps, enable `QTE_SIMULATOR__LOG_TICKS=true`. |
| No final candle | Check `--no-seal`, timeframe, timeout and ingestion logs. |
| Watcher misses logged candles | Compare NATS endpoints and subject prefix. |
| Host CLI cannot connect after port change | Update URLs as well as published ports. |
| Container strategy import fails | Re-run `make strategy-mount` and `make start` to install dependencies. |

## Command reference

```text
uv run qte-simulator serve   [--host] [--port]
uv run qte-simulator tick    [--symbol] [--bid] [--ask] [--last] [--volume] [--ts]
uv run qte-simulator bar     --open --high --low --close [--volume]
                             [--symbol] [--timeframe] [--anchor] [--spread]
                             [--no-seal] [--verify] [--expect-signal] [--timeout]
uv run qte-simulator replay  [--file F | --generate [N]] [--limit N]
                             [--symbol] [--timeframe] [--anchor] [--rate]
                             [--start-price] [--seed] [--volatility] [--drift]
                             [--spread] [--no-seal] [--verify] [--expect-signal]
                             [--timeout]
uv run qte-simulator walk    [--symbol] [--rate] [--speed] [--ticks] [--price]
                             [--volatility] [--spread] [--seed]
uv run qte-simulator stop    [--name walk:XAUUSD]
uv run qte-simulator status
uv run qte-simulator reset
uv run qte-simulator watch   [--symbol] [--timeframe] [--seconds]
```

Global `--url` and `--json` go before the subcommand. Symbol defaults to the
first engine symbol; timeframe defaults to the engine signal timeframe.
The simulator never guesses a price from the symbol: the first `--generate`
for a symbol needs `--start-price`, and `walk` needs `--price`, until a tick,
bar or replay has set a last price for it to continue from.
Both endpoints accept JSON over WebSocket: subscribe on `/stream` with
`{"op":"subscribe","symbols":["XAUUSD"]}`, or send to `/control` with
`{"op":"tick","symbol":"XAUUSD","last":2401.5}`.
Frames are defined in `qte_shared.providers.simulator.protocol`.

## Optional: switch ingestion to a vendor

This is outside the no-vendor rehearsal. Configure a key, set
`QTE_MARKET_DATA__PROVIDER=tiingo`, and create the plan with `make tiingo`.
Keep shadow mode on. On its first start against the vendor, ingestion discards
the candle state the simulator left in Redis — candle lists, open bars and the
candle outbox — and backfills from Tiingo, so future simulator bars cannot cause
real ticks to be discarded as late. Open positions from a rehearsal are not
touched: clear those deliberately before reading a vendor run's audit trail.

An idle simulator cannot race Tiingo: ingestion selects one provider.
To start only the vendor app services and their dependencies:

```bash
make market-plan
make strategy-requirements
docker compose up -d --build data-ingestion strategy-runner
docker compose stop market-simulator
```

Plain `make start` also starts the simulator. Broker delivery is a separate
setup described in [README.md](../README.md).

## Related reading

- [Architecture](architecture.md)
- [Broker payload contract](broker-contract.md)
- [Backtest report schema](backtest-report.md)
