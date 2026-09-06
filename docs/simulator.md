# Testing the pipeline locally with the market data simulator

A step-by-step rehearsal of the whole flow — **feed → ingestion → NATS →
strategy runner → signal** — on one machine, from a fresh clone to a closed
candle and an emitted signal, with no market open, no vendor key, no order ever
leaving the box, and no waiting fifteen minutes to see whether a bar closed.

The feed is the built-in **market data simulator**: a WebSocket server that
speaks the same protocol a vendor would. `data-ingestion` connects to it exactly
as it connects to Tiingo, so what you are exercising is the real pipeline; only
the prices are invented. Delivery to `algo-trading-broker` stays in **shadow
mode** throughout — signals are built and audited, never sent. Swapping the feed
for a real vendor is one variable, and it is the last section here.

```
docker compose  (make start)                 host CLIs you drive it with
────────────────────────────                 ───────────────────────────
market-simulator ──ws──▶ data-ingestion ──▶ Redis (warm-up window)
   (dev only)                  │         └──▶ NATS  QTE.candle.closed.<sym>.<tf>
                               │                          │
                               │                strategy-runner ──▶ __strategies__/*.py
                               │                          │
                               ▼                          ▼
                       postgres-audit         NATS SIGNALS.<strategy>  (shadow: not sent)
                       (audit trail)          NATS QTE.signal.emitted  (always)

  qte-simulator serve | tick | bar | replay | walk   ◀── drive from a host terminal
  qte-simulator watch  /  --verify                   ──▶ check the far end here
```

> **It only runs in dev.** Both the server and the provider call
> `require_dev_env()` and refuse to start unless `QTE_ENV=dev`. There is no
> override flag: the simulator fabricates prices, and an engine reading them
> would look entirely normal while trading them.

---

## 0. Prerequisites

- **Docker** and **Docker Compose v2** (`docker compose`, not `docker-compose`).
- **Python 3.13** and [uv](https://docs.astral.sh/uv/). The services run in
  containers, but the CLIs you drive them with (`qte-simulator`, `qte-control`,
  `qte-backtest`, `alembic`) run on the host.
- Free host ports `6379`, `5432`, `4222`, `8222`, `8901` — or edit the
  `QTE_*_PORT` values in `.env`. If `algo-trading-broker` already runs on this
  machine it usually owns Postgres and NATS, so shift QTE's.

```bash
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine
make install-dev              # uv sync, host-side tooling
```

Prefer to run the services on the host, under a debugger, rather than in
containers? Everything up to section 3 is the same; then follow
[Running the services on the host](#running-the-services-on-the-host) instead of
`make start`.

---

## 1. Write the `.env`

`.env` is git-ignored and read by every container through `env_file:`. Copy the
block below into `.env` at the repository root as-is — it is a complete, working
local configuration: dev environment, simulated feed, shadow mode on.

> **Why the URLs say `127.0.0.1`.** `docker-compose.yml` overrides the four
> service URLs with compose service names (`redis-cache`, `postgres-audit`,
> `nats`, `market-simulator`) for anything running *inside* the network, and
> `environment:` beats `env_file:` in compose. So the host-side addresses here
> are what the host CLIs use, and the containers never see them. One file, both
> worlds — do not "fix" them to service names, or `alembic` and `qte-simulator`
> on the host stop resolving.

```bash
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
QTE_RUNNER__ENABLED_STRATEGIES=[]
QTE_RUNNER__STRATEGY_PARAMS={}
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

The simulator is the one provider with no market-data plan: with no
`config/simulator.toml` it feeds **`XAUUSD` on `M15`**, and that is what the rest
of this walkthrough assumes. To rehearse on something else, set
`QTE_ENGINE__SYMBOLS`, `QTE_ENGINE__TIMEFRAMES` and `QTE_ENGINE__SIGNAL_TIMEFRAME`
explicitly.

Two knobs worth knowing before you start:

| Setting | Why you would touch it |
| --- | --- |
| `QTE_POSTGRES__ENABLED=false` | Postgres is only the audit trail and nothing on the tick path waits for it. `false` gives the shortest possible loop, and you can skip the schema step. |
| `QTE_ENGINE__TIMEFRAMES` | Every timeframe listed is another resampler on the same ticks, and another stream of candles in the log. Keep it to one while testing. |

---

## 2. Put a strategy in `__strategies__/`

The runner loads strategies **by path** from the git-ignored `__strategies__/`,
which compose mounts read-only into the containers. An empty directory means the
runner starts and does nothing.

To see the pipeline move without writing anything, use the worked example:

```bash
cp examples/__strategies__/ema_atr_breakout.py __strategies__/
make strategy-mount     # writes __strategies__/strategies.toml — required even with nothing to mount
make strategy-mapping   # config/strategies_mapping.toml from the template (git-ignored)
make audit              # validate it against the signal contract before it runs
```

`make strategy-mapping` never overwrites an existing file. The example publishes
`QTE_EXAMPLE_EMA_ATR`, while the template pairs `XAUUSD` with different names —
so either edit `config/strategies_mapping.toml` to list `QTE_EXAMPLE_EMA_ATR`
under `[symbols.XAUUSD]`, or delete the file entirely. With no mapping table
each strategy simply keeps the symbols it declares.

With your own private repo instead:

```bash
git clone <your private strategy repo> __strategies__/my-strategy
make strategy-mount STRATEGY=my-strategy   # its deps into this venv, for host-side backtests
make strategies                            # confirm the engine can see them
```

`STRATEGY` names the checkout under `__strategies__/`; leave it unset to mount
every checkout there that has a `pyproject.toml`. Either way, `strategy-mount`
installs the repo's deps, then runs `make strategy-audit STRATEGY=<name>`
against it — the same check `make audit` runs, scoped to that one repo,
printing only `true` or `false` — and records the result in
`__strategies__/strategies.toml`, an auto-generated file; never hand-edit or
delete it. `make strategy-requirements` reads it and freezes only the `true`
entries into `deploy/`; `make up`/`make start` run that for you but refuse to
start without the file existing, even if it lists nothing.

---

## 3. Bring the stack up

```bash
make start
```

That is `docker compose up -d --build`, preceded by `make strategy-requirements`
— which freezes the mounted strategy repos' dependencies into
`deploy/strategy-requirements.txt` so the image carries them too. It reads
`__strategies__/strategies.toml` to know what to freeze and refuses to run
without that file — run `make strategy-mount` first (step 2), even with no
private repo cloned. With no repos listed in it, it prints "nothing to freeze"
and carries on.

What comes up, and what each part is for:

| Service | Role |
| --- | --- |
| `redis-cache` | Hot state and the warm-up candle window (AOF on, survives restarts) |
| `postgres-audit` | The audit trail |
| `nats` | QTE's own event bus, JetStream enabled |
| `db-migrate` | One-shot `alembic upgrade head`, then exits 0 |
| `data-ingestion` | Feed → resampler → Redis + NATS |
| `strategy-runner` | Closed candles → strategies → signals |
| `market-simulator` | The dev WebSocket feed, published on `:8901` |

`data-ingestion` and `strategy-runner` wait on `db-migrate` finishing cleanly,
so a plain `up` migrates before anything reads the schema. **There is no init
script, and you do not run `make db-upgrade` by hand here** — Alembic owns the
schema, and the one-shot container is how it gets applied.

Check it:

```bash
docker compose ps           # db-migrate Exited (0), the rest Up
make logs                   # tail everything
```

Two log lines say the stack is healthy: ingestion reporting it connected to the
simulator feed, and the runner listing the strategies it loaded.

### Running the services on the host

When you want a debugger on `data-ingestion` or `strategy-runner`, run them
straight from the source tree instead of in containers, with only the three
infrastructure containers up. The `.env` above already points the host
addresses at `127.0.0.1`, so nothing needs editing.

There is no Make target for that first line on purpose: `make` starts the whole
stack or nothing, so a half-started system is something you ask compose for
explicitly rather than something a one-word command can leave behind.

```bash
docker compose up -d redis-cache postgres-audit nats   # terminal 0: the three of them

make db-upgrade    # apply the schema once (skip if QTE_POSTGRES__ENABLED=false)
make sim           # terminal 1: the simulator
make ingestion     # terminal 2: data-ingestion
make runner        # terminal 3: strategy-runner
```

Ingestion should say it attached:

```
Ingestion started provider=simulator symbols=['XAUUSD'] timeframes=['M15']
Simulator feed open url=ws://127.0.0.1:8901/stream symbols=XAUUSD
```

and the simulator should agree:

```bash
$ qte-simulator status
Simulator  up 27.5s, 0 ticks sent, 0 bars played
  feed #1  ('127.0.0.1', 37268)  XAUUSD  0 ticks
```

`no feed client attached` here means ingestion is not connected — check
`QTE_MARKET_DATA__PROVIDER` and `QTE_SIMULATOR__URL` before going further.
Everything in section 4 sends into a void if this line is missing, and the
simulator will happily report success doing it (the `→ 0 feed client(s)` in each
acknowledgement is the warning).

Postgres is only the audit trail, and nothing on the tick path waits for it. For
the shortest possible loop, set `QTE_POSTGRES__ENABLED=false` and skip
`db-upgrade` entirely.

---

## 4. Drive the feed, step by step

The simulator sends nothing until you tell it to. Every command below runs on
the **host**, against the published port. The `make` wrappers (`make bar`,
`make warmup`, …) pass `--verify` for you; the raw `qte-simulator` form is shown
alongside so you can see what they do.

```bash
make sim-status                   # = qte-simulator status — what it is doing, who is attached
```

### 4.1 One tick

```bash
$ qte-simulator tick --symbol XAUUSD --bid 2400.0 --ask 2400.4
tick XAUUSD @ 2400.2 (2026-08-24T14:45:24.822Z) → 1 feed client(s)
```

Mid price when both sides are quoted, `last` when there is one. Ingestion
writes it to Redis as the symbol's last tick and folds it into the open bar:

```bash
redis-cli get qte:tick:XAUUSD
```

No candle yet — a bar closes when its bucket ends, not when a tick arrives.
That is the next section.

### 4.2 One bar, and proof it came back

This is the check the simulator exists for. The wrapper is
`make bar O=2400 H=2412.5 L=2396.25 C=2408.75 [V=150]`; in full:

```bash
$ qte-simulator bar --symbol XAUUSD \
    --open 2400 --high 2412.5 --low 2396.25 --close 2408.75 --volume 150 --verify

Played 1 XAUUSD M15 bars as 5 ticks → 1 feed client(s)
  buckets 2026-08-24T15:00:00Z … 2026-08-24T15:00:00Z  (+ sealing tick)

Verify  1/1 candles republished by ingestion
  [OK      ] 2026-08-24T15:00:00+00:00 o=2400.0 h=2412.5 l=2396.25 c=2408.75 v=150.0 ticks=4
```

**What actually happened.** Ingestion has no notion of a bar — it has a
resampler that folds ticks into buckets. So "send a bar" means synthesising the
four ticks a bar is made of and letting the real resampler rebuild it:

```
open ──────── low ──────── high ──────── close        bullish (close ≥ open)
open ──────── high ─────── low ───────── close        bearish
t+0          t+¼d         t+½d          t+d−1s
```

Five ticks, not four: the fifth is a **sealing** tick one bucket later, which is
what closes the bar now rather than whenever the clock reaches the end of its
bucket. And the bucket is `15:00` rather than `14:45` because the tick in step
4.1 already opened `14:45` — a bar landing in an occupied bucket would inherit
that tick's price as its open. Every command continues one forward series; see
[Where bars are placed on the clock](#where-bars-are-placed-on-the-clock-and-why-it-matters).

`--verify` subscribes to `QTE.candle.closed.XAUUSD.M15` **before** sending, then
compares the candle that arrives against the bar that went in — open, high,
low, close, volume and tick count. It exits non-zero on a mismatch, so it works
in a script.

If a candle never arrives, the ticks did but the bar did not close. See
[When nothing arrives](#when-nothing-arrives).

### 4.3 A run of bars — warming the strategy up

A strategy does nothing until it has `warmup` bars. The bundled example wants
220 of them, so one bar will never produce a signal however good it looks.
Replay a few hundred — `make warmup` (synthetic, seeded, deterministic), or in
full:

```bash
$ qte-simulator replay --symbol XAUUSD --generate 300 --seed 7 --verify
Generated 300 M15 bars from 2408.75 (seed=7)
Played 300 XAUUSD M15 bars as 1201 ticks → 1 feed client(s)
  buckets 2026-08-24T15:30:00Z … 2026-08-27T18:15:00Z  (+ sealing tick)

Verify  300/300 candles republished by ingestion
  [OK      ] 2026-08-24T15:30:00+00:00 o=2408.75 h=2410.35 l=2407.34 c=2407.52 v=291.1 ticks=4
  [OK      ] 2026-08-27T18:15:00+00:00 o=2477.5 h=2479.86 l=2469.67 c=2470.73 v=238.5 ticks=4
```

Three hundred bars in about a second, every one of them checked. The runner
logs its progress as they land:

```
Warm-up QTE_EXAMPLE_EMA_ATR/XAUUSD M15: 0/220 candles from Redis
```

`--seed` makes the run reproducible: the same seed and the same start price
produce the same 300 bars, so a test that failed can be re-run identically. It
starts at 2408.75 rather than at a reference price because that is where step
4.2 left the series — consecutive commands continue one another, since a
resampled feed cannot produce a gap.

Real data works the same way — the file's timestamps are ignored and the bars
are re-anchored onto live buckets:

```bash
qte-simulator replay --symbol XAUUSD --file data/parquet/XAUUSD_M15.parquet --limit 400
qte-simulator replay --symbol XAUUSD --file scenario.jsonl        # {"open":…,"high":…,…}
```

Where they land differs, though, and deliberately: a file is anchored **past**
and generated bars **next** (see [Where bars are
placed](#where-bars-are-placed-on-the-clock-and-why-it-matters)). Prices that
printed belong in buckets that have happened, so a warm-up from real history
ends on the last completed bucket and runs backwards from there — Redis then
holds what the runner would have read had it been up all along, rather than a
window stamped days ahead of the clock. Pass `--anchor next` to override.

#### Warming up from the cached vendor history

The whole of that is one argument-free command, because the arguments live in
`.env`:

```bash
make warmup-cache          # = uv run qte-simulator replay
```

| Setting | What it gives the replay |
| --- | --- |
| `QTE_SIMULATOR_PARQUET_FILE` | the file, when neither `--file` nor `--generate` is given |
| `QTE_SIMULATOR__CACHE_BARS` | how many of its trailing bars (default: `QTE_REDIS__CANDLE_HISTORY`) |
| `QTE_SIMULATOR__GENERATE_BARS` | bars a bare `--generate` synthesises (default: `QTE_ENGINE__WARMUP_CANDLES`) |
| `QTE_ENGINE__SYMBOLS` | the symbol, when `--symbol` is left out — the first entry |
| `QTE_ENGINE__SIGNAL_TIMEFRAME` | the timeframe, when `--timeframe` is left out |

A run that size is more bars than the server accepts in one command, so the CLI
sends it in batches: the first is placed on the clock, the rest continue the
series it left, and only the last one seals. That is invisible in the output —
one line, one bucket range — and it is why the warm-up window can be as large
as the retention.

`make warmup-cache` deliberately does **not** pass `--verify`. Every bucket a
past-anchored run fills is already over, so ingestion's wall-clock flush can
close a bar between two of its own ticks: about one bar per
`QTE_INGESTION__FLUSH_INTERVAL` the replay lasts. In a few thousand warm-up
bars that is noise; against a bar-by-bar check it is a guaranteed red. `make
warmup`, which is forward-anchored, verifies every bar.

### 4.4 Making a signal happen

A random walk crosses a moving average when it feels like it. To *make* the
example strategy fire, replay a run with a strong drift after the warm-up —
`make signal` does exactly this (warm-up, then the drift replay below):

```bash
$ qte-simulator replay --symbol XAUUSD --generate 60 --seed 3 \
    --drift 0.004 --volatility 0.0015 --verify --expect-signal

Verify  60/60 candles republished by ingestion

Signals 1 emitted on XAUUSD
  QTE_EXAMPLE_EMA_ATR LONG price=2528.181922 qty=0.01 sl=2513.63404 tp1=2550.00374
  [shadow] uxid=91F305AE2AB34077
```

`[shadow]` means the signal was built, audited and mirrored on
`QTE.signal.emitted` but not delivered to the broker —
`QTE_BROKER__SHADOW_MODE=true`. That is the correct state for a rehearsal;
turning it off sends invented signals to real workers.

`--expect-signal` exits non-zero when nothing fires, which is what makes this a
test rather than a demo. When it does not fire, the message says what to look
at: the strategy may still be warming, it may not be mapped to this symbol in
`config/strategies_mapping.toml`, or the bar may simply not have met its rule.
All three are answers; "nothing happened" is not.

Watch the audit trail if Postgres is on:

```sql
SELECT strategy, symbol, action, price, delivery_status, shadow
FROM signals ORDER BY created_at DESC LIMIT 5;
```

### 4.5 A live-ish feed

For a strategy that overrides `on_tick`, or just to watch the thing run —
`make sim-walk` starts it, `make sim-stop` ends it:

```bash
qte-simulator walk --symbol XAUUSD --rate 5 --spread 0.3    # 5 ticks/s, real time
qte-simulator stop
```

At `--speed 1` (the default) that is a live feed: bars close on ingestion's
wall-clock flush, an M1 bar a minute and an M15 bar a quarter hour.

Which is a long time to watch. `--speed` is how many seconds of market time
pass per second of real time:

```bash
$ qte-simulator walk --symbol XAUUSD --rate 20 --speed 120
Walking XAUUSD at 20.0/s from 3213.617905, market time from 2026-08-28T09:45:06Z
at 120x (unbounded ticks)
```

An M15 bar every seven or eight seconds, with 150 ticks in each:

```
Candle closed XAUUSD M15 open_time=2026-08-28T09:45:00+00:00 … ticks=150
Candle closed XAUUSD M15 open_time=2026-08-28T10:00:00+00:00 … ticks=150
```

The walk picks up where the replay left it — both the price and the clock — so
it continues the same series rather than jumping back to the wall clock and
being dropped as late. That is automatic; see
[Where bars are placed](#where-bars-are-placed-on-the-clock-and-why-it-matters)
for why it has to be.

### 4.6 Watching the far end

`--verify` checks one command. To just watch — `make sim-watch`, or:

```bash
$ qte-simulator watch --symbol XAUUSD --timeframe M15
Watching QTE.candle.closed.XAUUSD.M15 and QTE.signal.emitted — Ctrl-C to stop
  candle 2026-08-24T15:30:00+00:00 o=2395.76 h=2414.21 l=2394.29 c=2412.44 ticks=150
  SIGNAL QTE_EXAMPLE_EMA_ATR LONG @ 2525.64 [shadow]
```

It is a plain NATS subscriber — nothing in the engine behaves differently
because it is attached. `make shadow-status` confirms from the other side that
nothing is reaching a broker.

---

## 5. Backtesting alongside the stack

The backtest engine is deliberately **not** in the runner image — replaying
history is a host job, and it only needs the parquet files:

```bash
make download                                    # provider history → data/parquet/
make backtest STRATEGY=QTE_EXAMPLE_EMA_ATR SYMBOL=XAUUSD TF=M15
make chart REPORT=data/reports/<file>.json       # interactive HTML dashboard
```

`make download` needs a real vendor key (`QTE_DATA_PROVIDER_API_KEY`) and a
plan naming what to fetch (`make tiingo`); the simulator serves no history.
Without one, import an MT5 CSV export instead:

```bash
make csv-import CSV=data/csv/XAUUSD_M15.csv TZ=EET
```

Reports land in `data/reports/`, which is git-ignored and mounted into the
containers.

---

## 6. Everyday operations

```bash
make logs                   # tail every service
make restart                # recreate the app containers, keep volumes and infra
make down                   # stop the stack; volumes survive
make db-current             # which migration the database is on
make sim-reset              # clear Redis, reset the simulator cursor, restart ingestion
```

`make nuke` is `docker compose down -v`: it **deletes the volumes**, audit trail
and Redis state included. Only when you want a genuinely empty stack.

---

## Where bars are placed on the clock, and why it matters

This is the one piece of the simulator worth understanding before you trust its
output.

Ingestion closes a bar in two ways: when a tick lands in a **later bucket**, and
when the **wall clock** passes the bucket's end (`Resampler.flush`, every
`QTE_INGESTION__FLUSH_INTERVAL`). The second one exists so a quiet market still
produces candles on schedule — and it is exactly what a replay of historical
timestamps collides with. Every bucket a replay fills is already over, so the
flush timer can fire *between* two ticks of the same bar and publish half of it.
Once per flush interval, for as long as the replay runs.

So the simulator keeps **one forward series** per symbol and every command
continues it. `--anchor next` — the default for `bar` and `replay` alike —
means *the first bucket nothing has been sent into yet*, and the run marches
forward from there. No bucket's end has passed, so the flush timer never
touches them: each bar is closed by the arrival of the next, and the last by an
explicit sealing tick. A loose `tick` counts as having touched a bucket, which
is why the bar in step 4.2 landed at `15:00` and not on top of the `14:45` tick.

`--anchor past` is the other placement: the run ends on the last **completed**
bucket, so the wall-clock flush is what closes its final bar. It is the default
for a replay read from a file, because history whose timestamps sit in the
future is not what a strategy reads on a live feed — and it accepts the flush
race that comes with it, at about one torn bar per flush interval the run
lasts. `--anchor auto`, the replay default, is exactly this choice: `past` for a
file, `next` for generated bars.

| | `next` (default) | `past` |
| --- | --- | --- |
| Where | the first untouched bucket, marching forward | ends on the last completed bucket |
| What closes the last bar | an explicit sealing tick, immediately | the wall-clock flush, within `QTE_INGESTION__FLUSH_INTERVAL` |
| Can the flush split a bar | no — no bucket's end has passed | in the ~1 ms its four ticks take to arrive; the risk grows with every bar in the run |
| Good for | generated bars, at any length | real history, and testing the flush path itself |
| Cost | candle timestamps run ahead of the clock | roughly one torn bar per flush interval the run lasts |

The cost of `next` is real: after a few hundred bars the series is days ahead of
the wall clock, and candles carry those timestamps. That is fine in a dev
fixture and would be unacceptable anywhere else — which is the same reason the
`QTE_ENV=dev` guard exists.

Two consequences to keep in mind:

**Anything the simulator sends afterwards must stay on that series.** It does,
automatically: an unstamped `tick` is stamped with the later of the wall clock
and the series, and `walk` starts from the later of the two as well. Send a tick
with an explicit `--ts` behind the series and ingestion will drop it —

```
WARNING Dropping late tick symbol=XAUUSD tf=M15 tick_bucket=2026-08-24 14:15:00+00:00
                                                open_bucket=2026-08-28 08:45:00+00:00
```

— which is the resampler protecting a bar strategies have already acted on, not
a bug.

**The simulator's cursor resets when it restarts; ingestion's does not.** Restart
them together, or the first thing a fresh simulator sends will land behind the
bar ingestion is still holding open. `--verify` says so explicitly when it
happens, and `make sim-reset` clears both ends at once.

---

## When nothing arrives

Work down this list; each row tells you which hop lost it.

| Symptom | Where it broke |
| --- | --- |
| `market-simulator` exits at boot, or `refused: … is a development-only component` | `QTE_ENV` is not `dev`. Both the server and the provider call `require_dev_env()`; there is no override flag. |
| `→ 0 feed client(s)` in the acknowledgement, or `qte-simulator status` shows no feed client | Ingestion is not attached. `QTE_MARKET_DATA__PROVIDER=simulator`? Right `QTE_SIMULATOR__URL`? Inside compose it must be the service name — that is the override's job, so check nothing has pinned it elsewhere. |
| Ingestion logs no ticks | Same as above, from the sending end. |
| Ingestion logs `Dropping late tick` | The resampler holds a bar ahead of what you sent — see [Where bars are placed](#where-bars-are-placed-on-the-clock-and-why-it-matters). `make sim-reset` clears Redis, resets the cursor and restarts ingestion. |
| A bar is sent, no candle closes | Bars close on the clock, not on the next tick, and `--verify` waits for the real close. If it still times out, `make sim-reset`. |
| Ingestion logs `Candle closed` but `watch` / `make sim-watch` sees nothing | The two are on different NATS clusters — compare `QTE_NATS__URL`. |
| Candle arrives, no signal | The strategy: warm-up (`make warmup` first), mapping (`make strategies` lists what loaded), or the rule genuinely not met. |
| Candle arrives with the wrong OHLC | A real finding. `QTE_SIMULATOR__LOG_TICKS=true` shows what was sent. |
| `db-migrate` exits non-zero | Read `docker compose logs db-migrate`. Nothing downstream starts until it exits 0, so app containers stuck in `Created` are the symptom, not the fault. |
| Port already allocated | Something else owns `5432`/`4222` — typically `algo-trading-broker`. Shift the `QTE_*_PORT` values; the container side is unaffected. |
| A strategy fails on import | Its dependencies are not in the image. Run `make strategy-requirements`, then `make up` to rebuild. |

The wrong-OHLC row is the one worth having. Everything else is wiring.

---

## Command reference

```
qte-simulator serve   [--host] [--port]
qte-simulator tick    --symbol [--bid] [--ask] [--last] [--volume] [--ts]
qte-simulator bar     --symbol --open --high --low --close [--volume]
                      [--timeframe] [--anchor next|past|<iso>] [--spread]
                      [--no-seal] [--verify] [--expect-signal] [--timeout]
qte-simulator replay  [--symbol] [--file F | --generate [N]] [--limit N]
                      [--timeframe] [--anchor] [--rate] [--seed]
                      [--start-price] [--volatility] [--drift]
                      [--spread] [--no-seal] [--verify] [--expect-signal]
qte-simulator walk    --symbol [--rate] [--speed] [--ticks] [--price]
                      [--volatility] [--spread] [--seed]
qte-simulator stop    [--name walk:XAUUSD]
qte-simulator status
qte-simulator reset
qte-simulator watch   --symbol [--timeframe] [--seconds]
```

Global: `--url` (control endpoint, default `QTE_SIMULATOR__CONTROL_URL`) and
`--json` (print the raw acknowledgement).

`--symbol` defaults to the first of `QTE_ENGINE__SYMBOLS` and `--timeframe` to
`QTE_ENGINE__SIGNAL_TIMEFRAME`, so a rehearsal cannot land on a symbol
ingestion never subscribed to by leaving the flag out. A `replay` with no
source reads `QTE_SIMULATOR_PARQUET_FILE`.

### Make wrappers

| Target | Runs |
| --- | --- |
| `make sim` | `qte-simulator serve` |
| `make sim-status` | `qte-simulator status` |
| `make bar O= H= L= C= [V=]` | one bar, round-tripped with `--verify` |
| `make warmup` | synthetic warm-up replay, seeded, `--verify` (`QTE_SIMULATOR__GENERATE_BARS`) |
| `make warmup-cache` | replay the cached vendor parquet (`QTE_SIMULATOR_PARQUET_FILE`), no `--verify` |
| `make signal` | warm-up, then a drift replay with `--expect-signal` |
| `make sim-walk` | `qte-simulator walk --rate 5` |
| `make sim-stop` | `qte-simulator stop` — every background generator |
| `make sim-watch` | `qte-simulator watch` |
| `make sim-reset` | stop + reset the cursor + `FLUSHDB` + restart ingestion and the runner |
| `make shadow-status` | `qte-control shadow status` |

### Driving it without the CLI

Both paths are plain JSON over WebSocket, so anything can drive them:

```bash
$ websocat ws://127.0.0.1:8901/stream
{"op":"subscribe","symbols":["XAUUSD"]}
← {"type":"tick","symbol":"XAUUSD","ts":"2026-08-24T14:00:00+00:00","last":2400.0,…}

$ websocat ws://127.0.0.1:8901/control
{"op":"status"}
{"op":"tick","symbol":"XAUUSD","last":2401.5}
{"op":"bars","symbol":"XAUUSD","timeframe":"M15","anchor":"next",
 "bars":[{"open":2400,"high":2410,"low":2398,"close":2408,"volume":12}]}
```

The frames are defined in `qte_shared.providers.simulator.protocol` — one
module, used by both ends, so a change cannot be applied to one side only.

---

## What it deliberately does not do

**No history.** The simulator serves `Capability.LIVE` and nothing else. A
backtest over invented bars would produce an equity curve that means nothing,
and a convincing fake of a backtest is worse than no backtest. Use
`make download` or `make csv-import` for history.

**No candles onto NATS directly.** It could publish `QTE.candle.closed` itself
and skip ingestion entirely. Then the test would prove that the simulator can
publish a candle, which nobody doubted. Ticks are the only thing it sends, so
the resampler is always in the path.

**No market model.** `--generate` is a random walk with a body and two wicks.
It is enough to warm an indicator window and move a strategy off the fence. It
is not data, and no conclusion about a strategy's edge survives contact with it.

---

## Swapping in a real vendor feed

Two things: the key in `.env`, and a plan saying what to feed.

```bash
# .env — one key name whichever vendor is on
QTE_MARKET_DATA__PROVIDER=tiingo
QTE_DATA_PROVIDER_API_KEY=<your key>
```

```bash
make tiingo        # writes config/tiingo.toml from the tracked template
```

That file is the market-data plan: one table per symbol, stating the market it
trades on and the bars it is resampled to, plus a `[provider]` table for the
vendor's own knobs (`backfill_history`, `max_rows_per_request`). It is
git-ignored, like the mapping table and for the same reason.

```toml
[symbols.XAUUSD]
market = "fx"
timeframes = ["M15"]
```

`make start` refuses to bring the stack up while the provider is a vendor and
its plan is missing — without one the engine would fall back to the
`QTE_ENGINE__*` defaults and subscribe to symbols nobody chose. Those variables
still work and still win when set, which is what makes
`QTE_ENGINE__SYMBOLS='["EURUSD"]' make backtest` a one-run override.

Then stop the simulator explicitly — leaving it running alongside a real vendor
puts two feeds on one symbol, racing the same resampler:

```bash
docker compose stop market-simulator
make restart
```

**Signals still go nowhere.** Delivery to `algo-trading-broker` needs
`QTE_BROKER__*` pointed at it *and* shadow mode turned off, which is a separate,
deliberate step — see "Sending signals to the broker" and "Going live (phase 6)"
in [`README.md`](../README.md). Leave `QTE_BROKER__SHADOW_MODE=true` for
everything described on this page.

---

## Related reading

- [`docs/architecture.md`](architecture.md) — why the stack is shaped this way.
- [`docs/broker-contract.md`](broker-contract.md) — what a signal payload looks like.
- [`docs/backtest-report.md`](backtest-report.md) — the report schema.
