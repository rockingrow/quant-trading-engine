# Running the stack locally with Docker

Step by step, from a fresh clone to a closed candle and an emitted signal — on
one machine, with no vendor key, no market open and no order ever leaving the
box.

The target is the **dev rehearsal loop**: the real pipeline (ingestion →
Redis/NATS → strategy runner → signal), fed by the built-in WebSocket
simulator, with delivery to the broker held in shadow mode. Swapping the feed
for a real vendor is one variable, and it is the last section here.

```
market-simulator ──ws──▶ data-ingestion ──▶ Redis (warm-up window)
   (dev only)                   │        └──▶ NATS  QTE.candle.closed.<sym>.<tf>
                                │                        │
                                │              strategy-runner ──▶ __strategies__/*.py
                                │                        │
                                ▼                        ▼
                          postgres-audit        NATS SIGNALS.<strategy>
                          (audit trail)         (shadow mode: built, not sent)
```

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

---

## 1. Write the `.env`

`.env` is git-ignored and read by every container through `env_file:`. Copy the
block below into `.env` at the repository root as-is — it is a complete,
working local configuration: dev environment, simulated feed, shadow mode on.

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

# ── Market data: the dev simulator, not a vendor ──────────────────────
QTE_MARKET_DATA__PROVIDER=simulator
QTE_TIINGO__API_KEY=
QTE_ENGINE__SYMBOLS=["XAUUSD"]
QTE_ENGINE__TIMEFRAMES=["M15"]
QTE_ENGINE__SIGNAL_TIMEFRAME=M15
QTE_INGESTION__MARKET_OVERRIDES={}
# The simulator serves no history, so start-up backfill is a no-op here.
# Warm the window by hand instead: `make warmup` or `make warmup-cache`.
QTE_INGESTION__BACKFILL_HISTORY=false
QTE_TIINGO__MAX_ROWS_PER_REQUEST=5000

# ── Simulator (refuses to run unless QTE_ENV=dev) ─────────────────────
# Host-side URL; compose swaps it for ws://market-simulator:8901/stream
# inside the network.
QTE_SIMULATOR__URL=ws://127.0.0.1:8901/stream
QTE_SIMULATOR__HOST=0.0.0.0
QTE_SIMULATOR__PORT=8901
QTE_SIMULATOR__CONTROL_URL=ws://127.0.0.1:8901/control
QTE_SIMULATOR__LOG_TICKS=false

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

# ── Audit trail ───────────────────────────────────────────────────────
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

# ── The trading account ───────────────────────────────────────────────
QTE_ACCOUNT__CAPITAL=1000.0
QTE_ACCOUNT__RISK_PERCENT=1.0
QTE_ACCOUNT__COMMISSION_PER_UNIT=0.0
QTE_ACCOUNT__CONTRACT_SIZE=1.0
QTE_ACCOUNT__MAX_QUANTITY=0.0
QTE_ACCOUNT__QUANTITY_PRECISION=4
```

Two knobs worth knowing before you start:

| Setting | Why you would touch it |
| --- | --- |
| `QTE_POSTGRES__ENABLED=false` | Postgres is only the audit trail and nothing on the tick path waits for it. `false` gives the shortest possible loop. |
| `QTE_ENGINE__TIMEFRAMES` | Every timeframe listed is another resampler on the same ticks, and another stream of candles in the log. Keep it to one while testing. |

---

## 2. Put a strategy in `__strategies__/`

The runner loads strategies **by path** from the git-ignored `__strategies__/`,
which compose mounts read-only into the containers. An empty directory means
the runner starts and does nothing.

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
under `[symbols.XAUUSD]`, or delete the file entirely. With no routing table
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

---

## 4. Drive the feed and watch a signal appear

The simulator sends nothing until you tell it to. Everything below runs on the
**host**, against the published port.

```bash
make sim-status                       # what it is doing, and who is attached
```

One bar, round-tripped through the whole pipeline:

```bash
make bar O=2400 H=2412.5 L=2396.25 C=2408.75
```

`--verify`, which the Make targets pass, waits on NATS for ingestion to
republish the closed candle — so the command fails rather than lying if the bar
was dropped.

Warm the indicator window, then push a move that should fire an entry:

```bash
make warmup                           # 300 synthetic bars, seeded — deterministic
make signal                           # warmup + a drift replay; fails if no signal fires
```

Prefer prices that actually printed? If you have vendor history in
`data/parquet/tiingo/`, replay that instead:

```bash
make warmup-cache CACHE_BARS=6000     # fills the whole Redis retention window
```

Watching, from a second terminal:

```bash
make sim-watch                        # closed candles + emitted signals on NATS
make shadow-status                    # confirm nothing is reaching a broker
```

A continuous feed, rather than one replay at a time:

```bash
make sim-walk                         # random walk until you stop it
make sim-stop                         # stop every background generator
```

---

## 5. Backtesting alongside the stack

The backtest engine is deliberately **not** in the runner image — replaying
history is a host job, and it only needs the parquet files:

```bash
make download                                    # provider history → data/parquet/
make backtest STRATEGY=QTE_EXAMPLE_EMA_ATR SYMBOL=XAUUSD TF=M15
make chart REPORT=data/reports/<file>.json       # interactive HTML dashboard
```

`make download` needs a real vendor key (`QTE_TIINGO__API_KEY`); the simulator
serves no history. Without one, import an MT5 CSV export instead:

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
make infra                  # only redis + postgres + nats, for host-side runs
make db-current             # which migration the database is on
```

Running a service on the host instead of in its container — useful when you
want a debugger on it — is `make ingestion` or `make runner`, with `make infra`
providing the backing services. The `.env` above already points at `127.0.0.1`,
so both work with no edit.

`make nuke` is `docker compose down -v`: it **deletes the volumes**, audit trail
and Redis state included. Only when you want a genuinely empty stack.

---

## 7. When nothing arrives

| Symptom | Cause and fix |
| --- | --- |
| `market-simulator` exits at boot | `QTE_ENV` is not `dev`. Both the server and the provider call `require_dev_env()`, and there is no override flag. |
| Ingestion logs no ticks | `QTE_MARKET_DATA__PROVIDER` is not `simulator`, or `QTE_SIMULATOR__URL` points somewhere else. Inside compose it has to be the service name — that is the override's job, so check nothing has pinned it elsewhere. |
| A bar is sent, no candle closes | Bars close on the clock, not on the next tick, and `--verify` waits for the real close. If it still times out: `make sim-reset` clears Redis, resets the cursor and restarts ingestion. |
| Candles close, no signal | The strategy has too little history — warm it with `make warmup` first — or routing is not pairing it with the symbol. `make strategies` lists what actually loaded. |
| `db-migrate` exits non-zero | Read `docker compose logs db-migrate`. Nothing downstream starts until it exits 0, so app containers stuck in `Created` are the symptom, not the fault. |
| Port already allocated | Something else owns `5432`/`4222` — typically `algo-trading-broker`. Shift the `QTE_*_PORT` values; the container side is unaffected. |
| A strategy fails on import | Its dependencies are not in the image. Run `make strategy-requirements`, then `make up` to rebuild. |

---

## 8. Swapping in a real vendor feed

Two variables, once you have a key:

```bash
QTE_MARKET_DATA__PROVIDER=tiingo
QTE_TIINGO__API_KEY=<your key>
QTE_INGESTION__BACKFILL_HISTORY=true    # warm Redis from vendor history at start-up
```

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

- [`docs/simulator.md`](simulator.md) — the simulator's own walkthrough, protocol and CLI.
- [`docs/architecture.md`](architecture.md) — why the stack is shaped this way.
- [`docs/broker-contract.md`](broker-contract.md) — what a signal payload looks like.
- [`docs/backtest-report.md`](backtest-report.md) — the report schema.
