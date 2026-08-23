# Quant Trading Engine (QTE)

An event-driven framework for developing, backtesting and running quantitative
trading strategies. The engine is public; **your alpha is not** — strategies
live in `__strategies__/`, which is git-ignored here and cloned from your own
private repository at deploy time.

QTE ingests market data from Tiingo, keeps hot state in Redis, audits every
signal into PostgreSQL, and publishes trade signals over NATS
to [`algo-trading-broker`](https://github.com/rockingrow/algo-trading-broker),
which fans them out to MT5 / Binance workers.

```
Tiingo WS ─▶ data-ingestion ─▶ Redis (hot state)
                    │
                    └─▶ NATS  QTE.candle.closed.<symbol>.<tf>
                                       │
                            strategy-runner ──▶ __strategies__/*.py
                                       │
                    ┌──────────────────┴──────────────────┐
                    ▼                                     ▼
      NATS SIGNALS.<strategy>                   PostgreSQL (audit, JSONB)
      (algo-trading-broker JetStream)
                    │
                    ▼
             MT5 / Binance workers
```

---

## Core concepts

**Event-driven.** Nothing blocks anything. Ingestion pushes; the runner reacts
to a candle close; the broker takes signals off a durable stream. A slow
Postgres delays a log line, never a trade.

**Write once, run anywhere.** A strategy implements `on_candle_closed(df, ctx)`
and returns `SignalIntent` objects. The backtest replay and the live runner
drive that same interface with the same indicator code, so the file that
produced a backtest curve is the file that trades.

**Plugin / blackbox strategies.** The engine never contains an edge. It loads
whatever `StrategyBase` subclasses it finds in `__strategies__/` by file path
— a mounted volume, not an installed package — so the public engine and your
private algorithms are versioned and released independently.

---

## Quick start

```bash
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine

cp .env.example .env          # fill in QTE_TIINGO__API_KEY at minimum
make install-dev              # uv sync

# Try the pipeline with the example strategy. __strategies__/ does not exist
# in a fresh clone — it is left out of git so `git clone <your repo>
# __strategies__` has an empty destination to land in.
mkdir -p __strategies__
cp examples/__strategies__/ema_atr_breakout.py __strategies__/

make infra                    # redis + postgres + nats
make db-upgrade               # create the schema (Alembic owns it, not an init script)
make download                 # Tiingo history → data/parquet/*.parquet
make backtest STRATEGY=QTE_EXAMPLE_EMA_ATR SYMBOL=XAUUSD
```

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), and Docker.

---

## Layout

| Path | What it is |
| --- | --- |
| `engines/shared/` | `qte_shared` — models, indicators, `StrategyBase`, NATS/Redis/Postgres adapters, the plugin loader, the signal factory. Every other engine depends on this and on nothing else in the repo. |
| `engines/data-ingestion/` | Tiingo WebSocket → resampler → Redis + NATS. |
| `engines/backtest-engine/` | History downloader, parquet store, replay loop, fill simulator, metrics, reports, `qte-backtest` CLI. |
| `engines/strategy-engine/` | The live runner: plugin loading, the NATS event loop, delivery to the broker, audit, and the `qte-control` operator CLI. |
| `__strategies__/` | **Git-ignored and untracked.** Your private strategy repo, cloned in whole. Absent from a fresh checkout by design. |
| `migrations/` | Alembic. One chain for the whole system; `env.py` imports every engine's models. |
| `deploy/` | The standalone NATS config. |
| `data/reports/` | Backtest reports, JSON + Markdown. Git-ignored. |
| `examples/__strategies__/` | A worked example of the plugin contract. Not an edge. |

---

## Writing a strategy

```python
from qte_shared.indicators import atr, crossover, ema
from qte_shared.models import SignalAction
from qte_shared.strategy_base import SignalIntent, StrategyBase


class MyEdge(StrategyBase):
    name = "MT5_GOLD_M15_V1"  # ← the NATS subject workers subscribe to
    symbols = ("XAUUSD",)
    timeframe = "M15"
    warmup = 220

    def on_candle_closed(self, df, context):
        fast, slow = ema(df["close"], 21), ema(df["close"], 55)
        if not bool(crossover(fast, slow).iloc[-1]):
            return None
        if context.open_uxid is not None:  # already in a trade
            return None

        close = float(df["close"].iloc[-1])
        risk = float(atr(df, 14).iloc[-1]) * 1.5
        return SignalIntent(
            action=SignalAction.LONG,
            price=close,
            quantity=0.01,
            sl=close - risk,
            tp1=close + risk * 1.5,
            tp2=close + risk * 3,
            tp1_percent=50.0,
            move_sl_to_be=True,
        )
```

Rules the framework enforces so you do not have to:

- `df` is an OHLCV frame indexed by candle **open time** in UTC, oldest first,
  and its last row is always a **closed** bar. It never contains a future bar.
- `name` must match the strategy the broker's workers are configured for — it
  *is* the NATS subject they subscribe to. Two strategies sharing a name is a
  loud error at load time.
- You never publish anything. Return intents; the runner attaches the bracket,
  mints/reuses the trade-cycle id, sends, and audits.
- `on_tick(price, ctx)` is optional. Override it only when an exit has to react
  faster than a bar close — the runner subscribes to ticks only if something does.

Discovery walks `__strategies__/` recursively, so a subfolder per instrument is
fine. Because the directory is a whole cloned repo rather than a tidy folder, it
skips hidden directories (`.git`, `.venv`, …) and the usual repo furniture —
`tests/`, `docs/`, `build/`, `node_modules/` and friends
(`qte_shared.plugin_loader.EXCLUDED_DIRECTORIES`). Files starting with `_` are
skipped too, so shared helpers live in `_helpers.py`. A file that fails to
import is logged and skipped: one broken strategy does not stop the others.

---

## Backtesting

```bash
uv run qte-backtest download --symbol XAUUSD --timeframe M15 --start 2023-01-01
uv run qte-backtest list
uv run qte-backtest run --strategy MT5_GOLD_M15_V1 --symbol XAUUSD \
    --spread 0.30 --commission 0.02 --quantity 0.01 --persist
```

The simulator is deliberately pessimistic — it is meant to disprove a strategy,
not flatter one:

- entries and exits cross the spread and pay slippage;
- when a bar's range covers both the stop and the target, the **stop** is taken
  (without tick data there is no ordering, and assuming the good one is how a
  losing strategy backtests profitably);
- a gap through a level fills at the bar's open, not at the level;
- a second entry while a position is open is **rejected**, mirroring the
  worker, which answers `REJECTED` rather than stacking positions;
- a position still open on the last bar is marked out, so unrealised P&L cannot
  quietly flatter the report.

`--persist` writes the run and every trade into Postgres (`backtest_runs`,
`backtest_trades`).

### The report

`--report` writes a JSON artefact for an agent to analyse plus a Markdown
companion for a human — same object, two renderings:

```bash
uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --report
# → data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.{json,md}
```

Beyond the headline metrics it carries what makes a result diagnosable: every
statistic in **R-multiples** as well as currency, **MAE/MFE per trade** (how far
price went against and for the position while it was open), each partial exit
leg, the exact broker payloads the run would have published, and a
`reading_guide` block spelling out the conventions an agent would otherwise
guess at.

The part worth having is `diagnostics` — a rule set that reads the finished run
and says what is wrong with it. Each finding states the threshold it tripped,
carries the numbers that tripped it, and proposes one concrete change:

```
Diagnostics       2 critical, 1 info
  [CRITICAL] EXITS_NEVER_TRIGGER: 1/1 exits were forced by the end of the data
             → Print entry, sl, tp1 and tp2 for the first trade and check the
               distances against the instrument's typical bar range. A stop
               should be a small multiple of ATR, not a multiple of price.
```

`report.is_trustworthy` is false whenever anything critical fired, so an agent
knows to stop reading the metrics as meaningful. The rule table and the JSON
schema are in [`docs/backtest-report.md`](docs/backtest-report.md).

---

## Sending signals to the broker

QTE emits exactly the payload `algo-trading-broker` validates — its
`WebhookPayload`: `strategy`, `symbol`, `timeframe`, `timestamp`,
`signal_uxid`, a `position` block, `indicators`, `inputs`, `token`. Two
transports carry it, selected with `QTE_BROKER__TRANSPORT`:

| | `nats` (default) | `http` |
| --- | --- | --- |
| Destination | JetStream `SIGNALS.<strategy>` | `POST /secret/webhook` |
| Why | The broker's own webhook endpoint writes to that same stream and its `SignalWorker` consumes it — we inherit its persistence, retry and de-duplication with no HTTP hop in the trade path. | Slower, but it is the path that verifies the `token` field. |
| Auth | Access to the NATS cluster **is** the authentication — the token is not checked on this path. | `QTE_BROKER__TOKEN`, matched against the broker's. |
| Use when | QTE and the broker share a trusted/private NATS cluster. | Anything crosses a boundary you do not control. |

Each publish carries a fresh `Nats-Msg-Id`, so a retried publish inside the
stream's duplicate window is stored once and a worker opens one position.

`signal_uxid` is the **trade cycle**: an entry mints it and every TP/SL/FLAT
that follows reuses it, which is how the broker groups a whole trade into one
broadcast. The runner keeps it in Redis, so a restart mid-trade still closes the
position it opened.

> The engine also mirrors every emitted signal on `QTE.signal.emitted` and rows
> it into Postgres, whether it was delivered, shadowed or failed.

---

## Operating a running engine

There is no web service. The one control that genuinely has to reach a *running*
process is shadow mode — the live/paper switch, which must not require
restarting a runner mid-position — and that travels on NATS like every other
engine event:

```bash
uv run qte-control shadow status   # is it paper or live right now?
uv run qte-control shadow on       # pause delivery to the broker
uv run qte-control shadow off      # GO LIVE — prompts unless you pass --yes
uv run qte-control ping            # which runners are up, and in what mode
```

The flag is written to Redis first and broadcast second, so a runner that starts
*after* the broadcast still comes up in the mode you last chose. If NATS is
unreachable the command says so explicitly rather than reporting success —
"stored, but the process running right now did not hear it" is a different
outcome from "applied".

Everything else the engine knows is a CLI command or a SQL query:

| Want | Do |
| --- | --- |
| What strategies are loaded | `ls __strategies__/`, or run a backtest — the loader logs each one it finds |
| Signal audit trail | `SELECT * FROM signals ORDER BY created_at DESC LIMIT 20` |
| One trade cycle end to end | `SELECT * FROM signals WHERE signal_uxid = '…' ORDER BY created_at` |
| Backtest a strategy | `uv run qte-backtest run --strategy … --symbol … --report` |
| Read a report | `data/reports/*.json` — the file an agent analyses |

## Database

Alembic owns the schema. There is no init script — one would only ever run on an
empty volume, which is exactly the case a migration tool exists to outgrow.

```bash
make db-upgrade                     # apply everything pending
make db-current                     # where is this database?
make db-revision M="add a column"   # autogenerate from model changes
make db-check                       # fail if the models have drifted
make db-downgrade                   # back out one revision
```

The DSN comes from `QTE_POSTGRES__DSN` through `migrations/env.py`; `alembic.ini`
deliberately does not set `sqlalchemy.url`, because two places to configure it is
one place for it to drift from what the engines actually connect to.

**Each engine owns the tables it writes**, with its models and repositories in
its own `db/` package:

| Package | Tables | Repository |
| --- | --- | --- |
| `qte_strategy_engine.db` | `signals` | `SignalRepository` |
| `qte_backtest.db` | `backtest_runs`, `backtest_trades` | `BacktestRepository` |
| `qte_shared.db` | `engine_events` | `EventRepository` |

`engine_events` sits in shared because every engine writes it and none owns it.
Ingestion has no `db/` package at all — it writes only that shared table, and
inventing a table to justify a folder would be the wrong way round.

They all share one `DeclarativeBase` (`qte_shared.db.base`). That is not
incidental: Alembic diffs the database against `Base.metadata`, so a model on a
different base would be invisible to autogenerate — its table would never be
created, and a later revision would see it in the database, fail to find it in
the metadata, and propose dropping it. For the same reason, adding an engine
that owns tables means adding its `models` import to `migrations/env.py`.
`tests/test_db_layout.py` enforces both.

The schema needs **no PostgreSQL extensions** — `docker-compose.yml` pins
`postgres:16-alpine`. If you later want vector search over the signal audit
trail, that is a new migration (`make db-revision M="add signal embeddings"`)
plus an image with pgvector; it is deliberately not carried as an unapplied
revision in the meantime, because `alembic upgrade head` is the reflexive
command and a revision that must not be applied is a trap.

## Deployment

```bash
# 1. Public engine
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine && cp .env.example .env   # then edit it

# 2. Private alpha, into the ignored directory. It is untracked and absent
#    from the checkout, so this clone lands in an empty destination.
git clone git@github.com:you/my-private-strategies.git __strategies__

# 3. Up, then create the schema
make up
make db-upgrade
make logs
```

`docker-compose.yml` ships a `nats` service for standalone development. In
production you normally point `QTE_BROKER__NATS_URL` at the **broker's** NATS,
because that is where the `SIGNALS` stream its workers consume actually lives —
publishing to a second cluster means nobody ever receives the signals.

### Going live (phase 6)

1. **Backtest** until the numbers hold up.
2. **Shadow mode** — `QTE_BROKER__SHADOW_MODE=true` (the default). Ingestion and
   strategies run, signals are built, logged and audited, and nothing reaches
   the broker.
3. **Reconcile** — read the `signals` table and check the entries and exits
   against the chart. Filtering by `signal_uxid` gives you one trade end to end,
   which is the same grouping the broker renders into a single broadcast.
4. **Go live** — `make shadow-off`. It takes effect on every running runner
   immediately, and the flag is stored in Redis so a restart comes up in the
   mode you last chose.

`make shadow-on` puts it back. That is the kill switch; keep it to hand.

---

## Development

```bash
make check     # ruff + pytest
make test
make lint
make format
```

The suite runs with no infrastructure — no Redis, Postgres or NATS needed.
`tests/test_broker_contract.py` pins the payload shape against the broker's
schema; if it fails, the two have drifted and signals will be rejected at
ingress. Fix the model, not the test.

### A note on `pandas_ta`

`qte_shared.indicators` is first-party rather than a `pandas_ta` wrapper. The
published `pandas-ta` build pins old NumPy/pandas internals and breaks on the
versions QTE runs, and it has no WaveTrend — the one indicator the broker's
payload schema actually names. Everything here is implemented directly, with
TradingView-compatible SMA seeding for `ema`/`rma` so a strategy ported off a
Pine chart crosses at the same bars. If you want the rest of the library:
`uv add pandas-ta`, then reach it through `indicators.pandas_ta_frame(df)`.

---

## License

MIT.
