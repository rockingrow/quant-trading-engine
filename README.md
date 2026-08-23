# Quant Trading Engine (QTE)

An event-driven framework for developing, backtesting and running quantitative
trading strategies. The engine is public; **your alpha is not** — strategies
live in `user_strategies/`, which is git-ignored here and cloned from your own
private repository at deploy time.

QTE ingests market data from Tiingo, keeps hot state in Redis, audits every
signal into PostgreSQL (pgvector-ready), and publishes trade signals over NATS
to [`algo-trading-broker`](https://github.com/rockingrow/algo-trading-broker),
which fans them out to MT5 / Binance workers.

```
Tiingo WS ─▶ data-ingestion ─▶ Redis (hot state)
                    │
                    └─▶ NATS  QTE.candle.closed.<symbol>.<tf>
                                       │
                            strategy-runner ──▶ user_strategies/*.py
                                       │
                    ┌──────────────────┴──────────────────┐
                    ▼                                     ▼
      NATS SIGNALS.<strategy>                   PostgreSQL (audit, JSONB)
      (algo-trading-broker JetStream)                     ▲
                    │                                     │
                    ▼                              api-gateway (FastAPI)
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
whatever `StrategyBase` subclasses it finds in `user_strategies/` by file path
— a mounted volume, not an installed package — so the public engine and your
private algorithms are versioned and released independently.

---

## Quick start

```bash
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine

cp .env.example .env          # fill in QTE_TIINGO__API_KEY at minimum
make install-dev              # uv sync

# Try the pipeline with the example strategy. user_strategies/ does not exist
# in a fresh clone — it is left out of git so `git clone <your repo>
# user_strategies` has an empty destination to land in.
mkdir -p user_strategies
cp examples/user_strategies/ema_atr_breakout.py user_strategies/

make infra                    # redis + postgres + nats
make download                 # Tiingo history → data/parquet/*.parquet
make backtest STRATEGY=QTE_EXAMPLE_EMA_ATR SYMBOL=XAUUSD
```

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), and Docker.

---

## Layout

| Path | What it is |
| --- | --- |
| `shared/` | `qte_shared` — models, indicators, `StrategyBase`, NATS/Redis/Postgres adapters, the plugin loader, the signal factory. Every other package depends on this and on nothing else in the repo. |
| `data-ingestion/` | Tiingo WebSocket → resampler → Redis + NATS. |
| `backtest-engine/` | History downloader, parquet store, replay loop, fill simulator, metrics, `qte-backtest` CLI. |
| `strategy-engine/` | The live runner: plugin loading, the NATS event loop, delivery to the broker, audit. |
| `api-gateway/` | FastAPI control plane. |
| `user_strategies/` | **Git-ignored and untracked.** Your private strategy repo, cloned in whole. Absent from a fresh checkout by design. |
| `deploy/` | Postgres init SQL (incl. pgvector) and the standalone NATS config. |
| `examples/user_strategies/` | A worked example of the plugin contract. Not an edge. |

---

## Writing a strategy

```python
from qte_shared.indicators import atr, crossover, ema
from qte_shared.models import SignalAction
from qte_shared.strategy_base import SignalIntent, StrategyBase


class MyEdge(StrategyBase):
    name = "MT5_GOLD_M15_V1"   # ← the NATS subject workers subscribe to
    symbols = ("XAUUSD",)
    timeframe = "M15"
    warmup = 220

    def on_candle_closed(self, df, context):
        fast, slow = ema(df["close"], 21), ema(df["close"], 55)
        if not bool(crossover(fast, slow).iloc[-1]):
            return None
        if context.open_uxid is not None:      # already in a trade
            return None

        close = float(df["close"].iloc[-1])
        risk = float(atr(df, 14).iloc[-1]) * 1.5
        return SignalIntent(
            action=SignalAction.LONG,
            price=close, quantity=0.01,
            sl=close - risk, tp1=close + risk * 1.5, tp2=close + risk * 3,
            tp1_percent=50.0, move_sl_to_be=True,
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

Discovery walks `user_strategies/` recursively, so a subfolder per instrument is
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

## Control plane

`api-gateway` (default `:8000`) is off the trading path — it reads the audit
trail and flips switches. Set `QTE_API__API_KEY` before this port is reachable
from anywhere but your laptop; mutating endpoints then require `X-API-KEY`.

| Endpoint | |
| --- | --- |
| `GET /health` | Liveness plus per-dependency state. Reports `degraded` instead of failing, so it can tell you *which* piece is down. |
| `GET /strategies` | Plugins currently discoverable in `user_strategies/`. |
| `GET /signals` | Newest-first audit trail; filter by `strategy`, `symbol`, `since`. |
| `GET /signals/cycle/{uxid}` | One whole trade cycle, oldest first — the reconciliation view. |
| `POST /backtest/run` | Replay a strategy and get the report back. |
| `GET /backtest/runs`, `GET /backtest/history` | Past runs; parquet on disk. |
| `POST /admin/shadow-mode` | Pause or resume delivery across every running runner. |

Interactive docs at `/docs`.

---

## Deployment

```bash
# 1. Public engine
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine && cp .env.example .env   # then edit it

# 2. Private alpha, into the ignored directory. It is untracked and absent
#    from the checkout, so this clone lands in an empty destination.
git clone git@github.com:you/my-private-strategies.git user_strategies

# 3. Up
make up && make logs
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
3. **Reconcile** — pull `GET /signals` and check the entries and exits against
   the chart. `GET /signals/cycle/{uxid}` gives you one trade end to end.
4. **Go live** — `make shadow-off` (or `POST /admin/shadow-mode {"enabled": false}`).
   It takes effect on every running runner immediately, and the flag is stored
   in Redis so a restart comes up in the mode you last chose.

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
