# Testing the pipeline with the market data simulator

A step-by-step rehearsal of the whole flow — **feed → ingestion → NATS →
strategy runner → signal** — with no market open, no vendor key, and no
waiting fifteen minutes to see whether a bar closed.

The simulator is a WebSocket server that speaks the same protocol a vendor
would. `data-ingestion` connects to it exactly as it connects to Tiingo, so
what you are exercising is the real pipeline; only the prices are invented.

```
qte-simulator serve                    ← you drive this from another terminal
        │  ws://…/stream   ticks
        ▼
  data-ingestion ──▶ Resampler ──▶ Redis (warm-up window)
        │                     └──▶ NATS  QTE.candle.closed.<symbol>.<tf>
        │                                        │
        │                             strategy-runner ──▶ __strategies__/*.py
        │                                        │
        │                                        ▼
        │                          NATS SIGNALS.<strategy>  (shadow: not sent)
        │                          NATS QTE.signal.emitted  (always)
        ▼
qte-simulator watch / --verify         ← and you check the far end from here
```

> **It only runs in dev.** Both the server and the provider call
> `require_dev_env()` and refuse to start unless `QTE_ENV=dev`. There is no
> override flag: the simulator fabricates prices, and an engine reading them
> would look entirely normal while trading them.

---

## 0. What you need

| | |
| --- | --- |
| Redis, Postgres, NATS | `make infra` (Postgres can be skipped — see below) |
| The schema | `make db-upgrade`, or set `QTE_POSTGRES__ENABLED=false` |
| A strategy | `cp examples/__strategies__/ema_atr_breakout.py __strategies__/` |
| A routing table | optional — without one the strategy keeps the symbols it declares |

Postgres is only the audit trail, and nothing on the tick path waits for it. If
you want the shortest possible loop, `QTE_POSTGRES__ENABLED=false` and skip
`db-upgrade` entirely.

---

## 1. Point ingestion at the simulator

In `.env`:

```bash
QTE_ENV=dev
QTE_MARKET_DATA__PROVIDER=simulator     # ← the whole switch
QTE_ENGINE__SYMBOLS=["XAUUSD"]
QTE_ENGINE__TIMEFRAMES=["M15"]
QTE_ENGINE__SIGNAL_TIMEFRAME=M15
QTE_BROKER__SHADOW_MODE=true            # build and audit signals, send nothing

# Where the feed is. 127.0.0.1 when ingestion runs on the host,
# ws://market-simulator:8901/stream inside compose.
QTE_SIMULATOR__URL=ws://127.0.0.1:8901/stream
```

Keep `QTE_ENGINE__TIMEFRAMES` short while testing. Every timeframe you list is
another resampler fed from the same ticks, and another stream of candles in the
log to read past.

---

## 2. Start the four processes

Four terminals, or `make sim-up` plus `make up` if you would rather use
compose. On the host:

```bash
make infra                       # terminal 0: redis + postgres + nats
make sim                         # terminal 1: the simulator
make ingestion                   # terminal 2: data-ingestion
make runner                      # terminal 3: strategy-runner
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
Everything below sends into a void if this line is missing, and the simulator
will happily report success doing it (the `→ 0 feed client(s)` in each
acknowledgement is the warning).

---

## 3. One tick

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

---

## 4. One bar, and proof it came back

This is the check the simulator exists for.

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
bucket. And the bucket is `15:00` rather than `14:45` because the tick in step 3
already opened `14:45` — a bar landing in an occupied bucket would inherit that
tick's price as its open. Every command continues one forward series; see
[Where bars are placed on the clock](#where-bars-are-placed-on-the-clock-and-why-it-matters).

`--verify` subscribes to `QTE.candle.closed.XAUUSD.M15` **before** sending, then
compares the candle that arrives against the bar that went in — open, high,
low, close, volume and tick count. It exits non-zero on a mismatch, so it works
in a script.

If a candle never arrives, the ticks did but the bar did not close. See
[When a candle does not arrive](#when-a-candle-does-not-arrive).

---

## 5. A run of bars — warming the strategy up

A strategy does nothing until it has `warmup` bars. The bundled example wants
220 of them, so one bar will never produce a signal however good it looks.
Replay a few hundred:

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
starts at 2408.75 rather than at a reference price because that is where step 4
left the series — consecutive commands continue one another, since a resampled
feed cannot produce a gap.

Real data works the same way — the file's timestamps are ignored and the bars
are re-anchored onto live buckets:

```bash
qte-simulator replay --symbol XAUUSD --file data/parquet/XAUUSD_M15.parquet --limit 400
qte-simulator replay --symbol XAUUSD --file scenario.jsonl        # {"open":…,"high":…,…}
```

---

## 6. Making a signal happen

A random walk crosses a moving average when it feels like it. To *make* the
example strategy fire, replay a run with a strong drift after the warm-up:

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
at: the strategy may still be warming, it may not be routed to this symbol in
`config/strategies_mapping.toml`, or the bar may simply not have met its rule.
All three are answers; "nothing happened" is not.

Watch the audit trail if Postgres is on:

```sql
SELECT strategy, symbol, action, price, delivery_status, shadow
FROM signals ORDER BY created_at DESC LIMIT 5;
```

---

## 7. A live-ish feed

For a strategy that overrides `on_tick`, or just to watch the thing run:

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
being dropped as late. That is automatic; see the next section for why it has
to be.

---

## 8. Watching the far end

`--verify` checks one command. To just watch:

```bash
$ qte-simulator watch --symbol XAUUSD --timeframe M15
Watching QTE.candle.closed.XAUUSD.M15 and QTE.signal.emitted — Ctrl-C to stop
  candle 2026-08-24T15:30:00+00:00 o=2395.76 h=2414.21 l=2394.29 c=2412.44 ticks=150
  SIGNAL QTE_EXAMPLE_EMA_ATR LONG @ 2525.64 [shadow]
```

It is a plain NATS subscriber — nothing in the engine behaves differently
because it is attached.

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
is why the bar in step 4 landed at `15:00` and not on top of the `14:45` tick.

`--anchor past` is the other placement, and it is worth knowing about because
it exercises a different path: the run ends on the last **completed** bucket,
so the wall-clock flush is what closes its final bar.

| | `next` (default) | `past` |
| --- | --- | --- |
| Where | the first untouched bucket, marching forward | ends on the last completed bucket |
| What closes the last bar | an explicit sealing tick, immediately | the wall-clock flush, within `QTE_INGESTION__FLUSH_INTERVAL` |
| Can the flush split a bar | no — no bucket's end has passed | in the ~1 ms its four ticks take to arrive; the risk grows with every bar in the run |
| Good for | anything, at any length | testing the flush path itself, one bar at a time |
| Cost | candle timestamps run ahead of the clock | not usable for a long replay |

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
happens.

---

## When a candle does not arrive

Work down this list; each step tells you which hop lost it.

| Symptom | Where it broke |
| --- | --- |
| `→ 0 feed client(s)` in the acknowledgement | Ingestion is not attached. `QTE_MARKET_DATA__PROVIDER=simulator`? Right `QTE_SIMULATOR__URL`? |
| `qte-simulator status` shows no feed client | Same, from the other end |
| Ingestion logs `Dropping late tick` | The resampler holds a bar ahead of what you sent — see above |
| Ingestion logs `Candle closed` but `watch` sees nothing | The two are on different NATS clusters — compare `QTE_NATS__URL` |
| Candle arrives, no signal | The strategy: warm-up, routing, or the rule genuinely not met |
| `refused: … is a development-only component` | `QTE_ENV` is not `dev` |
| Candle arrives with the wrong OHLC | A real finding. `QTE_SIMULATOR__LOG_TICKS=true` shows what was sent |

The last row is the one worth having. Everything else is wiring.

---

## Command reference

```
qte-simulator serve   [--host] [--port]
qte-simulator tick    --symbol [--bid] [--ask] [--last] [--volume] [--ts]
qte-simulator bar     --symbol --open --high --low --close [--volume]
                      [--timeframe] [--anchor next|past|<iso>] [--spread]
                      [--no-seal] [--verify] [--expect-signal] [--timeout]
qte-simulator replay  --symbol (--file F | --generate N) [--limit N]
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

`make sim`, `make sim-up`, `make sim-status`, `make sim-replay`, `make sim-bar`,
`make sim-walk`, `make sim-stop` and `make sim-watch` wrap the common ones.

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
