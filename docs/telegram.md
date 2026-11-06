# Telegram: notifications and the operator bot

Two independent halves: one-way notifications the runner and ingestion send
(the position lifecycle, errors, start and stop), and the operator's bot that
looks at a running engine and steers it.

## Telegram notifications for a position's lifecycle

One Telegram message per trade cycle, **edited in place** as the position
progresses: the entry posts it, and every `TP1`/`TP2`/`R_SL`/`SL`/`FLAT` that
follows appends to its Actions block until the header flips to `CLOSED`. It is
the same message shape `algo-trading-broker` broadcasts, so a trade reads the
same way whichever service posted it, and it is keyed by the same triple —
`strategy` + `symbol` + `signal_uxid`.

```
[⏳RUNNING]
📈 LONG XAUUSD (M15)
-----------
Price: 2340
Quantity: 0.05 | Risk: 1%
SL: 2330 | TP1: 2350 | TP2: 2360
2026-10-08 12:00:00 UTC
-----------
Actions:
-----------
🎯 TP1
Price: 2350 | Quantity: 0.025
2026-10-08 12:30:00 UTC
-----------
```

Create a send-only bot with @BotFather (the broker's own bot works too), add it
to the chats, and fill in `.env`:

| Variable | What it does |
| --- | --- |
| `QTE_TELEGRAM__BOT_TOKEN` | The bot. Empty switches the whole feature off. |
| `QTE_TELEGRAM__BROADCAST_CHAT_IDS` | The position: side, symbol, price, levels, action timeline. |
| `QTE_TELEGRAM__PRIVATE_CHAT_IDS` | The above plus the strategy name, the cycle id and how delivery went. |
| `QTE_TELEGRAM__TIMEZONE` | Display zone for the timestamps. Moves nothing about trading. |

**An empty chat list sends nothing to that audience** — which is how you run
broadcast-only, private-only or neither. Each is a comma-separated list, so one
audience can fan out to several chats, and a `<chat id>_<topic id>` entry
addresses a single topic inside a supergroup that has Topics enabled:

```ini
QTE_TELEGRAM__PRIVATE_CHAT_IDS="-1002173777783_924584,-1001111111111"
```

A shadow-mode signal is notified too, labelled `[🧪SHADOW]`, and an action the
broker never took is marked `⚠️ NOT DELIVERED` rather than quietly omitted.
Nothing here is on the trade path: the runner only queues the update, a
background worker does the sending, and a Telegram outage costs the chat its
update, never a signal. See
[architecture notes](architecture.md) — *Why one Telegram message per
position, edited in place*.

### Errors in the same chat

`QTE_TELEGRAM__LOG_ERRORS_ENABLED=true` forwards every `ERROR` log line the
runner writes to Telegram — the same switch `algo-trading-broker` spells
`TELEGRAM_LOG_ERRORS_ENABLED`, and off by default.

| Variable | What it does |
| --- | --- |
| `QTE_TELEGRAM__LOG_ERRORS_ENABLED` | Forward `ERROR` and `CRITICAL` records. |
| `QTE_TELEGRAM__LOG_DEDUP_WINDOW` | Seconds an identical error is suppressed for. Default `60`; `0` forwards the whole storm. |
| `QTE_TELEGRAM__LOG_CHAT_IDS` | Where errors go. Empty falls back to the **private** chats, never the broadcast ones. |
| `QTE_TELEGRAM__LOG_BOT_TOKEN` | A second bot for errors, so a ban on one channel does not take the other with it. Empty falls back to the main token. |

Dedup is keyed on the logger, the level and the message, so a reconnect loop
logging the same line every second is one message a minute, while a different
error on the same logger still gets through immediately. The Telegram modules
and the HTTP client underneath them are filtered out of forwarding, so a failing
send cannot log an error that triggers another send.

### Start and stop in the same chat

Every `make start` and `make stop` announces itself, on by default once a token
and a chat are set (`QTE_TELEGRAM__SERVICE_STATUS_ENABLED`):

```
🟢 strategy-runner UP
-----------
Book: dev.shadow.mt5
Strategies: XAUUSD M15 QTE_EXAMPLE_EMA_ATR
Transport: nats
Shadow mode: ON
-----------
2026-10-09 07:12:33 UTC
```

```
🛑 data-ingestion DOWN
-----------
Uptime: 4h 12m
-----------
2026-10-09 11:24:05 UTC
```

Two services speak: `data-ingestion` and `strategy-runner`. Redis, Postgres,
NATS and the Tailscale node are images this repository does not control, and
they do not need to announce themselves — neither service finishes starting
without them, so an UP message is itself proof that its dependencies answered.
A service that dies on the way up still sends a DOWN message, marked `did not
finish starting`, which is the case nobody is watching a terminal for.

Both announcements are awaited rather than queued: there are two per process
lifetime, and the DOWN one has to leave before the event loop does. Like
everything else here they go to `QTE_TELEGRAM__LOG_CHAT_IDS` (falling back to
the private chats), never to the broadcast audience, and a Telegram outage
never stops a service from stopping.

---

## Steering the engine from Telegram

The notifications above are one-way. The bot is the other half: thirteen commands
for looking at a running engine and steering it, shaped after
`algo-trading-broker`'s own bot so the two read alike — the same monospace
tables, the same Prev/Next paging, the same confirmation in front of anything
that closes a position.

It is its own service (`src/qte_bot`, `qte-bot`), off by default:

```ini
QTE_BOT__ENABLED=true
QTE_BOT__TELEGRAM_TOKEN=   # its own BotFather bot, not the notification one
QTE_BOT__ADMIN_IDS=        # your Telegram user id; an empty list answers nobody
```

`make start` then brings it up with everything else. It needs its own token
because only one process may poll `getUpdates` for a token at a time.

| Command | What it answers |
| --- | --- |
| `/redis` | Bars held per symbol and timeframe, and the newest one |
| `/runner` | Every strategy, its window, and whether it can decide on the next bar |
| `/strategies` | Each mounted strategy repository as a box: the build its `BUNDLE_VERSION.json` records and the strategies it publishes |
| `/mapping` | The symbol → strategy table as the file states it, with each pair's parameters and the per-strategy defaults |
| `/provider` | Each data provider's plan: the symbols it feeds, their market and timeframes, and the vendor's own knobs |
| `/positions` | What is open: symbol, timeframe, strategy, `signal_uxid`, opened |
| `/closed` | Positions that are over, newest first, paged |
| `/flat all \| SYMBOL \| STRATEGY` | Closes positions, after a confirmation |
| `/prevent all \| SYMBOL \| STRATEGY` | Stops deciding — on closed bars and on ticks |
| `/allow all \| SYMBOL \| STRATEGY` | Resumes deciding |
| `/warmup all \| SYMBOL` | Asks the vendor for the warm-up window again |
| `/flush all \| SYMBOL` | Drops the warm-up bars Redis holds |
| `/demo all \| STRATEGY` | Fires one demo open cycle per strategy — Telegram sees the card, no worker does |

`/strategies` is the one command that asks neither the runner nor a database. It
scans the `__strategies__/` mount and, for every strategy folder, prints one
box: the build from that folder's
[`BUNDLE_VERSION.json`](strategies.md#which-build-is-deployed-bundle_versionjson) and the
strategies the last `make strategy-mount` recorded for it, each marked as
passing or failing its audit. The two template folders are left out. It reports
what is **on disk**, which is not always what is running: a deploy replaces files
under a runner that keeps executing the code it already imported until it
restarts, so after a deploy restart the runner, then check `/runner` for what it
actually loaded.

`/mapping` and `/provider` read the two configuration files back —
[`config/strategies_mapping.toml`](mapping-and-sizing.md#pairing-symbols-with-strategies) and
each `config/data_providers/<provider>.toml` — so they answer "what is this
book configured to do" from the files an operator edits rather than from a
running process. Both tell an **absent** file from an **empty** one, because
those are different instructions: no mapping table means every strategy keeps
the symbols it declares on itself, while a table that maps no pairs means trade
nothing, and the two differ by a deploy. Compare either with `/runner` to see a
deploy that has not been restarted yet. No credential reaches `/provider`: the
plan parser drops every credential-shaped key as it reads the file, and the
vendor key lives in `.env`.

`/demo` is `make demo-signal` run from
the chat, and it is the one command that *does* something rather than asking:
it drives each mounted strategy over four synthetic regimes, publishes the
first entry one of them produces on `SIGNALS_DEMO.<strategy>`, and the broker
answers with a Telegram card and forwards nothing to a worker. It runs inside
the bot rather than in the runner — seconds of CPU per pair, which the live
loop must not spend — and writes nothing to Redis, Postgres or the outbox, so a
running runner is untouched. Its report keeps three outcomes apart: an envelope
that did not leave (transport), a strategy that **raised** while being driven
(the drill's finding — it would raise on a live bar too), and a strategy that
simply declined every regime, which is ordinary and still gets a synthetic
payload so the path is proven. Two notes before `/demo all`: it puts one card
per strategy into the channel, and one bot runs one drill at a time.

Three things are worth knowing before you use the acting commands:

* **`/prevent` does not drop bars.** They keep arriving and keep filling the
  window; only the decision is skipped, so `/allow` resumes on the next bar
  with no hole behind it. A strategy that also decides on ticks (`on_tick`) is
  not asked about them either while its pair is paused. It is stored in Redis,
  so it survives a runner restart, and it does not touch a position that is
  already open — that is what `/flat` is for, and `/flat` works even before the
  first bar after a restart has arrived.
* **`/flat` is an ordinary exit.** It goes out as a `FLAT` through the outbox,
  sized and audited like a strategy's own close, labelled `MANUAL_FLAT`. The
  reply says what it closed and what it refused, per cycle. A command the
  runner cannot serve — an unknown action, or one that failed — is answered
  with an error rather than left to time out.
* **`/warmup` and `/flush` are served by ingestion**, not by the bot reaching
  into Redis. The stored bars, the staging watermark and the resampler's
  in-flight bucket are one piece of state; dropping the bars from outside would
  leave the other two pointing at bars that no longer exist. Their symbol list
  comes from the data-provider file, so it is the list you edited.

There is no HTTP control plane and this adds none: the bot asks the running
services over NATS request/reply — the same actions `qte-control` uses — and
reads Redis and Postgres directly for `/redis`, `/positions` and `/closed`, so
those three still answer when the runner is down — as do `/strategies`,
`/mapping` and `/provider`, which read the mounted directory and the
configuration files, and `/demo`, which drives the mounted strategies itself.
