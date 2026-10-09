# NATS subjects & example payloads

Every subject the broker and its workers exchange messages on, with a runnable
JSON example for **every action that can occur**. Files are named
`<subject-group>.<action>.json`; open the file linked next to each action.

The subjects are grouped by direction of flow:

- [**Broker → Worker**](#broker--worker) — signals, admin directives, and
  configuration the broker pushes down to workers.
- [**Worker → Broker**](#worker--broker) — position events and the connect
  handshake workers push up to the broker.
- [**Internal (Broker → Broker)**](#internal-broker--broker) — the durable
  JetStream webhook buffer the broker feeds itself.

`account_id` appears in two forms across these subjects: the **bare** id stored
in the `accounts` table (e.g. `123456`, used on `ADMIN`/`TRADE`) and the
**worker id** `<market>-<gateway>-<account_id>` (e.g. `CRYPTO-BINANCE-7654321`,
used on `SYSTEM`).

---

# Broker → Worker

## `{strategy}` — trade signals

Each signal is published on the subject **equal to its `strategy` field** (e.g.
`MT5_GOLD_M5_V1`) — there is one subject per strategy, and a worker subscribes
only to the strategies it handles, so it never sees another strategy's traffic.

- **Entry / target / stop payloads** are a full `TradingSignal`: `signal_id`,
  `signal_uxid`, `timestamp`, `strategy`, `action`, `symbol`, `price`,
  `quantity`, plus the
  optional `sl` / `tp1` / `tp2` / `risk_percent` risk levels and the
  optional `use_equity_sizing` flag (size off account equity instead of
  balance). A **scale-in**
  additionally sets `is_scale_position: true`, `scale_strategy`, and a `scaling`
  block (`tp` / `sl` / `quantity`) describing the add.
- **The FLAT directive** is a lighter payload carrying only `signal_id`,
  `signal_uxid`, `strategy`, `timestamp`, `action`, `symbol` — no
  price/quantity, because it means "close everything on this strategy".

Two ids travel with every payload and they answer different questions:

- `signal_id` — **this one signal**, minted per persisted signal, so it is
  unique per action. It is the **de-duplication key**: a worker that sees a
  signal live and then again inside a `WORKER_CONNECTED_ACK`'s `retry_signals`
  drops the duplicate by this id.
- `signal_uxid` — the **trade cycle** the signal belongs to, shared by the entry
  and every TP/SL/FLAT that follows it. Use it to **correlate** a close with the
  position it opened — never for de-duplication, since a whole trade shares one
  value.
`action` is one of `SignalActionEnum`: `LONG`, `SHORT`, `TP1`, `TP2`, `R_SL`,
`SL`, `FLAT`.

| Action | Meaning | Example |
| ------ | ------- | ------- |
| `LONG` | Open a long entry | [`entry.long.json`](entry.long.json) |
| `SHORT` | Open a short entry | [`entry.short.json`](entry.short.json) |
| `LONG` (scale-in) | Add to an existing position (`is_scale_position` + `scaling` block) | [`entry.long.scale.json`](entry.long.scale.json) |
| `TP1` | First partial-close target hit | [`close.tp1.json`](close.tp1.json) |
| `TP2` | Second (final) close target hit | [`close.tp2.json`](close.tp2.json) |
| `R_SL` | Runner stop-loss (SL moved into profit) | [`close.r_sl.json`](close.r_sl.json) |
| `SL` | Stop-loss hit | [`close.sl.json`](close.sl.json) |
| `FLAT` | Close-all directive for the strategy (lightweight payload) | [`close.flat.json`](close.flat.json) |

## `ADMIN` / `ADMIN.<market>.<gateway>.<account_id>` — admin directives

Payload is an `AdminSignal` (`action`, `timestamp`, optional `strategy` /
`symbol` / `account_id` / `market` / `gateway`). `action` is one of
`AdminActionEnum`: `FLAT`, `BLOCK_SIGNAL`, `ALLOW_SIGNAL`.

**Routing depends on whether the directive is scoped to a single account:**

- **Account-scoped** — `account_id` is set, and `market` + `gateway` are
  **required** with it (an account id is only unique within a market/gateway
  pair). The directive is published to the **private** per-account subject
  `ADMIN.<market>.<gateway>.<account_id>` (e.g. `ADMIN.FOREX.MT5.123456`) that
  **only that one account's worker is subscribed to**. No other worker ever sees
  the message — nor the `account_id` — so each worker stays isolated to its own
  account.
- **Broadcast** — no `account_id` (a strategy/symbol-scoped or flat-everything
  directive). Published to the shared `ADMIN` subject and fanned out to **every**
  connected worker, which filters for itself client-side.

`BLOCK_SIGNAL` / `ALLOW_SIGNAL` are always account-scoped today (a user
toggling new-signal blocking for their own account), so they always go to the
private subject.

| Action | Scope | Subject | Example |
| ------ | ----- | ------- | ------- |
| `FLAT` | one account | `ADMIN.FOREX.MT5.123456` (private) | [`admin.flat.json`](admin.flat.json) |
| `FLAT` | strategy / symbol | `ADMIN` (broadcast) | [`admin.flat.broadcast.json`](admin.flat.broadcast.json) |
| `FLAT` | everything | `ADMIN` (broadcast) | [`admin.flat.all.json`](admin.flat.all.json) |
| `BLOCK_SIGNAL` | one account | `ADMIN.FOREX.MT5.123456` (private) | [`admin.block_signal.json`](admin.block_signal.json) |
| `ALLOW_SIGNAL` | one account | `ADMIN.FOREX.MT5.123456` (private) | [`admin.allow_signal.json`](admin.allow_signal.json) |

## `SYSTEM` — configuration & handshake replies (broker side)

The broker's outgoing half of the `SYSTEM` conversation. Each payload is a
`SystemSignal` subclass keyed by `action` (`SystemActionEnum`) and addressed to
a worker by its worker id (`account_id` in `<market>-<gateway>-<account_id>`
form). The handshake reply is sent on the request's **reply inbox** (from the
worker's NATS `request`) rather than the shared `SYSTEM` subject, so it reaches
only the worker that asked.

**A handshake gets exactly one reply.** A NATS reply inbox accepts a single
message — `request()` resolves its future (or, `old_style`, auto-unsubscribes at
`max_msgs=1`) on the first one and silently drops the rest — so the whole answer
travels inside one `WORKER_CONNECTED_ACK`:

- `strategy_magic_map` — strategy → magic number, from the `strategy_magic_map`
  setting, filtered to the strategies the worker announced. Always present;
  `{}` means nothing matched.
- `retry_signals` — every SIGNAL persisted in the last `max_retry_timeout`
  seconds for those same strategies, shaped exactly like the live payloads on
  the `{strategy}` subject so the worker can replay them through the same
  handler and de-duplicate by `signal_id`. Always present; `[]` means nothing
  to replay.
- `settings` — the worker's own `accounts.settings` blob: what its owner set
  from the bot, today `signal_blocked` (`/prevent` → `true`, `/allow` →
  `false`). Always present and always complete — an account that has never run
  a command gets the defaults — so a worker connecting after a restart, or
  reconnecting after being offline, honours what was set while it was away
  instead of coming up unblocked. The `BLOCK_SIGNAL`/`ALLOW_SIGNAL` ADMIN
  message stays the *live* push; this is the durable copy.
- `crypto_leverage_init` — allowed `symbols` + `default_leverage`, **only** for
  a crypto worker; `null` for every other market.

| Action | Sent | Meaning | Example |
| ------ | ---- | ------- | ------- |
| `WORKER_CONNECTED_ACK` | reply inbox (or `SYSTEM` for a fire-and-forget worker) | Handshake accepted; carries the worker's whole initial configuration | forex: [`system.worker_connected_ack.json`](system.worker_connected_ack.json) — crypto: [`system.worker_connected_ack.crypto.json`](system.worker_connected_ack.crypto.json) |
| `WORKER_CONNECTED_ERROR` | reply inbox | Handshake received but the broker could not build the initial config (carries `reason`). Sent *instead of* the ACK, never after it | [`system.worker_connected_error.json`](system.worker_connected_error.json) |
| `CRYPTO_LEVERAGE_INIT` | `SYSTEM` | Push allowed crypto `symbols` + `default_leverage` to workers that are **already connected**, after an admin changes the setting. A worker's connect-time copy rides inside the ACK instead | [`system.crypto_leverage_init.json`](system.crypto_leverage_init.json) |

---

# Worker → Broker

## `TRADE` — position events

Payload is a `PositionEvent`, published by a worker whenever a row in its local
`positions` table is inserted (`event: CREATED`) or updated (`event: UPDATED`).
Besides the trade fields (`symbol`, `action`, `volume`, `opened_price`,
`closed_price`, `sl`/`tp1`/`tp2`, …) it carries an **account snapshot**
(`account_id`, `account_name`, `gateway`, `account_leverage`,
`account_balance`) the broker needs to create/upsert the trade and address the
worker later. `ref_id` is the gateway's own order/ticket reference; `signal_id`
echoes back the id the worker was given on the SIGNAL payload, which ties the
event to the originating signal — and, through that signal's `signal_uxid`, to
the broadcast message the broker renders for the trade.

`status` is the **worker** position status; the broker maps it onto its own
trade lifecycle state via `broker/domain/trade_status.py` (shown in the last
column). A `REJECTED` event carries a `reject_reason` explaining why the worker
refused the SIGNAL — e.g. its MAX ORDER limit was hit, or it already holds an
open position for that symbol/strategy so the broker's new SIGNAL cannot be
taken.

| `event` | `status` | → Broker trade status | Example |
| ------- | -------- | --------------------- | ------- |
| `CREATED` | `OPENED` | `OPENED` | [`trade.created.opened.json`](trade.created.opened.json) |
| `CREATED` | `REJECTED` | `REJECTED` (MAX ORDER limit, has `reject_reason`) | [`trade.created.rejected.json`](trade.created.rejected.json) |
| `CREATED` | `REJECTED` | `REJECTED` (open position exists, has `reject_reason`) | [`trade.created.rejected.open_position.json`](trade.created.rejected.open_position.json) |
| `UPDATED` | `TP1` | `PARTIALLY_CLOSED` | [`trade.updated.tp1.json`](trade.updated.tp1.json) |
| `UPDATED` | `TP2` | `CLOSED` | [`trade.updated.tp2.json`](trade.updated.tp2.json) |
| `UPDATED` | `SL` | `CLOSED` | [`trade.updated.sl.json`](trade.updated.sl.json) |
| `UPDATED` | `R_SL` | `CLOSED` | [`trade.updated.r_sl.json`](trade.updated.r_sl.json) |
| `UPDATED` | `TERMINAL_CLOSED` | `CLOSED` | [`trade.updated.terminal_closed.json`](trade.updated.terminal_closed.json) |
| `UPDATED` | `FORCED_CLOSED` | `CLOSED` | [`trade.updated.forced_closed.json`](trade.updated.forced_closed.json) |
| `UPDATED` | `FLATTED` | `FLAT` | [`trade.updated.flatted.json`](trade.updated.flatted.json) |

## `SYSTEM` — connect announcement (worker side)

The worker's outgoing half of the `SYSTEM` conversation. Right after it connects
to NATS the worker publishes a single `WORKER_CONNECTED` announcing itself and
requesting initial configuration. The broker answers on the request's reply
inbox with exactly one message: a `WORKER_CONNECTED_ACK` carrying the whole
configuration, or a `WORKER_CONNECTED_ERROR`.

| Action | Meaning | Example |
| ------ | ------- | ------- |
| `WORKER_CONNECTED` | Worker announces its `account_id` (worker id), `market`, `gateway`, and the `strategies` it subscribes to; the strategies select both the `strategy_magic_map` entries and the `retry_signals` replay it gets back | [`system.worker_connected.json`](system.worker_connected.json) |

---

# Internal (Broker → Broker)

## `SIGNALS.<strategy>` — durable webhook buffer (JetStream)

Not exchanged with workers. The webhook endpoint (`POST /secret/webhook`)
enqueues the raw TradingView envelope onto the JetStream stream `SIGNALS`
(subject `SIGNALS.<strategy>`) and returns `202` immediately. The broker's own
`SignalWorker` then consumes each envelope and fans it out to the matching
`{strategy}` subject in the Broker → Worker section. The envelope wraps a
`WebhookPayload` (strategy, symbol, timeframe, the `position` block, and
optional `indicators` / `inputs`) under a `payload` key.

| Message | Example |
| ------- | ------- |
| Webhook envelope | [`signals.webhook_envelope.json`](signals.webhook_envelope.json) |
