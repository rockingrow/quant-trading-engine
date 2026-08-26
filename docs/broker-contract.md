# The `algo-trading-broker` contract

Everything QTE sends is validated by
[`rockingrow/algo-trading-broker`](https://github.com/rockingrow/algo-trading-broker).
That repo is the source of truth; this page records what QTE relies on, so a
drift is caught by reading rather than by a rejected trade.

Pinned by `tests/test_broker_contract.py`.

## The payload

QTE emits the broker's `WebhookPayload` (`broker/schemas/webhook_schema.py`):

```json
{
  "strategy": "MT5_GOLD_M15_V1",
  "symbol": "XAUUSD",
  "timeframe": "15",
  "timestamp": "2026-04-10T22:55:00Z",
  "signal_uxid": "9F2C4B7E18A3D605",
  "position": {
    "action": "LONG",
    "price": 2334.5, "quantity": 0.01,
    "sl": 2329.5, "tp1": 2340.0, "tp2": 2345.0,
    "risk_percent": 3.0, "tp1_percent": 50.0, "move_sl_to_be": false,
    "use_equity_sizing": false,
    "is_running": null, "is_scale_position": false,
    "scale_strategy": null, "scaling": null
  },
  "indicators": {},
  "inputs": {},
  "token": "shared-webhook-token"
}
```

`action` ∈ `LONG`, `SHORT`, `TP1`, `TP2`, `R_SL`, `SL`, `FLAT`
(`qte_shared.models.SignalAction`).

Field-level notes:

- **`timeframe` is TradingView's spelling** — the bare minute count (`"15"`,
  `"60"`), not QTE's `M15`. `qte_shared.timeframes.to_broker_timeframe` converts.
- **`price`/`quantity` are optional in the schema** because a `FLAT` carries
  neither — it means "close everything on this strategy". An *entry* without
  them passes the broker's validation and fails at the worker instead, one hop
  too late, so `BrokerSignal.validate_shape()` rejects it here.
- **`indicators`/`inputs`** are free-form and accepted with extra keys. They are
  the audit context: what the strategy saw and what it was configured with.
- **`quantity` is QTE's number, not the strategy's** — risk-sized against the
  configured account. See *Sizing* below.
- **`use_equity_sizing` mirrors `inputs.use_equity_sizing`**, which is the
  pair's ``use_equity_sizing`` in `config/strategies_mapping.toml`. QTE reports
  it and does not obey it: sizing is off a fixed capital either way.
- **`is_running`** is filled by QTE when the strategy leaves it unset: true on a
  partial that leaves a runner behind, false on the action that finishes the
  trade.
- A close **restates what the entry established** — `risk_percent`,
  `tp1_percent`, `move_sl_to_be` and the scaling block — so a worker reading one
  message can re-sync the trade from it. Anything the strategy sets on the close
  itself wins; it is describing the trade *now*.

## Sizing

QTE decides how big every entry is. A strategy is never told the balance — that
is what keeps a backtested file and a traded file the same file — so it proposes
at most a *proportion* and the engine converts it into units:

```
quantity = QTE_ACCOUNT__CAPITAL x risk_percent / 100 / |price - sl| / contract_size
```

Read it as "risk this many currency units if the stop is hit". `risk_percent`
comes from the pair's entry in `config/strategies_mapping.toml`, falling back to
`QTE_ACCOUNT__RISK_PERCENT`. `examples/algo-trading-broker/entry.long.json` is
this arithmetic: $1,000 at 3% over a $5 stop is 6 units.

The capital is **fixed** — it does not compound as the account moves. Sizing off
running equity would make a backtest's later trades depend on its own earlier
P&L, so two runs differing by one early trade would be sized differently for the
rest of the file and could not be compared. `use_equity_sizing` therefore travels
on the payload for the broker to act on and changes nothing here.

An entry that cannot be sized — no stop, or a stop on the entry price — falls
back to `QTE_RUNNER__DEFAULT_QUANTITY` (live) or `--quantity` (backtest). A
strategy that proposes `quantity=0` is declining the trade and is not sized into
one.

Closes are expressed in the size QTE holds, not the size the strategy thinks it
holds: the ratio between the two is remembered on the cycle, every close the
strategy emits is multiplied by it, and the result is clamped to what remains.
A close naming no size gets one — `TP1` takes `tp1_percent` of the **entry**
quantity, everything else takes the remainder. `FLAT` is the exception: it means
"close everything on this strategy" and carries no size by contract.

`qte-backtest run` uses the same sizer, so a backtested trade is the size the
runner would have sent.

## The two ids

| | `signal_id` | `signal_uxid` |
| --- | --- | --- |
| Minted by | the broker, per persisted signal | **QTE**, per trade cycle |
| Unique per | one action | one whole trade |
| Used for | de-duplication at the worker | correlating a close with its entry |

QTE only ever sets `signal_uxid`. An entry mints it; every TP/SL/FLAT that
follows reuses it. It is exactly 16 uppercase alphanumeric characters — the
broker 422s anything else, so `qte_shared.models` validates the shape locally
before a trade decision depends on it. (The broker's own examples are lowercase;
QTE normalises rather than rejecting, so either spelling round-trips.)

## The trade cycle

**One cycle per (strategy, symbol) at a time.** A second entry while one is open
is refused before it reaches the wire — the broker's workers answer `REJECTED`
rather than stacking, so a backtest that allowed it would be scoring trades live
trading will never take.

A cycle ends on `TP2`, `SL`, `R_SL` or `FLAT` — and also on a `TP1` that happens
to close the entry's whole quantity. A strategy taking "50%" of a position that
was sized at one unit has finished the trade, and treating that as a partial
would leave the runner holding a cycle the broker is done with, refusing every
entry that follows.

Deciding that needs the entry size and what is left of it, so the runner keeps
a whole record (`qte_shared.models.OpenPosition`) rather than a bare id, and
writes it **twice**:

| | Redis | Postgres |
| --- | --- | --- |
| Key | hash `qte:cycle:<strategy>`, field `<symbol>` | `open_positions`, unique on (strategy, symbol) |
| Role | hot copy, read on every bar | durable copy |
| Read on boot | first | when the cache has nothing |

The fallback is the point. An empty Redis is ambiguous — it means "flat" and it
means "someone re-provisioned the cache" — and reading it the wrong way opens a
second cycle against a position the broker is still carrying, leaving the first
one a ghost nobody will ever close. When the table answers, the runner re-seeds
Redis so the next boot is a cache hit again.

Bare-uxid values written by earlier runners are still read: the size is unknown,
so such a cycle stays open until a terminal action ends it.

## Transports

### `nats` (default)

Publish `{"payload": {...}}` to JetStream subject `SIGNALS.<strategy>`, stream
`SIGNALS`. This is the broker's *internal* durable webhook buffer: its own
`POST /secret/webhook` enqueues onto the same subject and its `SignalWorker`
consumes it, so QTE inherits that persistence, retry and de-duplication without
an HTTP hop in the trade path.

Two consequences worth stating plainly:

- **The `token` field is not verified on this path.** Access to the NATS cluster
  is the authentication. Keep the broker's NATS private, or set a NATS token.
- **QTE never creates or reconfigures the `SIGNALS` stream.** The broker owns
  it; editing another service's durability guarantees from here would be a bug.

Each publish carries a fresh `Nats-Msg-Id` so a retry inside the stream's
duplicate window is stored once.

### `http`

`POST {QTE_BROKER__HTTP_URL}/secret/webhook` with the payload as the JSON body.
This path *does* verify `token` (`QTE_BROKER__TOKEN`, matched against the
broker's). Use it across any boundary you do not control.

## What QTE does not do

- It does not subscribe to `{strategy}`, `ADMIN`, `SYSTEM` or `TRADE`. Those are
  the broker↔worker conversation; QTE is upstream of all of it.
- It does not receive execution feedback. A worker's fills go to the broker's
  `TRADE` subject and land in *its* database. Reconciling QTE's `signals` table
  against the broker's is a deliberate cross-check between two independent
  records, not a gap to be closed by wiring them together.
