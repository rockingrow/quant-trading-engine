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

## The two ids

| | `signal_id` | `signal_uxid` |
| --- | --- | --- |
| Minted by | the broker, per persisted signal | **QTE**, per trade cycle |
| Unique per | one action | one whole trade |
| Used for | de-duplication at the worker | correlating a close with its entry |

QTE only ever sets `signal_uxid`. An entry mints it; every TP/SL/FLAT that
follows reuses it. It is exactly 16 uppercase alphanumeric characters — the
broker 422s anything else, so `qte_shared.models` validates the shape locally
before a trade decision depends on it.

The runner persists the open cycle id per (strategy, symbol) in Redis, so a
restart mid-trade still closes the position it opened rather than orphaning it.

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
