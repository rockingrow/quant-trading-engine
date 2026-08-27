# Repository audit — 26 Aug 2026

Scope: shared contracts, live ingestion and strategy delivery, backtest parity,
plugin audit, and existing test coverage on `dev`.

Baseline before changes: Ruff clean; 439 tests passed and one environment-
dependent test failed. After changes: 450 tests pass; Ruff lint and format checks
are clean. Reverified after the lifecycle/position-sizing branch was merged into
`dev`: 547 tests pass.

## Fixed in this audit

| Severity | Finding | Resolution |
| --- | --- | --- |
| High | `SignalFactory.build()` changed the in-memory cycle before broker delivery. A failed entry left a ghost position; a failed exit forgot a real one. | Added two-phase build/deliver/commit semantics. Live state changes only after delivery succeeds (or shadow execution is accepted). |
| High | Two candle/tick callbacks, or two returned entry intents, could both observe a flat slot and orphan the first cycle. | Refuse a second entry and serialize each strategy/symbol slot with one async lock. |
| Medium | A strategy-emitted `TP1` closed the entire simulated position, unlike broker-side partial target handling. | Replay now closes the explicit quantity or the entry's `tp1_percent`, and applies breakeven-stop state consistently. |
| Medium | `NaN`/infinite prices passed `validate_shape()` and serialized to JSON `null`. | Market-data and broker numeric models now reject non-finite values at validation. |
| Low | The strategy-audit test assumed `pandas_ta` was absent, so installing it made the suite fail. | The fixture imports a unique deliberately nonexistent module. |
| High | A failed Core NATS publish permanently lost an already-retired candle close. | Completed candles are staged atomically with history in a Redis outbox, published oldest-first, and acknowledged only after publish succeeds. Duplicate crash recovery is rejected downstream by candle open time. |
| High | Broker timeouts were treated as definite failures and every retry received a new de-duplication ID. | Signals are staged in a Postgres outbox first; the row UUID is reused as `Nats-Msg-Id`/HTTP `Idempotency-Key`, timeouts remain `unknown`, affected pairs are blocked, and the retry loop/startup recovery replay the exact payload idempotently. |
| Medium | A failure halfway through service startup leaked NATS, Redis, broker, or feed connections. | Ingestion and strategy startup now unwind acquired resources in reverse order, including half-started feeds, and shutdown is idempotent. |

## Resolved follow-up findings

### 1. Candle-close delivery survives publish failure — High

`RedisState.stage_closed_candle()` now writes candle history and appends the
same candle to an ordered Redis outbox in one transaction. Ingestion drains the
outbox before accepting new ticks and before every timed flush, acknowledging a
row only after Core NATS publish returns. A crash in the final publish/ack gap
may replay the candle; the runner's existing open-time guard makes that replay
idempotent.

### 2. Broker timeout is durable and explicitly ambiguous — High

The runner now stages the exact broker envelope as a `pending` signal row before
sending. Its UUID is the delivery ID for every attempt. `BrokerSink` reports
timeouts as `unknown`; the runner leaves position state unchanged, blocks new
decisions for that pair, retries `unknown` rows continuously, and replays all
`pending`/`unknown` rows at startup with the same ID. Applied IDs are persisted
on partial positions so a replay cannot reduce `remaining` twice.

### 3. Partial startup failures unwind opened resources — Medium

Both service `start()` methods now catch any interrupted/failed acquisition and
run the same idempotent cleanup path as normal shutdown. Tasks, feeds, broker,
Redis, and NATS are released in reverse acquisition order, and a feed is tracked
before its `start()` hook so a half-started provider is still stopped.

## Verification

- `python -m pytest -q` → 559 passed
- `ruff check .` → clean
- `ruff format --check .` → 128 files already formatted
- `git diff --check` → clean
