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

## Open findings

### 1. Candle-close delivery is not recoverable after a publish failure — High

`Resampler` advances/deletes the completed bucket before
`IngestionService._emit_candle()` publishes it. `_emit_candle()` writes the bar
to Redis and then uses Core NATS. If that publish raises, the flush guard keeps
the task alive but nothing queues the candle for retry, and the runner does not
poll Redis for missed closes. The next flush therefore cannot recover the lost
strategy decision.

Recommended design: a durable candle-close outbox/JetStream consumer, or at
minimum an ordered retry queue whose entries are persisted before the
resampler's close is considered delivered. In-memory retry alone does not cover
a process crash between the Redis write and NATS publish.

### 2. A broker publish timeout has an ambiguous outcome — High

`BrokerSink` maps every exception to `failed`, and the runner intentionally does
not commit a failed cycle. A JetStream timeout can mean the message was stored
but its acknowledgement was lost; in that case the broker opens the trade while
the runner remains flat. The current `Nats-Msg-Id` is random on every call, so a
later retry would not deduplicate the original publish.

Recommended design: persist a signal outbox before sending, derive a stable
delivery id from that outbox row, retry with the same `Nats-Msg-Id`, and model
timeouts as `unknown` until JetStream/broker reconciliation resolves them.

### 3. Partial startup failures leak opened resources — Medium

Both service `run_forever()` methods call `start()` before entering their
`try/finally`. If NATS connects and Redis, the broker sink, provider startup, or
slot discovery then fails, `stop()` is never reached. This is especially easy
to hit in the strategy runner, which opens three clients before rejecting an
empty strategy set.

Recommended design: make startup transactional—track acquired resources and
unwind them in reverse order on any exception—then keep shutdown idempotent.

## Verification

- `python -m pytest -q` → 547 passed on `6183b92`
- `ruff check .` → clean
- `ruff format --check .` → 128 files already formatted
- `git diff --check` → clean
