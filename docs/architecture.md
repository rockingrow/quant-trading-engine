# Architecture notes

Decisions that are easy to reverse by accident, and why they are the way they
are.

## Why two NATS namespaces

`QTE.*` is ours — ticks and candle closes, on **core** NATS. At twenty ticks a
second a dropped message is replaced by a fresher one immediately, so paying
JetStream's persistence cost for market data buys nothing.

`SIGNALS.<strategy>` is the **broker's**, on JetStream. Losing a signal loses a
trade. Different guarantees, different transport, and the split is why the
engine can be casual about one and careful about the other.

In production both usually live on the same cluster (the broker's). QTE keeps
two configurable URLs anyway, because "the market data bus" and "the order bus"
are not the same concern and one day they may not be the same server.

## Why a bar closes on the clock, not on the next tick

A resampler that closes a bar when the *next* tick arrives publishes the M15
candle two minutes late in a thin session — and every worker downstream expected
it on the quarter hour. `Resampler.flush(now)` runs on a timer and closes any
bucket the clock has passed, regardless of feed activity. A bucket with no ticks
produces no candle: forward-filling a flat synthetic bar would feed strategies a
body that never traded, which corrupts any indicator with a range in it.

## Why each engine owns its own tables

`signals` is written only by the strategy runner; the backtest tables only by
the replay. Each lives in that engine's own `db/` package with the repository
that reads and writes it, so a change to how signals are audited touches one
engine rather than a module three engines import.

The exception is `engine_events`, which every engine writes and none owns; it
stays in `qte_shared.db`. Ingestion therefore has no `db/` package — it writes
only that shared table. Giving it an empty one, or inventing a table to fill it,
would be arranging the code to match a diagram rather than the writes.

What they do share is one `DeclarativeBase`. Alembic autogenerate diffs the
database against `Base.metadata`, so a model registered on a *different* base
would be invisible to it — and invisibility here does not fail loudly. The table
would never be created, and the next autogenerate would find it in the database,
fail to find it in the metadata, and propose `DROP TABLE`. The same trap applies
to `migrations/env.py`: an engine whose models it does not import is an engine
whose tables Alembic will offer to delete.

## Why Alembic replaced the init script

The schema used to be a `.sql` file mounted into the Postgres image's
`docker-entrypoint-initdb.d`. That hook runs exactly once, on an empty data
directory. Changing the schema afterwards meant either editing a file that would
never run again, or deleting the volume — and the volume is the audit trail.

The revision chain is deliberately split in two: the core schema runs on any
PostgreSQL, and pgvector plus the unmapped `signals.embedding` column is a
second, optional step. That keeps a speculative capability — nothing writes
`embedding` yet — from making a stock Postgres image insufficient to run the
engine.

## Why Redis holds state that Postgres does not

Redis is the hot path's memory: the last tick, the warm-up window, the open
cycle id per (strategy, symbol). It runs with AOF on, so a restart loses at most
the last write. The runner rebuilds its indicator window from Redis on boot
instead of waiting hours for live candles, which is what makes a restart resume
trading on the next close.

Postgres is the audit trail — written *after* the signal has gone out, and its
failures are logged rather than raised. A logging outage must not become a
runner that stops trading.

## Why the strategy returns intents instead of publishing

Three things a strategy is not allowed to own, all in `SignalFactory`:

1. **Cycle ids** — an entry mints one, every close reuses it. Getting this wrong
   makes the broker render an exit as an unrelated trade.
2. **The bracket** — SL/TP from the intent when set, from ATR/percentage
   defaults when not, so "never send a naked entry" is enforced in one place.
3. **Timeframe spelling** — QTE says `M15`, the broker's contract says `"15"`.

Because both drivers go through the factory, a signal in a backtest report is
byte-identical to the one a worker would have executed. Reconciliation compares
two rows, not two implementations.

## Why the plugin loader imports by path

`__strategies__/` is a mounted volume cloned from a private repo, not an
installed distribution. Requiring it to be pip-installable would drag the
private repo into the public build, and it would trade a fast edit-and-restart
loop for a version bump and an image rebuild on every change to a strategy.

Two consequences follow from "it is a whole repository":

- **Nothing is committed under that path**, not even a `.gitkeep`. `git clone`
  refuses a destination that already contains a file, so a tracked placeholder
  would break the one command the design exists to support.
- **Discovery filters repo furniture.** A bare `rglob("*.py")` would import the
  private repo's own test suite and, if a virtualenv lives in there, walk
  site-packages. Hidden directories and `EXCLUDED_DIRECTORIES` are skipped
  before the importer sees them.

A module that fails to import is logged and skipped — one broken strategy file
should not stop the other four from trading.

Duplicate strategy names are an error, not a warning to skim past: workers
subscribe by strategy name, so two algorithms sharing one would execute against
each other's positions.

## Why there is no control-plane service

There was one — a FastAPI gateway serving health, the audit trail, backtest
triggers and the shadow-mode switch. It was removed because almost everything it
served was already available more directly: listing strategies is `ls`, the
audit trail is a SQL query, running a backtest is a CLI command, and a report is
a file on disk that an agent can open. A web service in front of those is a
process to keep alive, secure and monitor in exchange for a second way to reach
the same data.

The exception is shadow mode, which genuinely has to reach a *process that is
already running* — flipping live/paper must not require a restart mid-position.
That is one message on `QTE.control`, so `qte-control` publishes it straight to
NATS and no service is needed to carry it.

Relatedly, `NatsBus.connect` bounds the *initial* connect even though reconnects
are unlimited: nats-py applies `max_reconnect_attempts` to both, so `-1` (what a
live feed wants) would make a first connect against a dead server hang forever.

## Why the fill simulator is pessimistic

Listed in the README. The one worth repeating: when a bar's range covers both
the stop and the target, the simulator takes the **stop**. Without tick data
there is no ordering, and assuming the favourable one is precisely how a losing
strategy backtests profitably.

## Why the repo root is searched for, not counted

`qte_shared.config` resolves `.env`, `__strategies__/` and `data/` relative to
the workspace root, and it finds that root by walking up until it hits the
`pyproject.toml` that declares `[tool.uv.workspace]` — not by counting parent
directories.

The difference matters because the counted version does not break loudly. Moving
`shared/` to `engines/shared/` adds one level, and `parents[2]` then points at
`engines/`: the engine keeps starting, silently reads no `.env`, and looks for
strategies in a directory that does not exist. Identifying the root by its
marker makes the layout free to change.
