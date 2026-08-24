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

There is one revision and it needs no extensions. An earlier draft carried
pgvector and an unmapped `signals.embedding` column as a second, optional
revision, on the theory that an agent might later want to ask which past trades
resembled this one. Nothing wrote that column, and an unapplied revision sitting
at the head of the chain is worse than no revision at all: `alembic upgrade head`
is what everyone types, so a step that must be skipped fails the command people
reach for by reflex. It was removed. When vector search is actually wanted it is
a migration written then, against a schema that exists by then.

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

## Why the strategy contract is structural, not nominal

The loader does not ask `issubclass(cls, StrategyBase)`. It asks whether a class
*behaves* like a strategy — a concrete `on_candle_closed`, a `name`, a
`history_window()` — and converts the intents it returns into this repo's
pydantic models at the boundary (`coerce_intent`). The audit recognises the
seven signal methods the same way, so a repo that restates the interface is
held to it without ever importing it.

The nominal check would have forced every strategy repository to
`import qte_shared`, and that one import decides a great deal:

- It could not run its own test suite, lint pass or release without this repo
  checked out beside it, and its results would depend on whichever revision of
  the engine happened to be on disk.
- It could not pin its own dependencies. A strategy that wants a particular
  `pandas-ta` would be arguing with the engine's lockfile through a package it
  does not own.
- The engine's refactors would become the strategy repo's problem. Renaming a
  module here would break a repository whose only real dependency on us is the
  shape of a dataclass.

What is actually shared is small and stable: seven action names the broker
validates, eighteen intent fields, and the four hooks a driver calls. Both sides
state it independently and `tests/test_plugin_contract.py` pins ours —
including a fixture plugin written without a single `qte_shared` import, driven
end to end into a `BrokerSignal`.

The cost is a second copy that can drift. The mitigation is that drift fails
loudly rather than silently: an unknown action raises where the intent is
converted, in our process with our stack trace, rather than arriving as a 422
from the broker after the trade decision has already been made.

## Why a plugin repo declares a manifest

Discovery prefers a manifest at the repo root — `strategies.py` or
`manifest.py`, whichever the repo chose — exposing `load_all() -> {alias:
class}` over scanning the tree. Declaring both is an error: which alias table
is deployed would otherwise depend on the loader's lookup order.

Scanning asks the engine to guess. It imports whatever is lying around, which
for a strategy repository means a half-finished experiment can start trading
because someone forgot it subclasses a base class — and it makes every file
path a de-facto public API, so moving `gold/m5.py` is a deployment change.

The manifest inverts that. The repo names what it publishes, under the alias the
broker's workers subscribe to, and keeps the freedom to reorganise everything
behind it. It also gives the plugin somewhere to bootstrap its own `sys.path`,
which is what lets a `src/`-layout repository be loaded from a mounted volume
with no install step.

The scan is still there for the one-file case, and the two mix: a loose
`ema_atr_breakout.py` is found beside a cloned repo that brought its own
manifest. What a manifest claims, the scan leaves alone — importing those
modules a second time would register every class twice.

## Why the strategy interface is seven methods, not one

A strategy could say everything it has to say through `on_candle_closed`, and
for a while it did. The problem is not that the single hook cannot express a
strategy; it is that it does not *declare* one. Reading a class told you nothing
about what it could emit without reading three hundred lines of branching, and
the two questions an operator actually asks — "does this ever take a partial?",
"does it trail its stop, or does the worker?" — had no answer short of reading
the body and hoping.

So `SignalStrategy` names one method per broker action: `long`, `short`, `tp1`,
`tp2` and `sl` required, `r_sl` and `flat` optional. The required five are the
two that open a position and the three that close one; a strategy with no stated
stop is not a strategy. The optional two are refinements — `r_sl` is a stop
moved to break-even or trailed, `flat` a close that is neither a target nor a
stop — and most strategies hand both to the broker-side bracket, which is a
legitimate answer and worth stating rather than leaving as an absence.

`on_candle_closed` did not go away; it is still what the drivers call, and
`SignalStrategy` implements it by asking the seven in a fixed order — exits
while a position is open (`sl`, `r_sl`, `tp1`, `tp2`, `flat`), entries while
flat (`long`, `short`, first answer wins). The stop is asked before any target
because a bar that both stopped out and reached a target stopped out. A method
returning someone else's action raises: a `tp1()` emitting a `SHORT` would
otherwise reach the broker as a perfectly valid payload.

Two contracts, then, and they are enforced in different places on purpose:

| | `StrategyBase` | `SignalStrategy` |
| --- | --- | --- |
| What it is | what the engine drives | what a strategy presents |
| Enforced by | the loader, at boot | `qte-strategy-audit`, at deploy |
| On failure | skip it, keep trading the rest | non-zero exit, before anything trades |

`StrategyBase` stays minimal because every private strategy repo has to be able
to restate it (see above) — anything added there is something they must all
copy. The requirements live in the signal interface instead, and they are
checked by a tool you run rather than by the process trying to trade, so a
half-migrated repo still backtests.

## Why the audit is a separate service

The loader logs and skips what it cannot drive, deliberately: one broken file
must not stop the other four from trading. That is the right behaviour for a
process holding open positions and the wrong one for a deploy, because "skipped"
and "there were none" are the same line in a log until the P&L does not arrive.

`qte-strategy-audit` runs the same discovery and keeps what the loader discards.
It reuses `StrategyLoader.collect()` rather than growing a second walker — a
second one would drift, and an audit that disagrees with the loader about what
is deployed is worse than no audit. Judgement is what it adds: signature arity,
instantiability, the signal surface, duplicate names, and the routing table
cross-checked in both directions.

It is its own workspace member because it has no business in the runner's image.
The runner imports strategies to trade them; the auditor imports them to refuse
them, and mixing the two would put `inspect.signature` calls and a findings
model in the process that is supposed to be reacting to a candle close.

## Why symbol → strategy pairing lives in a file

A strategy declaring `symbols = ("XAUUSD",)` is fine for one strategy and wrong
for a book. The answer to "what is trading gold right now" would live scattered
across a private repo's class attributes, and changing it would mean editing and
redeploying that repo — a code change to express an operational decision.

`config/strategies_mapping.toml` moves the pairing out of the code. It is a
matrix — symbol × strategy × parameters — which is why it is a file rather
than environment variables: flattening a matrix into `QTE_ROUTING__XAUUSD_0`
is how it stops being reviewable. TOML rather than YAML because `tomllib` is
in the standard library and this is parsed inside the trading process.

The real file is git-ignored and `config/strategies_mapping.example.toml` is
not. What you
trade and at what risk is position information; this repo is public. The
template keeps the schema reviewable in history while the book stays out of it,
which is the same split `__strategies__/` makes for the code.

Three details earn their complexity:

- **Per-pair parameters, not per-strategy.** One strategy running tighter on
  gold than on bitcoin is the ordinary case, and the runner builds one instance
  per pair so a strategy carrying state between bars never has gold's last bar
  deciding bitcoin's next one.
- **An absent file is not an empty one.** No file means fall back to what each
  strategy declares — the behaviour from before the table existed. A file that
  routes nothing means trade nothing. Those differ by a deploy, so the table's
  truthiness is "was a file read", not "does it list anything".
- **A name nobody publishes is an error at boot**, not a shrug. The symptom
  otherwise is a symbol that quietly trades nothing, which in a log is
  indistinguishable from a strategy that found no setups.

## Why the engine pins Python 3.13

Nothing in this repo needs it. The runner imports the plugins in
`__strategies__/` into its own process, so the engine's interpreter and its
plugins' have to be the same one — and `pandas-ta`, which they use, requires
≥ 3.12 and hard-pins a `numba` with no 3.14 wheel. The same reasoning puts a
`numpy<2.3` entry in `[tool.uv] constraint-dependencies`: one process means one
resolution, and the constraint belongs where the reason for it is, not as a
bogus upper bound on `qte-shared`'s own numpy dependency.

This is the real cost of the plugin seam: code is decoupled, the interpreter is
not. Nothing makes that go away, so it is written down instead.

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

## Why the strategy sees a bounded window

`StrategyBase.history_window()` is read by *both* drivers, and that is the whole
point of it existing. Before it did, the live runner kept a deque of
`max(warmup * 2, 400)` candles while the backtest passed `frame.iloc[:i+1]` —
the entire file. A strategy that read the whole frame therefore computed one
thing on the chart and another in production, silently, and in the direction
that flatters the backtest.

It also made the replay quadratic: 105k bars (about three years of M15) took
roughly twelve minutes, and doubling the history quadrupled the cost. Bounded,
the same replay is linear — measured at exactly 2.00x per doubling — and lands
around five minutes with two indicators.

The rest of that cost is per-bar indicator recomputation, and it is the price of
the guarantee: the backtest calls the same `ema()` on the same window the live
runner will. Making it faster means incremental indicators that carry state
between bars, which is a real design change rather than a tuning exercise.

## Why each service builds its own image

The workspace was split so that `uv sync --package` would have a boundary to cut
along; the Dockerfile now actually cuts there. `QTE_PACKAGE` selects one member,
and the difference is not cosmetic — the full workspace venv is 352 MB against
~142 MB per service, mostly pyarrow, which only the backtest engine needs.

The boundary only holds if the manifests stay honest, so
`tests/test_packaging.py` asserts the dependency graph is a star: leaf engines
may depend on `qte-shared` and nothing else in the workspace. A direct edge
between two leaves is how a microservice boundary quietly stops being one.

## Why the market data vendor sits behind an interface

Tiingo used to be spelled out in three places: a WebSocket client inside
`data_ingestion`, a REST downloader inside `backtest_engine`, a `tiingo_ticker`
property on `SymbolSpec` in shared, and a settings block on root `Settings`.
Nothing was wrong with any one of them; together they meant a second data
source — a broker feed, a recorded fixture, an exchange API for the crypto leg —
was a change to every engine plus the core config.

So the vendor moved behind `qte_shared/interfaces/market_data.py`, which
declares the two things QTE actually consumes: a `HistorySource` returning the
canonical OHLCV frame, and a `LiveFeed` pushing `Tick`s into a handler. A
vendor is one `MarketDataProvider` — a factory that owns its credentials,
endpoints and ticker spelling and hands back those two objects.

Three consequences are worth naming:

* **The engines never import a vendor.** They call `create_provider()`, which
  resolves `QTE_MARKET_DATA__PROVIDER` through a registry. Adding a vendor is a
  file under `qte_shared/providers/` and a `register_provider` call; swapping one
  is an environment variable.
* **The vendor's configuration lives with the vendor.** Root `Settings` carries
  the *choice* (`market_data.provider`) and no vendor block, so a second vendor
  never edits the core config. `QTE_TIINGO__*` is unchanged and now read by
  `qte_shared.providers.tiingo`.
* **Images stay split.** Built-ins are registered as import-path strings and
  imported only when created, and the vendor's client libraries are an extra on
  `qte-shared` (`qte-shared[tiingo]`) rather than a hard dependency. The
  strategy runner installs neither `httpx` nor `websockets` for a socket it
  never opens.

`SymbolSpec` kept the part that is genuinely vendor-independent — which market a
symbol trades on, since that decides which endpoint a provider reaches for — and
lost `tiingo_ticker`, which is now `MarketDataProvider.ticker_for()`.
