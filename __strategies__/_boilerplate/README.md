# `_boilerplate/` — what a strategy repository looks like from the engine's side

This is the only thing committed under `__strategies__/`. Everything else in
this directory is git-ignored: it is where **your private strategy repo** gets
cloned at deploy time, so the alpha never lands in the public engine's history.
This folder exists so that a fresh checkout still shows you the shape of what
belongs there.

```
__strategies__/                  ← mount point, git-ignored, read-only in the container
├── _boilerplate/                ← you are here (committed, on purpose)
│   ├── manifest.py              ← the only file the engine looks for
│   ├── pyproject.toml           ← its own deps and its own release cycle
│   ├── uv.lock                  ← what `make strategy-requirements` freezes for the image
│   ├── src/boilerplate/
│   │   ├── __init__.py
│   │   ├── contract.py          ← the engine's contract, restated — no engine import
│   │   ├── indicators.py        ← ema/atr/crossover, pandas + numpy only
│   │   ├── my_edge.py           ← the strategy class: one method per broker action
│   │   └── _helpers.py          ← leading `_` — never mistaken for a strategy
│   └── tests/
│       └── test_my_edge.py      ← `tests/` is never walked by the plugin scan
├── my-strategy/    ← your real repo, cloned whole
└── one_off.py                   ← a loose file also works — the "scan" path
```

## Copying it

```bash
cp -r __strategies__/_boilerplate __strategies__/my-strategies
cd __strategies__/my-strategies
git init                    # it is a repository, not a folder
```

Then, in order:

1. rename `src/boilerplate/` to your package and `MyEdge` to your strategy;
2. set `name` on the class — it **is** the NATS subject the broker's workers
   subscribe to (`SIGNALS.<name>`), so it has to match what they are configured
   for, and no two strategies mounted here may share it;
3. write `_rule()`. That is the only method with a hole in it;
4. publish it from `manifest.py` by adding it to `ALIASES`;
5. `make audit` from the engine root, then backtest it.

Its dependencies are not automatic: the runner imports plugins into its own
process, so whatever the repo needs has to be installed alongside the engine.
Both targets take `STRATEGY`, the checkout's name under `__strategies__/`
(leave it unset to act on every checkout there instead):

```bash
make strategy-mount         STRATEGY=my-strategies
make strategy-requirements  STRATEGY=my-strategies
```

## Isolation: this repo imports nothing from the engine

**The strategy logic is the core. The engine is a delivery layer around it.**
Nothing under `src/` imports `qte_shared`, or anything else out of `engines/` —
`tests/test_my_edge.py` fails the build if that ever changes. A rule about price
should not depend on the process that happens to carry its output to a broker,
and a repo that inverts that cannot be built, tested or moved without dragging
the machinery along.

What makes it possible is that the engine checks a strategy **structurally**,
never by ancestry: a concrete `on_candle_closed`, a `name`, a
`history_window()`, and intents whose *field names* it recognises. So this repo
restates the contract on its own side, in
[`src/boilerplate/contract.py`](src/boilerplate/contract.py) — `SignalAction`,
`SignalIntent`, `StrategyBase`, `SignalStrategy` and the dispatch order — and
the engine converts what comes back at the boundary. `qte-strategy-audit` reads
it the same way and still checks all seven signal methods; the audit output
above was produced against exactly this code.

The one rule the duplication imposes: **do not rename an intent field**. The
conversion reads by name, so a rename is a value silently dropped at the
boundary. Adding fields of your own is free — the engine ignores what it does
not know.

Indicators are owned here too, in
[`src/boilerplate/indicators.py`](src/boilerplate/indicators.py), computed with
pandas and numpy. Need more? Add the package to `pyproject.toml` and re-lock —
that is the whole dependency story:

```bash
uv add pandas-ta --project __strategies__/my-strategies
```

## The two files that matter

**`manifest.py`** is the integration. The engine walks one level into
`__strategies__/`, imports this file *by path*, calls `load_all()` and takes
back `{alias: class}`. It learns no package name and no directory layout, so
the repo reorganises itself freely — and publishing is opt-in, which is what
keeps a half-finished experiment in `src/` from trading because someone forgot
it subclassed a strategy base. A repo without a manifest still works: every
`.py` under the directory is imported and anything strategy-shaped is
collected, which is the right shape for one loose file and the wrong one for a
repository.

**`my_edge.py`** is the contract: `long`, `short`, `tp1`, `tp2` and `sl` are
required, `r_sl` and `flat` are optional and shown anyway. Decisions read only
`df` — OHLCV indexed by candle open time in UTC, oldest first, last row always
a *closed* bar — and return `SignalIntent` objects. The strategy publishes
nothing itself: the runner attaches the bracket, mints or reuses the trade-cycle
`signal_uxid`, delivers to the broker and writes the audit row.

As shipped, `_rule()` returns `False`, so this template loads, audits and
backtests end to end without ever emitting a signal. That is deliberate — a
template that traded the moment it was mounted is one nobody could safely leave
in place.

## Checking it

```bash
# From this repo, with no engine installed — pandas, numpy, pytest and nothing else:
uv run --project __strategies__/_boilerplate pytest

# From the engine root, as the engine will see it:
make audit                                    # the deploy gate: every published class, checked
make strategies                               # what the engine can see right now
uv run qte-backtest run --strategy QTE_BOILERPLATE_M15 --symbol XAUUSD --timeframe M15
```

Two complaints are the expected state for a template, not a broken install:

* `make audit` → `WARN QTE_BOILERPLATE_M15: loaded but routed to no symbol`.
  Once a routing table exists, a strategy it does not list is never handed a
  candle. Add it to a `[symbols.<SYMBOL>].strategies` list when you want it to
  run.
* the backtest → `[CRITICAL] NO_TRADES: The strategy never opened a position`.
  That is `_rule()` answering `False`. The replay still ran the whole file
  across every bar, which is what you want to see before you write the edge:
  once the rule says `True`, the same run reports fills.

## Further reading

"How strategies are found" and "The two contracts, and why there are two" in the
root [README](../../README.md) carry the whole path from a cloned repo to a
signal on the wire, plus the engine's own
`tests/test_plugin_loader.py` — another worked manifest repo that imports
nothing from the engine.
