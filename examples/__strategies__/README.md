# Example strategies

`__strategies__/` is git-ignored in this repo — it is where your **private**
strategy repo gets cloned, whole, so the alpha never lands in the public
engine's history:

```bash
git clone git@github.com:you/my-private-strategies.git __strategies__/my-strategies
```

Clone into a *subdirectory* rather than onto the mount point: `git clone`
refuses a destination that already contains a file, and one thing is committed
under that path — [`__strategies__/_boilerplate/`](../../__strategies__/_boilerplate/),
a template of what a strategy repository looks like from the engine's side.
Read it for the layout; read this directory for the contract.

To try the pipeline before you have a private repo:

```bash
cp examples/__strategies__/ema_atr_breakout.py __strategies__/
uv run qte-backtest run --strategy QTE_EXAMPLE_EMA_ATR --symbol XAUUSD --timeframe M15
```

`ema_atr_breakout.py` is a demonstration of the contract, not an edge. Read it
for the shape — one method per broker action, indicators from
`qte_shared.indicators`, a decision that reads only `df`, an ATR-derived
bracket, `SignalIntent` returned rather than published — then delete it.

Note what its `tp1`, `tp2` and `sl` do: nothing, on purpose. The bracket travels
with the entry and the broker's worker manages the exits, so the strategy has no
per-bar opinion about them. Saying that in three lines is the point of the
interface; the alternative is a reader inferring it from an absence.

Check it the way the engine will:

```bash
make audit
```

## The other way in

This example is the *scan* path: a loose file that subclasses `SignalStrategy`,
found by walking the directory. It is the right shape for one file and the wrong
one for a repository.

A real strategy repo declares a **manifest** instead — a `strategies.py` or
`manifest.py` at its root exposing `load_all()` returning `{alias: class}`. The engine imports that
one file and asks it what exists, so nothing here knows a module path, and the
repo need not import `qte_shared` at all: the contract is checked structurally
and the intents are converted at the boundary. That is what lets it keep its own
lockfile, its own Python and its own test suite.

See "How strategies are found" in the root README — it carries a diagram of the
whole path from a cloned repo to a signal on the wire — plus
`tests/test_plugin_loader.py` for a worked manifest repo that imports nothing
from the engine, and `tests/test_strategy_audit.py` for one that restates the
seven-method interface on its own side and is still checked against it.
