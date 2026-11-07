# QTE Signal Simulator

Strategies with **no edge**, mounted into QTE like any strategy repository, that
exist to drive full trade cycles through the engine and into
[`algo-trading-broker`](https://github.com/rockingrow/algo-trading-broker).
Never map them onto an account that holds real money.

## `SIGNAL_SIMULATOR_LIFECYCLE_V1`

Every closed bar opens a position (`LONG` or `SHORT`, with SL/TP1/TP2 from ATR)
under its own `signal_uxid`, and each position then ends one of three ways,
chosen at random, one step per bar:

| Outcome | Bar after entry | Bar after that |
| --- | --- | --- |
| `tp1_tp2` | `TP1` | `TP2` |
| `sl` | `SL` | — |
| `tp1_rsl` | `TP1` | `R_SL` (stop at break-even) |

The draw is reproducible: it is seeded by the cycle id, which is derived from
the symbol, timeframe, entry bar and `seed` param.

**Symbol and timeframe are not in the code.** Both come from the engine's
`config/strategies_mapping.toml`:

```toml
[symbols.XAUUSD]
strategies = ["SIGNAL_SIMULATOR_LIFECYCLE_V1"]

[symbols.XAUUSD.params.SIGNAL_SIMULATOR_LIFECYCLE_V1]
timeframe = "M1"                # must be a bar ingestion produces for XAUUSD
# outcome = "sl"                # force one path
# weights = [1, 1, 1]           # tp1_tp2, sl, tp1_rsl
# seed = 0
# tp1_qty_pc = 50               # must stay below 100
```

With the default of one cycle per pair, a new entry comes on the bar after the
previous cycle ended.

## Use

```bash
make check                                         # this repo: ruff + pytest
# from the engine root:
make strategy-mount STRATEGY=qte-signal-simulator  # audit + install + record
make backtest STRATEGY=SIGNAL_SIMULATOR_LIFECYCLE_V1 SYMBOL=XAUUSD TF=M15 FILE=...
```

The broker's workers must subscribe to `SIGNALS.SIGNAL_SIMULATOR_LIFECYCLE_V1`.

To switch the strategy off without deleting it, move its line from `ALIASES` to
`DISABLED` in `src/qss/registry.py`. `load_disabled()` hands the engine that list:
the engine does not load it, `make audit` shows it as `[ off]`, and a mapping
table still routing a symbol to it fails the audit with `mapped-to-disabled`.
Moving the line back returns it to service.

## `BUNDLE_VERSION.json`

Like every strategy folder, this one carries a build stamp at its root, beside
`manifest.py`. The values here are an example of what a build writes (the keys
are in the engine's README, under "Which build is deployed"); they describe no
real build. The engine's `/strategies` command never lists this folder, since
it is a template the loader skips.
