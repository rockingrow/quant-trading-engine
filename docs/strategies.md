# Writing and deploying strategies

How a strategy is written, how the engine finds it in `__strategies__/`, how one
is switched off, and the audit that decides whether what was cloned in is fit to
trade. Which symbols each strategy trades, and at what size, is in
[Mapping and sizing](mapping-and-sizing.md).

## Writing a strategy

A strategy presents **one method per broker action**. Five are required —
`long`, `short`, `tp1`, `tp2`, `sl` — and two are optional: `r_sl` (re-stop:
break-even or trailed) and `flat` (a close that is neither a target nor a stop).

```python
from qte_shared.indicators import atr, crossover, ema
from qte_shared.models import SignalAction
from qte_shared.strategies.strategy_base import SignalIntent, SignalStrategy


class MyEdge(SignalStrategy):
    name = "MT5_GOLD_SCALP"  # ← the NATS subject workers subscribe to
    symbols = ("XAUUSD",)  # ← a default; config/strategies_mapping.toml overrides it
    timeframe = "M15"
    warmup = 220

    def long(self, df, context):
        fast, slow = ema(df["close"], 21), ema(df["close"], 55)
        if not bool(crossover(fast, slow).iloc[-1]):
            return None

        close = float(df["close"].iloc[-1])
        risk = float(atr(df, 14).iloc[-1]) * 1.5
        return SignalIntent(
            action=SignalAction.LONG,
            price=close,
            quantity=0.01,
            sl=close - risk,
            tp1=close + risk * 1.5,
            tp2=close + risk * 3,
            tp1_percent=50.0,
            move_sl_to_be=True,
        )

    # The bracket travels with the entry and the broker's worker manages the
    # exits, so there is nothing to decide per bar. Saying so explicitly is the
    # point of the interface: an absence is not an answer.
    def short(self, df, context):
        return None

    def tp1(self, df, context):
        return None

    def tp2(self, df, context):
        return None

    def sl(self, df, context):
        return None
```

`SignalStrategy` implements `on_candle_closed` — the method the engine actually
calls — for you, by asking those seven in a fixed order:

| Position | Methods asked | Rule |
| --- | --- | --- |
| Holding (`context.open_uxid` set) | `sl` → `r_sl` → `tp1` → `tp2` → `flat` | every one is asked; taking `tp1` and trailing the stop on one bar is normal |
| Flat | `long` → `short` | the first that answers wins — a bar cannot be both |

The stop is asked before any target: if one bar both stopped out and reached a
target, the stop is what happened. A method that returns somebody else's action
— a `tp1()` returning a `SHORT` — raises rather than reaching the broker as a
valid-looking payload. Override `on_candle_closed` yourself if you need a
different order.

**What the framework guarantees, so you do not have to:**

- `df` is an OHLCV frame indexed by candle **open time** in UTC, oldest first,
  and its last row is always a **closed** bar. It never contains a future bar.
- `df` holds `history_window()` bars — `max(warmup * 2, 400)` by default, or
  whatever `max_history` you set. **The live runner uses the same number**, so a
  strategy that reads the whole frame (a running sum, a session VWAP) computes
  the same thing in both places.
- `name` must match the strategy the broker's workers are configured for — it
  *is* the NATS subject they subscribe to. Two strategies sharing a name is a
  loud error at load time and a hard failure in the audit.
- You never publish anything. Return intents; the runner attaches the bracket,
  mints or reuses the trade-cycle id, sends, and audits.
- `on_tick(price, ctx)` is optional. Override it only when an exit has to react
  faster than a bar close — the runner subscribes to ticks only if something does.

---

## How strategies are found

Two ways in, and the loader prefers the first.

**A manifest — for a strategy repository.** A plugin repo declares itself with a
`strategies.py` — or `manifest.py`, the loader takes either — at its root,
exposing `load_all()` returning `{alias: strategy class}`:

```python
# __strategies__/my-strategies/manifest.py
from mine.gold.m5 import GoldEdge

ALIASES = {"MT5_GOLD_M5_SCALP": GoldEdge}


def load_all():
    return dict(ALIASES)
```

The engine imports that one file and asks it what exists — so nothing on this
side knows a module path or a directory layout, the plugin repo reorganises
itself freely, and it decides which of its classes are deployed. A half-finished
experiment sitting in the tree cannot start trading because someone forgot it
was a strategy subclass. **The repo need not import `qte_shared` at all**: the
engine recognises a strategy structurally — a concrete `on_candle_closed`, a
`name`, a `history_window()` — and converts the intents it returns into its own
models. That is what lets a plugin repo run its own lint, test and release cycle
with this one nowhere in sight.

A manifest may expose a second hook, `load_settings()`, returning
`{alias: settings}` — what the engine should enforce *around* each strategy
rather than ask it about. Optional, and today it carries one setting, the
weekend flat:

```python
# __strategies__/my-strategies/src/mine/settings.py
SETTINGS = {
    "MT5_GOLD_M5_SCALP": {
        "weekend_flat": {
            "enabled": True,
            "flat_from": "FRI 17:00",  # when the instrument shuts
            "flat_until": "SUN 22:00",  # when it reopens
        },
    },
}


def load_settings():
    return SETTINGS
```

Inside that window the engine refuses new entries and closes any open position
with a `FLAT` — in the backtest and in the live runner alike. Exits are never
blocked. The strategy declares the calendar and does not implement it, because a
cut-off written inside one strategy is one the next strategy can forget to copy.
`QTE_ENGINE__WEEKEND_FLAT_TIMEZONE` (default `UTC`) decides whose clock those
times are read on, and `QTE_RUNNER__WEEKEND_FLAT_SWEEP_INTERVAL` how often the
runner also checks between bars — a stalled feed otherwise leaves no close to
act on. A repo with no settings hook, and a strategy missing from the table,
get no weekend flat; a setting that will not parse stops that strategy loading
rather than quietly reverting to "off".

`enabled` is the strategy's *default*; the pair decides. Set `use_weekend_flat`
in `config/strategies_mapping.toml` — under `[strategies.<name>]` for every pair,
or `[symbols.<symbol>.params.<name>]` for one — or pass `--param
use_weekend_flat=true` to a backtest, and it overrides the declared `enabled`
for that pair in both drivers. A window declared with `enabled = false` is still
parsed and validated, so switching it on is a mapping edit. Switching on a
window the repo never declared, or giving the key anything but `true`/`false`,
stops the runner (and the backtest) from starting that pair.

**A directory scan — for a single file.** Failing a manifest, every `.py` under
the directory is imported and anything that looks like a strategy is collected.
Drop a single `.py` file in and it runs, no ceremony.
The scan recurses, skips hidden directories and the usual repo furniture
(`tests/`, `docs/`, `build/`, …) and files starting with `_`. A file that fails
to import is logged and skipped: one broken strategy does not stop the others.

Their dependencies are not automatic — plugins are imported into the runner's
process, so whatever they need is installed alongside the engine:

```bash
make strategy-mount                          # every __strategies__/<name> with a pyproject.toml
make strategy-mount STRATEGY=my-strategies   # just that one: install deps, then audit it
make strategy-test STRATEGY=my-strategies    # run that repo's own suite, in its own venv
make strategies                              # list what the engine can see
make audit                                   # check that what it sees is fit to trade
```

`strategy-mount` records the audit result of every strategy each repo publishes
in `__strategies__/strategies.toml` — auto-generated, never hand-edited, one
table per repo and one line per strategy:

```toml
[strategies.my-strategies]
MT5_GOLD_M15_V1 = true
MT5_GOLD_M5_V1 = false
```

`make up` and `make dev` read it to freeze only the repos that still have a
passing strategy into the image, and refuse to start without it. So does the
runner: a strategy marked `false` is not loaded, because its dependencies were
never frozen into the image it would have to run in, and a repo with nothing
passing is not even imported. Fix what the audit found and mount it again.
`make strategies` lists what the engine can see; `uv run qte-strategy-mount
--show` lists what the mount recorded.

### Skipping a mounted bundle you do not want to trade

A bundle can be mounted, audited and perfectly healthy and still be something
this deployment has no use for — the `_boilerplate` reference, a simulator
harness. `strategies.toml` cannot say that: `false` there means *failed the
audit*, the deploy step then freezes none of that repo's dependencies, and the
file is regenerated on the next `make strategy-mount` anyway.

That choice lives in code instead, in `SKIPPED_REPOSITORIES` in
[`src/qte_shared/strategies/plugin_loader.py`](../src/qte_shared/strategies/plugin_loader.py):

```python
SKIPPED_REPOSITORIES = frozenset(
    {
        # The plugin-shape reference, not a tradable edge.
        "_boilerplate",
        # Exercises the position lifecycle against the simulator; mapped to no
        # symbol in production, so loading it only adds a startup warning.
        "qte-signal-simulator",
    }
)
```

Nothing under a listed directory is imported, by the runner or by the backtest,
and nothing in it is audited either. That second part is the reason both names
are in there: a loaded strategy that no symbol maps to is an audit *warning*,
and `QTE_RUNNER__AUDIT_ON_START=strict` refuses to start on a warning — so with
these two skipped the pre-flight audit reports four strategies, zero errors and
zero warnings instead of two warnings that block the start.

**Comment a name out to load that bundle again.** Nothing else has to change:
the clone stays where it is and its line in `strategies.toml` keeps its
verdict, so `make strategy-mount` does not have to run again.

### Switching one strategy off: `DISABLED`

`SKIPPED_REPOSITORIES` drops a whole bundle, and it is this deployment's choice.
Taking **one strategy** out of service is the repository's own choice, so it
lives in the repository, beside the alias table — the same table twice:

```python
# __strategies__/my-strategies/manifest.py (or the registry it re-exports)
ALIASES = {"MT5_GOLD_M15_SCALP": GoldScalp}  # published: the engine loads these
DISABLED = {"MT5_GOLD_M5_SCALP": GoldEdge}   # kept — code, tests, documents — but off


def load_all():
    return dict(ALIASES)


def load_disabled():  # the optional third hook: names only
    return sorted(DISABLED)
```

Moving a line from one table to the other is the whole switch. What the engine
does with the second one:

- it never instantiates a disabled strategy, in the runner or the backtest —
  `qte-backtest run --strategy <it>` says *disabled by its repository*, not *not
  found*;
- `make audit` lists it as `[ off]`, beside the strategies it checked, and counts
  it in the summary. That is not a finding: switching a strategy off is not a
  defect, and a strict audit still passes;
- a mapping table that still routes a symbol to it **is** a failure,
  `mapped-to-disabled`, and the runner logs it as an `ERROR` naming the
  repository. The table and the repository disagree about whether it trades, so
  with `QTE_RUNNER__AUDIT_ON_START=strict` the runner will not start until one of
  them gives way — take it off the `[symbols.<SYMBOL>].strategies` lists in the
  same deploy that disables it;
- an alias in both tables is not loaded and fails the audit; a `load_disabled()`
  that raises or returns anything but alias strings stops the whole repository,
  because an off switch that cannot be read must not leave a strategy trading.

Disabling a strategy does not itself close its existing positions. If its
pair is still mapped, strict startup audit rejects the configuration; when
startup audit permits it, orphan settlement reports the pair and leaves it
open. Remove its symbol mapping in the same deploy to retire the pair:
`QTE_RUNNER__CLOSE_ORPHAN_POSITIONS=close` then attempts a `FLAT` on startup,
subject to the delivery and recovery guards described in
[Operating a running engine](operations.md#coming-back-from-an-outage). Manifest code
still executes during discovery, so imports in the manifest remain the
repository author's responsibility.

A repository with no `load_disabled()` disables nothing. `_boilerplate` and
`qte-signal-simulator` carry the hook with an empty `DISABLED` table, so a
repository copied from either starts with the switch in place.

### Which build is deployed: `BUNDLE_VERSION.json`

Every strategy folder carries the stamp of its own build, at its root beside
`manifest.py`:

```
__strategies__/
├── my-strategies/
│   ├── manifest.py
│   ├── BUNDLE_VERSION.json     ← which build of this folder is deployed
│   └── src/…
└── other-strategies/
    ├── manifest.py
    └── BUNDLE_VERSION.json     ← its own, not shared with its neighbour
```

One per strategy folder, never one for `__strategies__/` as a whole: each
repository is built and deployed on its own, so two of them can be on different
builds at once. The **strategy repository's build writes it** and a deploy to
the production server always carries it along, which makes it the deploy
tracklog. The engine never writes it; it only reads it, to answer
[`/strategies`](telegram.md#steering-the-engine-from-telegram).

```json
{
  "branch": "dev",
  "built_at": "2026-10-20T01:29:14Z",
  "commit": "1185c39",
  "content": "e9d4e243285de83e",
  "dirty": true,
  "mode": "minified",
  "tree": "e5e209e41d1180cc98e1e5ce203b875651b7352b",
  "version": "20261020T012914Z-1185c39-e5e209e4-dirty"
}
```

| Key | What it is |
| --- | --- |
| `version` | one comparable token for the build: time, commit, tree, and `-dirty` when it applies |
| `built_at` | ISO 8601 UTC, to the second |
| `branch`, `commit` | where it was built from |
| `tree` | git tree hash of the built sources — survives an amend, which a commit hash does not |
| `content` | digest of the code as shipped — the only key that still describes a build made from a dirty tree |
| `dirty` | whether the tree had uncommitted changes, which makes `commit` and `tree` unreliable |
| `mode` | `minified` or `bytecode` |

The file is **optional**. A checkout that was never built has none, and a folder
without it is still listed by `/strategies` — with every strategy it publishes
and the version left blank. A file that exists but is not valid JSON is flagged
in the message instead of being skipped, and neither case stops the engine: the
stamp is a label, and nothing is loaded or refused because of it.

The two committed templates, [`_boilerplate`](../__strategies__/_boilerplate/BUNDLE_VERSION.json)
and [`qte-signal-simulator`](../__strategies__/qte-signal-simulator/BUNDLE_VERSION.json),
carry the example above so the shape is visible. They are in
`SKIPPED_REPOSITORIES`, so `/strategies` never lists them; if you copy one to
start your own repository, delete the copied stamp until your build writes a real
one.

> Why a manifest, why the contract is structural rather than nominal, why the
> weekend flat is the engine's job rather than a strategy's, and why the
> interface is seven methods rather than one:
> [architecture notes](architecture.md).

---

## Auditing what you cloned in

The loader is forgiving by design: what it cannot drive it logs and skips, so
one broken file does not stop the other four. That is right for a running
process and useless as a deploy check — "skipped" and "there were none" read
identically in a log until the P&L does not arrive.

`qte-strategy-audit` is the strict pass over the same directory:

```bash
make audit                                   # human-readable, fails on errors
make audit-strict                            # warnings fail too — what CI should run
uv run qte-strategy-audit --format json      # for anything that has to act on it
uv run qte-strategy-audit --format markdown  # for a pull request
```

```
Strategy audit - /app/__strategies__
Mapping table  - /app/config/strategies_mapping.toml

  [FAIL] MT5_GOLD_M5_SCALP  (GoldEdge, via manifest)
         /app/__strategies__/my-strategies/manifest.py
         signals: long, short, tp1, sl
         FAIL MT5_GOLD_M5_SCALP.tp2: required signal method tp2() is not implemented
              -> def tp2(self, df, context) -> IntentResult: return None - an explicit
                 'never' is an answer; an absence is not

1 strategies, 1 errors, 0 warnings
```

It fails a deploy on a strategy that would not load, is missing a signal method,
takes the wrong arity, declares a bad timeframe or warmup, duplicates another's
name, or is named in the mapping table and published by nobody. It warns on a
strategy nothing maps, a directory with no manifest, and an unnamed class. Every
finding carries a `fix`; the exit code is the product.

`__strategies__/` is a bind mount that can be pulled, edited or emptied after CI
ran, so the runner also audits its own book, in its own process, immediately
before it loads anything:

```bash
QTE_RUNNER__AUDIT_ON_START=warn      # log the report, start anyway (default)
QTE_RUNNER__AUDIT_ON_START=error     # refuse to start when the audit found errors
QTE_RUNNER__AUDIT_ON_START=strict    # refuse on warnings too
QTE_RUNNER__AUDIT_ON_START=off       # don't
```

`warn` is the default because it changes nothing about which strategies run: the
loader still skips what it cannot drive, and the report is only there to make
that skipping visible instead of silent. Turning it up to `error` trades a
degraded book for a stopped one — the right call when a half-populated book is
worse than none, the wrong one when four strategies trading beats zero.
