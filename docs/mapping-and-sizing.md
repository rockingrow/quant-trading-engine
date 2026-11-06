# Mapping symbols and sizing positions

Which strategy trades which symbol, with what parameters, and how big every
entry is. Both live in configuration rather than in the strategy, and both the
live runner and the backtest read them the same way.

## Pairing symbols with strategies

Which strategies trade which symbols lives in
**`config/strategies_mapping.toml`**, not in the code:

```toml
[symbols.XAUUSD]
strategies = ["MT5_GOLD_M5_SCALP"]

# Per-strategy defaults, applied wherever this strategy runs.
[strategies.MT5_GOLD_M5_SCALP]
risk_percent = 1.0

# Read by the engine, not the strategy: the candles this pair collects before
# it may decide. Omit it to keep the count the strategy declares.
warmup = 150

# Per-pair overrides beat the [strategies.*] defaults above, so one strategy
# can run tighter on gold than it does on everything else.
[symbols.XAUUSD.params.MT5_GOLD_M5_SCALP]
risk_percent = 0.5

# Parks a symbol without deleting its configuration.
[symbols.EURUSD]
enabled = false
strategies = ["MT5_FX_M15_V1"]
```

```bash
make strategy-mapping   # copy the template into place (never overwrites)
make audit              # verify every name in it against what __strategies__/ publishes
```

**The real file is git-ignored; the example beside it is not.** What you trade,
and at what risk, is position information and this repo is public — so the
schema stays reviewable in history while the book does not. Point
`QTE_ENGINE__MAPPING_FILE` elsewhere to mount it as a secret in production.

With no file at all nothing breaks: each strategy falls back to its own
`symbols` attribute, or to `QTE_ENGINE__SYMBOLS` when it declares none. A file
that exists but maps nothing means *trade nothing*, which is a different thing
and is treated as one.

`warmup` is the one setting there that decides *when* a pair starts trading
rather than how. A strategy declares the candles its slowest indicator needs;
the mapping may ask for another number, and both the runner and the backtest
honour it, so a replay still starts deciding on the same bar. Asking for fewer
than the strategy declares is allowed and logged — what it really needs is a
property of its indicators, which the engine cannot check — while asking for
more than its `max_history` is refused, because the runner's buffer could never
reach that count.

The runner builds one instance per `(symbol, strategy)` pair, so a strategy
carrying state between bars never has gold's last bar deciding what happens on
bitcoin's next one. A name in the table that nobody publishes is logged as an
error at boot, because the symptom otherwise is a symbol that quietly trades
nothing — which reads exactly like a strategy that found no setups.

---

## Position sizing and the trade cycle

**The engine decides how big every entry is, not the strategy.** A strategy is
never told the balance — that is what keeps a backtested file and a traded file
the same file — so size is settled in one place,
`qte_shared.strategies.sizing`, and both drivers go through it:

```
quantity = QTE_ACCOUNT__CAPITAL x risk_percent / 100 / |entry - stop| / contract_size
```

Read it as *risk this many currency units if the stop is hit*. `risk_percent` is
the pair's own value in `config/strategies_mapping.toml`, falling back to
`QTE_ACCOUNT__RISK_PERCENT`:

```bash
QTE_ACCOUNT__CAPITAL=1000.0          # the account, and what a % of risk is a % of
QTE_ACCOUNT__RISK_PERCENT=1.0        # fallback when a pair states none
QTE_ACCOUNT__MAX_RISK_PERCENT=0.0    # ceiling on any one entry's risk_percent; 0 = none
QTE_ACCOUNT__COMMISSION_PER_UNIT=0.0 # backtest cost, charged each side
QTE_ACCOUNT__SPREAD=0.0              # full bid/ask distance, each side pays half
QTE_ACCOUNT__SLIPPAGE=0.0            # on top of half the spread, each side
QTE_ACCOUNT__CONTRACT_SIZE=1.0
QTE_ACCOUNT__COST_PROFILE=           # which profile in config/costs.toml prices a fill
QTE_ACCOUNT__MAX_OPEN_POSITIONS=0    # account-wide position ceiling; 0 = uncapped
```

A pair whose mapping sets `dynamic_risk = true` is sized at the `risk_percent`
its strategy states on each entry instead, never above the pair's
`max_risk_percent` (unset, the pair's own `risk_percent`) or the account
ceiling — see [the broker contract, *Sizing*](broker-contract.md#sizing).

Those three costs are the *last* layer: one spread for the whole engine is
wrong as soon as a second symbol is replayed, so state them per symbol in
`config/costs.toml` instead — see [the cost profiles](backtesting.md#cost-profiles).

The capital is **fixed for a run**; it does not compound. Sizing off running
equity would make later trades depend on earlier P&L, so two backtests differing
by one early trade could not be compared.

**As many trade cycles per (strategy, symbol) as the strategy opens.**
`signal_uxid` is the cycle: an entry mints it and every close reuses it, which
is how the broker groups a whole trade into one broadcast. An entry that fires
while positions are already open stacks another one beside them, in the
backtest and live alike, each with its own id and its own bracket. The only
entry refused on these grounds is one reusing the id of a cycle already open.

**One ceiling, and it is on the account rather than the pair.**
`QTE_ACCOUNT__MAX_OPEN_POSITIONS` caps how many positions the runner holds
across every strategy and symbol together; `0`, the default, means uncapped.
It is the engine's copy of a limit the broker account enforces and the engine
cannot read: the worker that places the orders counts every active position on
the account and refuses an entry past its own limit, and that refusal never
comes back here — so without the ceiling the engine goes on managing a cycle
the account does not hold, sending its `TP1`/`TP2`/`SL` on schedule. Set it to
whatever the account enforces. Closes are never refused, a refusal is logged
and recorded as `entry_refused_account_full`, and the backtest has no
counterpart to it, which is why it is off by default.

A cycle ends on `TP2`, `SL`, `R_SL` or `FLAT` — **and on a `TP1` that closes the
entry's whole quantity**. Because that decision needs the sizes, the runner
keeps the whole position record in Redis *and* Postgres (`open_positions`), so a
restart mid-trade closes what it opened at the size that is left. See
[the broker contract](broker-contract.md#the-trade-cycle).
