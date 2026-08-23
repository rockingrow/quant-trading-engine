# Example strategies

`user_strategies/` is git-ignored in this repo — it is where your **private**
strategy repo gets cloned at deploy time, so the alpha never lands in the public
engine's history.

To try the pipeline before you have one:

```bash
cp examples/user_strategies/ema_atr_breakout.py user_strategies/
uv run qte-backtest run --strategy QTE_EXAMPLE_EMA_ATR --symbol XAUUSD --timeframe M15
```

`ema_atr_breakout.py` is a demonstration of the contract, not an edge. Read it
for the shape — indicators from `qte_shared.indicators`, a decision that reads
only `df`, an ATR-derived bracket, `SignalIntent` returned rather than published
— then delete it.
