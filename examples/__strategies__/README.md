# Example strategies

`__strategies__/` is git-ignored **and untracked** in this repo — it is where
your **private** strategy repo gets cloned, whole, so the alpha never lands in
the public engine's history:

```bash
git clone git@github.com:you/my-private-strategies.git __strategies__
```

Nothing is committed under that path, not even a `.gitkeep`, because `git clone`
refuses a destination that already contains a file. That means the directory is
absent from a fresh checkout and you create it yourself when you are not cloning
into it.

To try the pipeline before you have a private repo:

```bash
mkdir -p __strategies__
cp examples/__strategies__/ema_atr_breakout.py __strategies__/
uv run qte-backtest run --strategy QTE_EXAMPLE_EMA_ATR --symbol XAUUSD --timeframe M15
```

`ema_atr_breakout.py` is a demonstration of the contract, not an edge. Read it
for the shape — indicators from `qte_shared.indicators`, a decision that reads
only `df`, an ATR-derived bracket, `SignalIntent` returned rather than published
— then delete it.
