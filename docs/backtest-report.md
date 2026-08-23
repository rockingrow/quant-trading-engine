# The backtest report

`qte_backtest.report` writes two files per run: a JSON artefact meant for an AI
agent or a script, and a Markdown companion rendering the same object for a
human. They are not a summary and a detail view of different numbers — the
Markdown is the JSON, laid out for reading.

```bash
uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --report
# → data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.{json,md}
```

## What the JSON contains

| Block | Answers |
| --- | --- |
| `reading_guide` | The conventions an agent would otherwise have to guess — what R means, what the fill model assumes, why there is only ever one position. |
| `run` | Strategy, class, module, params, warm-up, starting equity. |
| `data` | Bar count, first and last bar, bars left after warm-up, gap count. |
| `costs` | Spread, slippage, commission, contract size, derived round-trip cost. |
| `metrics` | Currency **and** R-multiple statistics, excursion averages, exit-reason counts, direction split, exposure, streaks, equity curve. |
| `diagnostics` | Findings, most severe first, each with its threshold, its evidence and one concrete change. |
| `activity` | Trades taken, entries rejected, signals emitted. |
| `trades` | Every trade — entry, exit, bars held, initial risk, R-multiple, MAE/MFE, and each partial leg. |
| `signals` | The exact broker payloads the run would have published. |

Two design choices to know about:

- **The JSON carries every trade, not a sample.** It is a file, not a context
  window; an agent can filter it, but a truncated file cannot be un-truncated.
  Pass `--no-report-signals` if the emitted payloads are not wanted.
- **`schema_version` is the first key.** A consumer that learned this shape can
  detect that it changed.

## Reading it

Everything risk-normalised is in **R** — profit divided by the risk taken at
entry (`|entry - initial_sl| × quantity × contract_size`). Read expectancy and
payoff in R and treat the currency figures as scale. A trade that reached the
market without a stop has `r_multiple: null` and is excluded from every R
statistic; `metrics.trades_without_stop` counts them, and a critical finding
fires if there are any.

`initial_sl` is kept separately from `sl` on purpose. Moving a stop to breakeven
changes `sl`, and measuring R against the moved stop would make every trade's
risk appear to shrink after the fact and inflate the whole report.

**MAE/MFE** (`mae_r`, `mfe_r`) is how far price went against and for a trade
while it was open. The two comparisons that pay for themselves:

- high **MAE on winners** → the stop sits in the noise; losers are probably the
  same trade with slightly worse timing;
- high **MFE on losers** → the entries were right and the exits banked nothing.

Both have a diagnostic rule attached, so you do not have to spot them yourself.

## Diagnostics

`qte_backtest.diagnostics` runs a fixed set of rules over the finished run.
Every rule obeys the same three constraints, and one that cannot is not a
diagnostic but a vibe:

1. it fires on a **threshold stated in the finding**, so you can disagree with
   the threshold rather than reverse-engineer it;
2. it carries the **numbers that triggered it** in `evidence`, so the claim is
   checkable without re-running anything;
3. it proposes **one concrete change**, not a direction to think about.

Severity means: `critical` — the result is not trustworthy or the strategy is
broken; `warning` — real, act on it; `info` — worth knowing, not necessarily
wrong. `report.is_trustworthy` is false whenever anything critical fired, and
the CLI prints the findings after the summary so a report nobody opens still
gets read.

| Code | Fires when |
| --- | --- |
| `NO_TRADES` | Nothing was ever entered. |
| `SAMPLE_TOO_SMALL` | Fewer than 30 trades — no ratio means anything yet. |
| `WARMUP_DOMINATES` | Warm-up eats ≥25% of the history (critical at 50%). |
| `TRADES_WITHOUT_STOP` | Any trade reached the market with no stop. |
| `EXITS_NEVER_TRIGGER` | ≥50% of exits were the replay running out of bars — usually a units bug in the levels. |
| `ALL_EXITS_ARE_STOPS` | ≥90% stop-outs; targets are theoretical. |
| `TP2_NEVER_REACHED` | A second target was set and never hit. |
| `ENTRIES_REJECTED` | ≥30% of entry signals dropped because a position was open — the missing `context.open_uxid is None` guard. |
| `ONE_SIDED` | Every trade the same direction across a real sample. |
| `TRADES_LAST_ONE_BAR` | Average hold ≤1.5 bars, so intrabar path decides the result. |
| `STOP_INSIDE_COSTS` | Median risk under 3× the round-trip cost (critical under 1.5×). |
| `COSTS_EXCEED_EDGE` | Fees alone exceed gross profit. |
| `NEGATIVE_EXPECTANCY` | Expectancy below zero on a real sample. |
| `RESULT_CONCENTRATED` | One trade is ≥50% of the net result. |
| `DRAWDOWN_EXCEEDS_PROFIT` | Peak drawdown larger than the whole net profit. |
| `STOP_TOO_TIGHT` | Winners average ≥0.6R of adverse excursion first. |
| `LOSERS_WERE_WINNERS` | Losers averaged ≥1R in profit before dying. |
| `EXITS_LEAVE_MONEY` | Average MFE ≥2× what the average winner banks. |
| `ALWAYS_IN_MARKET` | ≥90% exposure — the entry filters are not binding. |
| `DATA_GAPS` | Consecutive bars further apart than one timeframe. |

Adding a rule is a function in `diagnostics.py` returning `Finding | None`,
registered in `_RULES`, plus a test that it fires on the fault and stays quiet
on a healthy run.

## Over HTTP

The control plane serves the same artefacts, so an agent with network access
does not need the filesystem:

| Endpoint | |
| --- | --- |
| `POST /backtest/run` | Runs and returns metrics, trades, findings and `trustworthy`. Writes the files unless `write_report: false`. |
| `GET /backtest/reports` | Report files, newest first. |
| `GET /backtest/reports/{name}` | One file whole. Names are resolved inside the reports directory and refused if they escape it. |

## What it does not do

The report describes one run of one strategy on one symbol. It does not compare
runs, sweep parameters, or walk forward. Those are separate jobs and putting
them here would make the schema mean different things in different files.
