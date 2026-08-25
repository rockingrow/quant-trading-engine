# The backtest report

`qte_backtest.report` writes a JSON artefact meant for an AI agent or a script,
a Markdown companion rendering the same object for a human, and — on request —
an HTML dashboard rendering it again as charts. They are not a summary and a
detail view of different numbers: the Markdown is the JSON laid out for reading,
and the HTML is the JSON drawn.

```bash
uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --report
# → data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.{json,md}

uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --report --chart
# → …{json,md,html}
```

## What the JSON contains

| Block | Answers |
| --- | --- |
| `reading_guide` | The conventions an agent would otherwise have to guess — what R means, what the fill model assumes, why there is only ever one position. |
| `run` | Strategy, class, module, params, warm-up, starting equity. |
| `data` | Bar count, first and last bar, bars left after warm-up, gap count. |
| `market` | A downsampled OHLC window of the replayed history, plus the buy-and-hold basis — the one thing the trades cannot re-derive. |
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
- **`market` is for drawing, never for computing.** Each row aggregates
  `bucket_bars` consecutive bars — first open, highest high, lowest low, last
  close — so a chart of a multi-year run stays a few hundred rows. Every metric
  in the report comes from the full series; anything computed off these rows
  would be a coarser answer wearing the same name.

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

## Drawing it

`qte-backtest chart` turns a report into a single self-contained HTML page laid
out like a strategy tester — the layout every discretionary trader already
reads, so the numbers land without a legend.

```bash
uv run qte-backtest chart data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.json
# → …20260823T150404Z.html

make chart REPORT=data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.json
```

It takes the **JSON and nothing else**, which is the property worth protecting:
a report kept from a run three months ago still draws, on a machine with no
parquet history, no strategy installed and no engine. Rendering is a separate
command rather than a flag you had to have remembered — the replay is the
expensive part, and the file is the artefact.

| Panel | Shows |
| --- | --- |
| Key stats | Total P&L, max drawdown, profitable trades, profit factor. |
| Performance | Cumulative P&L against buy-and-hold, with per-trade excursion and the underwater band as toggles. |
| Price and trades | The `market` window as candles, every entry marked and joined to its exit. |
| Performance analysis | Breakdown (gross profit/loss, P&L grouped by exit, direction or month), Periodical (CAGR, Sharpe, Sortino, P&L by day/week/month/year), Benchmarking (return against buy-and-hold, weekly, with correlation), Growth and decline (alternating run-ups and drawdowns, their durations). |
| Trades analysis | Distribution (returns histogram, winners/losers split), Streaks, and the full metric table split All / Long / Short. |
| Risk and excursion | MAE and MFE against realised R, and the R sequence trade by trade. |
| Diagnostics | Every finding, its evidence and its one concrete change. |
| List of trades | The whole trade list, sortable and filterable. |

Three rules the page keeps:

- **Nothing is fetched when it opens.** The stylesheet, the script and the data
  are inlined; there is no CDN and no build step. A report is something you
  email, or open on a machine with no network.
- **Nothing is invented.** A statistic that needs data the replay never had —
  intrabar equity, margin, liquidation — is absent rather than approximated. A
  tester's margin panel has no honest equivalent here, so there is no margin
  panel.
- **Every aggregate is re-derivable.** The trade list is the source and the
  panels are views over it, so a reader who distrusts a number can recompute it
  from the same file.

The derivations live in `qte_backtest.visualize.view` and are tested without a
browser; `render.py` only inlines them into the page.

## Getting at it

The files are the interface. `data/reports/` holds one JSON and one Markdown per
run, named `<strategy>_<symbol>_<timeframe>_<timestamp>`, so runs accumulate side
by side and comparing this one against the last is a diff rather than an
archaeology exercise.

```bash
make reports                          # what has been written
uv run qte-backtest run … --report    # write another
make chart REPORT=data/reports/….json # draw one
```

An agent reads `data/reports/*.json` directly. There is no HTTP service in front
of it, and adding one would put a process to keep alive between an agent and a
file it can already open. The dashboard is a file for the same reason — opening
it needs a browser, not a server.

## What it does not do

The report describes one run of one strategy on one symbol. It does not compare
runs, sweep parameters, or walk forward. Those are separate jobs and putting
them here would make the schema mean different things in different files.
