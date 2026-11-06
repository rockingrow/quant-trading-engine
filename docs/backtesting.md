# Backtesting

```bash
make download                                        # → data/parquet/<provider>/
make backtest STRATEGY=MY_EDGE SYMBOL=XAUUSD TF=M15 \
    FILE=data/parquet/tiingo/XAUUSD_M15.parquet
```

or the CLI directly, for the full set of knobs:

```bash
uv run qte-backtest download --symbol XAUUSD --timeframe M15 --market fx --start 2023-01-01
uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --timeframe M15 \
    --file data/parquet/tiingo/XAUUSD_M15.parquet --spread 0.30 --persist
```

Every history file lives under the source that produced it —
`data/parquet/tiingo/`, `data/parquet/mt5/` for a broker CSV import — and a
replay names the file it reads. The same pair from two sources disagrees about
session times, weekend gaps and volume, and choosing between them by convention
is how a run measures a book nobody meant to test.

A run starts from `QTE_ACCOUNT__CAPITAL` (default **$1,000**) and prices its
fills from the cost profile below, so P&L, max drawdown and profit
factor are figures about a real balance rather than about one arbitrary unit.
Entries are risk-sized against that capital exactly as the live runner sizes
them, and the pair's overrides in `config/strategies_mapping.toml` apply here
too — so a backtest measures the book you actually configured. `--persist`
writes the run and every trade into Postgres.

```
Capital           1,000.00 → 1,378.79   (+37.88%)
Net PnL           378.79   (fees 0.00)
Profit factor     1.294
Max drawdown      243.31  (18.50%)
```

## Cost profiles

What a fill costs is a property of the symbol and the account type, not of a
run — gold's measured spread is **0.25**, oil's is **0.04**, BTCUSDT's is around
**20** — so the three numbers live in `config/costs.toml`, one table per symbol
inside one profile per account type:

```bash
make costs    # copy config/costs.example.toml into place; git-ignored
```

```toml
default_profile = "exness_standard"

[profiles.exness_standard.symbols.XAUUSD]
spread = 0.25                 # measured median: 0.20 (2023-24), 0.16 (2025), 0.26 (2026)
slippage = 0.0
commission_per_unit = 0.0

[profiles.exness_zero.symbols.XAUUSD]
spread = 0.05                 # tighter spread, commission billed separately
slippage = 0.0
commission_per_unit = 0.035
```

A Standard account hides its fee inside the spread; a Zero/Raw account quotes a
tighter spread and bills commission. Whether an edge survives both is a real
question, and `--cost-profile exness_zero` answers it without editing anything.
Each cost resolves on its own, highest precedence first: `--spread` /
`--slippage` / `--commission` on the command line, then
`[profiles.<name>.symbols.<SYMBOL>]`, then that profile's `defaults`, then
`QTE_ACCOUNT__*`. Every report records which layer won for each number, under
`costs.profile` and `costs.sources`, so a run's prices can be traced rather
than guessed at.

The fill simulator is deliberately pessimistic — it is meant to disprove a
strategy, not flatter one:

- entries and exits cross the spread and pay slippage;
- when a bar's range covers both the stop and the target, the **stop** is taken
  (without tick data there is no ordering, and assuming the good one is how a
  losing strategy backtests profitably);
- a gap through a level fills at the bar's open, not at the level;
- a second entry while positions are open **stacks** another cycle beside them,
  as the live runner does; only an entry reusing an open cycle's id is refused;
- a position still open on the last bar is marked out, so unrealised P&L cannot
  quietly flatter the report.

## The report

`--report` writes a JSON artefact for an agent to analyse, a Markdown companion
for a human and an HTML dashboard — same object, three renderings.
`--report-format json` (or any comma-separated subset) writes fewer:

```bash
uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --timeframe M15 \
    --file data/parquet/tiingo/XAUUSD_M15.parquet --report
# → data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.{json,md,html}
```

Give `--report` a directory to keep one study's runs together —
`--report data/reports/YYYY-MM-DD-<topic>` — which is where
[AUDIT.md](../AUDIT.md) rule 5 expects every session's output to land.

Beyond the headline metrics it carries what makes a result diagnosable: every
statistic in **R-multiples** as well as currency, **MAE/MFE per trade** (how far
price went against and for the position while it was open), each partial exit
leg, and the exact broker payloads the run would have published.

The part worth having is `diagnostics` — a rule set that reads the finished run
and says what is wrong with it. Each finding states the threshold it tripped,
carries the numbers that tripped it, and proposes one concrete change:

```
Diagnostics       2 critical, 1 info
  [CRITICAL] EXITS_NEVER_TRIGGER: 1/1 exits were forced by the end of the data
             → Print entry, sl, tp1 and tp2 for the first trade and check the
               distances against the instrument's typical bar range. A stop
               should be a small multiple of ATR, not a multiple of price.
```

`report.is_trustworthy` is false whenever anything critical fired, so an agent
knows to stop reading the metrics as meaningful. The rule table and the JSON
schema are in [the backtest report reference](backtest-report.md).

## Seeing it

`qte-backtest chart` renders a report into one self-contained HTML page, laid
out like a strategy tester — the layout every discretionary trader already
reads:

```bash
make chart REPORT=data/reports/MY_EDGE_XAUUSD_M15_20260823T150404Z.json
uv run qte-backtest run --strategy MY_EDGE --symbol XAUUSD --timeframe M15 \
    --file data/parquet/tiingo/XAUUSD_M15.parquet --report --chart
```

The equity curve against buy-and-hold, the price window with every trade marked
on it, P&L by period, the returns distribution, streaks, run-ups and drawdowns,
MAE/MFE against realised R, the diagnostics and the full sortable trade list.

It fetches **nothing** when it opens — stylesheet, script and data are inlined,
so a report opens on a machine with no network. It takes the **JSON and nothing
else**, so a run from three months ago still draws without its history or its
strategy. And it **invents nothing**: statistics that need data the replay never
had — intrabar equity, margin, liquidation — are absent rather than
approximated.
