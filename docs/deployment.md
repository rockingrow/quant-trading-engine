# Deployment

From a bare server to a running production stack, then from paper to live.
Read [state isolation](state-isolation.md) before changing an existing
deployment.

## From a clean clone

```bash
# 1. Public engine
git clone https://github.com/rockingrow/quant-trading-engine
cd quant-trading-engine && cp .env.example .env   # then edit it

# 2. Private alpha, into the ignored directory
git clone git@github.com:you/my-private-strategies.git __strategies__/my-strategies

# 3. Its dependencies, which the runner imports into its own process
make strategy-mount STRATEGY=my-strategies
make strategies                      # confirm the engine can see them

# 4. Say what to feed and what trades it, then check the whole thing
make tiingo                          # config/data_providers/tiingo.toml — symbols, timeframes, vendor knobs
make strategy-mapping                # config/strategies_mapping.toml, git-ignored — edit it
make audit-strict                    # fails on anything the runner would skip

# 5. `make start-prod` freezes plugin requirements into deploy/ first,
#    and a one-shot db-migrate container creates the schema before the services
#    start.
make start-prod
make logs
```

Whatever deploys a strategy folder to the server should copy its
`BUNDLE_VERSION.json` along with it, inside that folder — the file is how
`/strategies` tells you afterwards which build is there.

## Logs

`make logs` streams what the *current* containers have written, so a recreate
takes that history with it. Each long-running service therefore also writes its
own daily file on the host: `logs/<YYYYMMDD>-data-ingestion.log` and
`logs/<YYYYMMDD>-strategy-runner.log`, at `QTE_LOG_LEVEL`, mounted from
`./logs` and rolling at midnight without a restart. One file per service, never
a shared one — both containers append to the same mounted directory, and a
shared file would tear their lines. Nothing is pruned; `QTE_LOG_DIR` moves them
and an empty value turns them off. Only these services write files: a CLI
(`qte-control`, `qte-backtest`, the audit) prints to the console and leaves no
dated file behind.

## One image per service

Each service builds its **own image**: `QTE_EXTRAS` selects which
`[project.optional-dependencies]` get installed, so the ingestion container
carries the vendor's socket but no pyarrow (84 MB, backtest only), and the
runner carries the broker's HTTP client and neither of the other two. Every
image ships all six services' modules, which is 612 KB against a venv of a few
hundred; the dependencies are the whole difference. The backtest CLI is
deliberately in no container — replaying history is done on the host, not
inside the live trading process.

## Which NATS the signals go to

`docker-compose.yml` ships a `nats` service for standalone development. In
production you normally point `ALGO_BROKER__NATS_URL` at the **broker's** NATS,
because that is where the `SIGNALS` stream its workers consume actually lives —
publishing to a second cluster means nobody ever receives the signals.

## Going live

1. **Backtest** until the numbers hold up.
2. **Shadow mode** — select `QTE_STATE__MODE=shadow` (the default). Ingestion and
   strategies run, signals are built, logged and audited, and nothing reaches
   the broker.
3. **Reconcile** — read the `signals` table and check the entries and exits
   against the chart. Filtering by `signal_uxid` gives you one trade end to end.
4. **Select live state** — stop paper, select `QTE_ENV=prod` and
   `QTE_STATE__MODE=live`, and restart into the separate live volumes. Keep
   delivery paused while reconciling the actual broker account. Paper positions
   are never carried into the live book.
5. **Enable delivery** — run `make shadow-off` with the matching live identity
   and confirm the target. The pause flag persists within that namespace.

`make shadow-on` pauses live decisions and delivery while preserving broker
positions. It does not create simulated trades in the live book.
