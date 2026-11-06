# Operating a running engine

## Shadow mode: the delivery pause

There is no web service. The one control that genuinely has to reach a *running*
process is the delivery pause. Paper and live use separate state selected by
`QTE_STATE__MODE` at startup. The pause travels on scoped NATS subjects. See
[state isolation and deployment transitions](state-isolation.md) before
changing an existing deployment:

```bash
uv run qte-control shadow status   # is it paper or live right now?
uv run qte-control shadow on       # pause delivery to the broker
uv run qte-control shadow off      # resume a live namespace; prompts for confirmation
uv run qte-control ping            # which runners are up, and in what mode
```

The flag is written to Redis first and broadcast second, so a runner that starts
*after* the broadcast still comes up in the mode you last chose. If NATS is
unreachable the command says so explicitly rather than reporting success.

## Is everything connected?

`make status` prints the stack's containers (and
the tailnet peers, with Tailscale on), then asks every link the engine depends
on and exits non-zero if one is down:

```text
State namespace: prod:live:mt5
  [ OK ] Redis            redis-cache:6379, runner claim held by 7c1e…
  [ OK ] Postgres         postgres-audit:5432/qte_audit
  [ OK ] NATS (QTE)       nats:4222
  [ OK ] strategy-runner  4 slot(s), delivering LIVE, namespace prod:live:mt5
  [ OK ] data-ingestion   providers mt5, 6 series, newest bar 2026-10-12T10:00:00+00:00
  [FAIL] Ingester (mt5)   no NATS client is subscribed under INGESTER_RPC.history.mt5 — …
  [ OK ] Broker           broker-nats-relay:4222: SIGNALS stream holds 0 undelivered signal(s); broker_signal_handler is pulling
```

The runner and ingestion answer a `ping` on their control subjects. The MT5
ingester and the broker are other machines with no ping in their contracts, so
neither is sent anything: the ingester is read off the NATS server's client
list (`/connz` — is anyone subscribed to the history request subject), and the
broker off its `SIGNALS` stream — a stream nothing is pulling from is a broker
that is down. Every probe is read-only. It runs inside a service container
(`qte-control status`), because that is where the stack's real namespace and,
with Tailscale on, the relay to the broker both live.

## Coming back from an outage

Open positions survive a restart by design.
Every transition is written to Redis *and* to the Postgres `open_positions`
table, so the runner reloads the cycle on start — from the cache, or from the
table when the cache came up empty — and the strategy carries on deciding on
it. Redis runs with AOF on its own volume and the cycle record has no TTL, so a
container restart, a lost cache or a re-provisioned Redis all keep the book.

Age does not close anything unless you ask it to:
`QTE_RUNNER__FLUSH_STALE_POSITIONS` is `off` by default, because a strategy
that holds a cycle until its own exit condition is met can be right to carry
one for days. Set it to `warn` to see what a `close` run would flatten, or
`close` with `QTE_RUNNER__STALE_POSITION_MAX_AGE` for the old age-gated
behaviour.

What *is* closed on start is the cycle your mapping table no longer claims —
a strategy dropped from a symbol's `strategies` list, or a symbol carrying
`enabled = false`. Nothing decides on those: no slot holds them, no bar reaches
them, and their size is still charged against the account on every later
sizing. `QTE_RUNNER__CLOSE_ORPHAN_POSITIONS` (default `close`) sends a `FLAT`
for each, recovering the timeframe the payload needs from the `signals` row
that opened the cycle. A pair the table *still* claims is reported and never
closed, however long its slot has been missing — that is a strategy that failed
to load or a repository that is not mounted, and a half-mounted
`__strategies__/` must not flatten the book. A refused delivery leaves the row
in place, so a `FLAT` the broker rejects over a closed weekend is retried at
the next start. Why these two are separate mechanisms:
[architecture notes](architecture.md).

## When NATS is down

Closed candles are staged in Redis before they are published, and a staged
close is removed only once the NATS server has confirmed it — a publish nats-py
merely buffered while reconnecting does not count. While ingestion has no
connection the closes stay staged (one `WARNING` per outage) and go out, oldest
first, when it returns. A staged entry that can never be published — it does
not decode, or it belongs to another state namespace — is moved to the
`outbox:candles:dead` list with an `ERROR` instead of holding back every close
behind it; the list keeps the newest 1,000. `QTE_INGESTION__PUBLISH_CONFIRM_TIMEOUT`
(default `5` seconds) is how long a publish may wait for that confirmation.

## Where to look

Everything else the engine knows is a CLI command or a SQL query:

| Want | Do |
| --- | --- |
| Signal audit trail | `SELECT * FROM signals ORDER BY created_at DESC LIMIT 20` |
| One trade cycle end to end | `SELECT * FROM signals WHERE signal_uxid = '…' ORDER BY created_at` |
| What is open right now | `SELECT strategy, symbol, signal_uxid, remaining, updated_at FROM open_positions` |
| What strategies are loaded | `make strategies` |
| Look at a report | `make chart REPORT=data/reports/….json`, then open the HTML |
| Rehearse the live path | `make sim`, then `qte-simulator replay …` (dev only) |

---

## Database

Alembic owns the schema. There is no init script — one would only ever run on an
empty volume, which is exactly the case a migration tool exists to outgrow.

```bash
make db-upgrade                     # apply everything pending
make db-current                     # where is this database?
make db-revision M="add a column"   # autogenerate from model changes
make db-check                       # fail if the models have drifted
make db-downgrade                   # back out one revision
```

**Each engine owns the tables it writes**, with its models and repositories in
its own `db/` package:

| Package | Tables |
| --- | --- |
| `qte_strategy_engine.db` | `signals` |
| `qte_backtest.db` | `backtest_runs`, `backtest_trades` |
| `qte_shared.db` | `engine_events` |

They all share one `DeclarativeBase`, because Alembic diffs the database against
`Base.metadata` and a model on a different base would be invisible to
autogenerate. For the same reason, adding an engine that owns tables means
adding its `models` import to `migrations/env.py`; `tests/test_db_layout.py`
enforces both. The schema needs no PostgreSQL extensions.
