# State isolation and deployment transitions

Every service selects an immutable state identity from `QTE_ENV`,
`QTE_STATE__MODE` and `QTE_MARKET_DATA__PROVIDER`. For example,
`prod:shadow:tiingo` and `prod:live:tiingo` hold separate books.
`QTE_STATE__MODE` defaults to `shadow`; disabling a broker shadow flag cannot
change this identity. Restart all services to change any identity component.

| Mode | Environment | Delivery | Runtime shadow flag |
| --- | --- | --- | --- |
| `dev` | `dev` only | Paper | Always paper |
| `shadow` | `dev`, `staging`, `prod` | Paper | Always paper |
| `live` | `staging`, `prod` | Broker, when enabled | Pauses or resumes delivery |

Synthetic providers, including `simulator`, require `dev` mode. Live starts
paused by default because `QTE_BROKER__SHADOW_MODE=true`. In live state,
`shadow on` stops strategy decisions and new orders while retaining broker
positions. It creates no simulated entries or exits. Confirmed broker outcomes
can still finish their local persistence while delivery is paused. In paper
state, `shadow off` is rejected. `QTE_BROKER__FORCE_SHADOW_MODE=true` always
prevents broker delivery.

## Storage and routing

| Resource | Example for production shadow Tiingo |
| --- | --- |
| Redis prefix | `qte:prod:shadow:tiingo` |
| Internal NATS prefix | `QTE.prod.shadow.tiingo` |
| Database `namespace` | `prod:shadow:tiingo` |
| Compose project | `qte-prod-shadow-tiingo` |
| Redis volume | `qte-prod-shadow-tiingo-redis` |
| Postgres volume | `qte-prod-shadow-tiingo-postgres` |
| NATS volume | `qte-prod-shadow-tiingo-nats` |

Custom Redis and NATS base prefixes still receive the identity suffix. All
controls, runner ownership, watermarks, ticks, candles and outboxes use it.
Broker subjects remain `SIGNALS.<strategy>` because they belong to the broker.
Separate internal subjects do not isolate broker accounts; configure and
reconcile the actual target account before enabling live delivery.

Postgres repositories scope every read, update and delete. Signals, open
positions, lifecycle events and backtest runs carry `namespace`; backtest
trades inherit their run's identity through the foreign key. Open positions
are unique by namespace, strategy and symbol, and their JSON state repeats the
namespace. Recovery refuses missing or mismatched position/outbox provenance.
This also isolates repositories using the same external database or Redis.
Namespaces are application boundaries, not database permissions or tenant ACLs.

Ingestion stamps both ticks and candles with `origin.namespace`,
`origin.provider` and `origin.synthetic` before caching or publishing. A
record with foreign provenance cannot be relabelled. Startup removes foreign
market caches, and the runner ignores unattributed or foreign market events
and history. Raw provider records and offline candles may omit origin until
they enter this producer boundary. Origin is diagnostic provenance, not a
cryptographic proof; restrict access to Redis and the internal NATS bus.

## Selecting a deployment

Set these values in the deployment's `.env` before starting:

```dotenv
QTE_ENV=prod
QTE_STATE__MODE=shadow
QTE_MARKET_DATA__PROVIDER=tiingo
QTE_BROKER__SHADOW_MODE=true
```

`make start-prod` applies the production overlay and selects production
volumes. For the simulator, use `QTE_ENV=dev`, `QTE_STATE__MODE=dev` and
`QTE_MARKET_DATA__PROVIDER=simulator`, then `make dev`. The dev overlay forces
both environment and mode to `dev`, including for its volume names. The
production overlay forces environment to `prod` and retains the selected
mode; selecting `dev` there fails startup validation.

Host tools such as `make ping` and `make shadow-status` must use the same
environment, mode, provider and published ports as the intended deployment.
Use the same Compose files for subsequent logs, stop and restart commands;
the selected project name changes when identity changes. Running deployments
concurrently also requires distinct host ports (`QTE_REDIS_PORT`,
`QTE_POSTGRES_PORT`, `QTE_NATS_PORT`, `QTE_NATS_MONITOR_PORT`) and matching
host-side service URLs. Shared provider history under `data/parquet/<provider>`
is a market-data cache, not a position book.

## Upgrading an existing deployment

1. Stop the old runner cleanly. Record its identity, positions and unfinished
   deliveries, and reconcile them against the broker before changing a live
   provider, mode or account. A fresh namespace does not prove the broker is flat.
2. Preserve the old volumes and configuration. The old `qte_*` volumes are
   deliberately not reused by the new Compose deployment. Do not rename or
   attach paper volumes to a live deployment.
3. Start the intended identity. Compose applies Alembic before applications
   start. When upgrading a shared external database, migration
   `b86f190e2c34` labels existing rows `legacy`; it never automatically imports
   them into a live namespace. Review those rows explicitly. There is no
   automatic legacy or cross-provider position migration.
4. Verify `make ping`, `make shadow-status`, scoped audit records, feed origin
   and warm-up. To move from paper to live, stop paper first, select
   `QTE_ENV=prod` and `QTE_STATE__MODE=live`, and restart with the live volumes.
   Keep delivery paused until actual broker positions and pending deliveries
   have been reconciled. Only then use `make shadow-off` for that live identity.

Returning to a prior identity resumes its retained state. It does not reset
that book. Never start the old and replacement live runner against the same
broker account simultaneously; runner ownership is per namespace. Migrating
an existing real book requires an operator-reviewed reconciliation, including
its cycle IDs, sizes, delivery IDs and JSON namespace. Copying paper state or
blindly updating the namespace column is not a valid migration.
