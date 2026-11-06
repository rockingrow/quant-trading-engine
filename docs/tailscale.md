# Connecting to algo-trading-broker over Tailscale

Tailscale is optional and off by default, and it runs as a container — nothing
is installed on the host. One `.env` switch, `TAILSCALE_ENABLED`, the same one
`algo-trading-broker` reads, decides how the engine is reached and how it
reaches the broker. The Makefile reads it (`make start`, `up`, `dev`,
`start-prod`); a bare `docker compose up` ignores it.

| | `TAILSCALE_ENABLED=false` | `TAILSCALE_ENABLED=true` |
| --- | --- | --- |
| Redis / Postgres / NATS host ports | every interface (`0.0.0.0`) | `127.0.0.1` only |
| MT5 ingester → engine NATS | `nats://<this host>:<QTE_NATS_PORT>` + `QTE_NATS__TOKEN` | `nats://<TAILSCALE_HOSTNAME>:4222` + `QTE_NATS__TOKEN`, from inside the tailnet |
| Runner → broker NATS | `ALGO_CONTAINER_BROKER_NATS_URL` (default `nats://host.docker.internal:4222`) | `nats://<TAILSCALE_BROKER_HOSTNAME>:4222` over the tailnet |

**Turn it on whenever the broker has it on** — its NATS is then reachable over
the tailnet only.

1. **Mint an auth key** in the Tailscale admin console (Settings → Keys),
   tagged `tag:engine`. The tailnet policy that grants `tag:engine` the
   broker's port 4222, and `tag:ingester` this engine's, is
   `config/tailscale/policy.hujson` — paste it into the admin console's
   Access controls. `algo-trading-broker` keeps the same file and a tailnet
   has one policy, so keep the two copies identical. The key is used
   for the first login only; the node identity is then kept in the stack's
   `*-tailscale` volume. Each book (`QTE_ENV` / `QTE_STATE__MODE` / provider)
   is its own stack and its own node, so a reusable key saves minting one per
   book.
2. **Fill in `.env`:**

   ```bash
   TAILSCALE_ENABLED=true
   TAILSCALE_AUTHKEY=tskey-auth-...
   TAILSCALE_HOSTNAME=qte-engine                 # this engine's tailnet name
   TAILSCALE_BROKER_HOSTNAME=algo-trading-broker # the broker's TAILSCALE_HOSTNAME
   ALGO_BROKER__NATS_TOKEN=...                   # the broker's NATS_TOKEN
   ```

   The `QTE_*_PORT` variables stay bare port numbers — the bind address comes
   from the switch.
3. **Start the stack** (`make start` / `make start-prod`) and check the node:

   ```bash
   make tailscale-status   # tailnet peers (the broker should be listed), Serve forwards,
                           # or the login URL if the node has not logged in
   ```

How it works: the `tailscale` service (userspace networking, profile
`tailscale`) forwards its tailnet ports 4222/8222/5432 to `nats` and
`postgres-audit` through Tailscale Serve (`config/tailscale/serve.json`). For
the outbound direction it also runs Tailscale's SOCKS5 proxy on the compose
network, and `broker-nats-relay` — a socat container — turns that into a plain
TCP port, because a NATS client cannot speak SOCKS. The Makefile points the
runner's `ALGO_BROKER__NATS_URL` at `nats://broker-nats-relay:4222`; the token,
the NATS protocol and the runner's reconnect behaviour pass through unchanged.
Host-side `ALGO_BROKER__NATS_URL` in `.env` is not routed over the tailnet.

The same policy file also carries a `nodeAttrs` block granting `tag:broker`
the Funnel attribute. That is for the broker, not the engine: TradingView posts
its webhooks from the public internet, and a grant only ever matches a tagged
tailnet node, so Funnel is what publishes one port of the broker on a public
`https://<name>.<tailnet>.ts.net` address and forwards it to the broker's
webhook port. Funnel bypasses every grant above it, so the webhook handler's
shared secret is the only thing standing between the internet and an order.
Keep the block in the copy you paste into the admin console — a tailnet has one
policy.

If the tailnet connection drops, NATS becomes unreachable the same way it
would from any other network fault; `uv run qte-control ping` says so
explicitly instead of reporting success — see
[Operating a running engine](operations.md#is-everything-connected).
