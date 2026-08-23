-- QTE audit schema. Runs once, on an empty data volume, via the postgres
-- image's docker-entrypoint-initdb.d hook.
--
-- This file is the DDL of record. The SQLAlchemy models in
-- shared/qte_shared/db/models.py mirror it for reads and writes, with one
-- deliberate exception: signals.embedding is created here and left unmapped,
-- because nothing in QTE writes it. It exists so an AI agent can embed a
-- signal's context later and ask "which past trades looked like this one"
-- without a schema migration standing in the way.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS signals (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    signal_uxid     VARCHAR(32) NOT NULL,
    strategy        VARCHAR(128) NOT NULL,
    symbol          VARCHAR(64) NOT NULL,
    timeframe       VARCHAR(16) NOT NULL,
    action          VARCHAR(16) NOT NULL,
    signal_time     TIMESTAMPTZ NOT NULL,
    price           DOUBLE PRECISION,
    quantity        DOUBLE PRECISION,
    sl              DOUBLE PRECISION,
    tp1             DOUBLE PRECISION,
    tp2             DOUBLE PRECISION,
    payload         JSONB NOT NULL,
    indicators      JSONB NOT NULL DEFAULT '{}'::jsonb,
    inputs          JSONB NOT NULL DEFAULT '{}'::jsonb,
    transport       VARCHAR(16) NOT NULL DEFAULT 'nats',
    delivery_status VARCHAR(16) NOT NULL DEFAULT 'shadow',
    delivery_error  TEXT,
    shadow          BOOLEAN NOT NULL DEFAULT TRUE,
    -- 1536 dims matches the common text-embedding size; change it before you
    -- store anything, not after.
    embedding       vector(1536)
);

CREATE INDEX IF NOT EXISTS ix_signals_strategy_created ON signals (strategy, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_signals_symbol_created   ON signals (symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_signals_uxid             ON signals (signal_uxid);
-- GIN over the raw envelope: "every signal whose indicators said in_session"
-- stays a single index scan as the table grows.
CREATE INDEX IF NOT EXISTS ix_signals_payload_gin      ON signals USING GIN (payload jsonb_path_ops);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    strategy     VARCHAR(128) NOT NULL,
    symbol       VARCHAR(64) NOT NULL,
    timeframe    VARCHAR(16) NOT NULL,
    period_start TIMESTAMPTZ,
    period_end   TIMESTAMPTZ,
    params       JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics      JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    id          SERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    signal_uxid VARCHAR(32),
    symbol      VARCHAR(64) NOT NULL,
    direction   VARCHAR(8) NOT NULL,
    opened_at   TIMESTAMPTZ NOT NULL,
    closed_at   TIMESTAMPTZ,
    entry_price DOUBLE PRECISION NOT NULL,
    exit_price  DOUBLE PRECISION,
    quantity    DOUBLE PRECISION NOT NULL,
    sl          DOUBLE PRECISION,
    tp1         DOUBLE PRECISION,
    tp2         DOUBLE PRECISION,
    exit_reason VARCHAR(16),
    gross_pnl   DOUBLE PRECISION NOT NULL DEFAULT 0,
    fees        DOUBLE PRECISION NOT NULL DEFAULT 0,
    net_pnl     DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_backtest_trades_run ON backtest_trades (run_id);

CREATE TABLE IF NOT EXISTS engine_events (
    id         SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    service    VARCHAR(64) NOT NULL,
    level      VARCHAR(16) NOT NULL DEFAULT 'INFO',
    event      VARCHAR(128) NOT NULL,
    payload    JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_engine_events_service_created
    ON engine_events (service, created_at DESC);
