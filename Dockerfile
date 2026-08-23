# One image, three entry points. The services differ only in which console
# script the container runs, so building them separately would mean three
# copies of the same dependency tree in the registry for no benefit.

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Manifests first: the dependency layer is then cached across every change that
# does not touch a pyproject or the lockfile.
COPY pyproject.toml uv.lock ./
COPY shared/pyproject.toml shared/
COPY data-ingestion/pyproject.toml data-ingestion/
COPY backtest-engine/pyproject.toml backtest-engine/
COPY strategy-engine/pyproject.toml strategy-engine/
COPY api-gateway/pyproject.toml api-gateway/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-workspace --no-dev

COPY shared/ shared/
COPY data-ingestion/ data-ingestion/
COPY backtest-engine/ backtest-engine/
COPY strategy-engine/ strategy-engine/
COPY api-gateway/ api-gateway/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# user_strategies/ is never baked in. It is a mounted volume, so the private
# repo can be updated (or pulled) without rebuilding the public engine.
RUN mkdir -p /app/user_strategies /app/data/parquet \
    && useradd --create-home --uid 10001 qte \
    && chown -R qte:qte /app
USER qte

CMD ["qte-api"]
