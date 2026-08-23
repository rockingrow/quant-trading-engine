# One Dockerfile, one image per service.
#
# QTE_PACKAGE selects which workspace member gets installed, so the ingestion
# image does not carry pyarrow (152 MB, backtest only) and neither carries the
# other's dependencies. docker-compose.yml passes it per service; the default
# builds the strategy runner.
#
# The workspace layout is what makes this possible: each engine declares only
# what it needs, so `uv sync --package` has a real boundary to cut along.

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Which workspace member this image is for.
ARG QTE_PACKAGE=qte-strategy-engine

# Manifests first: the dependency layer is then cached across every change that
# does not touch a pyproject or the lockfile. Every member's manifest is copied
# even though only one is installed — the lockfile describes the whole
# workspace, and uv reads all of them to resolve against it.
COPY pyproject.toml uv.lock ./
COPY engines/shared/pyproject.toml engines/shared/
COPY engines/data_ingestion/pyproject.toml engines/data_ingestion/
COPY engines/backtest_engine/pyproject.toml engines/backtest_engine/
COPY engines/strategy_engine/pyproject.toml engines/strategy_engine/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-workspace --no-dev --package ${QTE_PACKAGE}

COPY engines/ engines/
COPY migrations/ migrations/
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package ${QTE_PACKAGE}

# __strategies__/ is never baked in. It is a mounted volume, so the private
# repo can be updated (or pulled) without rebuilding the public engine.
RUN mkdir -p /app/__strategies__ /app/data/parquet /app/data/reports \
    && useradd --create-home --uid 10001 qte \
    && chown -R qte:qte /app
USER qte

CMD ["qte-strategy-runner"]
