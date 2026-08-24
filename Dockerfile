# One Dockerfile, one image per service.
#
# QTE_PACKAGE selects which workspace member gets installed, so the ingestion
# image does not carry pyarrow (152 MB, backtest only) and neither carries the
# other's dependencies. docker-compose.yml passes it per service; the default
# builds the strategy runner.
#
# The workspace layout is what makes this possible: each engine declares only
# what it needs, so `uv sync --package` has a real boundary to cut along.

FROM python:3.13-slim AS base

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
COPY engines/strategy_audit/pyproject.toml engines/strategy_audit/
COPY engines/market_simulator/pyproject.toml engines/market_simulator/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-workspace --no-dev --package ${QTE_PACKAGE}

COPY engines/ engines/
COPY migrations/ migrations/
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package ${QTE_PACKAGE}

# Strategies bring their own dependencies (pandas-ta and whatever else the
# private repo needs), and they are imported into *this* process — so they have
# to be installed here even though the code itself is a mounted volume.
# __strategies__/ is not in the build context (see .dockerignore), so the
# operator freezes them out of the plugin repo's own lockfile first:
#
#     make strategy-requirements   # writes deploy/strategy-requirements.txt
#
# Absent that file the image still builds; the runner then fails on the first
# import of a strategy that needs something it does not have.
COPY deploy/ deploy/
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -f deploy/strategy-requirements.txt ]; then \
        uv pip install --python /opt/venv/bin/python -r deploy/strategy-requirements.txt; \
    fi

# __strategies__/ is never baked in. It is a mounted volume, so the private
# repo can be updated (or pulled) without rebuilding the public engine.
RUN mkdir -p /app/__strategies__ /app/config /app/data/parquet /app/data/reports \
    && useradd --create-home --uid 10001 qte \
    && chown -R qte:qte /app
USER qte

CMD ["qte-strategy-runner"]
