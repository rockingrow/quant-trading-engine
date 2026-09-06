# One Dockerfile, one image per service.
#
# QTE_EXTRAS selects which optional dependency sets get installed, so the
# ingestion image does not carry pyarrow (84 MB, backtest only) and the runner
# carries no socket to a market data vendor. docker-compose.yml passes the set
# each service needs; the default builds the strategy runner.
#
# Every image ships every service's modules — 612 KB against a venv two orders
# of magnitude larger — so the extras are what the boundary is actually made
# of. See `[project.optional-dependencies]` in pyproject.toml.

FROM python:3.13-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Which optional dependency sets this image installs, space-separated. Empty
# means core only, which is what the auditor and the migrations image need.
ARG QTE_EXTRAS="broker"

# Manifest first: the dependency layer is then cached across every change that
# does not touch pyproject.toml or the lockfile. One manifest describes every
# service now, so there is nothing else to copy ahead of the source -- except
# README.md, which the root manifest names as its `readme` and which hatchling
# therefore refuses to build the wheel without.
COPY pyproject.toml uv.lock README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev \
        $(for extra in ${QTE_EXTRAS}; do echo --extra $extra; done)

COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev \
        $(for extra in ${QTE_EXTRAS}; do echo --extra $extra; done)

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
