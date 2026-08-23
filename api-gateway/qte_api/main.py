"""FastAPI application factory and uvicorn entry point."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from qte_shared.bus import NatsBus
from qte_shared.config import settings
from qte_shared.db import get_database
from qte_shared.logging_setup import configure_logging, get_logger

from qte_api import deps
from qte_api.routes import router

log = get_logger(__name__)

DESCRIPTION = """
Control plane for the Quant Trading Engine.

Read the audit trail, see which strategies are loaded, replay a backtest, and
flip shadow mode on or off. Nothing here sits between a strategy and the
broker — signals travel over NATS, not through this API.
"""


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    deps.bus = NatsBus(name="qte-api")
    try:
        await deps.bus.connect()
    except Exception as exc:
        # A control plane that refuses to start because NATS is down cannot
        # tell anyone that NATS is down. Come up degraded and say so on /health.
        log.error("NATS unavailable at startup: %s — /health will report degraded", exc)

    if settings.postgres.enabled:
        try:
            await get_database().create_all()
        except Exception as exc:
            log.error("Could not ensure the audit schema: %s", exc)

    log.info("Control plane ready env=%s port=%d", settings.env, settings.api.port)
    try:
        yield
    finally:
        if deps.bus is not None:
            await deps.bus.close()
            deps.bus = None
        await get_database().close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="QTE Control Plane",
        description=DESCRIPTION,
        version="0.1.0",
        lifespan=lifespan,
    )
    if settings.api.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.api.cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    app.include_router(router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run(
        "qte_api.main:app",
        host=settings.api.host,
        port=settings.api.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()
