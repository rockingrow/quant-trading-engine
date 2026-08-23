"""Structured-ish stdlib logging, configured once per process."""

from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False
_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"


def configure_logging(level: str | None = None) -> None:
    """Idempotently install a single stderr handler on the root logger."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    resolved = (level or os.getenv("QTE_LOG_LEVEL") or "INFO").upper()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(resolved)
    logging.getLogger("asyncio").setLevel("WARNING")
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
