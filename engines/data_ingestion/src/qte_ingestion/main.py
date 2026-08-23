"""Entry point for the ``data-ingestion`` container."""

from __future__ import annotations

import asyncio
import signal

from qte_shared.logging_setup import configure_logging, get_logger

from qte_ingestion.service import IngestionService

log = get_logger(__name__)


async def main() -> None:
    configure_logging()
    service = IngestionService()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGTERM, signal.SIGINT):
        # Docker stops a container with SIGTERM; without this the process dies
        # mid-bar and the sockets never drain.
        loop.add_signal_handler(signal_name, service.request_stop)
    await service.run_forever()


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    run()
