"""Entry point for the ``strategy-runner`` container."""

from __future__ import annotations

import asyncio
import logging
import signal

from qte_shared.logging_setup import configure_logging, get_logger
from qte_strategy_engine.runner import SERVICE_NAME, StrategyRunner
from qte_strategy_engine.settings import runner_settings

log = get_logger(__name__)


async def main() -> None:
    # Named, so this run also lands in QTE_LOG_DIR/<date>-strategy-runner.log:
    # `docker logs` keeps only what the current container wrote, and a decision
    # has to be readable after the container has been recreated.
    configure_logging(service=SERVICE_NAME)
    logging.getLogger("numba").setLevel(runner_settings.numba_log_level.upper())
    runner = StrategyRunner()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signal_name, runner.request_stop)
    await runner.run_forever()


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    run()
