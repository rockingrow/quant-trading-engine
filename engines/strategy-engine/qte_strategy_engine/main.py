"""Entry point for the ``strategy-runner`` container."""

from __future__ import annotations

import asyncio
import signal

from qte_shared.logging_setup import configure_logging, get_logger

from qte_strategy_engine.runner import StrategyRunner

log = get_logger(__name__)


async def main() -> None:
    configure_logging()
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
