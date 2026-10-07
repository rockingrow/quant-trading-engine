"""Structured-ish stdlib logging: a console handler always, a daily file per service.

Two sinks, for two different readers:

* **The console**, on stderr, configured by the first :func:`get_logger` call in
  any process. It is what ``docker compose logs`` shows, and what a CLI prints.
* **A daily file**, ``<QTE_LOG_DIR>/<YYYYMMDD>-<service>.log``, added only when a
  service asks for it by name (``configure_logging(service=SERVICE_NAME)`` in
  its entry point). It holds the same lines at the same level, so a day can be
  read back after the container has been recreated and its console scrollback
  is gone — which is the whole point: ``docker logs`` keeps only what the
  current container has written.

**Per service, not per process-tree.** The file name carries the service, so
``data-ingestion`` and ``strategy-runner`` write beside each other rather than
interleaving into one file. That matters here because both mount the same
directory from the host: two processes appending to one file would tear each
other's lines.

**Asked for, never assumed.** Only an entry point names a service, so importing
anything from this package — a test, a backtest, ``qte-control`` — writes no
file and creates no directory. Call order does not matter either: the file
handler is added on whichever call first names a service, long after
``get_logger`` has already set the console up at import time.

**A log file is never worth a service.** An unwritable directory (no mount, a
read-only filesystem) is reported once on the console and otherwise ignored.
Set ``QTE_LOG_DIR`` to an empty value to stop trying at all.

Nothing here deletes anything: files accumulate one per service per day, the
same as ``algo-trading-ingester``'s ``logs/``. Rotation at midnight happens
without a restart, on the clock the timestamps in the lines use — the images
run UTC, so in practice the stamp is a UTC date.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"

#: Directory the daily files go in. Empty disables them.
LOG_DIRECTORY_VARIABLE = "QTE_LOG_DIR"
DEFAULT_LOG_DIRECTORY = "logs"

#: The one console handler, and one file handler per service that asked.
_console_handler: logging.Handler | None = None
_file_handlers: dict[str, logging.Handler] = {}


def _date_stamp() -> str:
    return datetime.now().strftime("%Y%m%d")


class DailyFileHandler(logging.FileHandler):
    """Appends to ``<directory>/<date>-<service>.log``, rolling at midnight.

    The roll is checked per record rather than scheduled, so a process that
    started yesterday and has been quiet since writes its next line into
    today's file without being restarted.
    """

    def __init__(self, directory: Path, service: str, encoding: str = "utf-8") -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.service = service
        self.current_date = _date_stamp()
        super().__init__(self.path_for(self.current_date), mode="a", encoding=encoding)

    def path_for(self, date_stamp: str) -> Path:
        return self.directory / f"{date_stamp}-{self.service}.log"

    def emit(self, record: logging.LogRecord) -> None:
        today = _date_stamp()
        if today != self.current_date:
            self.close()
            self.current_date = today
            self.baseFilename = str(self.path_for(today))
            self.stream = self._open()
        super().emit(record)


def configure_logging(level: str | None = None, service: str | None = None) -> None:
    """Install the console handler once; add *service*'s daily file when named.

    Idempotent in both halves: the console is set up by the first call, and each
    service's file by the first call that names it.
    """
    resolved = (level or os.getenv("QTE_LOG_LEVEL") or "INFO").upper()
    _configure_console(resolved)
    if service:
        _configure_file(service)


def _configure_console(level: str) -> None:
    global _console_handler
    if _console_handler is not None:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT))
    root = logging.getLogger()
    # Replaces whatever a library installed, but keeps the file handlers this
    # module owns — the console is not always configured first.
    root.handlers[:] = [handler, *_file_handlers.values()]
    root.setLevel(level)
    logging.getLogger("asyncio").setLevel("WARNING")
    _console_handler = handler


def _configure_file(service: str) -> None:
    """Add *service*'s daily file to the root logger. Never raises."""
    if service in _file_handlers:
        return
    configured = os.getenv(LOG_DIRECTORY_VARIABLE)
    directory = (DEFAULT_LOG_DIRECTORY if configured is None else configured).strip()
    if not directory:
        return
    log = logging.getLogger(__name__)
    try:
        handler = DailyFileHandler(Path(directory), service)
    except OSError as failure:
        # A service that is otherwise ready to trade must not be stopped by a
        # missing mount, and the console still carries every line.
        log.warning(
            "Not writing %s logs to a file in %r: %s. The console is unaffected; point "
            "%s somewhere writable, or set it empty to stop trying.",
            service,
            directory,
            failure,
            LOG_DIRECTORY_VARIABLE,
        )
        return
    handler.setFormatter(logging.Formatter(_FORMAT))
    logging.getLogger().addHandler(handler)
    _file_handlers[service] = handler
    log.info(
        "Logging %s to %s (one file per day, nothing is pruned)", service, handler.baseFilename
    )


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
