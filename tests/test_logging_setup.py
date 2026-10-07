"""The console is always on; a daily file is added only when a service asks.

What is pinned here is the part an operator depends on after the fact: that a
service's day ends up in a file of its own, that two services never share one,
and that nothing about the file can stop the service from running.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from qte_shared import logging_setup
from qte_shared.logging_setup import LOG_DIRECTORY_VARIABLE, configure_logging, get_logger


@pytest.fixture(autouse=True)
def isolated_logging(monkeypatch, tmp_path):
    """Give each test its own log directory and its own handler state."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    monkeypatch.setattr(logging_setup, "_console_handler", None)
    monkeypatch.setattr(logging_setup, "_file_handlers", {})
    monkeypatch.setenv(LOG_DIRECTORY_VARIABLE, str(tmp_path / "logs"))
    monkeypatch.setenv("QTE_LOG_LEVEL", "INFO")
    yield
    for handler in logging_setup._file_handlers.values():
        handler.close()
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


def written(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.glob("*.log"))


def stamped(name: str) -> str:
    return f"{logging_setup._date_stamp()}-{name}.log"


def test_a_logger_on_its_own_writes_no_file_and_makes_no_directory(tmp_path):
    # Every module does `log = get_logger(__name__)` at import time. A test run,
    # a backtest or qte-control must not leave dated files behind for that.
    get_logger("qte_shared.test").info("console only")

    assert not (tmp_path / "logs").exists()
    assert logging_setup._file_handlers == {}
    assert logging_setup._console_handler is not None


def test_a_named_service_gets_its_own_dated_file(tmp_path):
    configure_logging(service="strategy-runner")
    get_logger("qte_strategy_engine.runner").info("slot ready")

    directory = tmp_path / "logs"
    assert written(directory) == [stamped("strategy-runner")]
    assert "slot ready" in (directory / stamped("strategy-runner")).read_text(encoding="utf-8")


def test_two_services_write_beside_each_other_not_into_one_file(tmp_path):
    # Both mount the same host directory; appending to one file would tear
    # each other's lines.
    configure_logging(service="data-ingestion")
    configure_logging(service="strategy-runner")

    assert written(tmp_path / "logs") == [
        stamped("data-ingestion"),
        stamped("strategy-runner"),
    ]


def test_the_file_is_added_however_late_the_service_is_named(tmp_path):
    # `get_logger` at import time configures the console long before a `main()`
    # names the service, so the second call has to still be able to add it.
    get_logger("qte_ingestion.service")
    assert logging_setup._file_handlers == {}

    configure_logging(service="data-ingestion")

    assert written(tmp_path / "logs") == [stamped("data-ingestion")]
    # And the console handler survived the addition.
    assert logging_setup._console_handler in logging.getLogger().handlers


def test_naming_one_service_twice_opens_one_file_handler(tmp_path):
    configure_logging(service="data-ingestion")
    configure_logging(service="data-ingestion")

    handlers = [
        handler
        for handler in logging.getLogger().handlers
        if isinstance(handler, logging_setup.DailyFileHandler)
    ]
    assert len(handlers) == 1


def test_an_unwritable_directory_is_reported_and_otherwise_ignored(tmp_path, monkeypatch, caplog):
    # A missing mount or a read-only filesystem must not stop a service that is
    # ready to trade.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    monkeypatch.setenv(LOG_DIRECTORY_VARIABLE, str(blocker / "logs"))
    # Installing the console replaces the root handlers, caplog's own included,
    # so it is put back before the call under test.
    get_logger("qte_strategy_engine.runner")
    logging.getLogger().addHandler(caplog.handler)

    with caplog.at_level(logging.WARNING):
        configure_logging(service="strategy-runner")

    assert "Not writing strategy-runner logs to a file" in caplog.text
    assert logging_setup._file_handlers == {}
    # The console is still there, so nothing is lost but the file.
    assert logging_setup._console_handler is not None


def test_an_empty_directory_setting_writes_no_file(tmp_path, monkeypatch):
    monkeypatch.setenv(LOG_DIRECTORY_VARIABLE, "")

    configure_logging(service="strategy-runner")

    assert logging_setup._file_handlers == {}
    assert not (tmp_path / "logs").exists()


def test_midnight_rolls_to_the_next_days_file_without_a_restart(tmp_path, monkeypatch):
    # A process that started yesterday and has been quiet since writes its next
    # line into today's file.
    monkeypatch.setattr(logging_setup, "_date_stamp", lambda: "20261006")
    configure_logging(service="data-ingestion")
    logger = get_logger("qte_ingestion.service")
    logger.info("yesterday")

    monkeypatch.setattr(logging_setup, "_date_stamp", lambda: "20261007")
    logger.info("today")

    directory = tmp_path / "logs"
    assert written(directory) == [
        "20261006-data-ingestion.log",
        "20261007-data-ingestion.log",
    ]
    assert "yesterday" in (directory / "20261006-data-ingestion.log").read_text(encoding="utf-8")
    today = (directory / "20261007-data-ingestion.log").read_text(encoding="utf-8")
    assert "today" in today and "yesterday" not in today


def test_the_file_carries_the_same_lines_as_the_console(tmp_path):
    # One level for both sinks: the file is the console, kept.
    configure_logging(service="strategy-runner")
    get_logger("qte_strategy_engine.runner").debug("not at INFO")
    get_logger("qte_strategy_engine.runner").warning("delivery paused")

    body = (tmp_path / "logs" / stamped("strategy-runner")).read_text(encoding="utf-8")
    assert "delivery paused" in body
    assert "not at INFO" not in body
    assert "WARNING" in body and "[qte_strategy_engine.runner]" in body
