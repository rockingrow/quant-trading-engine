"""``qte-strategy-audit`` — validate ``__strategies__/`` and exit non-zero if it is wrong.

    qte-strategy-audit                       # audit the configured directory
    qte-strategy-audit --format markdown     # for a pull request
    qte-strategy-audit --format json         # for anything that has to act on it
    qte-strategy-audit --strict              # warnings fail too

The exit code is the product. Everything else is there so a human can see what
the exit code meant.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

from qte_shared.config import settings
from qte_shared.logging_setup import configure_logging

from qte_strategy_audit.auditor import StrategyAuditor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qte-strategy-audit", description=__doc__)
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="Strategies directory; defaults to QTE_ENGINE__STRATEGIES_DIR",
    )
    parser.add_argument(
        "--routing",
        type=Path,
        default=None,
        help="Routing table; defaults to QTE_ENGINE__ROUTING_FILE",
    )
    parser.add_argument(
        "--no-routing",
        action="store_true",
        help="Audit the strategies only, without cross-checking any routing table",
    )
    parser.add_argument("--format", choices=["text", "markdown", "json"], default="text")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero on warnings as well as errors",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # The audit imports the plugins, and a plugin that logs on import should
    # not be the thing that decides this process's logging config.
    configure_logging()
    # A Markdown or JSON report carries whatever the strategy repo put in a
    # docstring or a name, and this runs on a Windows console as often as in a
    # container. Replace what the terminal cannot render rather than dying on
    # it: a mangled character still tells the operator what failed.
    with contextlib.suppress(AttributeError, ValueError):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    routing = None if args.no_routing else (args.routing or settings.engine.routing_file)
    report = StrategyAuditor(
        directory=args.dir or settings.engine.strategies_dir,
        routing_file=routing,
    ).run()

    if args.format == "json":
        print(json.dumps(report.as_dict(), indent=2))
    elif args.format == "markdown":
        print(report.to_markdown())
    else:
        print(report.to_text())

    return report.exit_code(strict=args.strict)


def run() -> None:
    sys.exit(main())


if __name__ == "__main__":
    run()
