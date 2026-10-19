"""``qte-strategy-mount`` — record what a mounted repository publishes, and how it fared.

    qte-strategy-mount --record my-strategies    # audit that repo, write its verdicts
    qte-strategy-mount --passing                 # repos with a strategy still worth freezing
    qte-strategy-mount --show                    # the whole record, for a human

``make strategy-mount`` installs a repository's dependencies and then calls
``--record`` on it. The audit runs *after* the install because the commonest
failure is a missing third-party import, which is exactly what installing
fixes. What comes back is one verdict per published strategy, and those go into
``__strategies__/strategies.toml`` — see
:mod:`qte_shared.strategies.mount_manifest` for the file and who reads it.

One repository at a time, replacing only its own table, because
``make strategy-mount STRATEGY=<name>`` has to refresh that repo without
re-auditing — or discarding — everything mounted beside it.
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path

from qte_shared.config import settings
from qte_shared.logging_setup import configure_logging, get_logger
from qte_shared.strategies.mount_manifest import MountManifest
from qte_strategy_audit.auditor import StrategyAuditor

log = get_logger(__name__)


def record_repository(directory: Path, repository: str) -> MountManifest:
    """Audit one mounted repository and write its verdicts into the manifest.

    The audit is run against the repository root rather than the whole
    strategies directory: a sibling repo that will not import is not this
    repo's problem, and mounting one must not be able to flip another's
    verdict. The mapping table is left out for the same reason — a strategy
    that no symbol maps to yet is unmounted, not unfit.
    """
    root = directory / repository
    if not root.is_dir():
        raise FileNotFoundError(f"No mounted strategy repository at {root}")

    report = StrategyAuditor(directory=root, mapping_file=None).run()
    verdicts = {entry.name: entry.ok for entry in report.strategies}

    manifest = _existing_or_fresh(directory).with_repository(repository, verdicts)
    path = manifest.write(directory)

    passed = sorted(name for name, ok in verdicts.items() if ok)
    failed = sorted(name for name, ok in verdicts.items() if not ok)
    log.info(
        "Recorded %s in %s: %s passing (%s), %s failing (%s)",
        repository,
        path,
        len(passed),
        ", ".join(passed) or "none",
        len(failed),
        ", ".join(failed) or "none",
    )
    return manifest


def _existing_or_fresh(directory: Path) -> MountManifest:
    """The manifest on disk, or an empty one when it cannot be built on.

    Recording is the step that *regenerates* this file, so a manifest it cannot
    parse — one in the flat, repo-level shape this format replaced, most of all
    — must not be able to block its own replacement. Sweeping the whole
    directory rebuilds every entry; refreshing one repo drops the rest, which
    is what the warning says.
    """
    try:
        return MountManifest.load(directory)
    except (OSError, ValueError) as error:
        log.warning(
            "Starting the mount manifest over (%s). Run `make strategy-mount` with no "
            "STRATEGY to record every mounted repository again.",
            error,
        )
        return MountManifest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qte-strategy-mount", description=__doc__)
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="Strategies directory; defaults to QTE_ENGINE__STRATEGIES_DIR",
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--record",
        metavar="REPOSITORY",
        help="Audit that mounted repository and write its per-strategy verdicts",
    )
    action.add_argument(
        "--passing",
        action="store_true",
        help="Print the repositories with at least one passing strategy, one per line",
    )
    action.add_argument(
        "--show",
        action="store_true",
        help="Print what the manifest currently records",
    )
    action.add_argument(
        "--ensure",
        action="store_true",
        help="Write the manifest if it does not exist, so a directory with no repos has one",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Recording imports the plugins, and a plugin that logs on import should
    # not be the thing that decides this process's logging config.
    configure_logging()
    with contextlib.suppress(AttributeError, ValueError):
        # ``newline`` as well as the encoding: --passing is read by a shell loop
        # in the Makefile, and on Windows the CRLF that print() would otherwise
        # emit becomes part of each repository name.
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", newline="\n")

    directory = Path(args.dir or settings.engine.strategies_dir)

    # The two writing actions repair what they find; the two reading ones stop
    # on a manifest they cannot parse. The deploy step must not freeze half a
    # book because a file lost a bracket, and regeneration must not be blocked
    # by the very file it regenerates.
    if args.ensure:
        # `make up` refuses to start without this file, so a directory holding
        # no repository at all still has to end the mount step with one. It
        # says "nothing is mounted", which is a different claim from its
        # absence: that one says "nobody has looked".
        path = _existing_or_fresh(directory).write(directory)
        print(f"Mount manifest ready - {path}")
        return 0

    try:
        if args.record:
            record_repository(directory, args.record)
            return 0
        manifest = MountManifest.load(directory)
    except (FileNotFoundError, OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1

    if args.passing:
        # One per line and nothing else: `make strategy-requirements` reads this.
        for repository in manifest.passing_repositories:
            print(repository)
        return 0

    print(_describe(manifest, directory))
    return 0


def _describe(manifest: MountManifest, directory: Path) -> str:
    """The record as a human reads it, deliberately ASCII for a Windows console."""
    lines = [f"Mount manifest - {MountManifest.path_in(directory)}"]
    if not manifest.repositories:
        lines.append("")
        lines.append("  Nothing mounted. Run `make strategy-mount`.")
        return "\n".join(lines)

    for repository in sorted(manifest.repositories):
        published = manifest.repositories[repository]
        lines.append("")
        lines.append(f"  {repository}")
        if not published:
            lines.append("         publishes no strategy")
            continue
        for alias in sorted(published):
            status = "ok" if published[alias] else "FAIL"
            lines.append(f"    [{status:>4}] {alias}")

    counts = manifest.verdicts
    lines.append("")
    lines.append(
        f"{len(counts)} strategies in {len(manifest.repositories)} repo(s), "
        f"{sum(counts.values())} passing audit"
    )
    return "\n".join(lines)


def run() -> None:
    sys.exit(main())


if __name__ == "__main__":
    run()
