"""What ``__strategies__/`` holds on disk, for ``/strategies``.

The runner knows which strategies it *loaded*; ``/runner`` answers that. This
module answers the other question an operator asks after a deploy: which
build of each strategy repository is sitting in the mount, and what does it
publish. It reads the directory itself, with no import of the private code, so
it works while the runner is down and never executes anything a repository
ships.

Three sources, each owning one fact:

* the **directory** says which repositories are mounted — a child of
  ``__strategies__/`` carrying a manifest, or one the mount record lists;
* ``strategies.toml`` (``make strategy-mount``) says which strategies each one
  publishes, and whether each passed the audit;
* ``BUNDLE_VERSION.json`` at the **root of each repository** says which build it
  is. A deploy writes it next to ``manifest.py``; a checkout that was never
  built simply has none, and is listed all the same with the version left blank.

The template repositories the loader never trades
(:data:`~qte_shared.strategies.plugin_loader.SKIPPED_REPOSITORIES`) are left
out here by the same set, so the two never disagree about what "the strategies"
are.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qte_shared.logging_setup import get_logger
from qte_shared.strategies.plugin_loader import (
    EXCLUDED_DIRECTORIES,
    MANIFEST_FILENAMES,
    SKIPPED_REPOSITORIES,
    read_mount_manifest,
)

log = get_logger(__name__)

#: Written by the strategy repository's own build, at its root, beside the
#: manifest. The engine only reads it; the keys are listed in the README.
BUNDLE_VERSION_FILENAME = "BUNDLE_VERSION.json"


@dataclass(frozen=True, slots=True)
class MountedRepository:
    """One strategy repository as it sits in the mount."""

    name: str
    #: Strategy alias to whether it passed the audit when the repository was
    #: last mounted. Empty when the mount record has nothing for this folder.
    strategies: dict[str, bool] = field(default_factory=dict)
    #: Whether ``strategies.toml`` has a table for this folder at all, which is
    #: what tells "publishes nothing" from "never mounted".
    recorded: bool = False
    #: The parsed ``BUNDLE_VERSION.json``, or ``None`` when there is none or it
    #: could not be read — :attr:`bundle_problem` says which.
    bundle: dict[str, Any] | None = None
    #: Why a ``BUNDLE_VERSION.json`` that exists could not be used. ``None``
    #: when the file is absent, which is allowed and not a problem.
    bundle_problem: str | None = None


def read_bundle_version(repository: Path) -> tuple[dict[str, Any] | None, str | None]:
    """The stamp in *repository*, or why there is none to show.

    Returns ``(stamp, None)`` for a readable stamp, ``(None, None)`` when the
    file is simply absent, and ``(None, reason)`` when it is there but cannot be
    used. Never raises: a stamp is a label on a build, and a half-written one
    must not stop the operator from seeing which repositories are mounted.
    """
    path = repository / BUNDLE_VERSION_FILENAME
    if not path.is_file():
        return None, None
    try:
        # utf-8-sig, because a file written by a Windows tool may open with a
        # byte-order mark that plain utf-8 would hand to the JSON parser.
        document = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as failure:
        log.warning("Could not read %s: %s", path, failure)
        return None, str(failure)
    if not isinstance(document, dict):
        return None, "the file must hold one JSON object"
    return document, None


def scan_mounted_repositories(directory: Path | str) -> list[MountedRepository]:
    """Every strategy repository mounted in *directory*, templates excluded.

    A repository is a direct child folder that carries a manifest or that the
    mount record lists. Hidden folders, the scan's usual exclusions
    (``__pycache__``, ``build`` …) and :data:`SKIPPED_REPOSITORIES` are never
    reported. Sorted by name, so the order does not change between two runs.
    """
    root = Path(directory)
    try:
        children = sorted(root.iterdir(), key=lambda child: child.name.lower())
    except OSError as failure:
        log.warning("Could not list %s: %s", root, failure)
        return []

    record = read_mount_manifest(root)
    repositories: list[MountedRepository] = []
    for child in children:
        if (
            not child.is_dir()
            or child.name.startswith(".")
            or child.name in EXCLUDED_DIRECTORIES
            or child.name in SKIPPED_REPOSITORIES
        ):
            continue
        recorded = child.name in record.repositories
        if not recorded and not any((child / name).is_file() for name in MANIFEST_FILENAMES):
            continue
        bundle, problem = read_bundle_version(child)
        repositories.append(
            MountedRepository(
                name=child.name,
                strategies=dict(record.repositories.get(child.name, {})),
                recorded=recorded,
                bundle=bundle,
                bundle_problem=problem,
            )
        )
    return repositories
