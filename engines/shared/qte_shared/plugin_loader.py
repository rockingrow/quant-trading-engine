"""Dynamic discovery of the private strategies in ``__strategies__/``.

This is the plugin seam. The engine is public; the alpha is not. The loader
imports whatever ``StrategyBase`` subclasses it finds in a directory that is
git-ignored here and cloned from a private repo at deploy time, so the two can
be versioned, reviewed and released completely independently.

Import is by file path rather than package name on purpose: the directory is a
mounted volume, not an installed distribution, and requiring it to be pip-
installable would drag the private repo into the public build.
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qte_shared.logging_setup import get_logger
from qte_shared.strategy_base import StrategyBase

log = get_logger(__name__)

#: Directory names never scanned for strategies. The loader points at a cloned
#: repository, which brings its own tests, docs and possibly a virtualenv along
#: with the code that matters. Hidden directories (``.git``, ``.venv``, …) are
#: excluded separately, by the leading dot.
EXCLUDED_DIRECTORIES = frozenset(
    {
        "__pycache__",
        "build",
        "dist",
        "docs",
        "env",
        "examples",
        "node_modules",
        "scripts",
        "site-packages",
        "test",
        "tests",
        "venv",
    }
)


@dataclass(slots=True)
class LoadedStrategy:
    """One discovered strategy class and where it came from."""

    name: str
    cls: type[StrategyBase]
    source: Path

    def instantiate(self, params: dict[str, Any] | None = None) -> StrategyBase:
        return self.cls(params or {})


class StrategyLoader:
    """Scans a directory for strategy classes and instantiates them."""

    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)

    def discover(self) -> list[LoadedStrategy]:
        """Import every module in the directory and collect strategy classes.

        A module that fails to import is logged and skipped rather than taking
        the process down: one broken strategy file should not stop the other
        four from trading.
        """
        if not self.directory.is_dir():
            log.warning("Strategy directory %s does not exist — nothing loaded", self.directory)
            return []

        found: list[LoadedStrategy] = []
        for path in sorted(self._candidate_files()):
            try:
                module = self._import_file(path)
            except Exception:
                log.exception("Failed to import strategy file %s — skipping", path)
                continue
            for _, member in inspect.getmembers(module, inspect.isclass):
                if not issubclass(member, StrategyBase) or member is StrategyBase:
                    continue
                if inspect.isabstract(member):
                    continue
                # Only classes *defined* in this file, so a strategy importing a
                # shared base from a sibling module does not register it twice.
                if member.__module__ != module.__name__:
                    continue
                name = member.name or member.__name__
                found.append(LoadedStrategy(name=name, cls=member, source=path))
                log.info("Discovered strategy %s from %s", name, path.name)

        self._warn_on_duplicates(found)
        return found

    def load_one(self, name: str, params: dict[str, Any] | None = None) -> StrategyBase:
        """Instantiate the single strategy called *name*."""
        for candidate in self.discover():
            if candidate.name == name or candidate.cls.__name__ == name:
                return candidate.instantiate(params)
        available = ", ".join(sorted(c.name for c in self.discover())) or "none"
        raise LookupError(
            f"Strategy {name!r} not found in {self.directory} (available: {available})"
        )

    def _candidate_files(self) -> list[Path]:
        """Every ``.py`` worth importing, once the repo furniture is filtered out.

        The directory is a whole cloned repository, not a tidy folder of
        strategy files, so a bare ``rglob`` would import that repo's test suite
        and — if a virtualenv lives in there — walk thousands of files in
        site-packages before finding anything. Both are skipped here rather
        than tolerated by the per-file error handler.
        """
        return [
            path
            for path in self.directory.rglob("*.py")
            if not path.name.startswith("_") and not self._is_excluded(path)
        ]

    def _is_excluded(self, path: Path) -> bool:
        relative = path.relative_to(self.directory)
        return any(
            # Hidden directories cover .git, .venv, .pytest_cache, .ruff_cache…
            part.startswith(".") or part in EXCLUDED_DIRECTORIES
            for part in relative.parts[:-1]
        )

    def _import_file(self, path: Path):
        """Import *path* under a namespaced module name.

        The ``__strategies__.`` prefix keeps a plugin called ``utils.py`` from
        shadowing anything real in ``sys.modules``.
        """
        relative = path.relative_to(self.directory).with_suffix("")
        module_name = "__strategies__." + ".".join(relative.parts)
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot build an import spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    @staticmethod
    def _warn_on_duplicates(found: list[LoadedStrategy]) -> None:
        """Two strategies sharing a name publish to the same broker subject.

        Workers subscribe by strategy name, so a duplicate means two different
        algorithms' signals arriving on one subject and executing against one
        another's positions. Loud warning, not a crash — the operator decides.
        """
        seen: dict[str, Path] = {}
        for entry in found:
            if entry.name in seen:
                log.error(
                    "Duplicate strategy name %r in %s and %s — both publish to the same "
                    "broker subject; rename one before going live",
                    entry.name,
                    seen[entry.name],
                    entry.source,
                )
            seen[entry.name] = entry.source


def load_strategies(directory: Path | str, only: list[str] | None = None) -> list[LoadedStrategy]:
    """Discover strategies, optionally filtered to an allow-list of names."""
    discovered = StrategyLoader(directory).discover()
    if only:
        wanted = {name.lower() for name in only}
        discovered = [
            entry
            for entry in discovered
            if entry.name.lower() in wanted or entry.cls.__name__.lower() in wanted
        ]
    return discovered
