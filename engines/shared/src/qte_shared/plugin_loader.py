"""Dynamic discovery of the private strategies in ``__strategies__/``.

This is the plugin seam. The engine is public; the alpha is not. The loader
imports whatever strategies it finds in a directory that is git-ignored here
and cloned from a private repo at deploy time, so the two can be versioned,
reviewed and released completely independently.

There are two ways in, and the loader prefers the first:

**A manifest.** A plugin repository declares itself by putting a
``strategies.py`` -- or ``manifest.py`` -- at its root that exposes
``load_all()`` returning ``{alias: strategy class}``. The engine imports that
one file and asks it what exists. Nothing on this side then knows a module
path, a package name or a
directory layout, so the plugin repo can reorganise itself freely, and it
decides for itself which of its classes are deployed — a half-finished
experiment sitting in the tree cannot start trading because someone forgot it
was a strategy subclass.

**A directory scan.** Failing a manifest, every ``.py`` under the directory is
imported and anything that looks like a strategy is collected. This is what
``examples/__strategies__/ema_atr_breakout.py`` uses: drop a single file in and
it runs, no ceremony. The two mix — a scan still covers loose files alongside a
cloned repo that brought its own manifest.

Import is by file path rather than package name in both cases: the directory is
a mounted volume, not an installed distribution, and requiring it to be pip-
installable would drag the private repo into the public build.
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from qte_shared.logging_setup import get_logger
from qte_shared.strategy_base import (
    StrategyLike,
    implements_strategy_contract,
    looks_like_a_strategy,
)

log = get_logger(__name__)

#: A plugin repository declares itself by putting one of these at its root…
#: Two names because the file answers to two readings: ``strategies.py`` says
#: what it contains, ``manifest.py`` says what it is. A repo picks one; the
#: order here is the order they are looked for, and a repo carrying both is a
#: mistake worth failing on rather than silently resolving.
MANIFEST_FILENAMES = ("strategies.py", "manifest.py")

#: …exposing this callable, which returns ``{alias: strategy class}``.
MANIFEST_HOOK = "load_all"

#: How deep below the strategies directory a manifest is looked for. One level
#: is what the layout produces: ``__strategies__/`` is the mount point and the
#: private repo is cloned into it, so its root is a child directory.
MANIFEST_DEPTH = 1

#: Directory names never scanned for strategies. The scan can be pointed at a
#: cloned repository, which brings its own tests, docs and possibly a
#: virtualenv along with the code that matters. Hidden directories (``.git``,
#: ``.venv``, …) are excluded separately, by the leading dot.
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
class Candidate:
    """A class a plugin repository offered, before anything has judged it.

    The loader throws away what it cannot drive; ``qte-strategy-audit`` has to
    report on exactly that, so collection and judgement are separate steps and
    this is what the first one produces.

    ``via`` says how it was found. A ``"manifest"`` candidate was named by the
    repo and is therefore certainly meant as a strategy; a ``"scan"`` candidate
    is a guess — see :func:`~qte_shared.strategy_base.looks_like_a_strategy`.
    """

    name: str
    obj: Any
    source: Path
    via: Literal["manifest", "scan"]


@dataclass(slots=True)
class LoadFailure:
    """A manifest or module the loader could not read, and why.

    The loader's answer to a broken file is to log it and carry on — one bad
    strategy must not stop the other four from trading. That leaves no trace an
    *auditor* can act on, and a manifest that raised then looks exactly like a
    repo that publishes nothing. So the loader also keeps the wreckage.
    """

    path: Path
    reason: str
    error: BaseException | None = None

    @property
    def detail(self) -> str:
        if self.error is None:
            return self.reason
        return f"{self.reason}: {type(self.error).__name__}: {self.error}"


@dataclass(slots=True)
class LoadedStrategy:
    """One discovered strategy class and where it came from.

    ``cls`` is a class the engine can drive, which is not the same as a
    :class:`~qte_shared.strategy_base.StrategyBase` subclass — see that
    module's docstring.
    """

    name: str
    cls: StrategyLike
    source: Path

    def instantiate(self, params: dict[str, Any] | None = None) -> StrategyLike:
        return self.cls(params or {})


class StrategyLoader:
    """Finds strategy classes under a directory and instantiates them."""

    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)
        #: Filled by :meth:`collect`, cleared at the start of each call.
        self.failures: list[LoadFailure] = []

    def collect(self) -> list[Candidate]:
        """Every class this directory offered, manifests first then loose files.

        Nothing is judged here. The loader goes on to drop what it cannot
        drive; ``qte-strategy-audit`` wants precisely those, so the two steps
        are separate and this is the first one.

        A manifest or a module that fails to import is logged and skipped
        rather than taking the process down: one broken strategy file should
        not stop the other four from trading.
        """
        self.failures = []
        if not self.directory.is_dir():
            log.warning("Strategy directory %s does not exist — nothing loaded", self.directory)
            return []

        self.failures = []
        candidates: list[Candidate] = []
        manifests = self._manifests()
        for manifest in manifests:
            candidates.extend(self._load_manifest(manifest))

        # Whatever a manifest speaks for is off limits to the scan: importing
        # a repo's modules a second time, individually, would register the
        # same class twice and import files the manifest deliberately omitted.
        candidates.extend(self._scan(claimed={manifest.parent for manifest in manifests}))
        return candidates

    def discover(self) -> list[LoadedStrategy]:
        """The subset of :meth:`collect` the engine can actually drive."""
        found: list[LoadedStrategy] = []
        for candidate in self.collect():
            if not implements_strategy_contract(candidate.obj):
                self._report_undrivable(candidate)
                continue
            found.append(
                LoadedStrategy(name=candidate.name, cls=candidate.obj, source=candidate.source)
            )
            log.info("Registered strategy %s from %s", candidate.name, candidate.source)

        self._warn_on_duplicates(found)
        return found

    @staticmethod
    def _report_undrivable(candidate: Candidate) -> None:
        """A manifest naming a non-strategy is an error; a scan's guess is not.

        The repo *said* the manifest entry was a strategy, so it failing the
        contract is a deployment that will not do what its author expected. A
        scanned class only ever looked like one, so the same message there
        would cry wolf over every helper with two familiar-looking methods.
        """
        described = getattr(candidate.obj, "__name__", candidate.obj)
        if candidate.via == "manifest":
            log.error(
                "%s publishes %r as %s, which the engine cannot drive: a strategy needs a "
                "concrete on_candle_closed, on_start, on_stop and history_window. Skipping.",
                candidate.source,
                candidate.name,
                described,
            )
        else:
            log.warning(
                "%s in %s looks like a strategy but the engine cannot drive it — "
                "run `make audit` to see what it is missing",
                described,
                candidate.source,
            )

    def load_one(self, name: str, params: dict[str, Any] | None = None) -> StrategyLike:
        """Instantiate the single strategy called *name*."""
        discovered = self.discover()
        for candidate in discovered:
            if candidate.name == name or candidate.cls.__name__ == name:
                return candidate.instantiate(params)
        available = ", ".join(sorted(entry.name for entry in discovered)) or "none"
        raise LookupError(
            f"Strategy {name!r} not found in {self.directory} (available: {available})"
        )

    # ── Manifests ─────────────────────────────────────────────────────

    def _manifests(self) -> list[Path]:
        """A manifest at the directory root, or one level below it.

        A repo declaring two manifests is refused outright. Picking one would
        mean the alias table the operator edited might not be the one deployed,
        and that is the single thing this seam exists to make unambiguous.
        """
        roots = [self.directory]
        if MANIFEST_DEPTH >= 1:
            roots += [
                child
                for child in sorted(self.directory.iterdir())
                if child.is_dir() and not child.name.startswith(".")
            ]

        found: list[Path] = []
        for root in roots:
            present = [root / name for name in MANIFEST_FILENAMES if (root / name).is_file()]
            if len(present) > 1:
                names = ", ".join(path.name for path in present)
                raise RuntimeError(
                    f"{root} declares more than one strategy manifest ({names}). Keep one — "
                    "which of them publishes the deployed strategies would otherwise depend "
                    "on the loader's lookup order."
                )
            found.extend(present)
        return found

    def _load_manifest(self, path: Path) -> list[Candidate]:
        """Ask one plugin repository what it publishes."""
        try:
            module = self._import_file(path)
        except Exception as error:
            log.exception("Failed to import the strategy manifest %s — skipping", path)
            self.failures.append(LoadFailure(path, "the manifest could not be imported", error))
            return []

        hook = getattr(module, MANIFEST_HOOK, None)
        if not callable(hook):
            log.error(
                "%s defines no %s() — a strategy manifest must expose it, returning "
                "{alias: strategy class}. Ignoring this repository.",
                path,
                MANIFEST_HOOK,
            )
            self.failures.append(LoadFailure(path, f"the manifest defines no {MANIFEST_HOOK}()"))
            return []

        try:
            published = dict(hook())
        except Exception as error:
            log.exception("%s.%s() raised — no strategies loaded from it", path, MANIFEST_HOOK)
            self.failures.append(LoadFailure(path, f"{MANIFEST_HOOK}() raised", error))
            return []

        return [
            Candidate(name=str(alias), obj=strategy, source=path, via="manifest")
            for alias, strategy in published.items()
        ]

    # ── Directory scan ────────────────────────────────────────────────

    def _scan(self, *, claimed: set[Path]) -> list[Candidate]:
        found: list[Candidate] = []
        for path in sorted(self._candidate_files(claimed)):
            try:
                module = self._import_file(path)
            except Exception as error:
                log.exception("Failed to import strategy file %s — skipping", path)
                self.failures.append(LoadFailure(path, "the module could not be imported", error))
                continue
            for _, member in inspect.getmembers(module, inspect.isclass):
                if not looks_like_a_strategy(member):
                    continue
                # Only classes *defined* in this file, so a strategy importing a
                # shared base from a sibling module does not register it twice.
                if member.__module__ != module.__name__:
                    continue
                name = getattr(member, "name", "") or member.__name__
                found.append(Candidate(name=name, obj=member, source=path, via="scan"))
        return found

    def _candidate_files(self, claimed: set[Path]) -> list[Path]:
        """Every ``.py`` worth importing, once the repo furniture is filtered out.

        The directory may hold a whole cloned repository rather than a tidy
        folder of strategy files, so a bare ``rglob`` would import that repo's
        test suite and — if a virtualenv lives in there — walk thousands of
        files in site-packages before finding anything. Both are skipped here
        rather than tolerated by the per-file error handler.
        """
        return [
            path
            for path in self.directory.rglob("*.py")
            if not path.name.startswith("_")
            and not self._is_excluded(path)
            and not any(root in path.parents for root in claimed)
        ]

    def _is_excluded(self, path: Path) -> bool:
        relative = path.relative_to(self.directory)
        return any(
            # Hidden directories cover .git, .venv, .pytest_cache, .ruff_cache…
            part.startswith(".") or part in EXCLUDED_DIRECTORIES
            for part in relative.parts[:-1]
        )

    # ── Import mechanics ──────────────────────────────────────────────

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
        try:
            spec.loader.exec_module(module)
        except Exception:
            # A half-initialised module left in sys.modules would be handed to
            # the next importer of the same path as if it had loaded cleanly.
            sys.modules.pop(module_name, None)
            raise
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
