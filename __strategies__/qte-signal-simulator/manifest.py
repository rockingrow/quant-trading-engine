"""Entry point the QTE engine imports by file path.

The engine walks one level into ``__strategies__/``, finds this file, imports
it and calls :func:`load_all` (and :func:`load_settings`). There is no
``qss`` on ``sys.path`` when that happens, so ``src/`` is put there first — the
mounted checkout is the install.
"""

from __future__ import annotations

import sys
from pathlib import Path

#: Root of the checkout, wherever it happens to be mounted.
REPO_ROOT = Path(__file__).resolve().parent
_SOURCE_ROOT = REPO_ROOT / "src"

if str(_SOURCE_ROOT) not in sys.path:
    # Prepended so a stale `qss` in site-packages can never shadow the checkout
    # the operator deployed and will edit.
    sys.path.insert(0, str(_SOURCE_ROOT))

from qss.registry import ALIASES, aliases, load, load_all  # noqa: E402
from qss.settings import SETTINGS, load_settings  # noqa: E402

__all__ = ["ALIASES", "REPO_ROOT", "SETTINGS", "aliases", "load", "load_all", "load_settings"]
