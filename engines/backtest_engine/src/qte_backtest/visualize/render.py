"""Assembling the dashboard into one file that works with nothing else.

The output is a single ``.html`` with the stylesheet, the script and the data
inlined. No CDN, no bundler, no server: a report is something you email to
someone, open on a machine with no network, or keep in a directory next to the
JSON it came from and still open in five years. Anything fetched at load time is
a way for that to stop being true.

The markup is built by the script from the embedded view object rather than
templated here, so the layout has one definition instead of two that drift.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Any

from qte_backtest.visualize.view import build_view

#: Bumped when the embedded view object changes shape, so a stale asset paired
#: with a fresh view is a visible mismatch rather than a blank panel.
VIEW_VERSION = "1"


def render_html(report: dict[str, Any], *, title: str | None = None) -> str:
    """Render *report* — a parsed backtest JSON — into a standalone page."""
    view = build_view(report)
    meta = view["meta"]
    heading = title or f"{meta['strategy']} — {meta['symbol']} {meta['timeframe']}"
    return _TEMPLATE.format(
        title=_escape(heading),
        css=_asset("dashboard.css"),
        js=_asset("dashboard.js"),
        view=_embed(view),
        version=VIEW_VERSION,
    )


def _asset(name: str) -> str:
    return (resources.files("qte_backtest.visualize.assets") / name).read_text(encoding="utf-8")


def _embed(view: dict[str, Any]) -> str:
    """Serialise the view so no value in it can close the script tag.

    ``</script>`` inside a JSON string ends the block in an HTML parser even
    though it is valid JSON — a strategy named ``</script>`` would otherwise
    turn the page into markup. Escaping the three characters that can start such
    a sequence keeps the payload valid JSON and inert as markup.
    """
    payload = json.dumps(view, ensure_ascii=False, default=str)
    return payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="generator" content="qte-backtest visualize v{version}">
<title>{title}</title>
<style>
{css}
</style>
</head>
<body>
<div id="app"></div>
<noscript>
  <p class="noscript">This report is drawn in the browser. The numbers behind it are in the
  JSON the page was rendered from — open that instead if scripts are off.</p>
</noscript>
<script id="view" type="application/json">{view}</script>
<script>
{js}
</script>
</body>
</html>
"""
