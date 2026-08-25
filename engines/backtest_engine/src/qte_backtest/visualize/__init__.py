"""An interactive HTML view of a backtest report.

``build_view`` derives the panels; ``render_html`` inlines them into one file.
The split exists so the derivations can be tested without a browser and without
parsing markup.
"""

from qte_backtest.visualize.render import render_html
from qte_backtest.visualize.view import build_view

__all__ = ["build_view", "render_html"]
