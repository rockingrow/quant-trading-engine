"""Your strategies live under here.

One package per repository, one module per strategy. The engine never imports
this package directly — ``manifest.py`` at the repo root does, and hands the
engine the classes it wants published.

Nothing in here imports the engine. The contract it is written against is
restated in :mod:`boilerplate.contract`, and the indicators it needs are in
:mod:`boilerplate.indicators`: the strategy logic is the core, and the engine
is a delivery layer that must be replaceable without touching it.
"""
