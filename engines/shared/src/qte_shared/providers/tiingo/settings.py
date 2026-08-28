"""Tiingo's own configuration block (prefix ``QTE_TIINGO__``).

It lives with the provider rather than on root :class:`Settings`, so the core
config has no vendor-shaped hole in it and a second vendor adds a file instead
of a field. The env names are unchanged, so an existing ``.env`` keeps working.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from qte_shared.interfaces.market_data import ProviderSettings


class TiingoSettings(ProviderSettings):
    """Endpoints and credentials -- REST for history, WebSocket for live."""

    model_config = SettingsConfigDict(env_prefix="QTE_TIINGO__", extra="ignore")

    api_key: str = ""
    rest_url: str = "https://api.tiingo.com"
    fx_ws_url: str = "wss://api.tiingo.com/fx"
    crypto_ws_url: str = "wss://api.tiingo.com/crypto"
    # Tiingo's WS "thresholdLevel": 5 = top-of-book quotes only, 0 = every trade.
    fx_threshold: int = 5
    crypto_threshold: int = 2
    request_timeout: float = 30.0
    #: Bars asked for in a single REST call. Tiingo caps an intraday response
    #: at a few thousand rows and signals it with a *200 and fewer bars* -- not
    #: an error -- so a range wider than the cap comes back quietly short. The
    #: history source pages under this budget instead; measured truncation on a
    #: free plan began between 5k and 7k rows, so the default leaves headroom.
    #: A paid plan can raise it to cut the number of round trips.
    max_rows_per_request: int = 5000
    #: Hard stop on pages for one range, so a vendor that stops making progress
    #: cannot spin forever. It has to clear the widest range a caller can ask
    #: for by default: `qte-backtest download` uses three years, which on M1 is
    #: a three-day page span and so ~365 pages. A lower ceiling would turn the
    #: silent truncation this module fixes back on at a different layer.
    max_pages: int = 600
