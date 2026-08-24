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
