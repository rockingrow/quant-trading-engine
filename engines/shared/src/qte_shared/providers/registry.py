"""Name -> provider class, resolved at call time.

The engines never import a vendor module. They ask for "the configured
provider", or for one by name, and get back a :class:`MarketDataProvider`.
That indirection buys three things:

* **Swapping a vendor is configuration.** ``QTE_MARKET_DATA__PROVIDER=tiingo``
  today, something else tomorrow, with no code edit anywhere in an engine.
* **Running two at once is possible.** Ingestion can take its live feed from
  one vendor while the backtest downloader pulls history from another, because
  each asks for the provider it wants by name.
* **Import stays lazy.** Built-ins are registered as ``"module:Class"`` strings
  and imported only when created, so an image that never touches a vendor does
  not pay for its dependencies -- the strategy runner installs neither
  ``httpx`` nor ``websockets`` on the vendor's account.

A third-party provider registers itself with :func:`register_provider`, either
as a decorator on the class or by passing the class in.
"""

from __future__ import annotations

from importlib import import_module

from qte_shared.config import settings
from qte_shared.interfaces.market_data import (
    Capability,
    MarketDataProvider,
    UnknownProvider,
    UnsupportedCapability,
)
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: Shipped with QTE, spelled as import paths so the module stays unimported
#: until something actually asks for that vendor.
_BUILTINS: dict[str, str] = {
    "tiingo": "qte_shared.providers.tiingo.provider:TiingoProvider",
}

#: name -> class, or "module:Class" for the not-yet-imported built-ins.
_REGISTRY: dict[str, type[MarketDataProvider] | str] = dict(_BUILTINS)


def register_provider(provider: type[MarketDataProvider] | None = None, *, name: str | None = None):
    """Register a provider class. Usable bare or as ``@register_provider``.

    Re-registering a name replaces it, which is what makes a local override of
    a shipped vendor -- a sandbox endpoint, a recorded fixture feed -- a couple
    of lines in a test or a private plugin.
    """

    def _register(cls: type[MarketDataProvider]) -> type[MarketDataProvider]:
        key = (name or cls.name or cls.__name__).lower()
        if not key:
            raise ValueError(f"{cls.__name__} needs a `name` to be registered under")
        if key in _REGISTRY and _REGISTRY[key] is not cls:
            log.info("Market data provider %r replaced by %s", key, cls.__name__)
        _REGISTRY[key] = cls
        return cls

    if provider is not None:
        return _register(provider)
    return _register


def unregister_provider(name: str) -> None:
    """Drop a registration, restoring the built-in of that name if there is one."""
    key = name.lower()
    if key in _BUILTINS:
        _REGISTRY[key] = _BUILTINS[key]
    else:
        _REGISTRY.pop(key, None)


def available_providers() -> list[str]:
    """Every registered name, sorted -- what a CLI would list."""
    return sorted(_REGISTRY)


def get_provider_class(name: str) -> type[MarketDataProvider]:
    """Resolve a name to its class, importing a built-in on first use."""
    key = (name or "").lower()
    entry = _REGISTRY.get(key)
    if entry is None:
        raise UnknownProvider(
            f"Unknown market data provider {name!r}. Registered: {', '.join(available_providers())}"
        )
    if isinstance(entry, str):
        module_path, _, attribute = entry.partition(":")
        entry = getattr(import_module(module_path), attribute)
        _REGISTRY[key] = entry
    return entry


def create_provider(
    name: str | None = None,
    *,
    capability: Capability | None = None,
    **kwargs,
) -> MarketDataProvider:
    """Build the named provider, or the configured one when *name* is omitted.

    Pass *capability* to state what the caller needs it for: the mismatch then
    surfaces at startup with the vendor's name in the message, rather than as
    an empty feed or a missing method once the engine is already running.
    """
    resolved = name or settings.market_data.provider
    provider = get_provider_class(resolved)(**kwargs)
    if capability is not None and not provider.supports(capability):
        raise UnsupportedCapability(
            f"Market data provider {provider.name!r} does not serve {capability.value!r}; "
            f"set QTE_MARKET_DATA__PROVIDER to one that does"
        )
    return provider
