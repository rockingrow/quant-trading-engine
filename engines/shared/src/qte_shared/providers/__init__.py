"""Concrete market-data vendors, reached through the registry.

Import :func:`create_provider`, not a vendor module. See
:mod:`qte_shared.interfaces.market_data` for the contract every provider here
implements and :mod:`qte_shared.providers.registry` for how a new one joins.
"""

from __future__ import annotations

from qte_shared.providers.registry import (
    available_providers,
    create_provider,
    get_provider_class,
    register_provider,
    unregister_provider,
)

__all__ = [
    "available_providers",
    "create_provider",
    "get_provider_class",
    "register_provider",
    "unregister_provider",
]
