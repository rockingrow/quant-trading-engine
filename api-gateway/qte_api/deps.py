"""Shared dependencies: the NATS handle and the optional API-key guard."""

from __future__ import annotations

from fastapi import Header, HTTPException, status
from qte_shared.bus import NatsBus
from qte_shared.config import settings

#: Set during app startup; the control-plane's one connection to the bus.
bus: NatsBus | None = None


def get_bus() -> NatsBus:
    if bus is None or not bus.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="NATS is not connected — the control plane cannot reach the runners",
        )
    return bus


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Guard the mutating endpoints.

    Unset ``QTE_API__API_KEY`` leaves them open, which is fine on a laptop and
    is not fine on a VPS — the readme says so and ``/health`` reports it.
    """
    if not settings.api.api_key:
        return
    if x_api_key != settings.api.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing X-API-KEY"
        )
