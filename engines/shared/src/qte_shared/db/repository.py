"""The lifecycle-event writer, available to every engine."""

from __future__ import annotations

from typing import Any

from qte_shared.db.models import EngineEvent
from qte_shared.db.session import Database, get_database
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)


class EventRepository:
    """Records service lifecycle events. Never raises."""

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()

    async def record_event(
        self,
        *,
        service: str,
        event: str,
        level: str = "INFO",
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Write one breadcrumb, swallowing any failure.

        These rows are diagnostic, not operational. An engine that crashed
        because it could not log that it had started would be a worse outcome
        than a missing breadcrumb.
        """
        try:
            async with self._db.session() as session:
                session.add(
                    EngineEvent(service=service, event=event, level=level, payload=payload or {})
                )
        except Exception as exc:
            log.debug("Engine event write failed (%s/%s): %s", service, event, exc)
