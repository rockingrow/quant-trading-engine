"""Credential-free signal records and transport-only authentication.

Audit, report and internal event consumers need trading data, never the broker
token. Delivery injects the current credential so stored retries remain usable
after rotation without retaining the old credential in the outbox.
"""

from typing import Any

from qte_shared.models import BrokerSignal


def signal_record(signal: BrokerSignal) -> dict[str, Any]:
    """Serialize trading fields without authentication, including legacy signals."""
    return signal.model_dump(mode="json", exclude={"token"})


def authenticated_signal(signal: BrokerSignal, credential: str) -> dict[str, Any]:
    """Build the broker wire body without mutating the signal kept for auditing."""
    return {**signal_record(signal), "token": credential}
