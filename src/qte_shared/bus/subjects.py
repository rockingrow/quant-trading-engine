"""Every NATS subject QTE touches, in one place.

Two namespaces meet here and they belong to different owners:

* ``QTE.*`` — ours. Market-data events between ingestion and the strategy
  runner. Nothing outside this repo subscribes to them.
* ``SIGNALS.<strategy>`` — the broker's. It is the JetStream buffer
  ``algo-trading-broker`` consumes with its own ``SignalWorker``; publishing
  there is the NATS equivalent of POSTing its ``/secret/webhook``. The shape of
  what we put on it is fixed by that repo, not by us.
"""

from __future__ import annotations

from qte_shared.config import settings


class Subjects:
    """Subject builders. Instantiate with a prefix or use the module default."""

    def __init__(self, prefix: str | None = None) -> None:
        namespace = settings.state_scope.namespace.replace(":", ".")
        self.prefix = f"{prefix or settings.nats.subject_prefix}.{namespace}"

    # ── QTE internal ────────────────────────────────────────────────

    def tick(self, symbol: str) -> str:
        return f"{self.prefix}.tick.{symbol}"

    def tick_wildcard(self) -> str:
        return f"{self.prefix}.tick.*"

    def candle_closed(self, symbol: str, timeframe: str) -> str:
        """e.g. ``QTE.candle.closed.XAUUSD.M15`` — one subject per pair+timeframe.

        Partitioning by both means a runner that only trades M15 gold never has
        to decode an M1 BTC message just to discard it.
        """
        return f"{self.prefix}.candle.closed.{symbol}.{timeframe}"

    def candle_closed_filter(self, symbol: str = "*", timeframe: str = "*") -> str:
        return f"{self.prefix}.candle.closed.{symbol}.{timeframe}"

    def engine_control(self) -> str:
        """The runner's control plane: shadow mode, the bar gate, status, FLAT."""
        return f"{self.prefix}.control"

    def ingestion_control(self) -> str:
        """Ingestion's own control plane: status, warm-up requests, flush.

        Separate from :meth:`engine_control` rather than shared with a
        discriminator in the payload, because these are request/reply actions
        and both services subscribe without a queue group: on one subject a
        request would be answered twice and the caller would read whichever
        reply arrived first.
        """
        return f"{self.prefix}.ingestion.control"

    def signal_emitted(self) -> str:
        """Mirror of every signal the runner produced, for observers/UI."""
        return f"{self.prefix}.signal.emitted"

    # ── Broker-owned ────────────────────────────────────────────────

    @staticmethod
    def broker_signal(strategy: str, prefix: str | None = None) -> str:
        """``SIGNALS.<strategy>`` — the broker's durable webhook buffer."""
        return f"{prefix or settings.broker.subject_prefix}.{strategy}"


subjects = Subjects()
