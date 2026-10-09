"""The operator's Telegram bot: look at a running engine, and steer it.

Nine commands, grouped the way the work goes — ``/redis``, ``/runner``,
``/positions`` and ``/closed`` to look; ``/flat``, ``/prevent``, ``/allow``,
``/warmup`` and ``/flush`` to act. Shaped after ``algo-trading-broker``'s own
bot (``bot/app/``) so the two read alike: the same monospace ``<pre>`` tables,
the same Prev/Next paging, the same confirmation in front of anything that
closes a position.

What differs is the wiring, and it is forced by this repository's
architecture: the broker's bot calls the broker's HTTP API, while QTE has no
HTTP control plane and does not want one, so this bot asks the running services
over NATS request/reply — the same actions ``qte-control`` uses — and reads
Redis and Postgres directly for the listings that do not need a live process.

It is a separate service because polling is a loop that must not share a
process with the trade path: a Telegram outage holds ``getUpdates`` open for as
long as it likes, and nothing in the runner should ever be behind that.
"""
