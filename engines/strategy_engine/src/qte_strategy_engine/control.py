"""``qte-control`` — the operator's switch for a running engine.

This is what is left of the control plane after the HTTP gateway was removed,
and it is smaller for a reason: the only control that genuinely needed to reach
a *running* process is shadow mode. Everything else the API used to serve —
listing strategies, reading the audit trail, running a backtest — is either a
CLI command already or a SQL query, and neither of those needs a web service
kept alive to answer it.

Shadow mode is different. It is the live/paper switch, and flipping it must not
require restarting the runner mid-position. So it travels the same way every
other engine event does: a message on ``QTE.control``, published straight to
NATS from here.

The flag is written to Redis first and broadcast second. A runner that starts
*after* the broadcast reads Redis on boot, so it comes up in the mode you last
chose rather than whatever the environment file says.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.logging_setup import configure_logging, get_logger

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qte-control", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    shadow = subparsers.add_parser(
        "shadow", help="Pause or resume delivery of signals to the broker"
    )
    shadow.add_argument(
        "state",
        choices=["on", "off", "status"],
        help="on = signals are built and audited but NOT sent; off = live; status = read it",
    )
    shadow.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt when going live",
    )

    subparsers.add_parser("ping", help="Ask the running runners to identify themselves")
    return parser


async def _set_shadow_mode(enabled: bool) -> None:
    state = RedisState()
    try:
        await state.connect()
        await state.set_flag("shadow_mode", enabled)
    except Exception as exc:
        # Refusing here is deliberate. If the flag cannot be stored, a runner
        # restarting later would silently come back in the *old* mode — and for
        # "off" that means going live again on its own. Better to fail loudly
        # and change nothing.
        _die(f"Could not reach Redis at {settings.redis.url} — nothing was changed.", exc)
    finally:
        await state.close()

    bus = NatsBus(name="qte-control")
    broadcast = False
    try:
        await bus.connect()
        await bus.publish(
            Subjects().engine_control(),
            {"action": "set_shadow_mode", "enabled": enabled},
        )
        broadcast = True
    except Exception as exc:
        # The Redis write already happened, so the next runner to start will
        # honour it. Saying so is the difference between "not applied" and
        # "applied, but not to the process running right now".
        log.error("Stored the flag but could NOT broadcast it — NATS unreachable: %s", exc)
    finally:
        await bus.close()

    await EventRepository().record_event(
        service="qte-control",
        event="shadow_mode_changed",
        level="WARNING",
        payload={"enabled": enabled, "broadcast": broadcast},
    )

    if enabled:
        print("Shadow mode ON — signals are built and audited but will NOT reach the broker.")
    else:
        print("Shadow mode OFF — signals are going LIVE to the broker.")
    if not broadcast:
        print(
            "WARNING: NATS was unreachable, so runners already running keep their old mode. "
            "Restart them, or re-run this once NATS is back."
        )


async def _show_shadow_mode() -> None:
    state = RedisState()
    try:
        await state.connect()
        stored = await state.get_flag("shadow_mode", None)
    except Exception as exc:
        _die(f"Could not reach Redis at {settings.redis.url}.", exc)
    finally:
        await state.close()

    if stored is None:
        print(
            f"No stored flag; runners fall back to QTE_BROKER__SHADOW_MODE="
            f"{settings.broker.shadow_mode}."
        )
    else:
        print(f"Shadow mode is {'ON (paper)' if stored else 'OFF (live)'}.")


async def _ping() -> None:
    bus = NatsBus(name="qte-control")
    try:
        await bus.connect()
    except Exception as exc:
        _die(f"Could not reach NATS at {settings.nats.url}.", exc)
    try:
        reply = await bus.request(Subjects().engine_control(), {"action": "ping"}, timeout=2.0)
        print(json.dumps(reply, indent=2))
    except Exception as exc:
        print(f"No runner answered within 2s: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    finally:
        await bus.close()


def _die(message: str, exc: Exception) -> None:
    """Fail the way a CLI should: one line of what went wrong, no traceback.

    An operator reaching for the kill switch needs to know which dependency is
    down, not which line of redis-py raised.
    """
    print(f"{message}\n  ({type(exc).__name__}: {exc})", file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    configure_logging()
    args = build_parser().parse_args()

    if args.command == "ping":
        asyncio.run(_ping())
        return

    if args.state == "status":
        asyncio.run(_show_shadow_mode())
        return

    enabled = args.state == "on"
    if not enabled and not args.yes:
        # Turning shadow mode off puts real orders on a real account. A typo
        # should not be enough to do that.
        answer = input("This sends live orders to the broker. Type 'live' to confirm: ")
        if answer.strip().lower() != "live":
            print("Aborted; shadow mode unchanged.")
            raise SystemExit(1)

    asyncio.run(_set_shadow_mode(enabled))


if __name__ == "__main__":
    main()
