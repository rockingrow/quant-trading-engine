"""``qte-control`` — the operator's switch for a running engine.

This is what is left of the control plane after the HTTP gateway was removed,
and it is smaller for a reason: the only control that genuinely needed to reach
a *running* process is shadow mode — plus ``owner``, for the one piece of state
a runner that is no longer running can leave behind. Everything else the API used to serve —
listing strategies, reading the audit trail, running a backtest — is either a
CLI command already or a SQL query, and neither of those needs a web service
kept alive to answer it.

The execution namespace selects paper or live at startup. Shadow mode pauses
live delivery without converting broker positions into simulated ones. Its
control message travels on the namespace-qualified NATS control subject.

The flag is written to Redis first and broadcast second. A runner that starts
*after* the broadcast reads Redis on boot, so it comes up in the mode you last
chose rather than whatever the environment file says.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NatsTimeoutError

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

    owner = subparsers.add_parser(
        "owner", help="Inspect or clear the runner ownership claim left by an unclean exit"
    )
    owner.add_argument(
        "operation",
        choices=["status", "clear"],
        help="status = show who holds the claim; clear = remove a stale claim",
    )
    owner.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt when clearing",
    )
    return parser


async def _set_shadow_mode(enabled: bool) -> None:
    state_scope = settings.state_scope
    if not enabled and state_scope.is_paper:
        raise SystemExit(
            "Paper state cannot send broker orders; select QTE_STATE__MODE=live and restart"
        )
    if not enabled and settings.broker.force_shadow_mode:
        raise SystemExit("ALGO_BROKER__FORCE_SHADOW_MODE forbids disabling shadow mode")
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

    print(f"State namespace: {state_scope.namespace}")
    if enabled and not state_scope.is_paper:
        print("Live delivery PAUSED; no paper positions are created in this namespace.")
    elif enabled:
        print("Shadow mode ON — signals are built and audited but will NOT reach the broker.")
    else:
        print("Shadow mode OFF — signals are going LIVE to the broker.")
    if not broadcast:
        print(
            "WARNING: NATS was unreachable. Runners refresh the stored flag before their "
            "next delivery and on periodic synchronization; use ping to check the running mode."
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

    if stored is not None and not isinstance(stored, bool):
        raise SystemExit("Persisted shadow_mode is invalid; runners refuse delivery")
    state_scope = settings.state_scope
    print(f"State namespace: {state_scope.namespace}")
    if state_scope.is_paper:
        print("Shadow mode is ON (paper), enforced by QTE_STATE__MODE.")
    elif settings.broker.force_shadow_mode:
        print("Live delivery is PAUSED, forced by ALGO_BROKER__FORCE_SHADOW_MODE.")
    elif stored is None:
        print(
            f"No stored flag; runners fall back to ALGO_BROKER__SHADOW_MODE="
            f"{settings.broker.shadow_mode}."
        )
    else:
        print(f"Live delivery is {'PAUSED' if stored else 'ENABLED'}.")


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


async def _read_runner_owner(redis_state: RedisState) -> str | None:
    try:
        await redis_state.connect()
        return await redis_state.runner_owner()
    except Exception as exc:
        _die(f"Could not reach Redis at {settings.redis.url} — nothing was changed.", exc)


async def _show_runner_owner() -> None:
    redis_state = RedisState()
    try:
        holder = await _read_runner_owner(redis_state)
    finally:
        await redis_state.close()
    print(f"State namespace: {settings.state_scope.namespace}")
    print(f"Ownership key:   {redis_state.key('runner', 'owner')}")
    print(f"Holder:          {holder if holder is not None else 'none — no runner claim'}")


async def _answering_runner() -> dict | None:
    """The runner that answers a ping, or ``None`` when none does.

    An unreachable NATS is not "no runner": nothing can be verified through a
    bus that is down, so that case refuses instead of returning ``None``.
    """
    control_bus = NatsBus(name="qte-control")
    try:
        await control_bus.connect()
    except Exception as exc:
        _die(
            f"Could not reach NATS at {settings.nats.url}, so no runner could be ruled out "
            "— nothing was changed.",
            exc,
        )
    try:
        return await control_bus.request(
            Subjects().engine_control(), {"action": "ping"}, timeout=2.0
        )
    except (NoRespondersError, NatsTimeoutError):
        return None
    except Exception as exc:
        # Anything else is a broken request, not an absent runner.
        _die("The runner ping failed, so no runner could be ruled out — nothing was changed.", exc)
    finally:
        await control_bus.close()


async def _clear_runner_owner(*, assume_yes: bool) -> None:
    """Remove a stale claim, and only a stale one.

    The claim never expires on purpose: a paused runner that outlived a lease
    would resume beside its replacement. So this refuses while any runner
    answers, asks before it deletes, and deletes only the holder it showed.
    """
    namespace = settings.state_scope.namespace
    redis_state = RedisState()
    try:
        holder = await _read_runner_owner(redis_state)
        print(f"State namespace: {namespace}")
        print(f"Ownership key:   {redis_state.key('runner', 'owner')}")
        if holder is None:
            print("No runner claim is held; nothing to clear.")
            return
        print(f"Holder:          {holder}")

        runner_reply = await _answering_runner()
        if runner_reply is not None:
            print(json.dumps(runner_reply, indent=2), file=sys.stderr)
            raise SystemExit(
                "A runner answered the ping, so the claim is not stale. Stop it first; "
                "nothing was changed."
            )

        if not assume_yes:
            print(
                "No runner answered the ping. A paused or hung runner does not answer "
                "either: confirm yourself that no runner process for this namespace is "
                "left on any host before continuing."
            )
            answer = input(f"Type the namespace ({namespace}) to clear its claim: ")
            if answer.strip() != namespace:
                print("Aborted; the claim is unchanged.")
                raise SystemExit(1)

        try:
            cleared = await redis_state.release_runner(holder)
        except Exception as exc:
            _die(f"Could not reach Redis at {settings.redis.url}.", exc)
        if not cleared:
            raise SystemExit(
                "The claim changed while this command ran — another runner took it. "
                "Nothing was removed."
            )
    finally:
        await redis_state.close()

    await EventRepository().record_event(
        service="qte-control",
        event="runner_owner_cleared",
        level="WARNING",
        payload={"holder": holder},
    )
    print("Cleared. The next runner to start will claim the namespace.")


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

    if args.command == "owner":
        if args.operation == "status":
            asyncio.run(_show_runner_owner())
        else:
            asyncio.run(_clear_runner_owner(assume_yes=args.yes))
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
