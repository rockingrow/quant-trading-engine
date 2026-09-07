"""``qte-simulator`` — run the dev feed, and drive it.

    qte-simulator serve                      the server ingestion connects to
    qte-simulator tick    --symbol XAUUSD --last 2412.5
    qte-simulator bar     --symbol XAUUSD --open 2400 --high 2410 --low 2398 \
                          --close 2408 --verify
    qte-simulator replay  --symbol XAUUSD --generate 300
    qte-simulator replay  --symbol XAUUSD --file data/parquet/XAUUSD_M15.parquet
    qte-simulator walk    --symbol XAUUSD --rate 5
    qte-simulator watch   --symbol XAUUSD
    qte-simulator status

``--symbol`` defaults to the first of ``QTE_ENGINE__SYMBOLS`` and
``--timeframe`` to ``QTE_ENGINE__SIGNAL_TIMEFRAME``, so in a configured dev
stack the two warm-ups are argument-free: ``replay --generate`` synthesises
``QTE_SIMULATOR__GENERATE_BARS`` bars, and a bare ``replay`` plays the last
``QTE_SIMULATOR__CACHE_BARS`` of ``QTE_SIMULATOR_PARQUET_FILE`` — real vendor
history, re-anchored onto the same forward series as generated bars.

``--verify`` is the part worth knowing about: it subscribes to QTE's own NATS
subjects before sending anything, then reports, bar by bar, whether the candle
ingestion published matches the bar that was played into it — and, with
``--expect-signal``, whether the strategy runner turned it into a signal. The
exit code is the verdict, so it works in a script.

Everything except ``serve`` needs a running server; ``--verify`` and ``watch``
additionally need NATS and the rest of the stack. See ``docs/simulator.md``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import timedelta
from typing import Any

from qte_shared.config import settings
from qte_shared.dev_only import DevOnlyError
from qte_shared.logging_setup import configure_logging, get_logger
from qte_shared.models import Candle
from qte_shared.timeframes import normalize_timeframe, timeframe_seconds
from qte_simulator.bars import anchor_open_times, generate_bars, reference_price
from qte_simulator.client import ControlClient, ControlError, SimulatorUnreachable
from qte_simulator.control import MAX_BARS
from qte_simulator.settings import simulator_settings
from qte_simulator.sources import SourceError, load_bars
from qte_simulator.verify import CandleCheck, FlowWatcher

log = get_logger(__name__)


# ── Parser ────────────────────────────────────────────────────────────────

#: Every command works on one symbol, and in a dev stack it is the symbol the
#: engine is watching — asking for it again on the command line is how a
#: rehearsal ends up aimed at a symbol ingestion never subscribed to.
_SYMBOL_HELP = "Defaults to the first of QTE_ENGINE__SYMBOLS"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qte-simulator", description=__doc__)
    parser.add_argument(
        "--url",
        default=None,
        help=f"Control endpoint (default {simulator_settings.control_url})",
    )
    parser.add_argument("--json", action="store_true", help="Print raw acknowledgements")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="Run the simulator server")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)

    tick = subparsers.add_parser("tick", help="Send a single tick")
    tick.add_argument("--symbol", default=None, help=_SYMBOL_HELP)
    tick.add_argument("--bid", type=float)
    tick.add_argument("--ask", type=float)
    tick.add_argument("--last", type=float)
    tick.add_argument("--volume", type=float, default=0.0)
    tick.add_argument("--ts", default=None, help="ISO-8601; defaults to now")

    bar = subparsers.add_parser("bar", help="Send one bar as ticks, and optionally verify it")
    bar.add_argument("--symbol", default=None, help=_SYMBOL_HELP)
    bar.add_argument("--timeframe", default=None, help="Defaults to QTE_ENGINE__SIGNAL_TIMEFRAME")
    bar.add_argument("--open", type=float, required=True, dest="open_")
    bar.add_argument("--high", type=float, required=True)
    bar.add_argument("--low", type=float, required=True)
    bar.add_argument("--close", type=float, required=True)
    bar.add_argument("--volume", type=float, default=0.0)
    _add_placement_arguments(bar, default_anchor="next")
    _add_verify_arguments(bar)

    replay = subparsers.add_parser("replay", help="Play a run of bars from a file or generated")
    replay.add_argument("--symbol", default=None, help=_SYMBOL_HELP)
    replay.add_argument("--timeframe", default=None)
    source = replay.add_mutually_exclusive_group()
    source.add_argument(
        "--file",
        default=None,
        help="parquet / csv / jsonl of OHLCV bars; defaults to QTE_SIMULATOR_PARQUET_FILE",
    )
    source.add_argument(
        "--generate",
        type=int,
        nargs="?",
        const=simulator_settings.generate_bars,
        metavar="N",
        help=(
            "Synthesise N bars instead of reading a file; bare --generate uses "
            f"QTE_SIMULATOR__GENERATE_BARS ({simulator_settings.generate_bars})"
        ),
    )
    replay.add_argument(
        "--limit",
        type=int,
        default=simulator_settings.cache_bars,
        help=(
            "Take only the last N bars of the file. Default QTE_SIMULATOR__CACHE_BARS "
            f"({simulator_settings.cache_bars})"
        ),
    )
    replay.add_argument("--start-price", type=float, default=None)
    replay.add_argument("--volatility", type=float, default=0.002)
    replay.add_argument("--drift", type=float, default=0.0)
    replay.add_argument("--seed", type=int, default=None, help="Makes --generate reproducible")
    replay.add_argument(
        "--rate", type=float, default=0.0, help="Bars per second; 0 = as fast as possible"
    )
    _add_placement_arguments(replay, default_anchor="auto")
    _add_verify_arguments(replay)

    walk = subparsers.add_parser("walk", help="Stream a random walk in the background")
    walk.add_argument("--symbol", default=None, help=_SYMBOL_HELP)
    walk.add_argument("--rate", type=float, default=2.0, help="Ticks per second (real time)")
    walk.add_argument("--ticks", type=int, default=0, help="0 = until stopped")
    walk.add_argument("--price", type=float, default=None)
    walk.add_argument("--volatility", type=float, default=0.0005)
    walk.add_argument("--spread", type=float, default=0.0)
    walk.add_argument("--seed", type=int, default=None)
    walk.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help=(
            "Seconds of market time per second of real time. 1 is a live feed; "
            "--speed 60 closes an M15 bar every 15 seconds. Default: 1"
        ),
    )

    stop = subparsers.add_parser("stop", help="Stop a background generator (default: all)")
    stop.add_argument("--name", default=None, help="e.g. walk:XAUUSD")

    subparsers.add_parser("status", help="What the simulator is doing right now")
    subparsers.add_parser("reset", help="Forget cursors and counters")

    watch = subparsers.add_parser("watch", help="Tail closed candles and emitted signals on NATS")
    watch.add_argument("--symbol", default=None, help=_SYMBOL_HELP)
    watch.add_argument("--timeframe", default=None)
    watch.add_argument("--seconds", type=float, default=0.0, help="0 = until Ctrl-C")

    return parser


def _add_placement_arguments(parser: argparse.ArgumentParser, *, default_anchor: str) -> None:
    parser.add_argument(
        "--anchor",
        default=default_anchor,
        help=(
            "next = the first bucket nothing has been sent into yet, marching forward "
            "(nothing the wall-clock flush can race); past = end on the last completed "
            "bucket instead (the wall-clock flush can split historical bars); "
            "auto = next for every source; or an ISO-8601 open time for the first bar. "
            f"Default: {default_anchor}"
        ),
    )
    parser.add_argument("--spread", type=float, default=0.0, help="Decorate ticks with bid/ask")
    parser.add_argument(
        "--no-seal",
        dest="seal",
        action="store_false",
        help="Leave the last bar open instead of closing it with a trailing tick",
    )
    parser.set_defaults(seal=True)


def _add_verify_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Check on NATS that ingestion republished each bar; exit 1 if not",
    )
    parser.add_argument(
        "--expect-signal",
        action="store_true",
        help="Also wait for the strategy runner to emit a signal on this symbol",
    )
    parser.add_argument(
        "--timeout", type=float, default=20.0, help="Seconds to wait when verifying"
    )


# ── Commands ──────────────────────────────────────────────────────────────


async def _tick(args: argparse.Namespace) -> int:
    async with ControlClient(args.url) as client:
        result = await client.send(
            "tick",
            symbol=args.symbol,
            bid=args.bid,
            ask=args.ask,
            last=args.last,
            volume=args.volume,
            ts=args.ts,
        )
    if args.json:
        return _print_json(result)
    print(
        f"tick {result['tick']['symbol']} @ {result['price']} "
        f"({result['tick']['ts']}) → {result['delivered']} feed client(s)"
    )
    return 0


async def _bar(args: argparse.Namespace) -> int:
    row = {
        "open": args.open_,
        "high": args.high,
        "low": args.low,
        "close": args.close,
        "volume": args.volume,
    }
    return await _play(args, [row])


async def _replay(args: argparse.Namespace) -> int:
    timeframe = _timeframe(args)
    source_file = _source_file(args)
    args.anchor = _resolve_anchor(args)
    if source_file is not None:
        try:
            rows = load_bars(source_file, limit=args.limit)
        except SourceError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print(f"Loaded {len(rows)} bars from {source_file}")
    elif args.generate is None:
        print(
            "error: replay needs bars — pass --file, pass --generate, or point "
            "QTE_SIMULATOR_PARQUET_FILE at a cached history file",
            file=sys.stderr,
        )
        return 2
    else:
        if args.generate < 1:
            print("error: --generate needs a positive number of bars", file=sys.stderr)
            return 2
        start = args.start_price or await _continue_from(args)
        # Generated locally and sent as plain OHLCV: placement is the server's
        # job (it owns the cursor), so the open times here are placeholders the
        # server replaces. Keeping generation on this side is what makes --seed
        # reproducible from the terminal that typed it.
        rows = [
            {
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in generate_bars(
                args.symbol,
                timeframe,
                _placeholder_times(args.generate, timeframe),
                start_price=start,
                volatility=args.volatility,
                drift=args.drift,
                seed=args.seed,
            )
        ]
        print(f"Generated {len(rows)} {timeframe} bars from {start:g} (seed={args.seed})")
    return await _play(args, rows)


def _source_file(args: argparse.Namespace) -> str | None:
    """Which file this replay reads, if any.

    ``--file`` wins; a bare ``replay`` falls back to the cached vendor history
    named by ``QTE_SIMULATOR_PARQUET_FILE``, which is what makes a warm-up a
    command with no arguments. ``--generate`` says "synthesise instead" and
    suppresses the fallback even when the setting is filled in.
    """
    if args.file:
        return args.file
    if args.generate is not None:
        return None
    return simulator_settings.parquet_file or None


def _resolve_anchor(args: argparse.Namespace) -> str:
    """Continue the forward series for every source unless placement is explicit.

    File timestamps are discarded anyway. Anchoring file prices in the past
    both races wall-clock closing and loses the whole replay after a generated
    warm-up has advanced ingestion. Preserve OHLCV by default; ``past`` remains
    an explicit diagnostic for late ticks and wall-clock closing.
    """
    if args.anchor != "auto":
        return args.anchor
    return "next"


async def _continue_from(args: argparse.Namespace) -> float:
    """Where a generated run starts: wherever this symbol last was.

    Falling back to a reference price on every command would put a gap between
    two consecutive replays — and a gap is the one thing a resampled feed
    cannot produce, so it would be an artefact of the fixture appearing in the
    chart the strategy reads.
    """
    async with ControlClient(args.url) as client:
        status = await client.send("status")
    return status.get("last_prices", {}).get(args.symbol.upper()) or reference_price(args.symbol)


def _placeholder_times(count: int, timeframe: str) -> list:
    from qte_simulator.bars import anchor_open_times

    return anchor_open_times(count, timeframe, mode="past")


async def _play(args: argparse.Namespace, rows: list[dict[str, Any]]) -> int:
    """Send a run of bars, and report — or verify — what should come out."""
    timeframe = _timeframe(args)
    watcher: FlowWatcher | None = None
    if args.verify or args.expect_signal:
        watcher = FlowWatcher(args.symbol, timeframe)
        try:
            # Before the send, not after: a fast replay produces its candles
            # while the command is still in flight.
            await watcher.start()
        except Exception as exc:
            print(
                f"error: cannot reach NATS at {settings.nats.url} to verify: {exc}", file=sys.stderr
            )
            return 2

    try:
        result = await _send_bars(args, timeframe, rows)

        if args.json:
            _print_json(result)
        else:
            expected = result["expected"]
            print(
                f"Played {result['bars']} {result['symbol']} {result['timeframe']} bars "
                f"as {result['ticks']} ticks → {result['delivered']} feed client(s)"
            )
            print(
                f"  buckets {expected[0]['open_time']} … {expected[-1]['open_time']}"
                f"{'  (+ sealing tick)' if result['sealed'] else '  (last bar left open)'}"
            )
            if not result["delivered"]:
                print(
                    "  no feed client was attached — nothing downstream saw this. "
                    "Is data-ingestion running with QTE_MARKET_DATA__PROVIDER=simulator?"
                )

        if watcher is None:
            return 0
        return await _report_verification(args, watcher, result)
    finally:
        if watcher is not None:
            await watcher.stop()


async def _send_bars(
    args: argparse.Namespace, timeframe: str, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """Play *rows* as one `bars` command, or as several, and merge the answers.

    The server caps a single command at :data:`~qte_simulator.control.MAX_BARS`
    bars, and a cache warm-up sized to the Redis retention is routinely larger
    than that, so a long run goes over in batches. Only the first batch is
    placed on the clock; the rest start immediately after the preceding batch,
    even for an explicit historical anchor. Sealing is left to the last batch — a seal
    between two batches opens a one-tick bucket in the middle of the run, and
    the batch after it would start one bucket late.
    """
    batches = [rows[start : start + MAX_BARS] for start in range(0, len(rows), MAX_BARS)]
    anchor = args.anchor
    if len(batches) > 1:
        anchor = _first_open_time(args.anchor, len(rows), timeframe)
    results: list[dict[str, Any]] = []
    async with ControlClient(args.url) as client:
        for position, batch in enumerate(batches):
            results.append(
                await client.send(
                    "bars",
                    symbol=args.symbol,
                    timeframe=timeframe,
                    bars=batch,
                    anchor=anchor,
                    spread=args.spread,
                    seal=args.seal and position == len(batches) - 1,
                    rate=getattr(args, "rate", 0.0),
                )
            )
            last_candle = Candle.model_validate(results[-1]["expected"][-1])
            anchor = (
                last_candle.open_time + timedelta(seconds=timeframe_seconds(timeframe))
            ).isoformat()
    return _merge_acknowledgements(results)


def _first_open_time(anchor: str, count: int, timeframe: str) -> str:
    """Resolve ``past`` to the explicit open time the whole run starts at.

    Batching hides the length of the run from the server, and ``past`` is
    defined by where the run *ends*: left alone, the server would place the
    first batch against the clock and every later batch after it, so the run
    would finish that many bars into the future. Computing the first bucket
    here keeps a batched run landing exactly where an unbatched one would.
    """
    if anchor != "past":
        return anchor
    return anchor_open_times(count, timeframe, mode="past")[0].isoformat()


def _merge_acknowledgements(results: list[dict[str, Any]]) -> dict[str, Any]:
    """One acknowledgement for a run that went over in several commands.

    Counts add up; the rest of the run is described by its last batch, which is
    the one that sealed it and the one whose delivery count is current.
    """
    merged = dict(results[-1])
    merged["bars"] = sum(result["bars"] for result in results)
    merged["ticks"] = sum(result["ticks"] for result in results)
    merged["expected"] = [row for result in results for row in result["expected"]]
    return merged


async def _report_verification(
    args: argparse.Namespace, watcher: FlowWatcher, result: dict[str, Any]
) -> int:
    expected = [Candle.model_validate(row) for row in result["expected"]]
    checks = await watcher.wait_for_candles(expected, timeout=args.timeout)
    failures = [check for check in checks if not check.ok]

    print(f"\nVerify  {len(checks) - len(failures)}/{len(checks)} candles republished by ingestion")
    for check in _interesting(checks):
        print(f"  [{check.verdict:8}] {check.expected.open_time.isoformat()} {_summarise(check)}")
    if failures and any(check.actual is None for check in failures):
        print(
            "\n  Missing candles mean the bar never came back out of ingestion. Check that\n"
            "  data-ingestion is running, that QTE_MARKET_DATA__PROVIDER=simulator, and that\n"
            f"  {args.symbol} is in QTE_ENGINE__SYMBOLS and {_timeframe(args)} in "
            "QTE_ENGINE__TIMEFRAMES.\n"
            "  If its log says 'Dropping late tick', its resampler is holding a bar ahead of\n"
            "  what you just sent — a forward-anchored run from a previous session. The\n"
            "  simulator's cursor resets when it restarts and ingestion's does not, so\n"
            "  restart data-ingestion too."
        )

    exit_code = 1 if failures else 0
    if args.expect_signal:
        signals = await watcher.wait_for_signal(timeout=args.timeout)
        if signals:
            print(f"\nSignals {len(signals)} emitted on {args.symbol}")
            for payload in signals:
                signal = payload["signal"]
                position = signal["position"]
                print(
                    f"  {signal['strategy']} {position['action']} "
                    f"price={position.get('price')} qty={position.get('quantity')} "
                    f"sl={position.get('sl')} tp1={position.get('tp1')} "
                    f"[{payload['delivery']['status']}] uxid={signal['signal_uxid']}"
                )
        else:
            print(
                f"\nSignals none within {args.timeout:g}s. That is a pass for the pipeline and\n"
                "  a question for the strategy: is it still warming up (the runner logs\n"
                "  'Warm-up n/m candles'), is it mapped to this symbol in\n"
                "  config/strategies_mapping.toml, and did this bar actually meet its rule?"
            )
            exit_code = 1
    return exit_code


def _interesting(checks: list[CandleCheck]) -> list[CandleCheck]:
    """Every failure, plus the first and last success — not 300 OK lines."""
    failures = [check for check in checks if not check.ok]
    if failures:
        return failures[:20]
    return checks[:1] + checks[-1:] if len(checks) > 1 else checks


def _summarise(check: CandleCheck) -> str:
    if check.actual is None:
        return "no candle arrived on NATS"
    if check.mismatches:
        return "; ".join(check.mismatches)
    candle = check.actual
    return (
        f"o={candle.open} h={candle.high} l={candle.low} c={candle.close} "
        f"v={candle.volume} ticks={candle.tick_count}"
    )


async def _walk(args: argparse.Namespace) -> int:
    async with ControlClient(args.url) as client:
        result = await client.send(
            "walk",
            symbol=args.symbol,
            rate=args.rate,
            ticks=args.ticks,
            price=args.price,
            volatility=args.volatility,
            spread=args.spread,
            seed=args.seed,
            speed=args.speed,
        )
    if args.json:
        return _print_json(result)
    print(
        f"Walking {result['symbol']} at {result['rate']}/s from {result['start_price']}, "
        f"market time from {result['starts_at']} at {result['speed']:g}x "
        f"({result['ticks']} ticks)"
    )
    print(f"  stop with `qte-simulator stop --name {result['generator']}`")
    return 0


async def _stop(args: argparse.Namespace) -> int:
    async with ControlClient(args.url) as client:
        result = await client.send("stop", name=args.name)
    if args.json:
        return _print_json(result)
    stopped = result["stopped"]
    print(f"Stopped {', '.join(stopped)}" if stopped else "Nothing was running")
    return 0


async def _reset(args: argparse.Namespace) -> int:
    async with ControlClient(args.url) as client:
        result = await client.send("reset")
    if args.json:
        return _print_json(result)
    print(
        "Cursors and counters cleared"
        + (f", stopped {result['stopped']}" if result["stopped"] else "")
    )
    return 0


async def _status(args: argparse.Namespace) -> int:
    async with ControlClient(args.url) as client:
        result = await client.send("status")
    if args.json:
        return _print_json(result)

    print(
        f"Simulator  up {result['uptime_seconds']}s, {result['ticks_sent']} ticks sent, "
        f"{result['bars_sent']} bars played"
    )
    clients = result["clients"]
    if clients:
        for entry in clients:
            print(
                f"  feed #{entry['id']}  {entry['remote']}  "
                f"{','.join(entry['symbols'])}  {entry['ticks']} ticks"
            )
    else:
        print("  no feed client attached — data-ingestion is not connected")
    if result["generators"]:
        print(f"  running: {', '.join(result['generators'])}")
    for symbol, timeframes in result["cursors"].items():
        for timeframe, moment in timeframes.items():
            price = result["last_prices"].get(symbol)
            print(f"  cursor {symbol} {timeframe} → {moment}  last={price}")
    return 0


async def _watch(args: argparse.Namespace) -> int:
    timeframe = _timeframe(args)
    watcher = FlowWatcher(args.symbol, timeframe)
    await watcher.start()
    print(
        f"Watching QTE.candle.closed.{args.symbol}.{timeframe} and QTE.signal.emitted "
        f"— Ctrl-C to stop"
    )
    seen_candles = 0
    seen_signals = 0
    loop = asyncio.get_running_loop()
    deadline = loop.time() + args.seconds if args.seconds else None
    try:
        while deadline is None or loop.time() < deadline:
            await asyncio.sleep(0.25)
            for open_time in sorted(watcher.candles)[seen_candles:]:
                candle = watcher.candles[open_time]
                print(
                    f"  candle {candle.open_time.isoformat()} o={candle.open} h={candle.high} "
                    f"l={candle.low} c={candle.close} v={candle.volume} "
                    f"ticks={candle.tick_count}"
                )
                seen_candles += 1
            while seen_signals < len(watcher.signals):
                payload = watcher.signals[seen_signals]
                signal = payload["signal"]
                print(
                    f"  SIGNAL {signal['strategy']} {signal['position']['action']} "
                    f"@ {signal['position'].get('price')} [{payload['delivery']['status']}]"
                )
                seen_signals += 1
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await watcher.stop()
    print(f"Saw {seen_candles} candles and {seen_signals} signals")
    return 0


# ── Plumbing ──────────────────────────────────────────────────────────────


def _timeframe(args: argparse.Namespace) -> str:
    return normalize_timeframe(args.timeframe or settings.engine.signal_timeframe)


def _fill_in_symbol(args: argparse.Namespace) -> bool:
    """Default ``--symbol`` to the symbol the engine is actually watching.

    Ingestion subscribes to ``QTE_ENGINE__SYMBOLS`` and nothing else, so a
    rehearsal on any other symbol is delivered to the simulator and then
    silently ignored downstream. Taking the default from the same setting makes
    the common case a command with no arguments and the mistake impossible to
    make by omission.
    """
    if not hasattr(args, "symbol") or args.symbol is not None:
        return True
    if not settings.engine.symbols:
        print(
            "error: no --symbol, and QTE_ENGINE__SYMBOLS is empty — there is nothing "
            "for the engine to receive this on",
            file=sys.stderr,
        )
        return False
    args.symbol = settings.engine.symbols[0]
    return True


def _print_json(payload: Any) -> int:
    print(json.dumps(payload, indent=2, default=str))
    return 0


_COMMANDS = {
    "tick": _tick,
    "bar": _bar,
    "replay": _replay,
    "walk": _walk,
    "stop": _stop,
    "status": _status,
    "reset": _reset,
    "watch": _watch,
}


def run(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)

    if args.command == "serve":
        from qte_simulator.server import main as serve_main

        try:
            asyncio.run(serve_main(args.host, args.port))
        except KeyboardInterrupt:
            log.info("Interrupted")
        except DevOnlyError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 3
        return 0

    if not _fill_in_symbol(args):
        return 2

    try:
        return asyncio.run(_COMMANDS[args.command](args))
    except KeyboardInterrupt:
        return 130
    except SimulatorUnreachable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ControlError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2


def _force_utf8_stdio() -> None:
    """Windows' default console codec (cp1252) cannot encode the arrows and
    check marks the CLI prints; a stray ``→`` in a status line crashes the
    whole command before ``--verify`` gets to run. Reconfiguring stdio to UTF-8
    is a no-op wherever the terminal already speaks it."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


def main() -> None:
    _force_utf8_stdio()
    raise SystemExit(run())


if __name__ == "__main__":
    main()
