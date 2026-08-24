# Audit: the market data simulator branch

Read of `claude/websocket-simulator-dev-pmmu82` at `caa141c`, 24 Aug 2026.

Baseline at the time of the audit: **399 tests pass**, `ruff check` and `ruff
format --check` both clean. Nothing below came from a failing test — these are
defects the suite does not currently reach.

Findings are ordered by what they cost. Each says whether it was reproduced by
running it or established by reading the code; the repro snippets are inline so
that fixing one starts from a failing case rather than from prose.

| # | Severity | What | Where |
|---|----------|------|-------|
| 1 | High | A flushed bar can be re-opened and republished with wrong OHLC | `resampler.py` |
| 2 | Medium | A bug in the tick handler is reported as a dropped feed | `simulator/feed.py`, `tiingo/ws.py` |
| 3 | Medium | The flush loop has no exception guard | `ingestion/service.py` |
| 4 | Medium | An inverted bracket reaches the wire with SL and TP1 at the same price | `signal_factory.py` |
| 5 | Medium | A second entry orphans the first trade cycle, silently | `signal_factory.py` |
| 6 | Low | A background generator that dies looks like one that finished | `control.py`, `hub.py` |
| 7 | Low | `/control` throws a traceback on disconnect; `/stream` does not | `server.py` |
| 8 | Low | `stop_generators` swallows its own caller's cancellation | `hub.py` |
| 9 | Low | A feed client is attached before it is welcomed | `server.py` |
| 10 | Low | Falsy-zero defaults turn explicit zeros into fallbacks | `cli.py`, `sources.py` |

Findings 1–3 reach past the simulator into the live trading path. That is the
fixture doing its job — it is faithful enough to expose real defects — and also
the reason two of them are already sitting in production code.

---

## 1. A flushed bar can be re-opened and republished with wrong OHLC

**High** · `engines/data_ingestion/src/qte_ingestion/resampler.py` — `Resampler.flush`
· *reproduced*

`flush()` closes a bar and then deletes the builder. The late-tick guard in
`add_tick` only compares against the builder currently open, so once that
builder is gone there is nothing to compare against: the next tick for the same
bucket creates a fresh builder for a bucket already published as
`is_closed=True`, and the next flush publishes it a second time — carrying only
the ticks that arrived after the first flush.

The module docstring says the drop-late-tick branch exists so that "reopening
that bar would repaint a candle strategies have acted on" cannot happen. It can;
the guard just does not survive a flush.

```python
r = Resampler("XAUUSD", ["M15"])
base = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)

r.add_tick(Tick(symbol="XAUUSD", ts=base,                          last=2400.0, volume=1))
r.add_tick(Tick(symbol="XAUUSD", ts=base + timedelta(seconds=200), last=2410.0, volume=1))

first = r.flush(base + timedelta(minutes=15))     # the wall-clock flush fires mid-bar

r.add_tick(Tick(symbol="XAUUSD", ts=base + timedelta(seconds=400), last=2395.0, volume=1))
r.add_tick(Tick(symbol="XAUUSD", ts=base + timedelta(seconds=899), last=2408.0, volume=1))
second = r.flush(base + timedelta(minutes=15))
```

```
flush #1: ('2026-08-24T09:00:00+00:00', o=2400.0, h=2410.0, l=2400.0, c=2410.0, ticks=2)
flush #2: ('2026-08-24T09:00:00+00:00', o=2395.0, h=2408.0, l=2395.0, c=2408.0, ticks=2)

SAME open_time published twice? True
```

The real bar is `o=2400 h=2410 l=2395 c=2408`. Neither published candle is it.

Downstream makes this worse rather than better. `StrategyRunner._feed_candle`
drops any candle whose `open_time` does not advance, so the **first, torn**
candle is the one the strategy acts on and the corrected one is discarded at
`log.debug`. The strategy trades a bar that never printed, and nothing above
debug level says so.

This is not simulator-only. It needs a tick that arrives after its own bucket
has ended — which is what feed lag and a vendor's post-reconnect replay produce,
the `TiingoLiveFeed` reconnect path included. In the simulator it is the
documented hazard of `--anchor past`, and `--verify` reports it as a MISMATCH;
in production nobody is running `--verify`.

**Fix.** Keep a per-timeframe watermark of the last bucket closed — set it in
both `flush()` and the bucket-advance branch of `add_tick` — and drop any tick
whose bucket is at or below it, reusing the existing "Dropping late tick"
warning. That makes the guard independent of whether a builder happens to be
alive, and it also covers `restore()` resurrecting a bucket that was published
just before the process died.

---

## 2. A bug in the tick handler is reported as a dropped feed

**Medium** · `providers/simulator/feed.py` and `providers/tiingo/ws.py` — `_run`
· *reproduced*

`await self._on_tick(tick)` runs inside the same `try` that wraps
`websockets.connect` and the receive loop, under a bare `except Exception`. Any
exception the consumer raises is caught by the reconnect handler, which tears
down a healthy socket, blames the far end, and backs off.

Three ticks sent, handler raises on the first:

```
WARNING [...simulator.feed] Simulator feed dropped (attempt 1):
        a bug inside ingestion's _handle_tick — retrying in 1.0s
INFO    [qte_simulator.hub] Feed client detached id=1 after 1 ticks

>>> handler was called 1 time(s) for 3 ticks sent
>>> feed.ticks_received = 1
```

Ticks 2 and 3 were lost during backoff; the socket was destroyed for a reason
that had nothing to do with it; and the log sends whoever is on call to the
wrong service. In production `_handle_tick` writes to Redis and publishes to
NATS, so a Redis blip reads as "Tiingo fx socket dropped".

The identical structure is in `TiingoLiveFeed._run`, so this is the live path,
not a fixture quirk.

**Fix.** Wrap the handler call in `_handle_raw` in its own `try`, log it as a
consumer failure with `log.exception`, and continue the receive loop — the shape
`NatsBus.subscribe` already uses for its `guarded` wrapper, re-raising
`CancelledError`. One bad tick should not cost a connection.

---

## 3. The flush loop has no exception guard

**Medium** · `engines/data_ingestion/src/qte_ingestion/service.py` — `_flush_loop`
· *static read*

The loop body calls `_emit_candle` — which touches Redis and NATS — with no
`try` anywhere in it. A single raised exception ends the task permanently.
Nothing awaits `_flush_task` until `stop()`, so nothing observes the failure,
and asyncio's "Task exception was never retrieved" only surfaces whenever the
task is garbage-collected.

What breaks is the guarantee the resampler docstring opens with: bars close on
the wall clock so a quiet market still produces candles. After this task dies,
bars only close when a later tick pushes the bucket over. On an illiquid symbol
out of session that can be a long time, and the service goes on looking healthy
— feeds attached, ticks flowing, no error.

**Fix.** Put a `try/except Exception: log.exception(...)` inside the `while`, so
one failed publish costs one flush interval rather than the loop. Re-raise
`CancelledError` so `stop()` still unwinds cleanly.

---

## 4. An inverted bracket reaches the wire with SL and TP1 at the same price

**Medium** · `engines/shared/src/qte_shared/signal_factory.py` — `BracketPolicy.apply`,
and `models.py` — `validate_shape` · *reproduced*

`BracketPolicy` takes the strategy's stop on trust and derives targets from
`risk = abs(price - sl)`. The absolute value means a stop on the *wrong side* of
the entry produces targets on the wrong side too — and because `tp1_r` is 1.0,
TP1 lands exactly on the stop.

```
# LONG at 2400, strategy sets sl=2450
action=LONG price=2400.0 sl=2450.0 tp1=2450.0 tp2=2500.0
-> stop is ABOVE the long entry, targets are above it
-> validate_shape() accepted it and it is now on the wire.
```

`validate_shape` is explicitly the place that catches "a payload the broker
would take but a worker cannot fill". It checks price and quantity, but not
which side of the entry the stop is on. A long whose stop sits above its entry
is stopped out on the fill; a stop and a target at one price is an order no
worker can sensibly resolve. A sign error in an ATR stop gets you here.

**Fix.** In `validate_shape`, for an entry carrying a stop, require `sl < price`
for LONG and `sl > price` for SHORT, and require each TP on the profit side.
Raising there keeps it off the wire entirely, which is that method's stated
contract. The factory is shared, so the backtest starts refusing the same signal
— which is the point: better a red backtest than a matching pair of bad fills.

---

## 5. A second entry orphans the first trade cycle, silently

**Medium** · `engines/shared/src/qte_shared/signal_factory.py` — `SignalFactory.build`
· *reproduced*

An entry writes `self._open_cycles[target_symbol] = uxid` unconditionally. If a
cycle is already open on that symbol the new id overwrites it, so every later
close resolves to the newer cycle and the first can never be closed.

```
first  LONG uxid=A7CA47741754441C  open_cycle=A7CA47741754441C
second LONG uxid=0C3A288C2E884337  open_cycle=0C3A288C2E884337
FLAT closes uxid=0C3A288C2E884337
-> cycle A7CA47741754441C can never be closed. No warning was logged.
```

The class docstring calls cycle ids something a strategy is "deliberately not
allowed" to own, because getting them wrong makes the broker render an exit as a
separate trade. This is that failure from the other direction: the broker keeps
broadcasting a trade QTE has forgotten how to close. Contrast the exit path,
which refuses loudly — a close with no cycle raises `ValueError` with a good
message. The entry path deserves the same standard.

**Fix.** At minimum `log.warning` when an entry overwrites a live cycle, naming
both ids so the orphan is greppable. Better: decide the policy explicitly —
refuse the second entry, or emit a FLAT for the old cycle first — since silently
pyramiding is unlikely to be what any strategy meant.

---

## 6. A background generator that dies looks like one that finished

**Low** · `qte_simulator/control.py` — `_walk.run`, and `hub.py` —
`register_generator` · *reproduced*

The walk task body has no exception handling, and the only thing watching it is
a done-callback that pops it from the registry. A crash and a clean finish leave
identical observable state.

```
after start,      status.generators = ['walk:XAUUSD']
after the crash,  status.generators = []
`stop` now reports: {'stopped': []}
```

Only asyncio's own "Task exception was never retrieved" ever mentions it, and
only whenever the task is collected. For a fixture whose job is to answer "did
the simulator send it, or did ingestion drop it?", a generator that can stop for
an unstated reason works against the purpose. `_walk` already logs a tidy "Walk
finished" on the success path; the failure path logs nothing.

**Fix.** Wrap the `run()` body in `try / except asyncio.CancelledError: raise /
except Exception: log.exception(...)`, and keep the last failure on the hub so
`status` can report `walk:XAUUSD — failed: …` instead of omitting it.

---

## 7. `/control` throws a traceback on disconnect; `/stream` does not

**Low** · `engines/market_simulator/src/qte_simulator/server.py` — `_serve_control`
· *reproduced*

`_serve_stream` wraps its receive loop in `try/except Exception` and logs one
debug line. `_serve_control` has no equivalent, so an abnormal close propagates
out of the connection handler. The same abrupt disconnect on each path:

```
===== killing a /control client mid-session =====
ERROR [websockets.server] connection handler failed
Traceback (most recent call last):
  File ".../qte_simulator/server.py", line 133, in _serve_control
    async for raw in connection:
websockets.exceptions.ConnectionClosedError: no close frame received or sent

===== killing a /stream client mid-session =====
INFO  [qte_simulator.hub] Feed client detached id=1 after 0 ticks
```

The trigger is routine — Ctrl-C during a paced `replay`, or a container going
away. An ERROR-level traceback for an expected event is noise in exactly the log
someone is reading to diagnose something else.

**Fix.** Give `_serve_control` the guard `_serve_stream` has, with a debug line
naming the remote.

---

## 8. `stop_generators` swallows its own caller's cancellation

**Low** · `engines/market_simulator/src/qte_simulator/hub.py` — `SimulatorHub.stop_generators`
· *static read*

The cleanup is `except (asyncio.CancelledError, Exception): pass`. Catching
`CancelledError` is right for the task being cancelled deliberately — but the
same handler also catches a `CancelledError` delivered because the *caller* was
cancelled while awaiting, and discards it. The caller then carries on as though
it were never cancelled. Reachable through `serve_forever`'s shutdown path and
through the `reset` command.

It is also the one place a dying generator's real exception could be surfaced,
and instead is dropped — the other half of finding 6.

**Fix.** Split the handlers: `except asyncio.CancelledError` re-raising when
`asyncio.current_task().cancelling()` is set, and `except Exception` logging what
the generator died of.

---

## 9. A feed client is attached before it is welcomed

**Low** · `engines/market_simulator/src/qte_simulator/server.py` — `_serve_stream`
· *static read*

`hub.attach()` registers the subscriber, and only then is the welcome frame
sent. A generator publishing on another task can reach this subscriber's `send`
before the welcome does, and the client sees a tick as its first frame. With no
subscribe filter set yet, `Subscriber.wants` returns true for everything, so the
window is open to all symbols.

Narrow, and harmless with the client in this repo — `SimulatorLiveFeed`
dispatches on frame `type` and does not care about order. It is a documented
protocol though: `protocol.py` shows welcome as the first frame, and the module
exists to be opened by hand with `websocat`. A stricter client written to that
doc would be right and would still break.

**Fix.** Send the welcome frame first, then attach.

---

## 10. Falsy-zero defaults turn explicit zeros into fallbacks

**Low** · `qte_simulator/cli.py`, `sources.py`, `control.py` · *static read*

`control.py` gets this right and says why — `_defaulted()` exists because
"`x or default` would rewrite an explicit zero into the default, which turns
`--rate 0` from a refusal into a surprise". Four places elsewhere still use the
`or` form:

- `cli.py` — `args.start_price or await _continue_from(args)`: `--start-price 0`
  silently queries the server instead.
- `cli.py` `_continue_from` and `control.py` `_walk` — a genuine last price of
  `0.0` falls back to `reference_price()`, putting a jump in the series the
  surrounding comments are specifically trying to avoid.
- `sources.py` — `if limit:` means `--limit 0` loads the whole file, not nothing.

None of these is likely to bite in practice; a zero price is not a real quote.
Worth aligning anyway, because the codebase has already decided which form it
prefers and written down why.

**Fix.** Use `is None` checks, or reuse `_defaulted`, at each of the four sites.

---

## What is already right

Worth stating plainly, because it shapes how much of the above is urgent.

- `require_dev_env` is called in the constructor of both halves, before a port is
  bound — and the "no override flag" argument in its docstring is correct.
- The cursor model genuinely solves the problem it claims to. Successive commands
  form one continuous series, the seal tick lands in the right bucket, and
  `_series_end` correctly treats a loose tick as having touched a bucket without
  it being a bar.
- `expected_candle` is derived from the bar rather than from the synthesised
  ticks, so `--verify` cannot agree with itself by construction. That is the
  difference between a test and a tautology.
- `FlowWatcher` subscribes before the send, so a fast replay cannot outrun its
  own verifier.
- The rounding in `generate_bars` is safe: rounding is monotonic, so the OHLC
  containment invariant survives it. Checked because it looked like a likely bug
  and is not.
- `MAX_BARS` at 5000 fits the websockets 1 MiB frame limit with room — about
  0.68 MiB at real-file float precision, against a ceiling near 7,400 bars. Thin
  headroom rather than a defect; worth a comment if `MAX_BARS` ever rises.

Findings 1 and 3 are the two to fix before this branch runs anywhere that
matters. Both are small, self-contained changes in `data_ingestion`, and both
are testable without a live feed.
