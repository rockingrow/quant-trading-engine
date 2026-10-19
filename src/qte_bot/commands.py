"""The command menu, and the help text that describes it.

One list, because this bot has one audience: the operators in
``QTE_BOT__ADMIN_IDS``. The broker's bot needs three menus because it serves
end users who may or may not have linked an account; here every command reads
or changes a live trading engine, so there is nothing to show someone who is
not an operator.

Ordered the way the work goes: look first (``/redis`` … ``/closed``), then act
(``/flat`` … ``/flush``).
"""

from __future__ import annotations

from aiogram.types import BotCommand

COMMANDS = [
    BotCommand(command="redis", description="Warm-up bars held per symbol"),
    BotCommand(command="runner", description="Strategies and whether they can decide"),
    BotCommand(command="positions", description="Open positions"),
    BotCommand(command="closed", description="Recently closed positions"),
    BotCommand(command="flat", description="Close positions: all | SYMBOL | STRATEGY"),
    BotCommand(command="prevent", description="Stop deciding: all | SYMBOL | STRATEGY"),
    BotCommand(command="allow", description="Resume deciding: all | SYMBOL | STRATEGY"),
    BotCommand(command="warmup", description="Re-request warm-up bars: all | SYMBOL"),
    BotCommand(command="flush", description="Drop warm-up bars: all | SYMBOL"),
    BotCommand(command="help", description="What each command does"),
]

HELP = """<b>QTE engine</b> — book <code>{namespace}</code>

<b>Look</b>
/redis — bars held per symbol and timeframe, and the newest one
/runner — every strategy, its window, and whether it can decide on the next bar
/positions — what is open right now
/closed — positions that are over, newest first

<b>Act</b>
/flat <code>all | SYMBOL | STRATEGY</code> — close positions (asks first)
/prevent <code>all | SYMBOL | STRATEGY</code> — stop deciding on closed bars
/allow <code>all | SYMBOL | STRATEGY</code> — resume deciding
/warmup <code>all | SYMBOL</code> — ask the vendor for the warm-up window again
/flush <code>all | SYMBOL</code> — drop the warm-up bars Redis holds

<b>What /prevent does not do</b>
Bars keep arriving and keep filling the window; only the decision is skipped,
so /allow resumes on the next bar with no hole behind it. It survives a runner
restart, and it does not touch a position that is already open — use /flat.
"""
