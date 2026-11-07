"""The bot reads a service that could not serve a request as one that is down."""

from __future__ import annotations

from qte_bot.engine_client import EngineClient


class AnsweringBus:
    def __init__(self, answer) -> None:
        self.answer = answer

    async def request(self, subject, payload, timeout=5.0):
        return self.answer


async def test_a_control_error_reads_as_no_runner():
    # Before: the error reply was shown as a runner with no strategies and
    # delivery "live". A runner that cannot answer `status` is not one.
    failing = AnsweringBus({"control_error": "status failed: ConnectionError: Redis"})
    assert await EngineClient(bus=failing).runner_status() is None


async def test_a_handler_s_own_error_is_still_an_answer():
    refusal = {"error": "the scope must name everything, a symbol or a strategy"}
    assert await EngineClient(bus=AnsweringBus(refusal)).flat() == refusal
