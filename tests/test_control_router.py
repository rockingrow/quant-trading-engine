"""The control router answers every request, even the ones it cannot serve."""

from __future__ import annotations

import json

from qte_shared.bus import CONTROL_ERROR_KEY, ControlRouter, is_control_failure


class Message:
    def __init__(self, data: bytes, reply: str = "reply.subject") -> None:
        self.data = data
        self.reply = reply


class ReplyBus:
    def __init__(self) -> None:
        self.replies: list[tuple[str, dict]] = []
        self.nc = self

    async def publish(self, subject, payload) -> None:
        self.replies.append((subject, json.loads(payload)))


def router_with(**handlers) -> ControlRouter:
    router = ControlRouter("test-service")
    for action, handler in handlers.items():
        router.register(action, handler)
    return router


async def answer(router: ControlRouter, data: bytes, reply: str = "reply.subject"):
    bus = ReplyBus()
    await router.dispatch(Message(data, reply), reply_bus=bus)
    return bus.replies


async def test_a_handler_reply_is_published_to_the_reply_subject():
    async def ping(command):
        return {"pong": command["action"]}

    assert await answer(router_with(ping=ping), b'{"action": "ping"}') == [
        ("reply.subject", {"pong": "ping"})
    ]


async def test_a_broadcast_handler_returning_none_sends_nothing():
    async def notify(command):
        return None

    assert await answer(router_with(set_shadow_mode=notify), b'{"action": "set_shadow_mode"}') == []


async def test_json_that_is_not_an_object_is_answered_not_raised():
    for payload in (b"[]", b'"ping"', b"42", b"not json at all"):
        [(_, body)] = await answer(router_with(), payload)
        assert is_control_failure(body)


async def test_a_handler_that_raises_still_answers():
    async def flat(command):
        raise RuntimeError("Redis went away")

    [(_, body)] = await answer(router_with(flat=flat), b'{"action": "flat"}')
    assert is_control_failure(body)
    assert "RuntimeError" in body[CONTROL_ERROR_KEY]
    assert "Redis went away" in body[CONTROL_ERROR_KEY]


async def test_nothing_is_published_without_a_reply_subject():
    async def flat(command):
        raise RuntimeError("boom")

    assert await answer(router_with(flat=flat), b'{"action": "flat"}', reply="") == []


def test_an_action_cannot_be_registered_twice():
    import pytest

    async def handler(command):
        return None

    router = router_with(ping=handler)
    with pytest.raises(ValueError, match="already handles"):
        router.register("ping", handler)


def test_a_handler_s_own_error_answer_is_not_a_failure():
    # `flat` answers an empty scope with an `error` of its own: that is an
    # answer, and a client must not read it as a service that is down.
    assert not is_control_failure({"error": "the scope must name everything"})
    assert is_control_failure({CONTROL_ERROR_KEY: "status failed"})
