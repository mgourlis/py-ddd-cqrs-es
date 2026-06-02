"""Comprehensive tests for the EventBus.

Covers registration, dispatch, pipeline behaviors, and failure isolation.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from pydomain.cqrs import EventBus
from pydomain.cqrs.behaviors import MessageContext, NextHandler
from pydomain.ddd.domain_event import DomainEvent

# ── Sample test types ────────────────────────────────────────────────────────


class _Evt(DomainEvent):
    data: str


class _OtherEvt(DomainEvent):
    value: int


# ════════════════════════════════════════════════════════════════════════════
# Registration
# ════════════════════════════════════════════════════════════════════════════


class TestRegister:
    """EventBus.register()"""

    @pytest.mark.anyio
    async def test_register_single_handler(self) -> None:
        """A single handler is called when event is dispatched."""
        bus = EventBus()
        called: list[bool] = [False]

        async def handler(event: _Evt) -> None:
            called[0] = True

        bus.register(_Evt, handler)
        await bus.dispatch(_Evt(data="test"))

        assert called[0] is True

    @pytest.mark.anyio
    async def test_register_multiple_handlers(self) -> None:
        """Multiple handlers for the same event type are all called."""
        bus = EventBus()
        results: list[int] = []

        async def handler1(event: _Evt) -> None:
            results.append(1)

        async def handler2(event: _Evt) -> None:
            results.append(2)

        bus.register(_Evt, handler1)
        bus.register(_Evt, handler2)
        await bus.dispatch(_Evt(data="test"))

        assert results == [1, 2]

    @pytest.mark.anyio
    async def test_register_multiple_types(self) -> None:
        """Handlers for different event types are isolated."""
        bus = EventBus()
        results: list[str] = []

        async def evt_handler(event: _Evt) -> None:
            results.append(f"evt:{event.data}")

        async def other_handler(event: _OtherEvt) -> None:
            results.append(f"other:{event.value}")

        bus.register(_Evt, evt_handler)
        bus.register(_OtherEvt, other_handler)

        await bus.dispatch(_Evt(data="hello"))
        await bus.dispatch(_OtherEvt(value=42))

        assert results == ["evt:hello", "other:42"]

    @pytest.mark.anyio
    async def test_register_with_behaviors(self) -> None:
        """Behaviors wrap the handler in onion order."""
        bus = EventBus()
        trace: list[str] = []

        class SpyBehavior:
            async def handle(self, ctx: MessageContext, next: NextHandler) -> Any:
                trace.append("before")
                result = await next()
                trace.append("after")
                return result

        async def handler(event: _Evt) -> None:
            trace.append("handler")

        bus.register(_Evt, handler, behaviors=[SpyBehavior()])
        await bus.dispatch(_Evt(data="test"))

        assert trace == ["before", "handler", "after"]


# ════════════════════════════════════════════════════════════════════════════
# Dispatch (single event)
# ════════════════════════════════════════════════════════════════════════════


class TestDispatch:
    """EventBus.dispatch()"""

    @pytest.mark.anyio
    async def test_dispatch_calls_handler(self) -> None:
        """dispatch() invokes the registered handler."""
        bus = EventBus()
        received: list[_Evt] = []

        async def handler(event: _Evt) -> None:
            received.append(event)

        bus.register(_Evt, handler)
        await bus.dispatch(_Evt(data="hello"))

        assert len(received) == 1
        assert received[0].data == "hello"

    @pytest.mark.anyio
    async def test_dispatch_returns_none(self) -> None:
        """dispatch() returns None for events."""
        bus = EventBus()

        async def handler(event: _Evt) -> None:
            pass

        bus.register(_Evt, handler)
        result = await bus.dispatch(_Evt(data="test"))

        assert result is None

    @pytest.mark.anyio
    async def test_dispatch_no_handlers(self) -> None:
        """Event with no registered handler is silently ignored."""
        bus = EventBus()
        result = await bus.dispatch(_Evt(data="no-handler"))

        assert result is None

    @pytest.mark.anyio
    async def test_dispatch_multiple_handlers_in_order(self) -> None:
        """Handlers are called in registration order."""
        bus = EventBus()
        order: list[int] = []

        async def handler1(event: _Evt) -> None:
            order.append(1)

        async def handler2(event: _Evt) -> None:
            order.append(2)

        async def handler3(event: _Evt) -> None:
            order.append(3)

        bus.register(_Evt, handler1)
        bus.register(_Evt, handler2)
        bus.register(_Evt, handler3)

        await bus.dispatch(_Evt(data="test"))

        assert order == [1, 2, 3]


# ════════════════════════════════════════════════════════════════════════════
# Dispatch (batch)
# ════════════════════════════════════════════════════════════════════════════


class TestDispatchMany:
    """EventBus.dispatch_many()"""

    @pytest.mark.anyio
    async def test_dispatch_many_calls_all_handlers(self) -> None:
        """dispatch_many() dispatches each event in the list."""
        bus = EventBus()
        results: list[str] = []

        async def handler(event: _Evt) -> None:
            results.append(event.data)

        bus.register(_Evt, handler)
        await bus.dispatch_many(
            [
                _Evt(data="a"),
                _Evt(data="b"),
                _Evt(data="c"),
            ]
        )

        assert results == ["a", "b", "c"]


# ════════════════════════════════════════════════════════════════════════════
# Failure isolation
# ════════════════════════════════════════════════════════════════════════════


class TestFailureIsolation:
    """Event handler failures are isolated."""

    @pytest.mark.anyio
    async def test_failure_logged_and_swallowed(self, caplog: Any) -> None:
        """A failing handler is logged and does not stop other handlers."""
        bus = EventBus()
        results: list[str] = []

        async def failing_handler(event: _Evt) -> None:
            results.append("fail")
            raise ValueError("handler failure")

        async def success_handler(event: _Evt) -> None:
            results.append("ok")

        bus.register(_Evt, failing_handler)
        bus.register(_Evt, success_handler)

        caplog.set_level(logging.ERROR, logger="pydomain.event_bus")
        await bus.dispatch(_Evt(data="test"))

        assert results == ["fail", "ok"]

        error_logs = [
            r
            for r in caplog.records
            if "Event handler" in r.getMessage() and r.levelno == logging.ERROR
        ]
        assert len(error_logs) == 1
        assert "handler failure" in str(error_logs[0].exc_info[1])

    @pytest.mark.anyio
    async def test_multiple_failures_all_isolated(self) -> None:
        """Multiple failing handlers are all logged; remaining handlers still run."""
        bus = EventBus()
        results: list[str] = []

        async def fail1(event: _Evt) -> None:
            results.append("fail1")
            raise ValueError("first")

        async def ok(event: _Evt) -> None:
            results.append("ok")

        async def fail2(event: _Evt) -> None:
            results.append("fail2")
            raise RuntimeError("second")

        bus.register(_Evt, fail1)
        bus.register(_Evt, ok)
        bus.register(_Evt, fail2)

        await bus.dispatch(_Evt(data="test"))

        assert results == ["fail1", "ok", "fail2"]

    @pytest.mark.anyio
    async def test_failure_in_first_handler_does_not_block_others(self) -> None:
        """First handler failing does not stop subsequent handlers."""
        bus = EventBus()
        results: list[str] = []

        async def fail_first(event: _Evt) -> None:
            results.append("fail")
            raise ValueError("boom")

        async def second(event: _Evt) -> None:
            results.append("second")

        async def third(event: _Evt) -> None:
            results.append("third")

        bus.register(_Evt, fail_first)
        bus.register(_Evt, second)
        bus.register(_Evt, third)

        await bus.dispatch(_Evt(data="test"))

        assert results == ["fail", "second", "third"]


# ════════════════════════════════════════════════════════════════════════════
# Exports
# ════════════════════════════════════════════════════════════════════════════


class TestExports:
    """EventBus is properly exported."""

    def test_eventbus_in_cqrs_module(self) -> None:
        """EventBus is importable from pydomain.cqrs."""
        from pydomain.cqrs import EventBus as EB

        assert EB is EventBus

    def test_eventbus_in_top_level(self) -> None:
        """EventBus is importable from pydomain."""
        import pydomain

        assert pydomain.EventBus is EventBus
