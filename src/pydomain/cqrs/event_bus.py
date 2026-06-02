"""Event bus for the CQRS layer.

The ``EventBus`` routes domain events to their registered handlers and
returns ``None``.  Multiple handlers are supported per event type and
run sequentially in registration order with failure isolation — a single
handler failure is logged and swallowed; remaining handlers continue.
"""

from __future__ import annotations

import logging

from pydomain.cqrs.behaviors import (
    MessageContext,
    MessageKind,
    MessagePipeline,
    PipelineBehavior,
)
from pydomain.cqrs.handlers import EventHandler
from pydomain.ddd.domain_event import DomainEvent

logger = logging.getLogger("pydomain.event_bus")


class EventBus:
    """Routes domain events to their registered handlers.

    Supports `N` handlers per event type.  Handlers are invoked
    sequentially in registration order.  If a handler raises, the
    exception is logged and swallowed — the remaining handlers continue
    uninterrupted.

    Use pipeline behaviors (such as
    :class:`~pydomain.cqrs.behaviors.EventIdempotencyBehavior`) to add
    cross-cutting concerns like idempotency at the handler level.
    """

    def __init__(self) -> None:
        self._handlers: dict[type[DomainEvent], list[MessagePipeline]] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register[TEvent: DomainEvent](
        self,
        event_type: type[TEvent],
        handler: EventHandler[TEvent],
        behaviors: list[PipelineBehavior] | None = None,
    ) -> None:
        """Register an event handler.

        Multiple handlers can be registered for the same event type.
        Handlers are invoked in registration order.

        The handler is wrapped in a ``MessagePipeline`` at registration
        time.  If pipeline behaviors are provided, they are composed
        around the handler in onion order (first behavior is outermost).

        Parameters
        ----------
        event_type:
            The domain event class to handle.
        handler:
            An ``EventHandler`` that receives a domain event instance.
            Event handlers return ``None`` (fire-and-forget).
        behaviors:
            Optional list of pipeline behaviors that wrap the handler.
        """
        self._handlers.setdefault(event_type, []).append(
            MessagePipeline(handler=handler, behaviors=behaviors),
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    async def dispatch(self, event: DomainEvent) -> None:
        """Dispatch a single event to its registered handlers.

        Parameters
        ----------
        event:
            The domain event to dispatch.
        """
        await self._execute(event)

    async def dispatch_many(self, events: list[DomainEvent]) -> None:
        """Dispatch events sequentially, one at a time.

        Event N+1 handlers run only after ALL handlers for event N finish.
        No concurrency — events are dispatched in strict sequence.

        Parameters
        ----------
        events:
            The domain events to dispatch.
        """
        for event in events:
            await self._execute(event)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _execute(self, event: DomainEvent) -> None:
        """Dispatch event to all registered handlers with failure isolation."""
        pipelines = self._handlers.get(type(event), [])
        for pipeline in pipelines:
            ctx = MessageContext(
                message=event,
                kind=MessageKind.EVENT,
                uow=None,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                metadata={},
            )
            try:
                await pipeline.execute(ctx, event)
            except Exception:
                logger.exception(
                    "Event handler %s failed for %s",
                    getattr(ctx.handler, "__name__", str(pipeline)),
                    type(event).__name__,
                )
