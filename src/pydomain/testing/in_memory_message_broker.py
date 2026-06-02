"""In-memory message broker for testing."""

from __future__ import annotations

from typing import Any

from pydomain.cqrs.integration_events import IntegrationEvent


class InMemoryMessageBroker:
    """In-memory message broker for testing.

    Captures all published events in a list for test assertions.
    ``start()`` and ``stop()`` are no-ops.

    Parameters
    ----------
    published:
        List of ``(topic, event, headers)`` tuples captured from
        ``publish()`` calls.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, IntegrationEvent, dict[str, str]]] = []

    async def publish(
        self,
        topic: str,
        event: IntegrationEvent,
        **kwargs: Any,
    ) -> None:
        """Append ``(topic, event, headers)`` to the published list.

        Parameters
        ----------
        topic:
            The topic the event was published to.
        event:
            The published integration event.
        **kwargs:
            Forwarded to ``published`` entries when they contain a
            ``headers`` key.
        """
        headers: dict[str, str] = kwargs.get("headers", {})
        self.published.append((topic, event, headers))

    async def start(self) -> None:
        """No-op. Included for protocol conformance."""

    async def stop(self) -> None:
        """No-op. Included for protocol conformance."""
