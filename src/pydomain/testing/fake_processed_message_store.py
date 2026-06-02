"""In-memory fake of ``ProcessedMessageStore`` for testing."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydomain.cqrs.idempotency import MISSING, ProcessedMessageStore


class FakeProcessedMessageStore(ProcessedMessageStore):
    """In-memory fake implementation of :class:`ProcessedMessageStore`.

    Uses a plain ``dict`` keyed by message UUID. Safe for single-threaded
    async test scenarios — not suitable for multi-threaded or multi-process
    tests.
    """

    def __init__(self) -> None:
        self._store: dict[UUID, Any] = {}

    async def get(self, message_id: UUID) -> Any:
        """Return the cached result, or ``MISSING`` if not found."""
        return self._store.get(message_id, MISSING)

    async def set(self, message_id: UUID, result: Any) -> None:
        """Store *result* keyed by *message_id*."""
        self._store[message_id] = result

    async def contains(self, message_id: UUID) -> bool:
        """Return ``True`` if *message_id* is present."""
        return message_id in self._store

    async def check_and_set(self, message_id: UUID) -> bool:
        """Atomically check and mark *message_id*.

        Returns ``True`` if already processed, ``False`` if this is the
        first time.
        """
        if message_id in self._store:
            return True
        self._store[message_id] = True
        return False
