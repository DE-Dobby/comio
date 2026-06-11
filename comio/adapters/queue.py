"""Asyncio queue adapter for comio.

Wraps an asyncio.Queue as both a Listener and a Writer/Batcher,
useful for gluing pipes together in-process.
"""

from __future__ import annotations

import asyncio
import typing as t

T = t.TypeVar("T")

_DONE = object()


class QueueListener(t.Generic[T]):
    """Listener backed by an asyncio.Queue.

    Yields items from the queue until ``close()`` is called
    and all remaining items are drained.
    """

    def __init__(self, q: asyncio.Queue[T]) -> None:
        self._q = q
        self._closed = False

    def close(self) -> None:
        """Signal that no more items will be added."""
        self._q.put_nowait(_DONE)

    async def listen(self) -> t.AsyncIterator[T]:
        while True:
            item = await self._q.get()
            if item is _DONE:
                break
            yield item


class QueueWriter(t.Generic[T]):
    """Writer/Batcher backed by an asyncio.Queue."""

    def __init__(self, q: asyncio.Queue[T]) -> None:
        self._q = q

    async def write(self, item: T) -> None:
        await self._q.put(item)

    async def batch(self, items: t.Sequence[T]) -> None:
        for item in items:
            await self._q.put(item)
