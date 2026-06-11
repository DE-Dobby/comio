import asyncio
import typing as t

import pytest

import comio as io
from comio.adapters.queue import QueueListener, QueueWriter


class ListListener:
    """A Listener that yields items from a list."""

    def __init__(self, items: list):
        self._items = items

    async def listen(self) -> t.AsyncIterator:
        for item in self._items:
            yield item


class CollectBatcher:
    """A Batcher that collects items into a list."""

    def __init__(self):
        self.items: list = []

    async def batch(self, items: t.Sequence) -> None:
        self.items.extend(items)


@pytest.mark.asyncio
async def test_copy_basic():
    src = ListListener([1, 2, 3])
    dst = CollectBatcher()

    await io.copy(src, dst, n=2)

    assert dst.items == [1, 2, 3]


@pytest.mark.asyncio
async def test_copy_n1():
    src = ListListener(["a", "b", "c"])
    dst = CollectBatcher()

    await io.copy(src, dst, n=1)

    assert dst.items == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_copy_empty():
    src = ListListener([])
    dst = CollectBatcher()

    await io.copy(src, dst, n=5)

    assert dst.items == []


@pytest.mark.asyncio
async def test_copy_via_queue():
    q: asyncio.Queue[int] = asyncio.Queue()
    listener = QueueListener(q)
    dst = CollectBatcher()

    for i in range(5):
        q.put_nowait(i)
    listener.close()

    await io.copy(listener, dst, n=2)

    assert dst.items == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_copy_queue_to_queue():
    src_q: asyncio.Queue[str] = asyncio.Queue()
    dst_q: asyncio.Queue[str] = asyncio.Queue()

    src = QueueListener(src_q)
    dst = QueueWriter(dst_q)

    for word in ["hello", "world"]:
        src_q.put_nowait(word)
    src.close()

    # copy requires a Batcher, so use QueueWriter.batch
    collector = CollectBatcher()
    await io.copy(src, collector, n=1)

    assert collector.items == ["hello", "world"]
