import asyncio
import typing as t

import pytest

from comio import pipe, PipeConfig
from comio.adapters.queue import QueueListener, QueueWriter


class ListListener:
    def __init__(self, items: list):
        self._items = items

    async def listen(self) -> t.AsyncIterator:
        for item in self._items:
            yield item


class CollectWriter:
    def __init__(self):
        self.items: list = []

    async def write(self, item) -> None:
        self.items.append(item)


@pytest.mark.asyncio
async def test_pipe_basic():
    src = ListListener([1, 2, 3])
    dst = CollectWriter()

    async def double(item: int) -> int:
        return item * 2

    await pipe(src, dst, double)

    assert dst.items == [2, 4, 6]


@pytest.mark.asyncio
async def test_pipe_with_hooks():
    src = ListListener(["a", "b"])
    dst = CollectWriter()
    log: list[str] = []

    async def on_boot():
        log.append("boot")

    async def on_close():
        log.append("close")

    cfg = PipeConfig(on_boot=[on_boot], on_close=[on_close])

    async def identity(item: str) -> str:
        return item

    await pipe(src, dst, identity, cfg=cfg)

    assert log == ["boot", "close"]
    assert dst.items == ["a", "b"]


@pytest.mark.asyncio
async def test_pipe_with_buffer():
    src = ListListener(list(range(10)))
    dst = CollectWriter()

    async def inc(item: int) -> int:
        return item + 1

    await pipe(src, dst, inc, cfg=PipeConfig(buffer=5))

    assert dst.items == list(range(1, 11))


@pytest.mark.asyncio
async def test_pipe_via_queue():
    q: asyncio.Queue[int] = asyncio.Queue()
    src = QueueListener(q)
    dst = CollectWriter()

    for i in range(3):
        q.put_nowait(i)
    src.close()

    async def square(item: int) -> int:
        return item * item

    await pipe(src, dst, square)

    assert dst.items == [0, 1, 4]


@pytest.mark.asyncio
async def test_pipe_chained_via_queue():
    """Two pipes chained through a queue — the pattern from Philosophy.md."""
    glue_q: asyncio.Queue[int] = asyncio.Queue()
    glue_src = QueueListener(glue_q)
    glue_dst = QueueWriter(glue_q)

    src = ListListener([1, 2, 3])
    dst = CollectWriter()

    async def double(item: int) -> int:
        return item * 2

    async def add_ten(item: int) -> int:
        return item + 10

    # pipe1: src -> double -> glue_q
    await pipe(src, glue_dst, double)
    glue_src.close()

    # pipe2: glue_q -> add_ten -> dst
    await pipe(glue_src, dst, add_ten)

    assert dst.items == [12, 14, 16]
