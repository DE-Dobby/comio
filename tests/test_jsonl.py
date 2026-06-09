import io
import json
import pytest

import comio
import typing as t

from comio import scroll, read_all, EOF
from comio.sync import scroll as sync_scroll, read_all as sync_read_all
from comio.adapters.jsonl import JsonL, AsyncJsonL


@pytest.fixture
def sample_file():
    """Create an in-memory JSONL file with 3 lines."""
    buf = io.StringIO()
    for i in range(3):
        buf.write(json.dumps({"id": i}) + "\n")
    buf.seek(0)
    return buf


# ── JsonL (sync) ── comio.sync.Reader / comio.sync.Writer ──


def test_sync_read_single_page(sample_file):
    reader = JsonL(sample_file)
    page = reader.read()
    assert page.items == [{"id": 0}]
    assert page.next_cursor is not EOF


def test_sync_read_with_cursor(sample_file):
    reader = JsonL(sample_file)
    first = reader.read()
    second = reader.read(cursor=first.next_cursor)
    assert second.items == [{"id": 1}]


def test_sync_read_eof(sample_file):
    reader = JsonL(sample_file)
    cursor = None
    for _ in range(3):
        page = reader.read(cursor=cursor)
        cursor = page.next_cursor
    last = reader.read(cursor=cursor)
    assert last.items == []
    assert last.next_cursor is EOF


def test_sync_scroll(sample_file):
    reader = JsonL(sample_file)
    pages = list(sync_scroll(reader))
    assert len(pages) == 3
    assert [p.items[0]["id"] for p in pages] == [0, 1, 2]


def test_sync_read_all(sample_file):
    reader = JsonL(sample_file)
    items = sync_read_all(reader)
    assert items == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_sync_write(tmp_path):
    path = tmp_path / "out.jsonl"
    with open(path, "w") as f:
        writer = JsonL(f)
        writer.write({"a": 1})
        writer.write({"b": 2})

    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1}
    assert json.loads(lines[1]) == {"b": 2}


def test_sync_read_all_from_cursor(sample_file):
    reader = JsonL(sample_file)
    first = reader.read()
    items = sync_read_all(reader, cursor=first.next_cursor)
    assert items == [{"id": 1}, {"id": 2}]


# ── AsyncJsonL ── comio.io.Reader / comio.io.Writer ──


@pytest.mark.asyncio
async def test_async_read_single_page(sample_file):
    reader = AsyncJsonL(sample_file)
    page = await reader.read()
    assert page.items == [{"id": 0}]
    assert page.next_cursor is not EOF


@pytest.mark.asyncio
async def test_async_read_with_cursor(sample_file):
    reader = AsyncJsonL(sample_file)
    first = await reader.read()
    second = await reader.read(cursor=first.next_cursor)
    assert second.items == [{"id": 1}]


@pytest.mark.asyncio
async def test_async_read_eof(sample_file):
    reader = AsyncJsonL(sample_file)
    cursor = None
    for _ in range(3):
        page = await reader.read(cursor=cursor)
        cursor = page.next_cursor
    last = await reader.read(cursor=cursor)
    assert last.items == []
    assert last.next_cursor is EOF


@pytest.mark.asyncio
async def test_async_scroll(sample_file):
    reader = AsyncJsonL(sample_file)
    pages = []
    async for page in scroll(reader):
        pages.append(page)
    assert len(pages) == 3
    assert [p.items[0]["id"] for p in pages] == [0, 1, 2]


@pytest.mark.asyncio
async def test_async_read_all(sample_file):
    reader = AsyncJsonL(sample_file)
    items = await read_all(reader)
    assert items == [{"id": 0}, {"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_async_write(tmp_path):
    path = tmp_path / "out.jsonl"
    with open(path, "w") as f:
        writer = AsyncJsonL(f)
        await writer.write({"a": 1})
        await writer.write({"b": 2})

    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1}
    assert json.loads(lines[1]) == {"b": 2}


@pytest.mark.asyncio
async def test_async_read_all_from_cursor(sample_file):
    reader = AsyncJsonL(sample_file)
    first = await reader.read()
    items = await read_all(reader, cursor=first.next_cursor)
    assert items == [{"id": 1}, {"id": 2}]


class AsyncJsonLBatcher:
    def __init__(self, f):
        self.f = f

    async def batch(self, items: t.Sequence[dict]) -> None:
        for item in items:
            self.f.write(json.dumps(item) + "\n")
        self.f.flush()


@pytest.mark.asyncio
async def test_copy_via_as_listener(sample_file):
    reader = AsyncJsonL(sample_file)
    out = io.StringIO()
    batcher = AsyncJsonLBatcher(out)

    listener = comio.as_listener(reader)
    await comio.copy(listener, batcher, n=2)

    out.seek(0)
    lines = [json.loads(l) for l in out.read().strip().split("\n")]
    assert lines == [{"id": 0}, {"id": 1}, {"id": 2}]



@pytest.mark.asyncio
async def test_pipe(sample_file):
    from comio import pipe

    reader = AsyncJsonL(sample_file)
    out = io.StringIO()
    writer = AsyncJsonL(out)
    listener = comio.as_listener(reader)

    async def add_flag(item: dict) -> dict:
        return {**item, "processed": True}

    await pipe(listener, writer, add_flag)

    out.seek(0)
    lines = [json.loads(l) for l in out.read().strip().split("\n")]
    assert lines == [
        {"id": 0, "processed": True},
        {"id": 1, "processed": True},
        {"id": 2, "processed": True},
    ]


@pytest.mark.asyncio
async def test_pipe_with_hooks(sample_file):
    from comio import pipe, PipeConfig

    reader = AsyncJsonL(sample_file)
    out = io.StringIO()
    writer = AsyncJsonL(out)
    listener = comio.as_listener(reader)

    log: list[str] = []

    async def on_boot():
        log.append("boot")

    async def on_close():
        log.append("close")

    cfg = PipeConfig(on_boot=[on_boot], on_close=[on_close])

    async def identity(item: dict) -> dict:
        return item

    await pipe(listener, writer, identity, cfg=cfg)

    assert log == ["boot", "close"]
    out.seek(0)
    lines = [json.loads(l) for l in out.read().strip().split("\n")]
    assert len(lines) == 3
