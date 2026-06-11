"""Composable async I/O primitives.

Protocols:

    Reader:   pull-based, paginated source   (read → Page)
    Listener: push-based, streaming source   (listen → AsyncIterator)
    Writer:   single-item sink               (write)
    Batcher:  multi-item sink                (batch)

Functions:

    scroll:      iterate pages from a Reader
    read_all:    drain a Reader into a list
    copy:        buffer a Listener into a Batcher
    as_listener: convert a Reader into a Listener
"""

from __future__ import annotations

import typing as t

T = t.TypeVar("T")
T_co = t.TypeVar("T_co", covariant=True)
T_contra = t.TypeVar("T_contra", contravariant=True)

Cursor: t.TypeAlias = t.Any
"""An opaque position marker within a data source.

Can be any type the data source uses to track position:
an integer offset, a string token, a complex object, etc.
Each Reader implementation defines how to interpret its cursors.
"""


EOF = object()
"""Singleton sentinel. Return this as ``next_cursor`` to signal
that there is no more data to read."""


class Page(t.Protocol, t.Generic[T_co]):
    """A single page of results returned by a Reader.

    Attributes:
        items: The items contained in this page.
        next_cursor: The cursor pointing to the next page,
            or ``EOF`` if this is the last page.
    """

    @property
    def items(self) -> t.Sequence[T_co]: ...

    @property
    def next_cursor(self) -> Cursor: ...


class Reader(t.Protocol, t.Generic[T_co]):
    """Pull-based, paginated data source.

    Implementors decide:
    - How ``cursor`` maps to a position in the underlying store.
    - What page size ``n`` means (item count, byte count, etc.).
    - When to return ``EOF`` as ``next_cursor`` to signal completion.
    """

    async def read(self, *, cursor: Cursor = None, n: int | None = None) -> Page[T_co]:
        """Read one page of data starting at ``cursor``.

        Args:
            cursor: Position to read from. ``None`` means the beginning.
            n: Requested page size. ``None`` lets the implementation choose.

        Returns:
            A Page whose ``next_cursor`` is ``EOF`` when no more data remains.
        """
        ...


class Listener(t.Protocol, t.Generic[T_co]):
    """Push-based, streaming data source.

    Unlike Reader which requires the caller to pull pages,
    a Listener yields items as they become available.
    """

    def listen(self) -> t.AsyncIterator[T_co]:
        """Start listening and yield items as they arrive.

        The iterator completes when the source is exhausted or closed.
        """
        ...


class Writer(t.Protocol, t.Generic[T_contra]):
    """Single-item sink.

    Accepts one item at a time via ``write()``.
    """

    async def write(self, item: T_contra) -> None:
        """Write a single item to the destination."""
        ...


class Batcher(t.Protocol, t.Generic[T_contra]):
    """Multi-item sink.

    Accepts a sequence of items at once via ``batch()``,
    enabling bulk inserts, buffered writes, etc.
    """

    async def batch(self, items: t.Sequence[T_contra]) -> None:
        """Write a batch of items to the destination."""
        ...


async def scroll(r: Reader[T_co], *, cursor: Cursor = None, n: int | None = None) -> t.AsyncIterator[Page[T_co]]:
    """Iterate pages from a Reader until the source is exhausted.

    Args:
        r: The reader to pull pages from.
        cursor: Starting position. ``None`` begins at the start.
        n: Requested page size per read. ``None`` lets the reader choose.

    Yields:
        Each non-empty Page. Stops when the page is empty
        or ``next_cursor`` is ``EOF``.
    """
    while True:
        page = await r.read(cursor=cursor, n=n)
        if not page.items:
            break
        yield page
        if page.next_cursor is EOF:
            break
        cursor = page.next_cursor


async def read_all(r: Reader[T_co], *, cursor: Cursor = None, n: int | None = None) -> t.Sequence[T_co]:
    """Drain a Reader and collect every item into a single sequence.

    Args:
        r: The reader to drain.
        cursor: Starting position. ``None`` begins at the start.
        n: Requested page size per read. ``None`` lets the reader choose.

    Returns:
        All items concatenated across every page.
    """
    items: t.List[T_co] = []
    async for page in scroll(r, cursor=cursor, n=n):
        items.extend(page.items)
    return items


async def copy(src: Listener[T_co], dst: Batcher[T_co], n: int | None = None) -> None:
    """Buffer items from a Listener and flush to a Batcher every ``n`` items.

    Args:
        src: Push-based source to consume from.
        dst: Batch sink to flush into.
        n: Buffer size, Defaults to 1. Flushes every ``n`` items, plus any remainder at the end.
    """
    n = n or 1
    buf: t.List[T_co] = []
    async for item in src.listen():
        buf.append(item)
        if len(buf) >= n:
            await dst.batch(buf)
            buf = []
    if buf:
        await dst.batch(buf)


def as_listener(r: Reader[T_co], *, cursor: Cursor = None, n: int | None = None) -> Listener[T_co]:
    """Convert a Reader into a Listener.

    Returns a Listener whose ``listen()`` yields individual items
    from the Reader's pages, so downstream code (e.g. ``copy``, ``pipe``)
    can treat both sources uniformly.
    """
    class _Listener:
        async def listen(self) -> t.AsyncIterator[T_co]:
            async for page in scroll(r, cursor=cursor, n=n):
                for item in page.items:
                    yield item

    return _Listener()
