"""Synchronous composable I/O primitives.

Mirrors the async ``comio.io`` module with blocking equivalents:

    Reader: pull-based, paginated source  (read -> Page)
    Writer: single-item sink              (write)
    Batcher: multi-item sink              (batch)
    Listener: push-based, streaming source (listen -> Iterator)
"""

from __future__ import annotations

import typing as t

from ..io import EOF, Cursor, Page

T = t.TypeVar("T")
T_co = t.TypeVar("T_co", covariant=True)
T_contra = t.TypeVar("T_contra", contravariant=True)


class Reader(t.Protocol, t.Generic[T_co]):
    """Synchronous pull-based, paginated data source."""

    def read(self, *, cursor: Cursor = None, n: int | None = None) -> Page[T_co]: ...


class Listener(t.Protocol, t.Generic[T_co]):
    """Synchronous push-based, streaming data source."""

    def listen(self) -> t.Iterator[T_co]: ...


class Writer(t.Protocol, t.Generic[T_contra]):
    """Synchronous single-item sink."""

    def write(self, item: T_contra) -> None: ...


class Batcher(t.Protocol, t.Generic[T_contra]):
    """Synchronous multi-item sink."""

    def batch(self, items: t.Sequence[T_contra]) -> None: ...


def scroll(r: Reader[T_co], *, cursor: Cursor = None, n: int | None = None) -> t.Iterator[Page[T_co]]:
    """Iterate pages from a Reader until the source is exhausted."""
    while True:
        page = r.read(cursor=cursor, n=n)
        if not page.items:
            break
        yield page
        if page.next_cursor is EOF:
            break
        cursor = page.next_cursor


def read_all(r: Reader[T_co], *, cursor: Cursor = None, n: int | None = None) -> t.Sequence[T_co]:
    """Drain a Reader and collect every item into a single sequence."""
    items: t.List[T_co] = []
    for page in scroll(r, cursor=cursor, n=n):
        items.extend(page.items)
    return items


def copy(src: Listener[T_co], dst: Batcher[T_co], n: int) -> None:
    """Buffer items from a Listener and flush to a Batcher every ``n`` items."""
    buf: t.List[T_co] = []
    for item in src.listen():
        buf.append(item)
        if len(buf) >= n:
            dst.batch(buf)
            buf = []
    if buf:
        dst.batch(buf)


def as_listener(r: Reader[T_co], *, cursor: Cursor = None, n: int | None = None) -> t.Iterator[T_co]:
    """Convert a Reader into an item-level Iterator."""
    for page in scroll(r, cursor=cursor, n=n):
        for item in page.items:
            yield item
