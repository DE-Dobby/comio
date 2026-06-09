"""comio.sync -- Synchronous I/O primitives."""

from .io import (
    Cursor,
    EOF,
    Page,
    Reader,
    Listener,
    Writer,
    Batcher,
    scroll,
    read_all,
    copy,
    as_listener,
)

__all__ = [
    # Primitives
    "Cursor",
    "EOF",
    # Protocols
    "Page",
    "Reader",
    "Listener",
    "Writer",
    "Batcher",
    # Functions
    "scroll",
    "read_all",
    "copy",
    "as_listener",
]
