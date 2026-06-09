"""comio – Composable I/O primitives for async Python."""

from .io import (
    Batcher,
    Cursor,
    EOF,
    Listener,
    Page,
    Reader,
    Writer,
    copy,
    as_listener,
    read_all,
    scroll,
)
from .pipe import Handler, PipeConfig, pipe

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
    "Handler",
    # Functions
    "scroll",
    "read_all",
    "copy",
    "as_listener",
    "pipe",
    # Config
    "PipeConfig",
]
