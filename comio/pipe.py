"""Streaming pipe: Listener -> Handler -> Writer.

Connects a push-based source to a sink through a transformation handler,
using in-memory streams with configurable backpressure.

    src --> [in_stream] --> handler --> [out_stream] --> dest

Three concurrent tasks (ingress / process / egress) are managed
by an anyio task group. When the source is exhausted, streams close
in order and the pipe shuts down gracefully.
"""

from __future__ import annotations

import typing as t

import anyio

from .io import Listener, Writer


In = t.TypeVar("In", covariant=True)
Out = t.TypeVar("Out", contravariant=True)
H_In = t.TypeVar("H_In", contravariant=True)
H_Out = t.TypeVar("H_Out", covariant=True)


class Handler(t.Protocol, t.Generic[H_In, H_Out]):
    """Transforms a single input item into a single output item."""

    async def __call__(self, item: H_In) -> H_Out: ...


class PipeConfig(t.TypedDict):
    buffer: t.NotRequired[int]
    """Buffer size for the memory streams between stages.
    Default is 0 (rendezvous channel = strongest backpressure)."""

    on_boot: t.NotRequired[t.Sequence[t.Callable[[], t.Awaitable[None]]]]
    """Hooks executed sequentially before starting the pipe."""

    on_close: t.NotRequired[t.Sequence[t.Callable[[], t.Awaitable[None]]]]
    """Hooks executed sequentially after shutdown (even on failure)."""


async def pipe(
    src: Listener[In],
    dest: Writer[Out],
    h: Handler[In, Out],
    *,
    cfg: PipeConfig | None = None,
) -> None:
    """Run a streaming pipe: ``src → h → dest``.

    Args:
        src: Push-based source that yields input items.
        dest: Sink that receives transformed output items.
        h: Handler that transforms each input into an output.
        cfg: Optional configuration for buffer size and lifecycle hooks.
    """
    if cfg is None:
        cfg = PipeConfig()

    for boot in cfg.get("on_boot", []):
        await boot()
    try:
        await _pipe(src, dest, h, buffer=cfg.get("buffer", 0))
    finally:
        for close in cfg.get("on_close", []):
            await close()


async def _pipe(
    src: Listener[In],
    dest: Writer[Out],
    h: Handler[In, Out],
    *,
    buffer: int,
) -> None:
    in_send, in_recv = anyio.create_memory_object_stream[In](buffer)
    out_send, out_recv = anyio.create_memory_object_stream[Out](buffer)

    async def ingress() -> None:
        """src → in_stream. Closes in_send when the source is exhausted."""
        async with in_send:
            async for item in src.listen():
                await in_send.send(item)

    async def process() -> None:
        """in_stream → handler → out_stream.
        Closes out_send when in_recv is exhausted."""
        async with out_send, in_recv:
            async for item in in_recv:
                await out_send.send(await h(item))

    async def egress() -> None:
        """out_stream → dest. Finishes when out_recv is closed."""
        async with out_recv:
            async for item in out_recv:
                await dest.write(item)

    async with anyio.create_task_group() as tg:
        tg.start_soon(egress)
        tg.start_soon(process)
        tg.start_soon(ingress)
