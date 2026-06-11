import asyncio

import typing as t

class Stdin:

    async def listen(self) -> t.AsyncIterator[str]:
        while True:
            yield await asyncio.to_thread(input)

class Stdout:

    async def write(self, item: str) -> None:
        print(item)

    async def batch(self, items: t.Sequence[str]) -> None:
        for item in items:
            print(item)


class StdinSync:
    
    def listen(self) -> t.Iterator[str]:
        while True:
            yield input()

class StdoutSync:
    
    def write(self, item: str) -> None:
        print(item)

    def batch(self, items: t.Sequence[str]) -> None:
        for item in items:
            print(item)