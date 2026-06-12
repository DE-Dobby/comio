import io
import typing as t
from dataclasses import dataclass

from ..io import EOF, Cursor


@dataclass
class Page:
    items: list[str]
    next_cursor: object


class TextFileReader:

    def __init__(self, f: io.TextIOBase):
        self.f = f

    async def read(self, cursor: Cursor | None = None, n: int | None = None) -> Page:
        self.f.seek(cursor or 0)
        items = []
        for _ in range(n or 1):
            line = self.f.readline()
            if line == "":
                return Page(items=items, next_cursor=EOF)
            items.append(line.strip())
        return Page(items=items, next_cursor=self.f.tell())
    

class TextFileWriter:
    
    def __init__(self, f: io.TextIOBase):
        self.f = f
    
    async def write(self, item: str) -> None:
        self.f.write(item + "\n")
        self.f.flush()

    async def batch(self, items: t.Sequence[str]) -> None:
        for item in items:
            self.f.write(item + "\n")
        self.f.flush()