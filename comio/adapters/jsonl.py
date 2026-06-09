import json
import io
from dataclasses import dataclass

from ..io import EOF, Cursor

@dataclass
class Page:
    items: list[dict]
    next_cursor: object

class JsonL:

    def __init__(self, f: io.TextIOBase):
        self.f = f

    def read(self, cursor: Cursor | None = None, n: int | None = None) -> Page:
        self.f.seek(cursor or 0)
        line = self.f.readline()
        if line == "":
            return Page([], EOF)
        return Page(items=[json.loads(line)], next_cursor=self.f.tell())

    def write(self, item: dict) -> None:
        self.f.write(json.dumps(item) + "\n")
        self.f.flush()


class AsyncJsonL:

    def __init__(self, f: io.TextIOBase):
        self.syncio = JsonL(f)

    async def read(self, cursor: Cursor | None = None, n: int | None = None) -> Page:
        return self.syncio.read(cursor=cursor, n=n)
    
    async def write(self, item: dict) -> None:
        self.syncio.write(item)