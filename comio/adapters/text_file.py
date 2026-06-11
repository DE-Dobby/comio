import io
import typing as t

class TextFileReader:

    def __init__(self, f: io.TextIOBase):
        self.f = f
    
    async def read(self, cursor: int | None = None, n: int | None = None) -> list[str]:
        self.f.seek(cursor or 0)
        items = []
        for _ in range(n or 1):
            line = self.f.readline()
            if line == "":
                break
            items.append(line.strip())
        return items
    

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