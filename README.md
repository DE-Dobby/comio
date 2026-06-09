# comio – Composable I/O

I/O primitives for async & sync Python. Designed to be used as:

```python
import comio as io          # async
import comio.sync as io     # sync
```

Giving you `io.Reader`, `io.Writer`, `io.Listener`, etc. — a Go-like DX for Python.

## Install

```bash
pip install comio
```

## Usage

### Async

#### Implement a Reader / Writer

Any object with the right method is a valid `io.Reader` or `io.Writer` — no base class needed.

```python
import comio as io

class AsyncJsonL:
    def __init__(self, f):
        self.f = f

    async def read(self, *, cursor=None, n=None) -> io.Page:
        self.f.seek(cursor or 0)
        line = self.f.readline()
        if line == "":
            return Page([], io.EOF)
        return Page(items=[json.loads(line)], next_cursor=self.f.tell())

    async def write(self, item: dict) -> None:
        self.f.write(json.dumps(item) + "\n")
```

#### Read pages with `scroll`

```python
reader = AsyncJsonL(open("data.jsonl"))

async for page in io.scroll(reader):
    print(page.items)
```

#### Drain everything with `read_all`

```python
items = await io.read_all(reader)
```

#### Resume from a cursor

```python
items = await io.read_all(reader, cursor=saved_cursor)
```

#### Normalize a Reader into a stream with `as_listener`

```python
async for item in io.as_listener(reader):
    process(item)
```

#### Buffered copy: Listener → Batcher

```python
await io.copy(listener, batcher, n=100)  # flush every 100 items
```

#### Streaming pipe with backpressure

```python
from comio import pipe

async def transform(item):
    return {**item, "processed": True}

await pipe(listener, writer, transform)
```

### Sync

```python
import comio.sync as io

class JsonL:
    def __init__(self, f):
        self.f = f

    def read(self, *, cursor=None, n=None):
        self.f.seek(cursor or 0)
        line = self.f.readline()
        if line == "":
            return Page([], io.EOF)
        return Page(items=[json.loads(line)], next_cursor=self.f.tell())

    def write(self, item: dict) -> None:
        self.f.write(json.dumps(item) + "\n")
```

```python
reader = JsonL(open("data.jsonl"))

for page in io.scroll(reader):
    print(page.items)

items = io.read_all(reader)

for item in io.as_listener(reader):
    process(item)
```

## Protocols

### Async (`comio`)

| Protocol      | Method   | Description                     |
|---------------|----------|---------------------------------|
| `Reader[T]`   | `read`   | Pull a page of items            |
| `Listener[T]` | `listen` | Push items as an async iterator |
| `Writer[T]`   | `write`  | Accept a single item            |
| `Batcher[T]`  | `batch`  | Accept a sequence of items      |

### Sync (`comio.sync`)

| Protocol      | Method   | Description                |
|---------------|----------|----------------------------|
| `Reader[T]`   | `read`   | Pull a page of items       |
| `Listener[T]` | `listen` | Push items as an iterator  |
| `Writer[T]`   | `write`  | Accept a single item       |
| `Batcher[T]`  | `batch`  | Accept a sequence of items |
