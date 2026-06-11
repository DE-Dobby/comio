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
import json
from dataclasses import dataclass
import comio as io

@dataclass
class Page:
    items: list
    next_cursor: object

class AsyncJsonL:
    def __init__(self, f):
        self.f = f

    async def read(self, *, cursor=None, n=None) -> Page:
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

#### Normalize a Reader into a Listener with `as_listener`

```python
listener = io.as_listener(reader)

async for item in listener.listen():
    process(item)
```

#### Buffered copy: Listener → Batcher

```python
await io.copy(listener, batcher, n=100)  # flush every 100 items
```

#### Streaming pipe with backpressure

```python
import comio as io
from comio import pipe, PipeConfig

listener = io.as_listener(reader)

async def transform(item):
    return {**item, "processed": True}

await pipe(listener, writer, transform, cfg=PipeConfig(buffer=10))
```

#### Glue pipes with asyncio queues

```python
import asyncio
from comio import pipe
from comio.adapters.queue import QueueListener, QueueWriter

q: asyncio.Queue = asyncio.Queue()
glue_src = QueueListener(q)
glue_dst = QueueWriter(q)

async def transform(item):
    return {**item, "step": 1}

async def enrich(item):
    return {**item, "step": 2}

# pipe1: listener -> transform -> queue
await pipe(listener, glue_dst, transform)
glue_src.close()

# pipe2: queue -> enrich -> final destination
await pipe(glue_src, writer, enrich)
```

### Sync

```python
import json
from dataclasses import dataclass
import comio.sync as io

@dataclass
class Page:
    items: list
    next_cursor: object

class JsonL:
    def __init__(self, f):
        self.f = f

    def read(self, *, cursor=None, n=None) -> Page:
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

listener = io.as_listener(reader)

for item in listener.listen():
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
| `Handler[I,O]`| `__call__`| Transform a single item         |

### Sync (`comio.sync`)


| Protocol      | Method   | Description                |
|---------------|----------|----------------------------|
| `Reader[T]`   | `read`   | Pull a page of items       |
| `Listener[T]` | `listen` | Push items as an iterator  |
| `Writer[T]`   | `write`  | Accept a single item       |
| `Batcher[T]`  | `batch`  | Accept a sequence of items |

## Adapters

| Adapter | Module | Implements |
|---------|--------|------------|
| `QueueListener` | `comio.adapters.queue` | `Listener` |
| `QueueWriter` | `comio.adapters.queue` | `Writer`, `Batcher` |
| `Stdin` | `comio.adapters.std` | `Listener` |
| `Stdout` | `comio.adapters.std` | `Writer`, `Batcher` |
| `TextFileReader` | `comio.adapters.text_file` | `Reader` |
| `TextFileWriter` | `comio.adapters.text_file` | `Writer`, `Batcher` |
