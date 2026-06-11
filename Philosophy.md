# Philosophy

Comio, the Composable I/O, is a small library that abstracts numerous I/O functions into 4 irreducible primitives.

## Turing Machine

The Turing machine, invented by Alan Mathison Turing, forms the foundation of modern computer science. It consists of multiple devices that an automatic machine must have. The following are its components:

- **Tape**, the writable medium that the machine writes to
- **Head**, the unit that the machine uses to read and write
- **State register**, the current state of the machine
- **Action table**, the instructions that the machine follows

These components are irreplaceable essentials for a modern computer to work. In other words, every operation that a computer performs can be condensed into:
- reading
- writing
- stating
- branching

## Reading and Writing

The ways to register state and branch are as diverse as business domains. Because of the broad spectrum of business logic, binding multiple logic paths into a single abstraction is almost impossible.

However, reading and writing can be abstracted. Reading needs a bucket to scoop from; writing needs a bucket to pour into.

At that point, comio decides to focus on the abstractions it can handle well: reading and writing.

## Read

The operation `Read` can be represented as the following model:

`Source.Read(p []byte) (n int, err error)`

This one line expresses the essence of reading. Reading needs:
- An internal marker for where we are reading from.
- A bucket to scoop into.
- A count of how many bytes the reader reads.
- Optional errors while reading.

Comio converts this model into:

`Source.read(c: Cursor, n: int) -> Page[T]`

Thanks to Python, the developer doesn't need to bring their own bucket. We need only a bookmark and the number of items to read.

But reading has two flavors. Sometimes we pull data at our own pace — this is `Reader`. Other times, data pushes itself to us as it arrives — this is `Listener`. Comio provides `as_listener` to bridge the two: any `Reader` can be converted into a `Listener`, so downstream code can treat both uniformly.

## Write

The operation `Write` mirrors reading:

`Dest.Write(p []byte) (n int, err error)`

Writing needs:
- A destination to pour data into.
- The data to write.
- A count of how many bytes were written.
- Optional errors while writing.

Comio converts this model into two variants:

`Dest.write(item: T) -> None` — for a single item at a time.

`Dest.batch(items: Sequence[T]) -> None` — for multiple items at once.

A `Writer` accepts one item per call. A `Batcher` accepts a sequence, enabling bulk inserts and buffered writes. The distinction is practical: some destinations perform far better with batched operations.

## Composition

We have deeply explored how programs that follow the Turing machine model — technically, all programs running on a modern computer — have both a source to read from and a destination to write to. In this context, we can define a program as a pipe.

Imagine a pipe that tints water blue. To change the color of water, we can choose a policy:
- Block the flow, gathering all the water until it drains, then tint it.
- Let it flow, tinting as it flows.

The former is `read_all(reader)`, and the latter is `scroll(reader)`.

```python
water = read_all(springhead) # could be definite but enormous.
blue_water = tint(water)
downstream.write(blue_water)
```

```python
async for water in scroll(springhead):
    blue_water = tint(water)
    downstream.write(blue_water)
```

Let's say we have 2 reservoirs, one full and the other drained.
If we need to move water to the other reservoir, we just lay a pipe and let it flow for every 100L.

```python
await copy(full, drained, 100)
```
We call this operation `Migration` in software development. This model represents how simple migration can be.

Let's jump to another example. We have a conveyor belt to pack bear dolls.
From the conveyor's point of view, it doesn't know when a bear doll comes in. It just runs the belt.

Sometimes, we have to stop the belt. If the receiver cannot deliver as fast as the doll producer, the belt should slow down or stop. This is backpressure.

Comio models this as `pipe`:

```
src --> [in_stream] --> handler --> [out_stream] --> dest
```

```python
await pipe(
    src=conveyor,          # Listener: dolls arrive when they arrive
    dest=packer,           # Writer: packs one doll at a time
    h=stuff_and_stitch,    # Handler: stuffs and stitches each doll
    cfg=PipeConfig(buffer=10),
)
```

Three concurrent tasks run inside the pipe — ingress, process, and egress. When the packer falls behind, the buffer fills up and the belt naturally slows down. When the conveyor runs out of dolls, the streams close in order and the pipe shuts down gracefully.

## Capability

We have deeply explored how the composition of reading, processing, and writing can work as one to solve problems.

Now, a real-world problem. Let's build a subscribing model:

```python
await pipe(
    src=rabbitmq,
    dest=fastapi_socket,
    h=transform,
)
```

Or plumb multiple pipes together:

```python
glue = asyncio.Queue()

await pipe(
    src=rabbitmq,
    dest=glue_sink,
    h=transform,
)

await pipe(
    src=glue_source,
    dest=database,
    h=enrich,
)
```

Each pipe is small and single-purpose. Chain them, and capability emerges from composition.

I want my fellow subscribers to know: **Capability emerges from the composition of small, irreducible verbs.** That's the core principle that comio follows.
