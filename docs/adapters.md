# Adapters

Adapters connect the framework's [Source and Sink contracts](architecture.md#trait-boundaries)
to concrete inputs and outputs. The current adapters support local development,
command-line pipelines, and tests without an external broker. They are available
through `beavers::adapters` and re-exported at the crate root.

| Adapter | Role | Typical use |
|---|---|---|
| `IterSource<T>` | Typed input | Fixtures, examples, finite jobs |
| `StdinSource<C, T>` | Line-delimited input | Files, pipes, interactive input |
| `InMemorySink<T>` | Typed output collection | Assertions and local inspection |
| `StdoutSink<C>` | Line-delimited output | JSON output, files, pipes |

Kafka, NATS JetStream, and SQS adapters are [planned](plan.md#implementation-order),
not implemented. The local adapters do not provide durable delivery guarantees.

## IterSource

Construct an IterSource from an array, vector, range, or other `IntoIterator`.
It stores the iterator and reads items lazily rather than collecting them first.
The iterator must be `Send + 'static`; input values must be
`Clone + Send + Sync + 'static`.

No serialization codec is needed. Each item becomes a `Delivery<T>`; decoding
clones its typed value. Iterator exhaustion produces `Receive::End` and the
subscription drains before completing.

The ACK callback increments a shared counter available through
`acknowledgements()`. This is a completion counter, not durable storage or a
redelivery mechanism.

## InMemorySink

InMemorySink stores published values in a shared vector. Cloning the sink shares
the same storage; `values()` returns a cloned snapshot. Preparation passes through
the typed value, and publication appends a clone. Values must implement
`Clone + Send + Sync + 'static`.

Use it with IterSource to exercise a complete subscription:

```rust
use beavers::{App, InMemorySink, IterSource, Result};
use std::sync::atomic::Ordering;

async fn double(value: u64) -> Result<u64> {
    Ok(value * 2)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = IterSource::new([1, 2, 3]);
    let acknowledgements = source.acknowledgements();
    let sink = InMemorySink::default();

    App::new()
        .subscribe(source, sink.clone(), double)
        .run()
        .await?;

    assert_eq!(sink.values(), vec![2, 4, 6]);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
    Ok(())
}
```

The example uses the default concurrency of one. Parallel handlers can publish
in a different order. The sink retains all output without a capacity limit, so
use it for bounded test runs rather than long-lived output storage.

## StdinSource

StdinSource uses a default-constructed `Decoder<T>` and reads newline-delimited
input. JSON input normally uses `StdinSource::<Json, _>::new()`, with the input type
inferred from the handler. With Json, input must also implement Serde's
`DeserializeOwned`.

```rust
use beavers::{App, Json, Result, StdinSource, StdoutSink};

async fn double(value: u64) -> Result<u64> {
    Ok(value * 2)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    App::new()
        .subscribe(
            StdinSource::<Json, _>::new(),
            StdoutSink::<Json>::new(),
            double,
        )
        .run()
        .await
}
```

For this example, feed one JSON number per line. For the repository's
[transform example](../examples/transform.rs), feed order objects instead:

```sh
printf '%s\n' '{"id":10}' '{"id":20}' | cargo run --example transform -- --stdin
```

Each `StdinMessage` owns its raw line. The runtime decodes it after receiving it,
so a decode failure is distinct from a receive I/O failure. Blank lines and
malformed JSON fail decoding; they are not skipped. A final nonempty line without
a trailing newline is still processed. EOF produces End after buffered lines
have been received. I/O or reader-thread startup failures are fatal receive errors.

The first receive starts a dedicated reader thread with a bounded channel. This
bounds queued lines, not individual line size; there is no maximum line-length
setting. Reading from a custom iterator or stdin is not a substitute for the
[planned blocking-handler pool](plan.md#handler-execution-model).

Closing the source closes its channel. An in-progress OS read may remain blocked,
but the detached reader does not prevent Tokio runtime shutdown. Use only one
stdin source per process and do not plan to reuse stdin after closing it. ACK is
a no-op completion marker; consumed lines cannot be recovered automatically.

## StdoutSink

StdoutSink default-constructs an `Encoder<T>`. `StdoutSink::<Json>::new()` requires
Serde-serializable output and emits compact JSON followed by a newline.

Preparation encodes the value once and appends the newline. Publication writes
and flushes the prepared bytes under a sink-local async mutex. A publish retry
reuses those bytes. Serialization failures occur during preparation and stop
processing without publication or input ACK.

Use stderr for logs and diagnostics so stdout can be redirected or piped:

```sh
cargo run --example transform > events.jsonl
```

Publication is not transactional. A partial write followed by retry may leave a
partial line or duplicate output. Flushing stdout does not guarantee durable disk
storage or acknowledgement from a downstream process. Concurrent jobs do not
preserve input order, and independent sink instances do not share this sink's
mutex.

For codec selection and custom serialization, see the [codec guide](codecs.md).

## Implementing another adapter

Implement `Source` with an associated `Message: SourceMessage` for input, or
`Sink<T>` with an associated prepared representation for output. Adapters own
transport-specific settings and resource cleanup. Codecs own serialization;
subscription runtime owns retries, concurrency, and ACK sequencing.

Keep these contracts explicit:

- A canceled receive must not silently lose a delivery.
- Decoding and preparing output must not publish or ACK.
- Dropping a received message must not ACK it.
- Successful publication must reach the sink's documented confirmation boundary.
- Broker-specific commit order and assignment validity belong to the adapter.

See [architecture](architecture.md) for ownership boundaries and the
[runtime guide](runtime.md) for retry, failure, and shutdown behavior.
