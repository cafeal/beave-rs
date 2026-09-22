# beave.rs

A lightweight Rust message processing framework built around typed handlers.

```text
Source → Subscription → Handler → Sink
```

The current prototype runs without an external broker. It includes iterator and
stdin sources, in-memory and JSON stdout sinks, bounded concurrency, retries,
and graceful shutdown. Kafka, NATS JetStream, and SQS support are planned.

## Quick start

```rust
use beavers::{App, IterSource, Json, Result, StdoutSink};

async fn double(value: u64) -> Result<u64> {
    Ok(value * 2)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    App::new()
        .subscribe(IterSource::new([1, 2, 3]), StdoutSink::<Json>::new(), double)
        .run()
        .await
}
```

Run the included example from this repository:

```sh
cargo run --example transform
printf '%s\n' '{"id":10}' '{"id":20}' | cargo run --example transform -- --stdin
```

The example writes one JSON event per line, such as `{"order_id":10}`.

## Documentation

- [Documentation index](docs/README.md)
- [Local adapters and usage examples](docs/adapters.md)
- [Codecs and serialization](docs/codecs.md)
- [Architecture and trait contracts](docs/architecture.md)
- [Runtime behavior, configuration, and limitations](docs/runtime.md)
- [Design plan and roadmap](docs/plan.md)

Synchronous handlers through `blocking(sync_handler)` are a recorded design
choice, not an implemented API. See the [execution model](docs/plan.md#handler-execution-model).

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
```

**Let application code process events. Let beave.rs manage the flow.**
