# beave.rs

A lightweight Rust message processing framework built around typed handlers.

```text
Source → Subscription → Handler → Sink
```

The current implementation includes local adapters, a bounded in-process
channel, optional Kafka and Apache Pulsar adapters, typed codecs, bounded
concurrency, retries, and graceful shutdown. NATS JetStream and SQS remain on
the roadmap.

## Quick start

```rust
use beavers::{App, IterSource, Json, Result, StdoutSink};

async fn double(value: u64) -> Result<u64> {
    Ok(value * 2)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    App::new()
        .subscribe("double", IterSource::new([1, 2, 3]), StdoutSink::<Json>::new(), double)
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

Kafka and Pulsar examples run against local brokers started with Docker
Compose, which also provides web consoles for both brokers:

```sh
docker compose up -d --wait
cargo kafka-produce
cargo kafka-process   # Ctrl-C to stop
docker compose down
```

`cargo kafka-produce` and the other broker commands are Cargo aliases defined in
`.cargo/config.toml`. See [local development brokers](docs/development.md) for
every example, console, and alias.

## Documentation

- [Documentation index](docs/README.md)
- [Adapters and usage examples](docs/adapters.md)
- [Codecs and serialization](docs/codecs.md)
- [Architecture and trait contracts](docs/architecture.md)
- [Runtime behavior, configuration, and limitations](docs/runtime.md)
- [Design plan and roadmap](docs/plan.md)
- [Local development brokers](docs/development.md)

The runtime emits `tracing` spans and `metrics` counters and histograms for
every delivery stage. The optional `opentelemetry` feature propagates trace
context through Kafka headers and Pulsar properties. See
[observability](docs/runtime.md#observability).

Kafka and Pulsar are optional Cargo features. See the [adapter guide](docs/adapters.md)
for feature flags and delivery semantics. Synchronous handlers run on a bounded
worker pool through `blocking(sync_handler)`; see
[blocking handlers](docs/runtime.md#blocking-handlers).

## Development

```sh
cargo fmt --check
cargo test
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo doc --all-features --no-deps
```

GitHub Actions runs these checks on every pull
request and on pushes to `main`. Live Kafka and Pulsar tests are ignored by
default; CI runs them against the Docker Compose brokers, and `cargo test-live`
runs them locally while the brokers are up.

**Let application code process events. Let beave.rs manage the flow.**
