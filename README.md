# beave-rs

[![crates.io](https://img.shields.io/crates/v/beavers.svg)](https://crates.io/crates/beavers)
[![docs.rs](https://img.shields.io/docsrs/beavers)](https://docs.rs/beavers)
[![CI](https://github.com/cafeal/beave-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/cafeal/beave-rs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](#license)

A lightweight Rust message processing framework built around typed handlers.

```text
Source → Subscription → Handler → Sink
```

The current implementation includes local adapters, a bounded in-process
channel that chains subscriptions with end-to-end acknowledgement, optional
Kafka, Apache Pulsar, RabbitMQ, and Amazon SQS adapters, an optional HTTP source
and sink, typed codecs, bounded concurrency, retries, and graceful shutdown.
NATS JetStream remains on the roadmap.

## Installation

```sh
cargo add beavers --features kafka
cargo add tokio --features macros,rt-multi-thread
cargo add anyhow
cargo add serde --features derive
```

No Cargo feature is enabled by default. Enable the adapters and codecs the
application uses:

| Feature | Enables |
|---|---|
| `kafka` | Kafka source and sink |
| `pulsar` | Apache Pulsar source and sink |
| `rabbitmq` | RabbitMQ source and sink |
| `sqs` | Amazon SQS source and sink |
| `http` | HTTP source and sink |
| `avro` | Avro codec |
| `protobuf` | Protobuf codec |
| `opentelemetry` | Trace-context propagation through broker metadata |
| `health` | `/livez` and `/readyz` endpoints |
| `testing` | Fabricated broker records for handler tests |

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

A Kafka subscription has the same shape. The handler sees only the decoded
value; the framework commits each offset after the output is published:

```rust
use beavers::{
    App, Json, Result, Subscription,
    adapters::kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[derive(Serialize)]
struct Invoice {
    order_id: u64,
    amount_cents: u64,
}

async fn invoice(order: Order) -> Result<Invoice> {
    Ok(Invoice { order_id: order.id, amount_cents: order.amount_cents })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = KafkaSource::<Json, Order>::new(KafkaSourceConfig::new(
        "localhost:9092",
        "invoicing",
        ["orders"],
    ));
    let sink = KafkaSink::<Json, Invoice>::new(KafkaSinkConfig::new("localhost:9092", "invoices"));
    App::new()
        .subscription(Subscription::forward("invoice", source, sink, invoice).concurrency(4))
        .run()
        .await
}
```

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
- [Versioning and releases](docs/releasing.md)

The runtime emits `tracing` spans and `metrics` counters and histograms for
every delivery stage. The optional `opentelemetry` feature propagates trace
context through Kafka headers and Pulsar properties. See
[observability](docs/runtime.md#observability).

The optional `health` feature serves `/livez` and `/readyz` for Kubernetes
probes; see [health checks](docs/runtime.md#health-checks).

Kafka, Pulsar, RabbitMQ, SQS, and HTTP are optional Cargo features. See the [adapter guide](docs/adapters.md)
for feature flags and delivery semantics. Synchronous handlers run on a bounded
worker pool through `blocking(sync_handler)`; see
[blocking handlers](docs/runtime.md#blocking-handlers).

## Development

The minimum supported Rust version is 1.94.1.

```sh
cargo fmt --check
cargo test
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo doc --all-features --no-deps
```

GitHub Actions runs these checks on every pull
request and on pushes to `main`. Live Kafka, Pulsar, RabbitMQ, and SQS tests are ignored
by default; CI runs them against the Docker Compose brokers, and `cargo test-live`
runs them locally while the brokers are up.

## License

Licensed under the [MIT license](LICENSE-MIT).

**Let application code process events. Let beave-rs manage the flow.**
