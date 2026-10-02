//! Typed message processing with explicit delivery and shutdown semantics.
//!
//! Application code is a typed `Input → Output` [`Handler`]. The framework owns everything
//! around it: receiving from a [`Source`], decoding, retries, concurrency, ordering,
//! publishing to a [`Sink`], acknowledgement, dead letters, and graceful shutdown.
//!
//! ```text
//! Source → Subscription → Handler → Sink
//! ```
//!
//! # Quick start
//!
//! ```
//! use beavers::{App, IterSource, Json, Result, StdoutSink};
//!
//! async fn double(value: u64) -> Result<u64> {
//!     Ok(value * 2)
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     App::new()
//!         .subscribe("double", IterSource::new([1, 2, 3]), StdoutSink::<Json>::new(), double)
//!         .run()
//!         .await
//! }
//! ```
//!
//! # Concepts
//!
//! - A [`Source`] receives deliveries; each delivery ([`SourceMessage`]) owns its
//!   acknowledgement and is acknowledged only after its outputs are published.
//! - A [`Handler`] turns a decoded input into an output, or into zero or many outputs with
//!   [`Emit`]. Its errors are classified with [`HandlerError`] and [`Classify`].
//! - A [`Sink`] prepares an output once and publishes it, with retries.
//! - A [`Subscription`] connects one source, handler, and sink with [`Middleware`], a
//!   dead-letter sink, an [`ErrorPolicy`], and a [`SubscriptionConfig`].
//! - An [`App`] runs subscriptions together and shuts them down on SIGINT or SIGTERM.
//! - [`Decoder`] and [`Encoder`] implementations such as [`Json`] convert payloads.
//!
//! Delivery is at least once. Duplicates are possible after a failure or a broker
//! rebalance; Kafka-to-Kafka subscriptions can use [`TransactionalSink`] instead.
//!
//! # Cargo features
//!
//! | Feature | Enables |
//! |---|---|
//! | `kafka` | Kafka source and sink in `adapters::kafka` |
//! | `pulsar` | Apache Pulsar source and sink in `adapters::pulsar` |
//! | `rabbitmq` | RabbitMQ source and sink in `adapters::rabbitmq` |
//! | `sqs` | Amazon SQS source and sink in `adapters::sqs` |
//! | `http` | HTTP source and sink in `adapters::http` |
//! | `avro` | The `Avro` codec |
//! | `protobuf` | The `Protobuf` codec |
//! | `opentelemetry` | Trace-context propagation with `TraceContext` |
//! | `health` | `/livez` and `/readyz` endpoints in [`health`] |
//! | `testing` | Fabricated broker records for handler tests in `testing` |
//!
//! No feature is enabled by default.
//!
//! # Guides
//!
//! - [Runtime behavior and configuration](https://github.com/cafeal/beave-rs/blob/main/docs/runtime.md)
//! - [Adapters and delivery semantics](https://github.com/cafeal/beave-rs/blob/main/docs/adapters.md)
//! - [Codecs](https://github.com/cafeal/beave-rs/blob/main/docs/codecs.md)
//! - [Architecture and trait contracts](https://github.com/cafeal/beave-rs/blob/main/docs/architecture.md)
//! - [Testing](https://github.com/cafeal/beave-rs/blob/main/docs/testing.md)
//! - [Versioning and releases](https://github.com/cafeal/beave-rs/blob/main/docs/releasing.md)
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod adapters;
pub mod app;
pub mod blocking;
pub mod codec;
pub mod dead_letter;
pub mod error_policy;
pub mod forward;
pub mod handler;
pub mod health;
pub mod message;
pub mod middleware;
pub mod propagation;
pub mod retry;
pub mod shutdown;
pub mod sink;
pub mod source;
pub mod subscription;
#[cfg(feature = "opentelemetry")]
pub mod telemetry;
#[cfg(feature = "testing")]
pub mod testing;
pub mod tombstone;
pub mod transaction;

pub use adapters::{
    ChannelOutput, ChannelRaw, ChannelReceiver, ChannelSender, ChannelSink, ChannelSource,
    InMemorySink, IterSource, StdinSource, StdoutSink, channel,
};
pub use app::App;
pub use blocking::{Blocking, BlockingPool, blocking};
#[cfg(feature = "avro")]
pub use codec::Avro;
#[cfg(feature = "protobuf")]
pub use codec::Protobuf;
pub use codec::{Decoder, Encoder, Json, RawBytes, Utf8};
pub use dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails};
pub use error_policy::{ErrorPolicy, FailureAction, FailureKind};
pub use forward::{SamePlatform, ValueRecord};
pub use handler::{Classify, Emit, Handler, HandlerError, Result};
pub use health::Health;
pub use message::{Delivery, OrderingKey, SourceMessage};
pub use middleware::{Flow, MapMetadata, Middleware};
pub use propagation::PropagationCarrier;
pub use retry::{Jitter, RetryPolicy};
pub use shutdown::CancellationToken;
pub use sink::{Completion, PublishRejected, Sink};
pub use source::{Receive, ReceiveError, Source, SourceItem, SourceRaw};
pub use subscription::{ProcessingOrder, Subscription, SubscriptionConfig, TransactionBatch};
#[cfg(feature = "opentelemetry")]
pub use telemetry::TraceContext;
pub use tombstone::{PropagateTombstones, TombstonePublish, TombstoneRecord, Tombstones};
pub use transaction::{TransactionEntry, TransactionalSink};
