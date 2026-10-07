//! Typed message processing with explicit delivery and shutdown semantics.
//!
//! Application code is a typed `Input → Output` [`Handler`]. The framework owns everything
//! around it: receiving from a [`Source`], decoding, retries, concurrency, ordering,
//! publishing to a [`Sink`], acknowledgement, dead letters, and graceful shutdown.
//!
//! ```text
//! App
//! ├── Subscription: Kafka source  → handler → Kafka sink
//! ├── Subscription: Pulsar source → handler → HTTP sink
//! └── Subscription: SQS source    → handler → RabbitMQ sink
//! ```
//!
//! Each [`Subscription`] connects one source, one handler, and one sink. An [`App`] runs any
//! number of subscriptions side by side and shuts them down together.
//!
//! # Quick start
//!
//! Summarize each article on a Kafka topic with an LLM and publish the summaries to
//! another topic. The handler sees only the decoded value; a transient API failure is
//! retried, and each offset is committed after its summary is published.
//!
//! ```no_run
//! # #[cfg(feature = "kafka")]
//! # mod example {
//! use beavers::{
//!     App, Classify, Result, Utf8,
//!     adapters::kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig},
//! };
//!
//! async fn summarize(article: String) -> Result<String> {
//!     let summary = call_llm(&format!("Summarize: {article}")).await.retry()?;
//!     Ok(summary)
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let articles = KafkaSource::<Utf8, _>::new(KafkaSourceConfig::new(
//!         "localhost:9092",
//!         "summarizer",
//!         ["articles"],
//!     ));
//!     let summaries = KafkaSink::<Utf8, _>::new(KafkaSinkConfig::new("localhost:9092", "summaries"));
//!
//!     App::new()
//!         .subscribe("summarize", articles, summaries, summarize)
//!         .run()
//!         .await
//! }
//! # async fn call_llm(prompt: &str) -> anyhow::Result<String> {
//! #     Ok(prompt.to_owned())
//! # }
//! # }
//! # fn main() {}
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
pub use subscription::{
    ByRecord, ByValue, IntoHandler, ProcessingOrder, Subscription, SubscriptionConfig,
    TransactionBatch,
};
#[cfg(feature = "opentelemetry")]
pub use telemetry::TraceContext;
pub use tombstone::{PropagateTombstones, TombstonePublish, TombstoneRecord, Tombstones};
pub use transaction::{TransactionEntry, TransactionalSink};
