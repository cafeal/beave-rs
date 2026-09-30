//! Typed message processing with explicit delivery and shutdown semantics.
//!
//! Contracts live in [`source`], [`sink`], [`handler`], and [`codec`].
//! [`subscription`] owns processing; [`app`] supervises subscriptions.
//! Built-in local transports live in [`adapters`].

pub mod adapters;
pub mod app;
pub mod blocking;
pub mod codec;
pub mod dead_letter;
pub mod error_policy;
pub mod forward;
pub mod handler;
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
pub mod tombstone;
pub mod transaction;

pub use adapters::{
    ChannelReceiver, ChannelSender, ChannelSink, ChannelSource, InMemorySink, IterSource,
    StdinSource, StdoutSink, channel,
};
pub use app::App;
pub use blocking::{Blocking, BlockingPool, blocking};
#[cfg(feature = "avro")]
pub use codec::Avro;
#[cfg(feature = "protobuf")]
pub use codec::Protobuf;
pub use codec::{Decoder, Encoder, Json, RawBytes, Utf8};
pub use dead_letter::DeadLetter;
pub use error_policy::{ErrorPolicy, FailureAction, FailureKind};
pub use forward::{SamePlatform, ValueRecord};
pub use handler::{Classify, Emit, Handler, HandlerError, Result};
pub use message::{Delivery, OrderingKey, SourceMessage};
pub use middleware::{Flow, MapMetadata, Middleware};
pub use propagation::PropagationCarrier;
pub use retry::{Jitter, RetryPolicy};
pub use shutdown::CancellationToken;
pub use sink::{Completion, Sink};
pub use source::{Receive, ReceiveError, Source, SourceItem, SourceRaw};
pub use subscription::{ProcessingOrder, Subscription, SubscriptionConfig};
#[cfg(feature = "opentelemetry")]
pub use telemetry::TraceContext;
pub use tombstone::{PropagateTombstones, TombstonePublish, TombstoneRecord, Tombstones};
pub use transaction::TransactionalSink;
