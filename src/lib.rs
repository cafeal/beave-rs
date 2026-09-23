//! Typed message processing with explicit delivery and shutdown semantics.
//!
//! Contracts live in [`source`], [`sink`], [`handler`], and [`codec`].
//! [`subscription`] owns processing; [`app`] supervises subscriptions.
//! Built-in local transports live in [`adapters`].

pub mod adapters;
pub mod app;
pub mod codec;
pub mod handler;
pub mod message;
pub mod retry;
pub mod shutdown;
pub mod sink;
pub mod source;
pub mod subscription;

pub use adapters::{
    ChannelSink, ChannelSource, InMemorySink, IterSource, StdinSource, StdoutSink, channel,
};
pub use app::App;
#[cfg(feature = "avro")]
pub use codec::Avro;
#[cfg(feature = "protobuf")]
pub use codec::Protobuf;
pub use codec::{Decoder, Encoder, Json, RawBytes, Utf8};
pub use handler::{Emit, Handler, HandlerError, Result};
pub use message::{Delivery, SourceMessage};
pub use retry::RetryPolicy;
pub use shutdown::CancellationToken;
pub use sink::Sink;
pub use source::{Receive, ReceiveError, Source, SourceItem};
pub use subscription::{Subscription, SubscriptionConfig};
