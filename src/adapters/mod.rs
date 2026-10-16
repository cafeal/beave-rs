//! Local transports and optional broker adapters.
//!
//! The local transports and [`channel`] are always available. The `http`,
//! `kafka`, `pulsar`, `pubsub`, `rabbitmq`, `servicebus`, and `sqs` modules are
//! enabled by Cargo features of the same names.
mod iter;
mod memory;
mod stdin;
mod stdout;

pub use iter::{Acknowledgements, IterSource};
pub use memory::InMemorySink;
pub use stdin::{StdinMessage, StdinSource};
pub use stdout::StdoutSink;

mod channel;
pub use channel::{
    ChannelMessage, ChannelOutput, ChannelRaw, ChannelReceiver, ChannelSender, ChannelSink,
    ChannelSource, channel,
};

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(any(
    feature = "kafka",
    feature = "pulsar",
    feature = "rabbitmq",
    feature = "servicebus"
))]
mod pending;
#[cfg(feature = "pubsub")]
pub mod pubsub;
#[cfg(feature = "pulsar")]
pub mod pulsar;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "servicebus")]
pub mod servicebus;
#[cfg(feature = "sqs")]
pub mod sqs;
