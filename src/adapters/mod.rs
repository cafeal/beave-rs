//! Local transports and optional broker adapters.
//!
//! The local transports and [`channel`] are always available. The `http`,
//! `kafka`, `pulsar`, and `rabbitmq` modules are enabled by Cargo features
//! of the same names.
mod iter;
mod memory;
mod stdin;
mod stdout;

pub use iter::IterSource;
pub use memory::InMemorySink;
pub use stdin::{StdinMessage, StdinSource};
pub use stdout::StdoutSink;

mod channel;
pub use channel::{
    ChannelOutput, ChannelRaw, ChannelReceiver, ChannelSender, ChannelSink, ChannelSource, channel,
};

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(any(feature = "kafka", feature = "pulsar", feature = "rabbitmq"))]
mod pending;
#[cfg(feature = "pulsar")]
pub mod pulsar;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
