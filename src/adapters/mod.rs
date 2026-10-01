//! Local transports and optional broker adapters.
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
#[cfg(any(feature = "kafka", feature = "pulsar"))]
mod pending;
#[cfg(feature = "pulsar")]
pub mod pulsar;
