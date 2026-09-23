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
pub use channel::{ChannelSink, ChannelSource, channel};

#[cfg(feature = "kafka")]
pub mod kafka;
