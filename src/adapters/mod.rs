//! Broker-independent sources and sinks for local execution and testing.
mod iter;
mod memory;
mod stdin;
mod stdout;

pub use iter::IterSource;
pub use memory::InMemorySink;
pub use stdin::{StdinMessage, StdinSource};
pub use stdout::StdoutSink;
