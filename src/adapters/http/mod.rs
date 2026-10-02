//! HTTP adapter: an HTTP/1.1 server whose requests become deliveries, and a
//! client sink that sends each output as a request.
//!
//! Enabled by the `http` Cargo feature. See `docs/adapters/http.md` for the
//! response, delivery, and publication contracts.

mod config;
mod metrics;
mod record;
mod server;
mod sink;
mod source;

pub use config::{HttpMethod, HttpSinkConfig, HttpSourceConfig, ResponseTiming};
pub use record::{HttpMetadata, HttpPublish, HttpRecord};
pub use sink::{HttpPrepared, HttpSink};
pub use source::{HttpMessage, HttpSource};
