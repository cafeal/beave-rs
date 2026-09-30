//! HTTP source adapter: an HTTP/1.1 server whose requests become deliveries.

mod config;
mod record;
mod server;
mod source;

pub use config::HttpSourceConfig;
pub use record::{HttpMetadata, HttpRecord};
pub use source::{HttpMessage, HttpSource};
