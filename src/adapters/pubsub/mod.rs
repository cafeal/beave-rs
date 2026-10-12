//! Google Cloud Pub/Sub source and sink adapters.

mod client;
mod config;
mod dead_letter;
mod inherit;
mod record;
mod sink;
mod source;

pub use config::{PubSubCredentials, PubSubSinkConfig, PubSubSourceConfig};
pub use dead_letter::{PubSubDeadLetter, PubSubOrigin};
pub use inherit::PubSubInherit;
pub use record::{PubSubAttributes, PubSubMetadata, PubSubPublish, PubSubRecord};
pub use sink::{PubSubPrepared, PubSubSink};
pub use source::{PubSubMessage, PubSubSource};
