//! Apache Pulsar source and sink adapters.
//!
//! Enabled by the `pulsar` Cargo feature. See `docs/adapters/pulsar.md` for
//! subscriptions, acknowledgements, and delivery guarantees.

mod client;
mod config;
mod dead_letter;
mod inherit;
mod producer;
mod record;
mod resend;
mod sink;
mod source;
mod tombstone;

pub use config::{
    PulsarAuthentication, PulsarSinkConfig, PulsarSourceConfig, PulsarSubscriptionType,
};
pub use dead_letter::PulsarDeadLetter;
pub use inherit::PulsarInherit;
pub use record::{PulsarMessageId, PulsarMetadata, PulsarPublish, PulsarRecord};
pub use sink::{PulsarPrepared, PulsarSink};
pub use source::{PulsarMessage, PulsarSource};
