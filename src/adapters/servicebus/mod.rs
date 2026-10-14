//! Azure Service Bus source and sink adapters.
//!
//! Enabled by the `servicebus` Cargo feature. See `docs/adapters/servicebus.md`
//! for message locks, settlement, and delivery guarantees.

mod auth;
mod client;
mod config;
mod connection;
mod convert;
mod dead_letter;
mod inherit;
mod lock;
mod management;
mod record;
mod sink;
mod source;

pub use client::ServiceBus;
pub use dead_letter::{ServiceBusDeadLetter, ServiceBusOrigin};
pub use inherit::ServiceBusInherit;
#[cfg(feature = "testing")]
pub(crate) use record::text_properties;
pub use record::{
    ServiceBusApplicationProperties, ServiceBusMetadata, ServiceBusProperties, ServiceBusPublish,
    ServiceBusRecord, ServiceBusValue,
};
pub use sink::{ServiceBusPrepared, ServiceBusSink};
pub use source::{ServiceBusMessage, ServiceBusSource};
