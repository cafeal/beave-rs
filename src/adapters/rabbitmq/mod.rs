//! RabbitMQ (AMQP 0-9-1) source and sink adapters.
//!
//! Enabled by the `rabbitmq` Cargo feature. See `docs/adapters/rabbitmq.md` for
//! acknowledgements, publisher confirms, and delivery guarantees.

mod config;
mod connection;
mod convert;
mod dead_letter;
mod inherit;
mod record;
mod sink;
mod source;

pub use config::{RabbitMqSinkConfig, RabbitMqSourceConfig};
pub use dead_letter::{RabbitMqDeadLetter, RabbitMqOrigin};
pub use inherit::RabbitMqInherit;
#[cfg(feature = "testing")]
pub(crate) use record::text_headers;
pub use record::{
    RabbitMqHeaders, RabbitMqMetadata, RabbitMqProperties, RabbitMqPublish, RabbitMqRecord,
    RabbitMqValue,
};
pub use sink::{RabbitMqPrepared, RabbitMqSink};
pub use source::{RabbitMqMessage, RabbitMqSource};
