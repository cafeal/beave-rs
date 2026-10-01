//! Kafka source and sink adapters.

mod config;
mod dead_letter;
mod inherit;
mod progress;
mod record;
mod sink;
mod source;
mod tombstone;
mod transaction;

pub use config::{KafkaSinkConfig, KafkaSourceConfig};
pub use dead_letter::KafkaDeadLetter;
pub use inherit::KafkaInherit;
#[cfg(feature = "testing")]
pub(crate) use record::text_headers;
pub use record::{KafkaMetadata, KafkaPublish, KafkaRecord};
pub use sink::{KafkaPrepared, KafkaSink};
pub use source::{KafkaMessage, KafkaSource};
pub use transaction::KafkaTransactionalSink;
