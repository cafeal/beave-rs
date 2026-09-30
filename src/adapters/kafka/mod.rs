//! Kafka source and sink adapters.

mod config;
mod inherit;
mod progress;
mod record;
mod sink;
mod source;
mod tombstone;
mod transaction;

pub use config::{KafkaSinkConfig, KafkaSourceConfig};
pub use inherit::KafkaInherit;
pub use record::{KafkaMetadata, KafkaPublish, KafkaRecord};
pub use sink::{KafkaPrepared, KafkaSink};
pub use source::{KafkaMessage, KafkaSource};
pub use transaction::KafkaTransactionalSink;
