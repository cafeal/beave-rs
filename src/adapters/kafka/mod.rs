//! Kafka source and sink adapters.

mod config;
mod progress;
mod record;
mod sink;
mod source;

pub use config::{KafkaSinkConfig, KafkaSourceConfig};
pub use record::{KafkaMetadata, KafkaPublish, KafkaRecord};
pub use sink::{KafkaPrepared, KafkaSink};
pub use source::{KafkaMessage, KafkaSource};
