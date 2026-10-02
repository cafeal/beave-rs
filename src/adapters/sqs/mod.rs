//! Amazon SQS source and sink adapters.

mod client;
mod config;
mod convert;
mod dead_letter;
mod inherit;
mod lease;
mod record;
mod sink;
mod source;

pub use config::{SqsCredentials, SqsSinkConfig, SqsSourceConfig};
pub use dead_letter::{SqsDeadLetter, SqsOrigin};
pub use inherit::SqsInherit;
#[cfg(feature = "testing")]
pub(crate) use record::text_attributes;
pub use record::{SqsAttributeValue, SqsAttributes, SqsMetadata, SqsPublish, SqsRecord};
pub use sink::{SqsPrepared, SqsSink};
pub use source::{SqsMessage, SqsSource};
