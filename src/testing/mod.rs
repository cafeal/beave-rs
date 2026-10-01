//! Utilities for testing handlers, middleware, and error policies without a
//! broker (`testing` feature).

mod deliveries;
mod record;
mod source;

pub use deliveries::{Deliveries, DeliveryState};
pub use record::TestRecord;
#[cfg(feature = "kafka")]
pub use record::kafka_record;
#[cfg(feature = "pulsar")]
pub use record::pulsar_record;
#[cfg(feature = "kafka")]
pub use source::KafkaTestSource;
#[cfg(feature = "pulsar")]
pub use source::PulsarTestSource;
pub use source::{TestMessage, TestSource};
