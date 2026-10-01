use crate::propagation::PropagationCarrier;
use serde::Serialize;
use std::collections::HashMap;

/// Read-only facts about a received message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PulsarMetadata {
    /// The topic the message was received from. For a partitioned topic this
    /// is the partition's topic, such as `persistent://tenant/ns/orders-partition-2`.
    pub topic: String,
    pub message_id: PulsarMessageId,
    /// Broker publish time in milliseconds since the Unix epoch.
    pub publish_time: u64,
}

/// The broker-assigned position of a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct PulsarMessageId {
    pub ledger_id: u64,
    pub entry_id: u64,
    /// Partition index, or `-1` for a non-partitioned topic.
    pub partition: i32,
    /// Index within a batched entry, or `-1` when the entry is not a batch.
    pub batch_index: i32,
}

/// A decoded Pulsar delivery. `value` is `None` when the producer marked the
/// message value as null, which topic compaction treats as a key deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PulsarRecord<T> {
    pub value: Option<T>,
    pub key: Option<Vec<u8>>,
    pub properties: HashMap<String, String>,
    pub event_time: Option<u64>,
    pub metadata: PulsarMetadata,
}

impl<T> PulsarRecord<T> {
    pub fn metadata(&self) -> &PulsarMetadata {
        &self.metadata
    }
}

/// User-controlled Pulsar output. A `None` value is a tombstone, published as
/// an empty payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulsarPublish<T> {
    pub value: Option<T>,
    pub properties: HashMap<String, String>,
    pub key: Option<Vec<u8>>,
    pub ordering_key: Option<Vec<u8>>,
    pub event_time: Option<u64>,
}

impl<T> PulsarPublish<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: Some(value),
            properties: HashMap::new(),
            key: None,
            ordering_key: None,
            event_time: None,
        }
    }

    pub fn tombstone(key: Vec<u8>) -> Self {
        Self {
            value: None,
            properties: HashMap::new(),
            key: Some(key),
            ordering_key: None,
            event_time: None,
        }
    }
}

/// Fields are message properties.
impl<T> PropagationCarrier for PulsarPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.properties.insert(name.to_owned(), value);
    }
}
