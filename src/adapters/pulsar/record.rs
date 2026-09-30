use crate::propagation::PropagationCarrier;
use pulsar::message::proto::MessageIdData;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulsarMetadata {
    pub topic: String,
    pub message_id: MessageIdData,
    pub publish_time: u64,
}

/// A decoded Pulsar delivery. `value` is `None` when the producer marked the
/// message value as null, which topic compaction treats as a key deletion.
#[derive(Clone, Debug, PartialEq, Eq)]
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
