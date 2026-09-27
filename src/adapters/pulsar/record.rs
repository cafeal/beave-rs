use pulsar::message::proto::MessageIdData;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulsarMetadata {
    pub topic: String,
    pub message_id: MessageIdData,
    pub publish_time: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulsarRecord<T> {
    pub value: T,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulsarPublish<T> {
    pub value: T,
    pub properties: HashMap<String, String>,
    pub key: Option<Vec<u8>>,
    pub ordering_key: Option<Vec<u8>>,
    pub event_time: Option<u64>,
}

impl<T> PulsarPublish<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            properties: HashMap::new(),
            key: None,
            ordering_key: None,
            event_time: None,
        }
    }
}
