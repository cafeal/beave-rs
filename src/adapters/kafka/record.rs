/// Read-only delivery location; never copied into producer routing implicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

/// A decoded Kafka delivery, including nullable values and source metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaRecord<T> {
    pub key: Option<Vec<u8>>,
    pub value: Option<T>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub metadata: KafkaMetadata,
}

impl<T> KafkaRecord<T> {
    pub fn metadata(&self) -> &KafkaMetadata {
        &self.metadata
    }
}

/// User-controlled Kafka output. Source topic, partition, offset, and timestamp are excluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaPublish<T> {
    pub key: Option<Vec<u8>>,
    pub value: Option<T>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl<T> KafkaPublish<T> {
    pub fn new(value: T) -> Self {
        Self {
            key: None,
            value: Some(value),
            headers: Vec::new(),
        }
    }

    pub fn tombstone(key: Vec<u8>) -> Self {
        Self {
            key: Some(key),
            value: None,
            headers: Vec::new(),
        }
    }
}
