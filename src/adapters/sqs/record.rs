use crate::propagation::PropagationCarrier;
use serde::Serialize;
use std::{collections::BTreeMap, time::Duration};

/// The value of an SQS message attribute.
///
/// A custom type label, such as `Number.float` or `String.json`, is read as its
/// base type and not published again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum SqsAttributeValue {
    /// A text value.
    String(String),
    /// A number in its decimal text form, as SQS stores it.
    Number(String),
    /// A binary value.
    Binary(Vec<u8>),
}

impl SqsAttributeValue {
    /// The text of a `String` value.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

impl From<&str> for SqsAttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for SqsAttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// Message attributes by name. SQS accepts at most 10 per message.
pub type SqsAttributes = BTreeMap<String, SqsAttributeValue>;

/// Read-only facts about a received message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SqsMetadata {
    /// The queue the source consumes.
    pub queue_url: String,
    /// The ID SQS assigned to the message when it was sent.
    pub message_id: String,
    /// How many times SQS has handed the message to a consumer, including
    /// this time. A queue's redrive policy compares it with `maxReceiveCount`.
    pub receive_count: u32,
    /// When the message was sent, in milliseconds since the Unix epoch.
    pub sent_timestamp: Option<u64>,
    /// When the message was first received, in milliseconds since the Unix epoch.
    pub first_receive_timestamp: Option<u64>,
    /// The message group of a FIFO queue message.
    pub message_group_id: Option<String>,
    /// The deduplication ID of a FIFO queue message.
    pub deduplication_id: Option<String>,
    /// The sequence number SQS assigned to a FIFO queue message.
    pub sequence_number: Option<String>,
}

/// A decoded SQS message. SQS has no null body, so the value is always present.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SqsRecord<T> {
    /// The decoded message body.
    pub value: T,
    /// The message attributes.
    pub attributes: SqsAttributes,
    /// Read-only facts about the received message.
    pub metadata: SqsMetadata,
}

impl<T> SqsRecord<T> {
    /// Read-only facts about the received message.
    pub fn metadata(&self) -> &SqsMetadata {
        &self.metadata
    }
}

/// User-controlled SQS output. The queue comes from the sink configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqsPublish<T> {
    /// The value encoded as the message body.
    pub value: T,
    /// The message attributes, at most 10.
    pub attributes: SqsAttributes,
    /// Required by a FIFO queue; messages of one group are delivered in order.
    pub message_group_id: Option<String>,
    /// Deduplication ID for a FIFO queue without content-based deduplication.
    pub deduplication_id: Option<String>,
    /// How long the message stays invisible after it is sent, up to 15
    /// minutes. FIFO queues accept only a queue-wide delay.
    pub delay: Option<Duration>,
}

impl<T> SqsPublish<T> {
    /// An output with no attributes, group, deduplication ID, or delay.
    pub fn new(value: T) -> Self {
        Self {
            value,
            attributes: SqsAttributes::new(),
            message_group_id: None,
            deduplication_id: None,
            delay: None,
        }
    }

    /// Sets the FIFO message group.
    pub fn with_message_group_id(mut self, group: impl Into<String>) -> Self {
        self.message_group_id = Some(group.into());
        self
    }
}

/// Fields are `String` message attributes.
impl<T> PropagationCarrier for SqsPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.attributes
            .insert(name.to_owned(), SqsAttributeValue::String(value));
    }
}

/// `String` attributes, in name order.
#[cfg(feature = "testing")]
pub(crate) fn text_attributes(attributes: &SqsAttributes) -> Vec<(&str, &str)> {
    attributes
        .iter()
        .filter_map(|(name, value)| Some((name.as_str(), value.as_str()?)))
        .collect()
}
