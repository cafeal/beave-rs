use crate::propagation::PropagationCarrier;
use serde::Serialize;
use std::collections::BTreeMap;

/// Message attributes by name. Pub/Sub accepts at most 100 per message, names
/// of at most 256 bytes that do not start with `goog`, and values of at most
/// 1024 bytes.
pub type PubSubAttributes = BTreeMap<String, String>;

/// Read-only facts about a received message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct PubSubMetadata {
    /// The subscription the source consumes.
    pub subscription: String,
    /// The ID Pub/Sub assigned to the message when it was published.
    pub message_id: String,
    /// When Pub/Sub received the message from its publisher, in milliseconds
    /// since the Unix epoch.
    pub publish_time: Option<i64>,
    /// How many times Pub/Sub has delivered the message, including this time.
    /// Pub/Sub counts only on subscriptions with a dead-letter policy.
    pub delivery_attempt: Option<u32>,
}

/// A decoded Pub/Sub message. Pub/Sub has no null message data, so the value
/// is always present.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PubSubRecord<T> {
    /// The decoded message data.
    pub value: T,
    /// The message attributes.
    pub attributes: PubSubAttributes,
    /// The ordering key the message was published with.
    pub ordering_key: Option<String>,
    /// Read-only facts about the received message.
    pub metadata: PubSubMetadata,
}

/// User-controlled Pub/Sub output. The topic comes from the sink configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PubSubPublish<T> {
    /// The value encoded as the message data.
    pub value: T,
    /// The message attributes.
    pub attributes: PubSubAttributes,
    /// Messages of one ordering key are delivered in publication order on a
    /// subscription with message ordering enabled.
    pub ordering_key: Option<String>,
}

impl<T> PubSubPublish<T> {
    /// An output with no attributes or ordering key.
    pub fn new(value: T) -> Self {
        Self {
            value,
            attributes: PubSubAttributes::new(),
            ordering_key: None,
        }
    }

    /// Sets the ordering key.
    pub fn with_ordering_key(mut self, key: impl Into<String>) -> Self {
        self.ordering_key = Some(key.into());
        self
    }
}

/// Fields are message attributes.
impl<T> PropagationCarrier for PubSubPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.attributes.insert(name.to_owned(), value);
    }
}
