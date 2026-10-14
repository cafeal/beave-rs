use crate::propagation::PropagationCarrier;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

/// A typed application property value.
///
/// The variants follow the AMQP 1.0 primitive types that Service Bus accepts in
/// application properties, so a property keeps its type when a record is
/// forwarded. Service Bus refuses binary values and stores symbols as
/// strings; a received symbol is read as `String`, and binary, decimal, and
/// described values are skipped.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ServiceBusValue {
    /// A property without a value.
    Null,
    /// A boolean.
    Bool(bool),
    /// An unsigned 8-bit integer (`ubyte`).
    U8(u8),
    /// An unsigned 16-bit integer (`ushort`).
    U16(u16),
    /// An unsigned 32-bit integer (`uint`).
    U32(u32),
    /// An unsigned 64-bit integer (`ulong`).
    U64(u64),
    /// A signed 8-bit integer (`byte`).
    I8(i8),
    /// A signed 16-bit integer (`short`).
    I16(i16),
    /// A signed 32-bit integer (`int`).
    I32(i32),
    /// A signed 64-bit integer (`long`).
    I64(i64),
    /// A 32-bit float.
    F32(f32),
    /// A 64-bit float.
    F64(f64),
    /// A Unicode character.
    Char(char),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    /// A UUID's 16 bytes.
    Uuid([u8; 16]),
    /// A UTF-8 string.
    String(String),
}

impl ServiceBusValue {
    /// The text of a `String` value.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

impl From<&str> for ServiceBusValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for ServiceBusValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for ServiceBusValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for ServiceBusValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

/// Application properties by name.
pub type ServiceBusApplicationProperties = BTreeMap<String, ServiceBusValue>;

/// Message properties that applications set and read.
///
/// Service Bus limits the identifiers, session IDs, and partition key to 128
/// characters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ServiceBusProperties {
    /// Application message identifier, which duplicate detection compares.
    /// A received numeric, UUID, or binary identifier is read as its decimal,
    /// hyphenated, or hexadecimal text.
    pub message_id: Option<String>,
    /// Application correlation identifier, such as the ID of a request being
    /// answered.
    pub correlation_id: Option<String>,
    /// The session the message belongs to on a session-enabled entity.
    pub session_id: Option<String>,
    /// Address a reply should be sent to.
    pub reply_to: Option<String>,
    /// Session a reply should be sent to.
    pub reply_to_session_id: Option<String>,
    /// Application-specific label, which subscription filters can match.
    pub subject: Option<String>,
    /// MIME type of the body, such as `application/json`.
    pub content_type: Option<String>,
    /// Address the message is intended for, reserved for auto-forwarding
    /// scenarios and not interpreted by Service Bus.
    pub to: Option<String>,
    /// How long the message stays in the entity before it expires, capped by
    /// the entity's default time to live.
    pub time_to_live: Option<Duration>,
    /// Selects the partition of a partitioned entity. On a session message it
    /// must equal the session ID.
    pub partition_key: Option<String>,
}

/// Read-only facts about a received message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ServiceBusMetadata {
    /// The entity the source receives from.
    pub entity_path: String,
    /// The number Service Bus assigned to the message when it was enqueued,
    /// unique within the entity.
    pub sequence_number: i64,
    /// When the message was enqueued, in milliseconds since the Unix epoch.
    pub enqueued_time: Option<i64>,
    /// When a scheduled message was due, in milliseconds since the Unix epoch.
    pub scheduled_enqueue_time: Option<i64>,
    /// How many times Service Bus has handed the message to a receiver,
    /// including this time. The entity's `MaxDeliveryCount` moves the message
    /// to its dead-letter subqueue once exceeded.
    pub delivery_count: u32,
    /// The lock token of this delivery, as a hyphenated UUID.
    pub lock_token: String,
    /// The entity a message read from a dead-letter subqueue was dead-lettered
    /// from, when it was forwarded or expired from another entity.
    pub dead_letter_source: Option<String>,
}

/// A decoded Service Bus message.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ServiceBusRecord<T> {
    /// The decoded message body.
    pub value: T,
    /// The application properties.
    pub application_properties: ServiceBusApplicationProperties,
    /// Message properties set by the sender.
    pub properties: ServiceBusProperties,
    /// Read-only facts about the received message.
    pub metadata: ServiceBusMetadata,
}

/// User-controlled Service Bus output. The entity comes from the sink
/// configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceBusPublish<T> {
    /// The value encoded as the message body.
    pub value: T,
    /// The application properties.
    pub application_properties: ServiceBusApplicationProperties,
    /// Message properties.
    pub properties: ServiceBusProperties,
    /// Keeps the message invisible until this time. Service Bus stores it at
    /// once, so a successful publication still means the message is durable.
    pub scheduled_enqueue_time: Option<SystemTime>,
}

impl<T> ServiceBusPublish<T> {
    /// An output with no application properties, message properties, or schedule.
    pub fn new(value: T) -> Self {
        Self {
            value,
            application_properties: ServiceBusApplicationProperties::new(),
            properties: ServiceBusProperties::default(),
            scheduled_enqueue_time: None,
        }
    }

    /// Sets the message ID, which duplicate detection compares.
    pub fn with_message_id(mut self, message_id: impl Into<String>) -> Self {
        self.properties.message_id = Some(message_id.into());
        self
    }

    /// Sets the session ID, required by a session-enabled entity.
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.properties.session_id = Some(session_id.into());
        self
    }
}

/// Fields are `String` application properties.
impl<T> PropagationCarrier for ServiceBusPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.application_properties
            .insert(name.to_owned(), ServiceBusValue::String(value));
    }
}

/// `String` application properties, in name order.
#[cfg(feature = "testing")]
pub(crate) fn text_properties(properties: &ServiceBusApplicationProperties) -> Vec<(&str, &str)> {
    properties
        .iter()
        .filter_map(|(name, value)| Some((name.as_str(), value.as_str()?)))
        .collect()
}
