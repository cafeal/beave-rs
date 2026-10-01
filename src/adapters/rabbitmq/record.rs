use crate::propagation::PropagationCarrier;
use serde::Serialize;
use std::collections::BTreeMap;

/// A typed AMQP field value, as carried in message headers.
///
/// The variants follow the AMQP 0-9-1 field types that RabbitMQ accepts, so a
/// header keeps its type when a record is forwarded. Short and long strings are
/// both read as `String`; a long string that is not UTF-8 is read as `Bytes`.
/// `String` is published as a long string and `Bytes` as a byte array.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum RabbitMqValue {
    Void,
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    /// `value` divided by 10 to the power of `scale`.
    Decimal {
        scale: u8,
        value: u32,
    },
    String(String),
    Bytes(Vec<u8>),
    /// Seconds since the Unix epoch.
    Timestamp(u64),
    Array(Vec<RabbitMqValue>),
    Table(BTreeMap<String, RabbitMqValue>),
}

impl RabbitMqValue {
    /// The text of a `String` value.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

impl From<&str> for RabbitMqValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for RabbitMqValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for RabbitMqValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for RabbitMqValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

/// Message headers, the AMQP `headers` property, by name.
pub type RabbitMqHeaders = BTreeMap<String, RabbitMqValue>;

/// AMQP basic properties that applications set and read.
///
/// The delivery mode is chosen by the sink's
/// [`persistent`](super::RabbitMqSinkConfig::persistent) setting and reported
/// in [`RabbitMqMetadata::persistent`]. Text properties are limited to 255
/// bytes by the protocol.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RabbitMqProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    /// Per-message TTL in milliseconds, as text.
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    /// Seconds since the Unix epoch.
    pub timestamp: Option<u64>,
    /// The AMQP `type` property.
    pub kind: Option<String>,
    /// Checked by RabbitMQ against the publishing connection's user.
    pub user_id: Option<String>,
    pub app_id: Option<String>,
}

/// Read-only facts about a received message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RabbitMqMetadata {
    /// The queue the source consumes.
    pub queue: String,
    /// The exchange the message was published to; empty for the default exchange.
    pub exchange: String,
    /// The routing key the message was published with.
    pub routing_key: String,
    /// Whether the broker delivered the message before, to this or another consumer,
    /// without receiving an acknowledgement.
    pub redelivered: bool,
    /// Whether the message was published with the persistent delivery mode.
    pub persistent: bool,
    /// The delivery's tag on the source's channel.
    pub delivery_tag: u64,
}

/// A decoded RabbitMQ delivery. AMQP has no null body, so the value is always present.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RabbitMqRecord<T> {
    pub value: T,
    pub headers: RabbitMqHeaders,
    pub properties: RabbitMqProperties,
    pub metadata: RabbitMqMetadata,
}

impl<T> RabbitMqRecord<T> {
    pub fn metadata(&self) -> &RabbitMqMetadata {
        &self.metadata
    }
}

/// User-controlled RabbitMQ output. The exchange comes from the sink configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct RabbitMqPublish<T> {
    pub value: T,
    /// Overrides the sink's configured routing key.
    pub routing_key: Option<String>,
    pub headers: RabbitMqHeaders,
    pub properties: RabbitMqProperties,
}

impl<T> RabbitMqPublish<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            routing_key: None,
            headers: RabbitMqHeaders::new(),
            properties: RabbitMqProperties::default(),
        }
    }

    pub fn with_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = Some(routing_key.into());
        self
    }
}

/// Fields are string headers.
impl<T> PropagationCarrier for RabbitMqPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.headers
            .insert(name.to_owned(), RabbitMqValue::String(value));
    }
}

/// String headers, in name order.
#[cfg(feature = "testing")]
pub(crate) fn text_headers(headers: &RabbitMqHeaders) -> Vec<(&str, &str)> {
    headers
        .iter()
        .filter_map(|(name, value)| Some((name.as_str(), value.as_str()?)))
        .collect()
}
