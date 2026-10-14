//! Conversions between record fields and AMQP message sections.
use super::record::{ServiceBusApplicationProperties, ServiceBusProperties, ServiceBusValue};
use crate::error::{Error, ensure};
use fe2o3_amqp::types::{
    messaging::{
        Annotations, ApplicationProperties, Body, Data, Header, Message, MessageAnnotations,
        MessageId, Properties, annotations::OwnedKey,
    },
    primitives::{Binary, SimpleValue, Symbol, Timestamp, Uuid, Value},
};
use std::{
    fmt::Write as _,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(super) const SEQUENCE_NUMBER: &str = "x-opt-sequence-number";
pub(super) const ENQUEUED_TIME: &str = "x-opt-enqueued-time";
pub(super) const SCHEDULED_ENQUEUE_TIME: &str = "x-opt-scheduled-enqueue-time";
pub(super) const LOCKED_UNTIL: &str = "x-opt-locked-until";
pub(super) const PARTITION_KEY: &str = "x-opt-partition-key";
pub(super) const DEAD_LETTER_SOURCE: &str = "x-opt-deadletter-source";
pub(super) const LOCK_TOKEN: &str = "x-opt-lock-token";

/// Longest identifier, session ID, or partition key Service Bus accepts.
const MAX_ID_LENGTH: usize = 128;

pub(super) fn value_from_amqp(value: &SimpleValue) -> Option<ServiceBusValue> {
    Some(match value {
        SimpleValue::Null => ServiceBusValue::Null,
        SimpleValue::Bool(value) => ServiceBusValue::Bool(*value),
        SimpleValue::Ubyte(value) => ServiceBusValue::U8(*value),
        SimpleValue::Ushort(value) => ServiceBusValue::U16(*value),
        SimpleValue::Uint(value) => ServiceBusValue::U32(*value),
        SimpleValue::Ulong(value) => ServiceBusValue::U64(*value),
        SimpleValue::Byte(value) => ServiceBusValue::I8(*value),
        SimpleValue::Short(value) => ServiceBusValue::I16(*value),
        SimpleValue::Int(value) => ServiceBusValue::I32(*value),
        SimpleValue::Long(value) => ServiceBusValue::I64(*value),
        SimpleValue::Float(value) => ServiceBusValue::F32(value.0),
        SimpleValue::Double(value) => ServiceBusValue::F64(value.0),
        SimpleValue::Char(value) => ServiceBusValue::Char(*value),
        SimpleValue::Timestamp(value) => ServiceBusValue::Timestamp(value.milliseconds()),
        SimpleValue::Uuid(value) => ServiceBusValue::Uuid(*value.as_inner()),
        SimpleValue::String(value) => ServiceBusValue::String(value.clone()),
        SimpleValue::Symbol(value) => ServiceBusValue::String(value.0.clone()),
        SimpleValue::Binary(_)
        | SimpleValue::Decimal32(_)
        | SimpleValue::Decimal64(_)
        | SimpleValue::Decimal128(_)
        | SimpleValue::Described(_) => return None,
    })
}

pub(super) fn value_to_amqp(value: &ServiceBusValue) -> SimpleValue {
    match value {
        ServiceBusValue::Null => SimpleValue::Null,
        ServiceBusValue::Bool(value) => SimpleValue::Bool(*value),
        ServiceBusValue::U8(value) => SimpleValue::Ubyte(*value),
        ServiceBusValue::U16(value) => SimpleValue::Ushort(*value),
        ServiceBusValue::U32(value) => SimpleValue::Uint(*value),
        ServiceBusValue::U64(value) => SimpleValue::Ulong(*value),
        ServiceBusValue::I8(value) => SimpleValue::Byte(*value),
        ServiceBusValue::I16(value) => SimpleValue::Short(*value),
        ServiceBusValue::I32(value) => SimpleValue::Int(*value),
        ServiceBusValue::I64(value) => SimpleValue::Long(*value),
        ServiceBusValue::F32(value) => SimpleValue::from(*value),
        ServiceBusValue::F64(value) => SimpleValue::from(*value),
        ServiceBusValue::Char(value) => SimpleValue::Char(*value),
        ServiceBusValue::Timestamp(value) => {
            SimpleValue::Timestamp(Timestamp::from_milliseconds(*value))
        }
        ServiceBusValue::Uuid(value) => SimpleValue::Uuid(Uuid::from(*value)),
        ServiceBusValue::String(value) => SimpleValue::String(value.clone()),
    }
}

pub(super) fn application_properties_from_amqp(
    properties: Option<&ApplicationProperties>,
) -> ServiceBusApplicationProperties {
    properties
        .into_iter()
        .flat_map(|properties| properties.0.iter())
        .filter_map(|(name, value)| Some((name.clone(), value_from_amqp(value)?)))
        .collect()
}

/// A message annotation by its symbol name.
pub(super) fn annotation<'a>(message: &'a Message<Body<Value>>, name: &str) -> Option<&'a Value> {
    find(&message.message_annotations.as_ref()?.0, name)
}

pub(super) fn find<'a>(annotations: &'a Annotations, name: &str) -> Option<&'a Value> {
    annotations.iter().find_map(|(key, value)| match key {
        OwnedKey::Symbol(symbol) if symbol.0 == name => Some(value),
        _ => None,
    })
}

pub(super) fn timestamp(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Timestamp(timestamp) => Some(timestamp.milliseconds()),
        _ => None,
    }
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Symbol(symbol) => Some(symbol.0.clone()),
        _ => None,
    }
}

/// A message ID as text: a string as is, a number in decimal, a UUID
/// hyphenated, and binary in lowercase hexadecimal.
pub(super) fn message_id_text(id: &MessageId) -> String {
    match id {
        MessageId::String(id) => id.clone(),
        MessageId::Ulong(id) => id.to_string(),
        MessageId::Uuid(id) => uuid_text(id.as_inner()),
        MessageId::Binary(id) => id.iter().fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        }),
    }
}

/// A UUID in its hyphenated form.
pub(super) fn uuid_text(bytes: &[u8; 16]) -> String {
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        let _ = write!(text, "{byte:02x}");
    }
    text
}

pub(super) fn properties_from_amqp(message: &Message<Body<Value>>) -> ServiceBusProperties {
    let properties = message.properties.as_ref();
    let field = |get: fn(&Properties) -> Option<&String>| properties.and_then(get).cloned();
    ServiceBusProperties {
        message_id: properties
            .and_then(|properties| properties.message_id.as_ref())
            .map(message_id_text),
        correlation_id: properties
            .and_then(|properties| properties.correlation_id.as_ref())
            .map(message_id_text),
        session_id: field(|properties| properties.group_id.as_ref()),
        reply_to: field(|properties| properties.reply_to.as_ref()),
        reply_to_session_id: field(|properties| properties.reply_to_group_id.as_ref()),
        subject: field(|properties| properties.subject.as_ref()),
        content_type: properties
            .and_then(|properties| properties.content_type.as_ref())
            .map(|content_type| content_type.0.clone()),
        to: field(|properties| properties.to.as_ref()),
        time_to_live: message
            .header
            .as_ref()
            .and_then(|header| header.ttl)
            .map(|ttl| Duration::from_millis(ttl.into())),
        partition_key: text(annotation(message, PARTITION_KEY)),
    }
}

/// The message body as bytes, or a description of a body that is not binary
/// data together with its AMQP encoding.
pub(super) fn body_bytes(body: &Body<Value>) -> Result<Vec<u8>, (&'static str, Vec<u8>)> {
    match body {
        Body::Data(sections) => Ok(sections
            .iter()
            .flat_map(|section| section.0.iter().copied())
            .collect()),
        Body::Empty => Ok(Vec::new()),
        Body::Value(value) => Err((
            "an AMQP value",
            serde_amqp::to_vec(&value.0).unwrap_or_default(),
        )),
        Body::Sequence(sequences) => Err((
            "an AMQP sequence",
            sequences
                .iter()
                .flat_map(|sequence| serde_amqp::to_vec(&sequence.0).unwrap_or_default())
                .collect(),
        )),
    }
}

/// Builds the message to send, checking the limits Service Bus would reject.
pub(super) fn message_to_amqp(
    body: Vec<u8>,
    application_properties: &ServiceBusApplicationProperties,
    properties: &ServiceBusProperties,
    scheduled_enqueue_time: Option<SystemTime>,
) -> Result<Message<Data>, Error> {
    for (name, value) in [
        ("message ID", &properties.message_id),
        ("correlation ID", &properties.correlation_id),
        ("session ID", &properties.session_id),
        ("reply-to session ID", &properties.reply_to_session_id),
        ("partition key", &properties.partition_key),
    ] {
        if let Some(value) = value {
            ensure!(
                value.chars().count() <= MAX_ID_LENGTH,
                Error::invalid_record,
                "Service Bus {name} must be at most {MAX_ID_LENGTH} characters"
            );
        }
    }
    if let (Some(session), Some(partition_key)) =
        (&properties.session_id, &properties.partition_key)
    {
        ensure!(
            session == partition_key,
            Error::invalid_record,
            "Service Bus partition key must equal the session ID of a session message"
        );
    }
    let ttl = properties
        .time_to_live
        .map(|ttl| {
            ensure!(
                !ttl.is_zero(),
                Error::invalid_record,
                "Service Bus time to live must be positive"
            );
            u32::try_from(ttl.as_millis()).map_err(|_| {
                Error::invalid_record("Service Bus time to live must be under 49 days")
            })
        })
        .transpose()?;
    let mut annotations = Annotations::new();
    if let Some(partition_key) = &properties.partition_key {
        annotations.insert(
            OwnedKey::Symbol(Symbol::from(PARTITION_KEY)),
            Value::String(partition_key.clone()),
        );
    }
    if let Some(time) = scheduled_enqueue_time {
        let milliseconds = time
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::invalid_record("Service Bus schedule is before the Unix epoch"))?
            .as_millis();
        annotations.insert(
            OwnedKey::Symbol(Symbol::from(SCHEDULED_ENQUEUE_TIME)),
            Value::Timestamp(Timestamp::from_milliseconds(
                i64::try_from(milliseconds).map_err(|_| {
                    Error::invalid_record("Service Bus schedule is too far in the future")
                })?,
            )),
        );
    }
    let amqp_properties = Properties {
        message_id: properties.message_id.clone().map(MessageId::String),
        correlation_id: properties.correlation_id.clone().map(MessageId::String),
        group_id: properties.session_id.clone(),
        reply_to: properties.reply_to.clone(),
        reply_to_group_id: properties.reply_to_session_id.clone(),
        subject: properties.subject.clone(),
        content_type: properties.content_type.as_deref().map(Symbol::from),
        to: properties.to.clone(),
        ..Properties::default()
    };
    Ok(Message {
        header: ttl.map(|ttl| Header {
            ttl: Some(ttl),
            ..Header::default()
        }),
        delivery_annotations: None,
        message_annotations: (!annotations.is_empty()).then_some(MessageAnnotations(annotations)),
        properties: Some(amqp_properties),
        application_properties: (!application_properties.is_empty()).then(|| {
            ApplicationProperties(
                application_properties
                    .iter()
                    .map(|(name, value)| (name.clone(), value_to_amqp(value)))
                    .collect(),
            )
        }),
        body: Data(Binary::from(body)),
        footer: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn received(message: Message<Data>) -> Message<Body<Value>> {
        Message {
            header: message.header,
            delivery_annotations: message.delivery_annotations,
            message_annotations: message.message_annotations,
            properties: message.properties,
            application_properties: message.application_properties,
            body: Body::Data(vec![message.body].into()),
            footer: message.footer,
        }
    }

    #[test]
    fn application_properties_round_trip() {
        let properties: ServiceBusApplicationProperties = [
            ("text", ServiceBusValue::from("hello")),
            ("flag", ServiceBusValue::Bool(true)),
            ("count", ServiceBusValue::I64(-4)),
            ("small", ServiceBusValue::U8(7)),
            ("ratio", ServiceBusValue::F64(0.5)),
            ("at", ServiceBusValue::Timestamp(1_700_000_000_000)),
            ("id", ServiceBusValue::Uuid([7; 16])),
            ("none", ServiceBusValue::Null),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
        let message = message_to_amqp(Vec::new(), &properties, &Default::default(), None).unwrap();
        assert_eq!(
            application_properties_from_amqp(message.application_properties.as_ref()),
            properties
        );
    }

    #[test]
    fn message_properties_round_trip() {
        let properties = ServiceBusProperties {
            message_id: Some("m-1".into()),
            correlation_id: Some("c-1".into()),
            session_id: Some("customer-7".into()),
            reply_to: Some("replies".into()),
            reply_to_session_id: Some("r-1".into()),
            subject: Some("created".into()),
            content_type: Some("application/json".into()),
            to: Some("orders".into()),
            time_to_live: Some(Duration::from_secs(60)),
            partition_key: Some("customer-7".into()),
        };
        let at = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let message = received(
            message_to_amqp(b"body".to_vec(), &Default::default(), &properties, Some(at)).unwrap(),
        );
        assert_eq!(properties_from_amqp(&message), properties);
        assert_eq!(
            timestamp(annotation(&message, SCHEDULED_ENQUEUE_TIME)),
            Some(1_700_000_000_123)
        );
        assert_eq!(body_bytes(&message.body).unwrap(), b"body");
    }

    #[test]
    fn invalid_outputs_are_refused() {
        let long = "x".repeat(129);
        for properties in [
            ServiceBusProperties {
                message_id: Some(long.clone()),
                ..Default::default()
            },
            ServiceBusProperties {
                session_id: Some("a".into()),
                partition_key: Some("b".into()),
                ..Default::default()
            },
            ServiceBusProperties {
                time_to_live: Some(Duration::ZERO),
                ..Default::default()
            },
            ServiceBusProperties {
                time_to_live: Some(Duration::from_secs(50 * 24 * 60 * 60)),
                ..Default::default()
            },
        ] {
            assert!(
                message_to_amqp(Vec::new(), &Default::default(), &properties, None).is_err(),
                "{properties:?}"
            );
        }
    }

    #[test]
    fn identifiers_are_read_as_text() {
        assert_eq!(message_id_text(&MessageId::Ulong(42)), "42");
        assert_eq!(
            message_id_text(&MessageId::Binary(Binary::from(vec![0x0a, 0xff]))),
            "0aff"
        );
        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ];
        assert_eq!(uuid_text(&uuid), "12345678-9abc-def0-0123-456789abcdef");
    }

    #[test]
    fn bodies_other_than_data_are_reported_with_their_encoding() {
        use fe2o3_amqp::types::messaging::AmqpValue;
        let (kind, encoded) =
            body_bytes(&Body::Value(AmqpValue(Value::String("text".into())))).unwrap_err();
        assert_eq!(kind, "an AMQP value");
        assert!(!encoded.is_empty());
        assert_eq!(body_bytes(&Body::Empty).unwrap(), Vec::<u8>::new());
    }
}
