//! Conversions between the adapter's record fields and AMQP protocol types.
use super::record::{RabbitMqHeaders, RabbitMqProperties, RabbitMqValue};
use crate::error::{BoxError, Context, Error};
use lapin::{
    BasicProperties,
    types::{AMQPValue, ByteArray, DecimalValue, FieldArray, FieldTable, ShortString},
};

/// Delivery mode of a message the broker writes to disk in durable queues.
const PERSISTENT: u8 = 2;

pub(super) fn short_string(what: &str, value: &str) -> Result<ShortString, Error> {
    ShortString::try_new(value).map_err(|_| Error::config(format!("RabbitMQ {what} is too long")))
}

pub(super) fn value_from_amqp(value: &AMQPValue) -> RabbitMqValue {
    match value {
        AMQPValue::Boolean(value) => RabbitMqValue::Bool(*value),
        AMQPValue::ShortShortInt(value) => RabbitMqValue::I8(*value),
        AMQPValue::ShortShortUInt(value) => RabbitMqValue::U8(*value),
        AMQPValue::ShortInt(value) => RabbitMqValue::I16(*value),
        AMQPValue::ShortUInt(value) => RabbitMqValue::U16(*value),
        AMQPValue::LongInt(value) => RabbitMqValue::I32(*value),
        AMQPValue::LongUInt(value) => RabbitMqValue::U32(*value),
        AMQPValue::LongLongInt(value) => RabbitMqValue::I64(*value),
        AMQPValue::Float(value) => RabbitMqValue::F32(*value),
        AMQPValue::Double(value) => RabbitMqValue::F64(*value),
        AMQPValue::DecimalValue(DecimalValue { scale, value }) => RabbitMqValue::Decimal {
            scale: *scale,
            value: *value,
        },
        AMQPValue::ShortString(value) => RabbitMqValue::String(value.as_str().to_owned()),
        AMQPValue::LongString(value) => match std::str::from_utf8(value.as_bytes()) {
            Ok(text) => RabbitMqValue::String(text.to_owned()),
            Err(_) => RabbitMqValue::Bytes(value.as_bytes().to_vec()),
        },
        AMQPValue::FieldArray(values) => {
            RabbitMqValue::Array(values.as_slice().iter().map(value_from_amqp).collect())
        }
        AMQPValue::Timestamp(value) => RabbitMqValue::Timestamp(*value),
        AMQPValue::FieldTable(table) => RabbitMqValue::Table(headers_from_amqp(Some(table))),
        AMQPValue::ByteArray(value) => RabbitMqValue::Bytes(value.as_slice().to_vec()),
        AMQPValue::Void => RabbitMqValue::Void,
    }
}

fn value_to_amqp(value: &RabbitMqValue) -> Result<AMQPValue, BoxError> {
    Ok(match value {
        RabbitMqValue::Void => AMQPValue::Void,
        RabbitMqValue::Bool(value) => AMQPValue::Boolean(*value),
        RabbitMqValue::I8(value) => AMQPValue::ShortShortInt(*value),
        RabbitMqValue::U8(value) => AMQPValue::ShortShortUInt(*value),
        RabbitMqValue::I16(value) => AMQPValue::ShortInt(*value),
        RabbitMqValue::U16(value) => AMQPValue::ShortUInt(*value),
        RabbitMqValue::I32(value) => AMQPValue::LongInt(*value),
        RabbitMqValue::U32(value) => AMQPValue::LongUInt(*value),
        RabbitMqValue::I64(value) => AMQPValue::LongLongInt(*value),
        RabbitMqValue::F32(value) => AMQPValue::Float(*value),
        RabbitMqValue::F64(value) => AMQPValue::Double(*value),
        RabbitMqValue::Decimal { scale, value } => AMQPValue::DecimalValue(DecimalValue {
            scale: *scale,
            value: *value,
        }),
        RabbitMqValue::String(value) => AMQPValue::LongString(value.as_bytes().into()),
        RabbitMqValue::Bytes(value) => AMQPValue::ByteArray(ByteArray::from(value.clone())),
        RabbitMqValue::Timestamp(value) => AMQPValue::Timestamp(*value),
        RabbitMqValue::Array(values) => AMQPValue::FieldArray(FieldArray::from(
            values
                .iter()
                .map(value_to_amqp)
                .collect::<Result<Vec<_>, BoxError>>()?,
        )),
        RabbitMqValue::Table(table) => AMQPValue::FieldTable(headers_to_amqp(table)?),
    })
}

pub(super) fn headers_from_amqp(table: Option<&FieldTable>) -> RabbitMqHeaders {
    table
        .map(|table| {
            table
                .inner()
                .iter()
                .map(|(name, value)| (name.as_str().to_owned(), value_from_amqp(value)))
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn headers_to_amqp(headers: &RabbitMqHeaders) -> Result<FieldTable, BoxError> {
    let mut table = FieldTable::default();
    for (name, value) in headers {
        let value = value_to_amqp(value).with_context(|| format!("RabbitMQ header {name:?}"))?;
        table.insert(short_string("header name", name)?, value);
    }
    Ok(table)
}

pub(super) fn properties_from_amqp(properties: &BasicProperties) -> RabbitMqProperties {
    let text = |value: &Option<ShortString>| value.as_ref().map(|value| value.as_str().to_owned());
    RabbitMqProperties {
        content_type: text(properties.content_type()),
        content_encoding: text(properties.content_encoding()),
        priority: *properties.priority(),
        correlation_id: text(properties.correlation_id()),
        reply_to: text(properties.reply_to()),
        expiration: text(properties.expiration()),
        message_id: text(properties.message_id()),
        timestamp: *properties.timestamp(),
        kind: text(properties.kind()),
        user_id: text(properties.user_id()),
        app_id: text(properties.app_id()),
    }
}

pub(super) fn is_persistent(properties: &BasicProperties) -> bool {
    *properties.delivery_mode() == Some(PERSISTENT)
}

/// Builds the basic properties of a published message.
pub(super) fn properties_to_amqp(
    properties: &RabbitMqProperties,
    headers: &RabbitMqHeaders,
    persistent: bool,
) -> Result<BasicProperties, BoxError> {
    let mut amqp = BasicProperties::default();
    let text = |name: &str, value: &Option<String>| {
        value
            .as_deref()
            .map(|value| short_string(name, value))
            .transpose()
    };
    if let Some(value) = text("content type", &properties.content_type)? {
        amqp = amqp.with_content_type(value);
    }
    if let Some(value) = text("content encoding", &properties.content_encoding)? {
        amqp = amqp.with_content_encoding(value);
    }
    if let Some(value) = properties.priority {
        amqp = amqp.with_priority(value);
    }
    if let Some(value) = text("correlation ID", &properties.correlation_id)? {
        amqp = amqp.with_correlation_id(value);
    }
    if let Some(value) = text("reply-to", &properties.reply_to)? {
        amqp = amqp.with_reply_to(value);
    }
    if let Some(value) = text("expiration", &properties.expiration)? {
        amqp = amqp.with_expiration(value);
    }
    if let Some(value) = text("message ID", &properties.message_id)? {
        amqp = amqp.with_message_id(value);
    }
    if let Some(value) = properties.timestamp {
        amqp = amqp.with_timestamp(value);
    }
    if let Some(value) = text("type", &properties.kind)? {
        amqp = amqp.with_type(value);
    }
    if let Some(value) = text("user ID", &properties.user_id)? {
        amqp = amqp.with_user_id(value);
    }
    if let Some(value) = text("app ID", &properties.app_id)? {
        amqp = amqp.with_app_id(value);
    }
    if !headers.is_empty() {
        amqp = amqp.with_headers(headers_to_amqp(headers)?);
    }
    if persistent {
        amqp = amqp.with_delivery_mode(PERSISTENT);
    }
    Ok(amqp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn header_values_keep_their_type_through_amqp() {
        let headers: RabbitMqHeaders = [
            ("bool", RabbitMqValue::Bool(true)),
            ("i8", RabbitMqValue::I8(-1)),
            ("u16", RabbitMqValue::U16(7)),
            ("i64", RabbitMqValue::I64(-5)),
            ("f64", RabbitMqValue::F64(1.5)),
            (
                "decimal",
                RabbitMqValue::Decimal {
                    scale: 2,
                    value: 314,
                },
            ),
            ("text", RabbitMqValue::from("hello")),
            ("bytes", RabbitMqValue::Bytes(vec![0xff, 0])),
            ("time", RabbitMqValue::Timestamp(1_700_000_000)),
            ("void", RabbitMqValue::Void),
            (
                "nested",
                RabbitMqValue::Table(BTreeMap::from([(
                    "list".to_owned(),
                    RabbitMqValue::Array(vec![RabbitMqValue::I32(1), RabbitMqValue::from("a")]),
                )])),
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
        let table = headers_to_amqp(&headers).unwrap();
        assert_eq!(headers_from_amqp(Some(&table)), headers);
    }

    #[test]
    fn non_utf8_long_strings_are_read_as_bytes() {
        let value = AMQPValue::LongString(vec![0xff, 0xfe].into());
        assert_eq!(
            value_from_amqp(&value),
            RabbitMqValue::Bytes(vec![0xff, 0xfe])
        );
    }

    #[test]
    fn properties_round_trip_and_set_the_delivery_mode() {
        let properties = RabbitMqProperties {
            content_type: Some("application/json".into()),
            priority: Some(3),
            correlation_id: Some("c-1".into()),
            expiration: Some("60000".into()),
            message_id: Some("m-1".into()),
            timestamp: Some(1_700_000_000),
            kind: Some("order".into()),
            app_id: Some("beavers".into()),
            ..RabbitMqProperties::default()
        };
        let amqp = properties_to_amqp(&properties, &RabbitMqHeaders::new(), true).unwrap();
        assert_eq!(properties_from_amqp(&amqp), properties);
        assert!(is_persistent(&amqp));
        assert!(amqp.headers().is_none());
        let transient = properties_to_amqp(&properties, &RabbitMqHeaders::new(), false).unwrap();
        assert!(!is_persistent(&transient));
    }

    #[test]
    fn overlong_short_strings_are_errors() {
        let properties = RabbitMqProperties {
            message_id: Some("x".repeat(256)),
            ..RabbitMqProperties::default()
        };
        let error = properties_to_amqp(&properties, &RabbitMqHeaders::new(), true).unwrap_err();
        assert!(error.to_string().contains("message ID is too long"));
        let headers = RabbitMqHeaders::from([("y".repeat(256), RabbitMqValue::Void)]);
        assert!(headers_to_amqp(&headers).is_err());
    }
}
