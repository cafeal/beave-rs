//! Conversions between record fields and SQS API types.
use super::record::{SqsAttributeValue, SqsAttributes};
use crate::error::BoxError;
use aws_sdk_sqs::{primitives::Blob, types::MessageAttributeValue};
use std::collections::HashMap;

pub(super) fn attribute_from_sqs(value: &MessageAttributeValue) -> Option<SqsAttributeValue> {
    let data_type = value.data_type();
    let base = data_type.split('.').next().unwrap_or(data_type);
    match base {
        "String" => Some(SqsAttributeValue::String(value.string_value()?.to_owned())),
        "Number" => Some(SqsAttributeValue::Number(value.string_value()?.to_owned())),
        "Binary" => Some(SqsAttributeValue::Binary(
            value.binary_value()?.as_ref().to_vec(),
        )),
        _ => None,
    }
}

pub(super) fn attributes_from_sqs(
    attributes: Option<&HashMap<String, MessageAttributeValue>>,
) -> SqsAttributes {
    attributes
        .into_iter()
        .flatten()
        .filter_map(|(name, value)| Some((name.clone(), attribute_from_sqs(value)?)))
        .collect()
}

pub(super) fn attributes_to_sqs(
    attributes: &SqsAttributes,
) -> Result<HashMap<String, MessageAttributeValue>, BoxError> {
    attributes
        .iter()
        .map(|(name, value)| {
            let builder = MessageAttributeValue::builder();
            let builder = match value {
                SqsAttributeValue::String(value) => builder.data_type("String").string_value(value),
                SqsAttributeValue::Number(value) => builder.data_type("Number").string_value(value),
                SqsAttributeValue::Binary(value) => builder
                    .data_type("Binary")
                    .binary_value(Blob::new(value.clone())),
            };
            Ok((name.clone(), builder.build()?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_round_trip() {
        let attributes: SqsAttributes = [
            ("text", SqsAttributeValue::from("hello")),
            ("count", SqsAttributeValue::Number("42".into())),
            ("bytes", SqsAttributeValue::Binary(vec![0, 0xff])),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
        let sqs = attributes_to_sqs(&attributes).unwrap();
        assert_eq!(attributes_from_sqs(Some(&sqs)), attributes);
    }

    #[test]
    fn custom_type_labels_are_read_as_their_base_type() {
        let value = MessageAttributeValue::builder()
            .data_type("Number.float")
            .string_value("1.5")
            .build()
            .unwrap();
        assert_eq!(
            attribute_from_sqs(&value),
            Some(SqsAttributeValue::Number("1.5".into()))
        );
    }
}
