use crate::propagation::PropagationCarrier;
use serde::Serialize;

/// Read-only delivery location; never copied into producer routing implicitly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct KafkaMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

/// A decoded Kafka delivery, including nullable values and source metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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

/// Headers with UTF-8 values, in record order; null and binary headers are skipped.
pub(crate) fn text_headers<'a>(
    headers: impl IntoIterator<Item = (&'a str, Option<&'a [u8]>)>,
) -> Vec<(&'a str, &'a str)> {
    headers
        .into_iter()
        .filter_map(|(name, value)| Some((name, std::str::from_utf8(value?).ok()?)))
        .collect()
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

/// Fields are UTF-8 header values. Setting a field removes every header with that name.
impl<T> PropagationCarrier for KafkaPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.headers.retain(|(header, _)| header != name);
        self.headers
            .push((name.to_owned(), Some(value.into_bytes())));
    }
}

#[cfg(test)]
mod tests {
    use super::text_headers;

    #[test]
    fn propagation_fields_are_text_headers() {
        let headers = [
            ("traceparent", Some(&b"00-trace"[..])),
            ("null", None),
            ("binary", Some(&[0xff_u8][..])),
            ("tracestate", Some(&b"vendor=1"[..])),
        ];
        assert_eq!(
            text_headers(headers),
            vec![("traceparent", "00-trace"), ("tracestate", "vendor=1")]
        );
    }
}
