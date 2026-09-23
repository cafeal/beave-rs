#![cfg(feature = "avro")]

use std::io::Cursor;

use apache_avro::{
    Schema, from_value, reader::datum::GenericDatumReader, writer::datum::GenericDatumWriter,
};
use beavers::{Avro, Decoder, Encoder};
use serde::{Deserialize, Serialize};

const EVENT_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "Event",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "kind", "type": {
      "type": "enum",
      "name": "Kind",
      "symbols": ["Created", "Deleted"]
    }},
    {"name": "note", "type": ["null", "string"], "default": null},
    {"name": "items", "type": {"type": "array", "items": "long"}}
  ]
}
"#;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Event {
    id: i64,
    kind: Kind,
    note: Option<String>,
    items: Vec<i64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
enum Kind {
    Created,
    Deleted,
}

fn event() -> Event {
    Event {
        id: 42,
        kind: Kind::Created,
        note: Some("hello".into()),
        items: vec![3, 5, 8],
    }
}

#[test]
fn roundtrips_record_enum_nullable_and_array() {
    let codec = Avro::new(EVENT_SCHEMA).unwrap();
    let value = event();

    let bytes = codec.encode(&value).unwrap();
    let decoded: Event = codec.decode(&bytes).unwrap();
    assert_eq!(decoded, value);

    let nullable = Event {
        note: None,
        ..value
    };
    let bytes = codec.encode(&nullable).unwrap();
    let decoded: Event = codec.decode(&bytes).unwrap();
    assert_eq!(decoded, nullable);
}

#[test]
fn uses_raw_datum_api_without_container_framing() {
    let codec = Avro::new(EVENT_SCHEMA).unwrap();
    let schema = Schema::parse_str(EVENT_SCHEMA).unwrap();
    let value = event();
    let writer = GenericDatumWriter::builder(&schema).build().unwrap();
    let expected = writer.write_ser_to_vec(&value).unwrap();
    let encoded = codec.encode(&value).unwrap();
    assert_eq!(encoded, expected);

    let mut input = Cursor::new(encoded.as_slice());
    let reader = GenericDatumReader::builder(&schema).build().unwrap();
    let decoded_value = reader.read_value(&mut input).unwrap();
    assert_eq!(input.position(), encoded.len() as u64);
    assert_eq!(from_value::<Event>(&decoded_value).unwrap(), value);
}

#[test]
fn malformed_and_truncated_input_is_rejected() {
    let codec = Avro::new(EVENT_SCHEMA).unwrap();

    let malformed: anyhow::Result<Event> = codec.decode(&[0x80]);
    assert!(malformed.is_err());

    let encoded = codec.encode(&event()).unwrap();
    let truncated = &encoded[..encoded.len() - 1];
    let result: anyhow::Result<Event> = codec.decode(truncated);
    assert!(result.is_err());
}

#[test]
fn trailing_bytes_are_rejected() {
    let codec = Avro::new(EVENT_SCHEMA).unwrap();
    let mut encoded = codec.encode(&event()).unwrap();
    encoded.push(0);

    let result: anyhow::Result<Event> = codec.decode(&encoded);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("trailing"));
}

#[test]
fn invalid_schema_is_rejected_at_construction() {
    assert!(Avro::new(r#"{"type":"record","name":"Broken"}"#).is_err());
}
