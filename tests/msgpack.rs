#![cfg(feature = "msgpack")]

use beavers::{BoxError, Decoder, Encoder, MessagePack};
use serde::{Deserialize, Serialize, de::IgnoredAny};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: u64,
    item: String,
    tags: Vec<String>,
    note: Option<String>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct OrderId {
    id: u64,
}

fn order() -> Order {
    Order {
        id: 42,
        item: "ビーバー".into(),
        tags: vec!["a".into(), "b".into()],
        note: None,
    }
}

#[test]
fn roundtrips_serde_values() {
    let codec = MessagePack::new();

    let bytes = codec.encode(&order()).unwrap();
    let decoded: Order = codec.decode(&bytes).unwrap();
    assert_eq!(decoded, order());
}

#[test]
fn encodes_structs_as_maps_keyed_by_field_name() {
    let bytes = MessagePack::new().encode(&order()).unwrap();

    let fields: BTreeMap<String, IgnoredAny> = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(
        fields.keys().map(String::as_str).collect::<Vec<_>>(),
        ["id", "item", "note", "tags"]
    );
    let partial: OrderId = MessagePack::new().decode(&bytes).unwrap();
    assert_eq!(partial, OrderId { id: 42 });
}

#[test]
fn decodes_the_positional_array_form() {
    let bytes = rmp_serde::to_vec(&order()).unwrap();

    let decoded: Order = MessagePack::new().decode(&bytes).unwrap();
    assert_eq!(decoded, order());
}

#[test]
fn rejects_malformed_and_trailing_input() {
    let codec = MessagePack::new();
    let mut bytes = codec.encode(&order()).unwrap();

    let truncated: Result<Order, BoxError> = codec.decode(&bytes[..bytes.len() - 1]);
    assert!(truncated.is_err());

    bytes.push(0xc0);
    let trailing: Result<Order, BoxError> = codec.decode(&bytes);
    let error = trailing.unwrap_err();
    assert!(error.to_string().contains("1 trailing byte"), "{error}");
}

#[test]
fn is_default_constructible() {
    let _: MessagePack<Order> = Default::default();
}
