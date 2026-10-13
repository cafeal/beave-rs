#![cfg(feature = "cbor")]

use beavers::{BoxError, Cbor, Decoder, Encoder};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Reading {
    sensor: String,
    values: Vec<f64>,
    raw: Option<Vec<u8>>,
}

fn reading() -> Reading {
    Reading {
        sensor: "dam-1".into(),
        values: vec![1.5, -2.0],
        raw: Some(vec![0, 0xff]),
    }
}

#[test]
fn roundtrips_serde_values() {
    let codec = Cbor;

    let bytes = codec.encode(&reading()).unwrap();
    let decoded: Reading = codec.decode(&bytes).unwrap();
    assert_eq!(decoded, reading());
}

#[test]
fn interoperates_with_ciborium_directly() {
    let mut expected = Vec::new();
    ciborium::into_writer(&reading(), &mut expected).unwrap();

    assert_eq!(Cbor.encode(&reading()).unwrap(), expected);
    let decoded: Reading = Cbor.decode(&expected).unwrap();
    assert_eq!(decoded, reading());
}

#[test]
fn rejects_malformed_and_trailing_input() {
    let codec = Cbor;
    let mut bytes = codec.encode(&reading()).unwrap();

    let truncated: Result<Reading, BoxError> = codec.decode(&bytes[..bytes.len() - 1]);
    assert!(truncated.is_err());

    bytes.push(0xf6);
    let trailing: Result<Reading, BoxError> = codec.decode(&bytes);
    let error = trailing.unwrap_err();
    assert!(error.to_string().contains("1 trailing byte"), "{error}");
}

#[test]
fn is_default_constructible() {
    let _: Cbor = Default::default();
}
