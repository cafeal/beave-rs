#![cfg(feature = "protobuf")]

use beavers::{Decoder, Encoder, Protobuf};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct Child {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct Envelope {
    #[prost(uint32, tag = "1")]
    id: u32,
    #[prost(message, repeated, tag = "2")]
    children: Vec<Child>,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
}

#[test]
fn roundtrip_nested_repeated_and_empty_messages() {
    let codec = Protobuf;
    let value = Envelope {
        id: 42,
        children: vec![
            Child {
                name: "first".into(),
            },
            Child {
                name: "second".into(),
            },
        ],
        payload: vec![0, 1, 0xff],
    };

    let bytes = codec.encode(&value).unwrap();
    let decoded: Envelope = codec.decode(&bytes).unwrap();
    assert_eq!(decoded, value);

    let empty = Envelope::default();
    assert!(codec.encode(&empty).unwrap().is_empty());
    let decoded_empty: Envelope = codec.decode(&[]).unwrap();
    assert_eq!(decoded_empty, empty);
}

#[test]
fn malformed_and_truncated_input_is_rejected() {
    let codec = Protobuf;

    let malformed: anyhow::Result<Envelope> = codec.decode(&[0x80]);
    assert!(malformed.is_err());
    let truncated: anyhow::Result<Envelope> = codec.decode(&[0x1a, 0x03, b'x']);
    assert!(truncated.is_err());
}

#[test]
fn interoperates_with_prost_directly_without_framing() {
    let codec = Protobuf;
    let value = Envelope {
        id: 7,
        children: vec![Child {
            name: "prost".into(),
        }],
        payload: vec![9, 8, 7],
    };

    let prost_bytes = value.encode_to_vec();
    let decoded: Envelope = codec.decode(&prost_bytes).unwrap();
    assert_eq!(decoded, value);

    let codec_bytes = codec.encode(&value).unwrap();
    assert_eq!(Envelope::decode(codec_bytes.as_slice()).unwrap(), value);
    assert_eq!(codec_bytes, prost_bytes);
}

#[test]
fn codec_is_default_constructible() {
    let _: Protobuf = Default::default();
}
