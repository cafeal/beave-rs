use beavers::{Decoder, Encoder, RawBytes, Utf8};

#[test]
fn raw_bytes_preserves_empty_and_arbitrary_payloads() {
    let codec = RawBytes;

    for payload in [Vec::new(), vec![0, 0xff, b'\n', 0x80, 0]] {
        assert_eq!(codec.decode(&payload).unwrap(), payload);
        assert_eq!(codec.encode(&payload).unwrap(), payload);
    }
}

#[test]
fn utf8_preserves_empty_unicode_bom_and_line_endings() {
    let codec = Utf8;

    for value in ["", "hello", "こんにちは🦫", "\u{feff}header\r\nbody\n"] {
        let encoded = codec.encode(&value.to_owned()).unwrap();
        assert_eq!(encoded, value.as_bytes());
        assert_eq!(codec.decode(&encoded).unwrap(), value);
    }
}

#[test]
fn utf8_rejects_malformed_input() {
    let error = Utf8.decode(&[0xf0, 0x28, 0x8c, 0x28]).unwrap_err();

    assert!(error.downcast_ref::<std::str::Utf8Error>().is_some());
}

#[test]
fn codecs_are_default_constructible() {
    let _: RawBytes = Default::default();
    let _: Utf8 = Default::default();
}
