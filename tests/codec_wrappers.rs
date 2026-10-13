#![cfg(any(feature = "base64", feature = "gzip", feature = "zstd"))]

use beavers::{Decoder, Encoder, Json, RawBytes};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Event {
    id: u64,
    body: String,
}

fn event() -> Event {
    Event {
        id: 7,
        body: "beaver ".repeat(100),
    }
}

#[cfg(feature = "base64")]
mod base64 {
    use super::*;
    use beavers::Base64;

    #[test]
    fn encodes_the_inner_payload_as_padded_standard_base64() {
        let codec = Base64::new(RawBytes);

        assert_eq!(codec.encode(&vec![0xfb, 0xff]).unwrap(), b"+/8=");
        assert_eq!(codec.decode(b"+/8=").unwrap(), vec![0xfb, 0xff]);
        assert_eq!(codec.encode(&Vec::new()).unwrap(), b"");
    }

    #[test]
    fn roundtrips_through_the_inner_codec() {
        let codec: Base64<Json> = Base64::default();

        let bytes = codec.encode(&event()).unwrap();
        assert!(bytes.iter().all(u8::is_ascii));
        let decoded: Event = codec.decode(&bytes).unwrap();
        assert_eq!(decoded, event());
    }

    #[test]
    fn rejects_invalid_text() {
        let codec = Base64::new(RawBytes);

        for input in [&b"+/8"[..], b"-_8=", b"+/8=\n", b"@@@@"] {
            assert!(codec.decode(input).is_err(), "{input:?}");
        }
    }
}

#[cfg(feature = "gzip")]
mod gzip {
    use super::*;
    use beavers::{BoxError, Gzip};
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    fn gzip(payload: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn roundtrips_through_the_inner_codec_and_shrinks_repetitive_payloads() {
        let codec: Gzip<Json> = Gzip::default();

        let bytes = codec.encode(&event()).unwrap();
        assert_eq!(&bytes[..2], [0x1f, 0x8b]);
        assert!(bytes.len() < Json.encode(&event()).unwrap().len() / 4);
        let decoded: Event = codec.decode(&bytes).unwrap();
        assert_eq!(decoded, event());
    }

    #[test]
    fn decodes_payloads_from_other_gzip_writers_and_concatenated_members() {
        let codec = Gzip::new(RawBytes);

        let mut bytes = gzip(b"hello ");
        bytes.extend(gzip(b"world"));
        assert_eq!(codec.decode(&bytes).unwrap(), b"hello world");
    }

    #[test]
    fn every_level_roundtrips() {
        for level in 0..=9 {
            let codec = Gzip::new(RawBytes).with_level(level);
            let bytes = codec.encode(&b"payload".to_vec()).unwrap();
            assert_eq!(codec.decode(&bytes).unwrap(), b"payload");
        }
    }

    #[test]
    #[should_panic(expected = "gzip compression level must be 0 to 9")]
    fn rejects_levels_above_nine() {
        let _ = Gzip::new(RawBytes).with_level(10);
    }

    #[test]
    fn rejects_payloads_that_decompress_beyond_the_limit() {
        let bytes = gzip(&[0; 1024]);

        let at_limit = Gzip::new(RawBytes).with_max_decoded_len(1024);
        assert_eq!(at_limit.decode(&bytes).unwrap().len(), 1024);

        let below_limit = Gzip::new(RawBytes).with_max_decoded_len(1023);
        let error = below_limit.decode(&bytes).unwrap_err();
        assert!(error.to_string().contains("more than 1023 byte"), "{error}");
    }

    #[test]
    fn rejects_malformed_input() {
        let result: Result<Vec<u8>, BoxError> = Gzip::new(RawBytes).decode(b"not gzip");
        assert!(result.is_err());
    }
}

#[cfg(feature = "zstd")]
mod zstd {
    use super::*;
    use beavers::{BoxError, Zstd};

    #[test]
    fn roundtrips_through_the_inner_codec_and_shrinks_repetitive_payloads() {
        let codec: Zstd<Json> = Zstd::default();

        let bytes = codec.encode(&event()).unwrap();
        assert_eq!(&bytes[..4], [0x28, 0xb5, 0x2f, 0xfd]);
        assert!(bytes.len() < Json.encode(&event()).unwrap().len() / 4);
        let decoded: Event = codec.decode(&bytes).unwrap();
        assert_eq!(decoded, event());
    }

    #[test]
    fn decodes_payloads_from_other_writers_and_concatenated_frames() {
        let codec = Zstd::new(RawBytes);

        let mut bytes = ::zstd::encode_all(&b"hello "[..], 19).unwrap();
        bytes.extend(::zstd::encode_all(&b"world"[..], 1).unwrap());
        assert_eq!(codec.decode(&bytes).unwrap(), b"hello world");
    }

    #[test]
    fn negative_and_maximum_levels_roundtrip() {
        for level in [-5, 1, 22] {
            let codec = Zstd::new(RawBytes).with_level(level);
            let bytes = codec.encode(&b"payload".to_vec()).unwrap();
            assert_eq!(codec.decode(&bytes).unwrap(), b"payload");
        }
    }

    #[test]
    #[should_panic(expected = "zstd compression level must be in")]
    fn rejects_unsupported_levels() {
        let _ = Zstd::new(RawBytes).with_level(23);
    }

    #[test]
    fn rejects_payloads_that_decompress_beyond_the_limit() {
        let bytes = ::zstd::encode_all(&[0; 1024][..], 3).unwrap();

        let at_limit = Zstd::new(RawBytes).with_max_decoded_len(1024);
        assert_eq!(at_limit.decode(&bytes).unwrap().len(), 1024);

        let below_limit = Zstd::new(RawBytes).with_max_decoded_len(1023);
        let error = below_limit.decode(&bytes).unwrap_err();
        assert!(error.to_string().contains("more than 1023 byte"), "{error}");
    }

    #[test]
    fn rejects_malformed_input() {
        let result: Result<Vec<u8>, BoxError> = Zstd::new(RawBytes).decode(b"not zstd");
        assert!(result.is_err());
    }
}

#[cfg(all(feature = "base64", feature = "gzip"))]
#[test]
fn wrappers_compose() {
    use beavers::{Base64, Gzip};

    let codec: Base64<Gzip<Json>> = Base64::default();

    let bytes = codec.encode(&event()).unwrap();
    assert!(bytes.iter().all(u8::is_ascii));
    let decoded: Event = codec.decode(&bytes).unwrap();
    assert_eq!(decoded, event());
}
