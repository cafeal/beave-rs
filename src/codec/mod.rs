//! Serialization contracts and built-in codecs.
use crate::error::BoxError;

mod json;
pub use json::Json;

/// Names the typed value a codec converts to and from bytes.
///
/// Sources and sinks take the payload type from their codec, so
/// `KafkaSink<Json<Invoice>>` publishes `Invoice` values as JSON.
pub trait Codec: Send + Sync + 'static {
    /// The typed payload value.
    type Value;
}

/// Converts a delivery payload into the codec's typed value.
///
/// A decode failure is routed as [`FailureKind::Decode`](crate::FailureKind), which stops
/// the subscription by default.
pub trait Decoder: Codec {
    /// Decode one payload.
    fn decode(&self, bytes: &[u8]) -> Result<Self::Value, BoxError>;
}

/// Converts the codec's typed value into a payload for publication.
pub trait Encoder: Codec {
    /// Encode one value.
    fn encode(&self, value: &Self::Value) -> Result<Vec<u8>, BoxError>;
}

#[cfg(feature = "avro")]
mod avro;
#[cfg(feature = "base64")]
mod base64;
#[cfg(any(feature = "gzip", feature = "zstd"))]
mod bounded;
#[cfg(feature = "cbor")]
mod cbor;
#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "msgpack")]
mod msgpack;
#[cfg(feature = "protobuf")]
mod protobuf;
mod raw_bytes;
mod typed;
mod utf8;
#[cfg(feature = "zstd")]
mod zstd;
#[cfg(feature = "base64")]
pub use self::base64::Base64;
#[cfg(feature = "zstd")]
pub use self::zstd::Zstd;
#[cfg(feature = "avro")]
pub use apache_avro::AvroSchema;
#[cfg(feature = "avro")]
pub use avro::Avro;
#[cfg(feature = "cbor")]
pub use cbor::Cbor;
#[cfg(feature = "gzip")]
pub use gzip::Gzip;
#[cfg(feature = "msgpack")]
pub use msgpack::MessagePack;
#[cfg(feature = "protobuf")]
pub use protobuf::Protobuf;
pub use raw_bytes::RawBytes;
pub use utf8::Utf8;
