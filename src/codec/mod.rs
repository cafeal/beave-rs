//! Serialization contracts and built-in codecs.
use crate::error::BoxError;

mod json;
pub use json::Json;

/// Converts a delivery payload into a typed value.
///
/// A decode failure is routed as [`FailureKind::Decode`](crate::FailureKind), which stops
/// the subscription by default.
pub trait Decoder<T>: Send + Sync + 'static {
    /// Decode one payload.
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError>;
}

/// Converts a typed value into a payload for publication.
pub trait Encoder<T>: Send + Sync + 'static {
    /// Encode one value.
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError>;
}

#[cfg(feature = "avro")]
mod avro;
#[cfg(feature = "protobuf")]
mod protobuf;
mod raw_bytes;
mod utf8;
#[cfg(feature = "avro")]
pub use avro::Avro;
#[cfg(feature = "protobuf")]
pub use protobuf::Protobuf;
pub use raw_bytes::RawBytes;
pub use utf8::Utf8;
