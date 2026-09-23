//! Serialization contracts and built-in codecs.

mod json;
pub use json::Json;

pub trait Decoder<T>: Send + Sync + 'static {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<T>;
}
pub trait Encoder<T>: Send + Sync + 'static {
    fn encode(&self, value: &T) -> anyhow::Result<Vec<u8>>;
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
