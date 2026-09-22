//! Serialization contracts and built-in codecs.

mod json;
pub use json::Json;

pub trait Decoder<T>: Send + Sync + 'static {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<T>;
}
pub trait Encoder<T>: Send + Sync + 'static {
    fn encode(&self, value: &T) -> anyhow::Result<Vec<u8>>;
}
