use super::{Decoder, Encoder};
use crate::error::BoxError;

/// JSON codec for any type implementing `serde` traits.
#[derive(Default)]
pub struct Json;
impl<T: serde::de::DeserializeOwned> Decoder<T> for Json {
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}
impl<T: serde::Serialize> Encoder<T> for Json {
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        Ok(serde_json::to_vec(value)?)
    }
}
