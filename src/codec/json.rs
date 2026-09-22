use super::{Decoder, Encoder};

#[derive(Default)]
pub struct Json;
impl<T: serde::de::DeserializeOwned> Decoder<T> for Json {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(bytes)?)
    }
}
impl<T: serde::Serialize> Encoder<T> for Json {
    fn encode(&self, value: &T) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }
}
