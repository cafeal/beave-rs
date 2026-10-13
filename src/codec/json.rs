use std::marker::PhantomData;

use super::typed::typed_codec;
use super::{Decoder, Encoder};
use crate::error::BoxError;

/// JSON codec for a type implementing `serde` traits.
pub struct Json<T>(PhantomData<fn() -> T>);

typed_codec!(Json);

impl<T: serde::de::DeserializeOwned + 'static> Decoder for Json<T> {
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl<T: serde::Serialize + 'static> Encoder for Json<T> {
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        Ok(serde_json::to_vec(value)?)
    }
}
