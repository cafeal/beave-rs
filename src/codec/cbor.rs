use serde::{Serialize, de::DeserializeOwned};

use std::marker::PhantomData;

use super::typed::typed_codec;
use super::{Decoder, Encoder};
use crate::error::{BoxError, Error};

/// CBOR (RFC 8949) codec for any type implementing `serde` traits.
///
/// Decoding rejects bytes after the value so one payload cannot silently
/// contain more than one value.
pub struct Cbor<T>(PhantomData<fn() -> T>);

typed_codec!(Cbor);

impl<T: DeserializeOwned + 'static> Decoder for Cbor<T> {
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        let mut input = bytes;
        let value = ciborium::from_reader(&mut input)?;
        if !input.is_empty() {
            return Err(Error::invalid_record(format!(
                "CBOR value has {} trailing byte(s)",
                input.len()
            ))
            .into());
        }
        Ok(value)
    }
}

impl<T: Serialize + 'static> Encoder for Cbor<T> {
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(value, &mut bytes)?;
        Ok(bytes)
    }
}
