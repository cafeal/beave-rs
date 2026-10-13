use serde::{Serialize, de::DeserializeOwned};

use super::{Decoder, Encoder};
use crate::error::{BoxError, Error};

/// MessagePack codec for any type implementing `serde` traits.
///
/// Structs encode as maps keyed by field name, so payloads stay readable by
/// other MessagePack implementations and tolerate added or reordered fields.
/// Decoding rejects bytes after the value so one payload cannot silently
/// contain more than one value.
#[derive(Clone, Copy, Debug, Default)]
pub struct MessagePack;

impl<T: DeserializeOwned> Decoder<T> for MessagePack {
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        let mut input = bytes;
        let value = T::deserialize(&mut rmp_serde::Deserializer::new(&mut input))?;
        if !input.is_empty() {
            return Err(Error::invalid_record(format!(
                "MessagePack value has {} trailing byte(s)",
                input.len()
            ))
            .into());
        }
        Ok(value)
    }
}

impl<T: Serialize> Encoder<T> for MessagePack {
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        Ok(rmp_serde::to_vec_named(value)?)
    }
}
