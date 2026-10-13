use super::{Codec, Decoder, Encoder};
use crate::error::BoxError;
use std::str::from_utf8;

/// A codec for strings containing valid UTF-8.
#[derive(Clone, Copy, Debug, Default)]
pub struct Utf8;

impl Codec for Utf8 {
    type Value = String;
}

impl Decoder for Utf8 {
    fn decode(&self, bytes: &[u8]) -> Result<String, BoxError> {
        Ok(from_utf8(bytes)?.to_owned())
    }
}

impl Encoder for Utf8 {
    fn encode(&self, value: &String) -> Result<Vec<u8>, BoxError> {
        Ok(value.as_bytes().to_vec())
    }
}
