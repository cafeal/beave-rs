use super::{Decoder, Encoder};
use crate::error::BoxError;

/// A codec that passes byte vectors through unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct RawBytes;

impl Decoder<Vec<u8>> for RawBytes {
    fn decode(&self, bytes: &[u8]) -> Result<Vec<u8>, BoxError> {
        Ok(bytes.to_vec())
    }
}

impl Encoder<Vec<u8>> for RawBytes {
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, BoxError> {
        Ok(value.clone())
    }
}
