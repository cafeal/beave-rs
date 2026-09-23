use super::{Decoder, Encoder};

/// A codec that passes byte vectors through unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct RawBytes;

impl Decoder<Vec<u8>> for RawBytes {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

impl Encoder<Vec<u8>> for RawBytes {
    fn encode(&self, value: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
        Ok(value.clone())
    }
}
